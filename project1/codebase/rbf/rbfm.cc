#include "rbfm.h"

const size_t SlotSize = 2 * sizeof(uint32_t);

//https://stackoverflow.com/questions/12276675/modulus-with-negative-numbers-in-c
int mod(int a, int b)
{
    return ((a % b) + b) % b;
}

unsigned char *getNullFlags(const vector<Attribute> &recordDescriptor, const void *data, uint32_t &length) {
    uint32_t numberOfFields = recordDescriptor.size();

    //Calculate the number of null flag bits and allocate enough memory
    length = ceil((double)numberOfFields / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *)malloc(length);

    //Retrieve the null flags and advance the position
    memcpy(nullsIndicator, (char *)data, length);
    return nullsIndicator;
}

/* Reads from raw valuesData and returns the necessary space for storing them in a writable record.
 *   - valuesData must point directly to the first value.
 */
const uint32_t getValuesLengthForRecord(const vector<Attribute> &recordDescriptor, const void *valuesData, const unsigned char *nullsIndicator) {
    uint32_t position = 0;
    uint32_t nbytes = 0;
    int index = 0;
    for (Attribute descriptor : recordDescriptor)
    {
        ++index;

        int nullBitPosition = mod(-index, CHAR_BIT);
        bool nullBit = nullsIndicator[(index - 1) / CHAR_BIT] & (1 << nullBitPosition);
        if (nullBit)
        {
            continue;
        }

        switch (descriptor.type)
        {
        case TypeInt:
        {
            position += sizeof(uint32_t);
            nbytes += sizeof(uint32_t);
            break;
        }
        case TypeReal:
        {
            position += sizeof(float);
            nbytes += sizeof(float);
            break;
        }
        case TypeVarChar:
        {
            uint32_t varCharSize = 0;
            memcpy(&varCharSize, (char *)valuesData + position, sizeof(varCharSize)); // Get the length.
            position += sizeof(varCharSize) + varCharSize; // Skip over the length value itself and the VarChar.
            nbytes += varCharSize; // We don't need to store the length for our WR format.
            break;
        }
        }
    }

    return nbytes; 
}

const void *getWritableRecord(const vector<Attribute> &recordDescriptor, const void *data, size_t &recordSize) {
    uint32_t nullsFlagLength = 0;
    unsigned char *nullsIndicator = getNullFlags(recordDescriptor, data, nullsFlagLength);

    const uint32_t fieldCount = recordDescriptor.size();
    const uint32_t fieldOffsetSize = sizeof(uint32_t);
    const uint32_t fieldOffsetTotalLength = fieldCount * fieldOffsetSize;

    char *valuesData = (char *)data + nullsFlagLength;
    uint32_t valuesLength = getValuesLengthForRecord(recordDescriptor, valuesData, nullsIndicator);

    const int recordValuesStart = sizeof(fieldCount) + nullsFlagLength + fieldOffsetTotalLength;
    recordSize = recordValuesStart + valuesLength;
    unsigned char *record = (unsigned char *)malloc(recordSize);


    int positionInRecord = 0;

    // Set field length.
    memcpy(record + positionInRecord, &fieldCount, sizeof(fieldCount));
    positionInRecord += sizeof(fieldCount);

    // Set null bytes.
    memcpy(record + positionInRecord, nullsIndicator, nullsFlagLength);
    positionInRecord += nullsFlagLength;

    // Iterate through our fields, copying the data's values
    // while simultaneously setting resulting field offsets.

    const uint32_t dataValuesStart = nullsFlagLength; // Values follow null flags.
    int positionInData = dataValuesStart;

    int recordPreviousValueEnd = recordValuesStart;

    int index = 0;
    for (Attribute descriptor : recordDescriptor)
    {
        ++index;

        int nullBitPosition = mod(-index, CHAR_BIT);
        bool nullBit = nullsIndicator[(index - 1) / CHAR_BIT] & (1 << nullBitPosition);
        if (nullBit)
        {
            continue;
        }

        int fieldSize = 0;
        switch (descriptor.type)
        {
            case TypeInt:
            {
                fieldSize = sizeof(uint32_t);
                break;
            }
            case TypeReal:
            {
                fieldSize = sizeof(float);
                break;
            }
            case TypeVarChar:
            {
                // Get length of VarChar.
                memcpy(&fieldSize, (char *)data + positionInData, sizeof(uint32_t));
                positionInData += sizeof(uint32_t); // Setup "pointer" to the VarChar itself.
                break;
            }
        }

        // Copy field value from data to record.
        const uint32_t recordCurrentValueStart = recordPreviousValueEnd;
        memcpy(record + recordCurrentValueStart, (char *)data + positionInData, fieldSize);
        positionInData += fieldSize;

        // "Point" field offset at end of value.
        const uint32_t recordCurrentValueEnd = recordCurrentValueStart + fieldSize;
        memcpy(record + positionInRecord, &recordCurrentValueEnd, fieldOffsetSize);
        positionInRecord += fieldOffsetSize;

        recordPreviousValueEnd = recordCurrentValueEnd;
    }

    free(nullsIndicator);
    return (void *)record;
}

void *findPageForRecord(FileHandle &fileHandle, const void *record) {
    return 0;
}

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;

PagedFileManager *RecordBasedFileManager::pfm;

RecordBasedFileManager *RecordBasedFileManager::instance()
{
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

/* SLOT: [uint32_t length, uint32_t offset] 
   0-indexed
*/
int findOffset(int slotNum, const void *page, int *length = nullptr)
{
    int offset;
    size_t freeSpacePointerLength = sizeof(uint32_t);
    size_t slotCounterLength = sizeof(uint32_t);
    int slotOffset = PAGE_SIZE - freeSpacePointerLength - slotCounterLength - SlotSize * (slotNum + 1);
    if (length != nullptr)
    {
        memcpy(length, (char *)page + slotOffset, sizeof(uint32_t));
    }
    slotOffset += sizeof(uint32_t);
    memcpy(&offset, (char *)page + slotOffset, sizeof(uint32_t));
    return offset;
}

int getFreeSpace (const char *page) 
{
    int freeSpaceStart = page[PAGE_END_INDEX];

    // Check if the slot count is 0
    if (page[PAGE_END_INDEX - 1] == 0) 
    {
        int slotCountPos = PAGE_SIZE - 2 * sizeof(uint32_t);
        return slotCountPos - freeSpaceStart;
    }
    
    int lastSlot = page[PAGE_END_INDEX - 1] - 1; // adjust slot number        
    int freeSpaceEnd = findOffset(lastSlot, page, nullptr);
    
    return freeSpaceEnd - freeSpaceStart;
}

/* 
to do:

bool recordFits(const char *page, int recordSize)
{
    return recordSize + SlotSize() <= getFreeSpace(page);
}


int RecordBasedFileManager::getRecordSize (const vector<Attribute> &recordDescriptor, const void *data) {

}


PageNum RecordBasedFileManager::findPageForRecord (FileHandle &fileHandle, const void *record) {
    n = fileHandle.getNumPages();
    PageNum page = n - 1;
    recordSize = getRecordSize();
    if (recordFits(page, recordSize) {
        return page;
    }
    for (page_i = 0; page_i < n - 1; page_i++){
        if (recordFits(page_i, record) {
            return page_i;
        }
    }
    pfm->appendPage();
    return n;
}

*/


RC RecordBasedFileManager::createFile(const string &fileName)
{
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
    size_t recordLength = 0;
    const void *record = getWritableRecord(recordDescriptor, data, recordLength);
    void *page = findPageForRecord(fileHandle, record);
    

    const uint32_t freeSpaceOffsetPosition = PAGE_END_INDEX;
    uint32_t *freeSpaceOffsetValue = (uint32_t *)page + freeSpaceOffsetPosition;

    const uint32_t slotCountPosition = freeSpaceOffsetPosition - 1;
    uint32_t *slotCount = (uint32_t *)page + slotCountPosition;
    free(const_cast<void *>(record));
    return -1;
}


//EXAMPLE RECORD LAYOUT: [byteArray:nullFlags, int*:pointerToIntegerField, float*:pointerToRealField, void*:pointerToVarCharField, int:integerField, float:realField, int:lengthOfVarCharField, charArray:varCharField]
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{

    //read page into memory
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    //find the offset of the record in the page
    int length = 0;
    int recordOffset = findOffset(rid.slotNum, page, &length);
    int positionInData = 0;
    int positionInRecord = 0;

    //Calculate the length of the null bits field
    int numberOfFields = recordDescriptor.size();
    int nullFlagLength = ceil((double)numberOfFields / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *)malloc(nullFlagLength);

    //Retrieve the null flags, write them to data and advance the position
    memcpy(nullsIndicator, (char *)page + recordOffset, nullFlagLength);
    memcpy(data, nullsIndicator, nullFlagLength);
    positionInData += nullFlagLength;
    positionInRecord += nullFlagLength;

    int index = 0;
    for (Attribute descriptor : recordDescriptor)
    {
        ++index;
        //Where in the 8 bits is the flag? The Most Significant Bit is bit 7 and Least 0
        //SEE SLACK CHAT FOR EXPLAINATION: https://cmps181group.slack.com/archives/CHR7PLWT1/p1555617848047000
        //NOTE: -a MOD b =
        int nullBitPosition = mod(-index, CHAR_BIT);

        //1 array element for every CHAR_BIT fields so [0] = 0:7 [1] = 8:15 etc.
        //Create a Mask with a "1" in the correct position and AND it with the value of nullsIndicator
        bool nullBit = nullsIndicator[(index - 1) / CHAR_BIT] & (1 << nullBitPosition);

        if (!nullBit)
        {
            switch (descriptor.type)
            {
            case TypeInt:
            {
                //Declare an empty double pointer and copy the value of the record pointer into it.
                //(note:) Dereference recordPointerPointer once to get the value of the pointer and twice to get
                //the value of the data at that position
                int **recordPointerPointer = nullptr;
                memcpy(recordPointerPointer, (char *)page + recordOffset + positionInRecord, sizeof(uint32_t));

                //copy the memory at *recordPointer to data
                memcpy((char *)data + positionInRecord, *recordPointerPointer, sizeof(uint32_t));
                positionInRecord += sizeof(uint32_t);
                positionInData += sizeof(uint32_t);
            }
            break;
            case TypeReal:
            {
                //Declare an empty double pointer and copy the value of the record pointer into it.
                //(note:) Dereference recordPointerPointer once to get the value of the pointer and twice to get
                //the value of the data at that position
                float **recordPointerPointer = nullptr;
                memcpy(recordPointerPointer, (char *)page + recordOffset + positionInRecord, sizeof(uint32_t));

                //copy the memory at *recordPointer to data
                memcpy((char *)data + positionInRecord, *recordPointerPointer, sizeof(uint32_t));
                positionInRecord += sizeof(uint32_t);
                positionInData += sizeof(uint32_t);
            }
            break;
            case TypeVarChar:
            {
                //Declare an empty double pointer and copy the value of the record into it
                //(note:) Dereference recordPointerPointer once to get the value of the pointer and twice to get
                //the value of the data at that position
                void **varCharRecordPointerPointer = nullptr;
                memcpy(varCharRecordPointerPointer, (char *)page + recordOffset + positionInRecord, sizeof(uint32_t));
                //Find the length of the varchar field by copying the first 4 bytes of the record
                int *sizeOfVarCharPointer = nullptr;
                memcpy(sizeOfVarCharPointer, *varCharRecordPointerPointer, sizeof(uint32_t));

                //Copy the varChar field of length *sizeOfVarCharPointer*sizeof(char)+sizeof(uint32_t) <-for the length of the size indicator
                memcpy((char *)data + positionInRecord, *varCharRecordPointerPointer, (*sizeOfVarCharPointer) * sizeof(char) + sizeof(uint32_t));
            }
            break;
            }
        }
    }
    free(page);
    free(nullsIndicator);
    return 0;
}

// The format is as follows:
// field1-name: field1-value  field2-name: field2-value ... \n
// (e.g., age: 24  height: 6.1  salary: 9000
//        age: NULL  height: 7.5  salary: 7500)
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    int numberOfFields = recordDescriptor.size();
    int position = 0;

    //Calculate the number of null flag bits and allocate enough memory
    int nullFlagLength = ceil((double)numberOfFields / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *)malloc(nullFlagLength);

    //Retrieve the null flags and advance the position
    memcpy(nullsIndicator, (char *)data, nullFlagLength);
    position += nullFlagLength;

    int index = 0;
    for (Attribute descriptor : recordDescriptor)
    {
        ++index;
        cout << descriptor.name << ": ";
        //Where in the 8 bits is the flag? The Most Significant Bit is bit 7 and Least 0
        //SEE SLACK CHAT FOR EXPLAINATION: https://cmps181group.slack.com/archives/CHR7PLWT1/p1555617848047000
        //NOTE: -a MOD b =
        int nullBitPosition = mod(-index, CHAR_BIT);

        //1 array element for every CHAR_BIT fields so [0] = 0:7 [1] = 8:15 etc.
        //Create a Mask with a "1" in the correct position and AND it with the value of nullsIndicator
        bool nullBit = nullsIndicator[(index - 1) / CHAR_BIT] & (1 << nullBitPosition);

        //If the null indicator is 1 then skip this iteration
        if (nullBit)
        {
            cout << "NULL ";
            continue;
        }
        switch (descriptor.type)
        {
        case TypeInt:
        {
            int number = 0;
            memcpy(&number, (char *)data + position, sizeof(int));
            cout << number << " ";
            position += sizeof(int);
            break;
        }
        case TypeReal:
        {
            float number = 0;
            memcpy(&number, (char *)data + position, sizeof(float));
            cout << number << " ";
            position += sizeof(float);
            break;
        }
        case TypeVarChar:
        {
            int varCharSize = 0;
            memcpy(&varCharSize, (char *)data + position, sizeof(int));
            char varChar[varCharSize + 1];
            memcpy(&varChar, (char *)data + position + sizeof(int), varCharSize);
            varChar[varCharSize] = '\0';
            cout << varChar << " ";
            position += sizeof(int) + varCharSize;
            break;
        }
        }
    }
    cout << endl;
    free(nullsIndicator);
    return 0;
}
