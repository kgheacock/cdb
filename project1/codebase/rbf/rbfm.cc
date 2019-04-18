#include "rbfm.h"

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;

const size_t SlotSize = 2 * sizeof(uint32_t);

PagedFileManager *RecordBasedFileManager::pfm;

//https://stackoverflow.com/questions/12276675/modulus-with-negative-numbers-in-c

int mod(int a, int b)
{
    return ((a % b) + b) % b;
}
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
/* todo 

long long RecrodBasedFileManager::getFreeSpace (const String fileName, Filehandle &fileHandle, PageNum page) {
    long long pageOffset,freeSpaceStart, freeSpaceEnd;
    char* freeSpaceOffset, numSlots;
    
    pageOffset = (page + 1) * sizeOfPage - 4;
    fs->seekg(pageOffset);               // seek to page free space offset 
    fs->read(freeSpaceOffset, 4);        // get free space offset
    fs->seekg(freeSpaceOffset, fs.cur);  // seek to beginnig of free space from current position 
    freeSpaceStart = fs->tellg;          // get absolute position

    fs->seekg(pageOffset -4);            
    fs->read(numSlots, 4);               
    fs->seekg(-numberOfSlots * 2, fs.cur); // seek to end of free space
    freeSpaceEnd = fs->tellg;              // get absolute position                
    return freeSpaceEnd - freeSpaceStart;  // return size of free space
}

unsigned long RecordBasedFileManager::getRecordSize (const vector<Attribute> &recordDescriptor, const void *data) {

}

bool RecordBasedFileManager::recordFits(PageNum page, int recordSize) {
    return recordSize + getSlotSize() <= getFreeSpace(page);
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
        int nullBitPosition = (CHAR_BIT) - (index % CHAR_BIT);
        //1 array element for every CHAR_BIT fields so [0] = 0:7 [1] = 8:15 etc.
        //Create a Mask with a "1" in the correct position and AND it with the value of nullsIndicator
        bool nullBit = nullsIndicator[(index + 1) / CHAR_BIT] & (1 << nullBitPosition);
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
        int nullBitPosition = mod(-1 * index, CHAR_BIT);
        //1 array element for every CHAR_BIT fields so [0] = 0:7 [1] = 8:15 etc.
        //Create a Mask with a "1" in the correct position and AND it with the value of nullsIndicator
        bool nullBit = nullsIndicator[(index + 1) / CHAR_BIT] & (1 << nullBitPosition);
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
            std::cout << number << " ";
            position += sizeof(int);
            break;
        }
        case TypeReal:
        {
            float number = 0;
            memcpy(&number, (char *)data + position, sizeof(float));
            std::cout << number << " ";
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
            std::cout << varChar << " ";
            position += sizeof(int) + varCharSize;
            break;
        }
        }
    }
    std::cout << std::endl;
    free(nullsIndicator);
    return 0;
}
