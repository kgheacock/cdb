#include "rbfm.h"

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
    if (_rbf_manager)
    {
        delete _rbf_manager;
    }
}

size_t getSlotSize()
{
    return sizeof(uint32_t) * 2;
}

/* todo 

unsigned long RecrodBasedFileManager::getFreeSpace

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

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
    return -1;
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
    memcpy(&nullsIndicator, (char *)data, nullFlagLength);
    position += nullFlagLength;

    int index = 0;
    for (Attribute descriptor : recordDescriptor)
    {
        ++index;
        cout << descriptor.name << ": ";
        //Where in the 8 bits is the flag? The Most Significant Bit is bit 7 and Least 0
        int nullBitPosition = (CHAR_BIT)-index;
        //1 array element for every CHAR_BIT fields so [0] = 0:7 [1] = 8:15 etc.
        //Create a Mask with a "1" in the correct position and AND it with the value of nullsIndicator
        bool nullBit = nullsIndicator[(index + 1) / CHAR_BIT] & (1 << nullBitPosition);
        //If the null indicator is 1 then skip this iteration
        if (nullBit)
            continue;
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
        ++index;
    }
    std::cout << std::endl;
    return 0;
}