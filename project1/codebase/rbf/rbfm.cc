#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

PagedFileManager* RecordBasedFileManager::pfm;

RecordBasedFileManager* RecordBasedFileManager::instance()

{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
    if (_rbf_manager) {
        delete _rbf_manager;
    }
}

size_t getSlotSize() {
    return sizeof(uint32_t) * 2;
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
  
RC RecordBasedFileManager::createFile(const string &fileName) {
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}
