
#include "ix.h"

IndexManager *IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::_pf_manager = 0;

IndexManager *IndexManager::instance()
{
    if (!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
    rootPage = -1;
}

IndexManager::~IndexManager()
{
    //TODO: check for allocated member variables that need to be discarded and delete/free them
}
RC IndexManager::createEmptyPage(IXFileHandle &index_file, void *page, bool isLeafPage, int &pageNumber, int leftSibling = 0, int rightSibling = 0)
{
    if (isLeafPage)
    {
        int number_of_entries = 0;

        //|isLeaf|NumEntries|freeSpaceOffset|leftChild|rightChild|
        int offset = 0;
        memcpy(page, &isLeafPage, sizeof(bool));
        offset += sizeof(bool);
        memcpy((char *)page + offset, &number_of_entries, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy((char *)page + offset, &LEAF_PAGE_HEADER_SIZE, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy((char *)page + offset, &leftSibling, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy((char *)page + offset, &rightSibling, sizeof(uint32_t));
    }
    else
    {
        //|isLeaf|NumEntries|freeSpaceOffset|
        int number_of_entries = 0;
        int offset = 0;
        memcpy(page, &isLeafPage, sizeof(bool));
        offset += sizeof(bool);
        memcpy((char *)page + offset, &number_of_entries, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy((char *)page + offset, &INTERIOR_PAGE_HEADER_SIZE, sizeof(uint32_t));
    }
    //find page number to insert
    void *pageData = malloc(PAGE_SIZE);
    int numberOfPages = index_file.ufh->getNumberOfPages();
    for (int i = 1; i <= numberOfPages; ++i)
    {
        index_file.ufh->readPage(i, pageData);
        if (findNumberOfEntries(pageData) == 0)
        {
            pageNumber = i;
        }
        else
        {
            pageNumber = -1;
        }
    }
    return SUCCESS;
}
int IndexManager::findNumberOfEntries(const void *page)
{
    int numberOfEntires = 0;
    memcpy(&numberOfEntires, (char *)page + 1, sizeof(uint32_t));
    return numberOfEntires;
}
RC IndexManager::createFile(const string &fileName)
{
    IndexManager::fileName = fileName;
    RC rc = _pf_manager->createFile(fileName.c_str());
    if (rc)
        return rc;

    IXFileHandle fileHandle;
    rc = openFile(fileName, fileHandle);
    if (rc)
        return rc;

    // create the root page - a leaf page at first
    void *page = calloc(PAGE_SIZE, 1);
    rootPage = 1;
    rc = createEmptyPage(fileHandle, page, true, rootPage);
    if (rc)
    {
        free(page);
        return rc;
    }
    rc = fileHandle.ufh->readPage(rootPage, page);
    if (rc)
    {
        free(page);
        return rc;
    }
    free(page);
    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), *ixfileHandle.ufh);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return _pf_manager->closeFile(*ixfileHandle.ufh);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    //TODO: call insertToTree. Assumes underlying file is open (?)
    return -1;
}
int IndexManager::findTrafficCop(const void *val, const Attribute attr, const void *pageData)
{
    //TODO: ???
    return -1; //delete
}
bool IndexManager::isRoot(int pageNumber)
{
    //TODO: return wether a given page pageNumber is the root page. make sure that the global variable
    //      is being set properly by other methods if root is updated

    return pageNumber == rootPage;
}
void IndexManager::insertEntryInPage(void *page, const void *key, const RID &rid, const Attribute &attr, bool isLeafNode, int rightChild = -1)
{
    //TODO: search the given page using IXFile_Iterator and find the correct position to insert either a Leaf entry
    //      or TrafficCop entry into *page. Check to make sure it will fit first. If the position is in middle, make sure
    //      to not write over existing data. If iterator reaches EOF, the correct position is at the end
    int entrySize = isLeafNode ? findLeafEntrySize(key, attr) : findInteriorNodeSize(key, attr);
    const int RID_SIZE = sizeof(uint32_t) * 2;
    void *entryToInsert = malloc(entrySize);
    if (isLeafNode)
    {
        int offset = 0;
        memcpy(entryToInsert, key, entrySize - RID_SIZE);
        offset = entrySize - RID_SIZE;
        memcpy((char *)entryToInsert + offset, &rid.pageNum, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy((char *)entryToInsert + offset, &rid.slotNum, sizeof(uint32_t));
    }
    else
    {
        int offset = 0;
        memcpy(entryToInsert, key, entrySize - sizeof(uint32_t));
        offset = entrySize - sizeof(uint32_t);
        memcpy((char *)entryToInsert, &rightChild, sizeof(uint32_t));
    }
    int offset = 0;
    int numberOfEntries = 0;
    void *entry = malloc(PAGE_SIZE);
    void *slotData = malloc(PAGE_SIZE);
    int previousOffset = isLeafNode ? LEAF_PAGE_HEADER_SIZE : INTERIOR_PAGE_HEADER_SIZE;
    //previous offset will contain the value of the offset that contains the greatest entry that is < than key
    while (getNextEntry(page, offset, numberOfEntries, entry, slotData, attr, isLeafNode) != IX_EOF)
    {
        int result = 0;
        int varCharLength = 0;
        switch (attr.type)
        {
        case TypeInt:
        case TypeReal:
            result = memcmp(entry, key, sizeof(uint32_t));
            break;
        case TypeVarChar:
            memcpy(&varCharLength, key, sizeof(uint32_t));
            result = memcmp(entry, key, sizeof(uint32_t) + varCharLength);
            break;
        }
        if (result >= 0)
            break;
        previousOffset = offset;
    }
    int freeSpaceOffset = findFreeSpaceOffset(page);
    //Move everything from: previousOffset to freeSpaceOffset to: previousOffset + entrySize
    void *partToMove = malloc(freeSpaceOffset - previousOffset);
    memcpy(partToMove, (char *)page + previousOffset, freeSpaceOffset - previousOffset);
    //Copy new entry to page
    memcpy((char *)page + previousOffset, entryToInsert, entrySize);
    //Recopy the previously saved part
    memcpy((char *)page + previousOffset + entrySize, partToMove, freeSpaceOffset - previousOffset);
    //Set freespaceOffset
    freeSpaceOffset += entrySize;
    memcpy((char *)page + 5, &freeSpaceOffset, sizeof(uint32_t));
}
int IndexManager::findLeafEntrySize(const void *val, const Attribute attr)
{
    int slot_size = sizeof(uint32_t) * 2;
    int key_size = 0;
    switch (attr.type)
    {
    case TypeInt:
    case TypeReal:
        key_size = 4;
        break;
    case TypeVarChar:
        memcpy(&key_size, val, sizeof(uint32_t));
        if (key_size <= 0)
            throw "Since values are never null, the VarChar length should never be 0. Check format of val";
        break;
    }

    return key_size + slot_size;
}
int IndexManager::findFreeSpaceOffset(const void *pageData)
{
    int free_space_offset_position = sizeof(bool) + sizeof(uint32_t);
    int free_space_offset = -1;
    memcpy(&free_space_offset, (char *)pageData + free_space_offset_position, sizeof(uint32_t));
    if (free_space_offset < std::max(LEAF_PAGE_HEADER_SIZE, INTERIOR_PAGE_HEADER_SIZE))
        throw "Page is corrupted. free_space_offset should never be less than the header size";
    return free_space_offset;
}
bool IndexManager::willEntryFit(const void *pageData, const void *val, const Attribute attr, bool isLeafValue)
{
    int free_space_offset = freeSpaceOffset(pageData, isLeafValue);
    int entry_size = 0;
    if (isLeafValue)
    {
        entry_size = findLeafEntrySize(val, attr);
    }
    else
    {
        entry_size = findInteriorNodeSize(val, attr);
    }
    return entry_size + free_space_offset >= PAGE_SIZE;
}
int IndexManager::findInteriorNodeSize(const void *val, const Attribute &attr)
{
    int key_size = 0;
    const int pagePointerSize = 4;
    switch (attr.type)
    {
    case TypeInt:
    case TypeReal:
        key_size = 4;
        key_size += pagePointerSize;
        break;
    case TypeVarChar:
        memcpy(&key_size, val, sizeof(uint32_t));
        if (key_size <= 0)
            throw "Since values are never null, the VarChar length should never be 0. Check format of val";
        key_size += pagePointerSize;
        break;
    }
    return key_size;
}
void IndexManager::splitPage(IXFileHandle &ixfileHandle, const Attribute &attribute, void *inPage, void *newChildEntry, bool isLeafPage, int &newPageNumber)
{
}
RC IndexManager::getNextEntry(void *page, int &currentOffset, int &entryCount, void *fieldValue, void *slotData, const Attribute attr, bool isLeafPage)
{
    if (currentOffset == 0)
    {
        currentOffset = findFreeSpaceOffset(page);
    }
    if (entryCount == findNumberOfEntries(page))
        return IX_EOF;
    const int RID_SIZE = sizeof(uint32_t) * 2;
    if (isLeafPage)
    {
        switch (attr.type)
        {
        case TypeInt:
        case TypeReal:
            memcpy(fieldValue, (char *)page + currentOffset, sizeof(uint32_t));
            currentOffset += sizeof(uint32_t);
            memcpy(slotData, (char *)page + currentOffset, RID_SIZE);
            currentOffset += RID_SIZE;
            break;
        case TypeVarChar:
        {
            int varCharSize = 0;
            memcpy(&varCharSize, (char *)page + currentOffset, sizeof(uint32_t));
            memcpy(fieldValue, (char *)page + currentOffset, sizeof(uint32_t) + varCharSize);
            currentOffset += sizeof(uint32_t) + varCharSize;
            memcpy(fieldValue, (char *)page + currentOffset, RID_SIZE);
            currentOffset += RID_SIZE;
        }
        break;
        }
    }
    else
    {
        switch (attr.type)
        {
        case TypeInt:
        case TypeReal:
            memcpy(fieldValue, (char *)page + currentOffset, sizeof(uint32_t));
            currentOffset += sizeof(uint32_t);
            memcpy(slotData, (char *)page + currentOffset, sizeof(uint32_t));
            currentOffset += sizeof(uint32_t);
            break;
        case TypeVarChar:
        {
            int varCharSize = 0;
            memcpy(&varCharSize, (char *)page + currentOffset, sizeof(uint32_t));
            memcpy(fieldValue, (char *)page + currentOffset, sizeof(uint32_t) + varCharSize);
            currentOffset += sizeof(uint32_t) + varCharSize;
            memcpy(slotData, (char *)page + currentOffset, sizeof(uint32_t));
            currentOffset += sizeof(uint32_t);
        }
        break;
        }
    }
    entryCount++;
    return SUCCESS;
}
//IN PROGRESS
void IndexManager::insertToTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, int nodePointer, void *newChild)
{
    void *pageData = malloc(PAGE_SIZE);
    ixfileHandle.ufh->readPage(nodePointer, pageData);
    if (!isLeafPage(pageData))
    {
        int pagePointer = findTrafficCop(key, attribute, pageData); //if page is empty return -1
        //if pagePointer is -1
        insertToTree(ixfileHandle, attribute, key, rid, pagePointer, newChild);
        if (newChild == nullptr)
        {
            free(pageData);
            return;
        }

        else
        {
            if (willEntryFit(pageData, key, attribute, false))
            {
                insertEntryInPage(pageData, key, rid, attribute, false);
                free(pageData);
                return;
            }
            else
            {
                newChild = malloc(PAGE_SIZE);
                int newPageNumber = -1;
                splitPage(ixfileHandle, attribute, pageData, newChild, leafPage, newPageNumber);
                if (isRoot(nodePointer))
                {
                    updateRoot(); //Change the global pointer, set the root to now point to the current pages
                    insertEntryInPage(pageData, newChild, rid, attribute, false);
                    //TODO:: UPDATE root pointer global variable
                }
                free(pageData);
                return;
            }
        }
    }
    else //Is a leaf page
    {
        if (willEntryFit(pageData, key, attribute, true))
        {
            insertEntryInPage(pageData, newChild, rid, attribute, true);
            free(pageData);
            return;
        }
        else
        {
            newChild = malloc(PAGE_SIZE);
            splitPage(pageData, newChild);
            free(pageData);
            return;
        }
    }
}
RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
}

bool IndexManager::isLeafPage(const void *page)
{
    int val = -1;
    memcpy(&val, page, sizeof(bool));
    if (val == -1)
        throw "isLeafPage is indeterminate. check page for corruption";
    return val;
}
//------------------------------------------------------------------------------------------
//-----------------------IX_ScanIterator------------------------------------------------
//------------------------------------------------------------------------------------------
IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}
//------------------------------------------------------------------------------------------
//-----------------------IXFileHandle------------------------------------------------
//------------------------------------------------------------------------------------------
IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    ufh = new FileHandle();
}

IXFileHandle::~IXFileHandle()
{
    delete (ufh);
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return ufh->collectCounterValues(readPageCount, writePageCount, appendPageCount);
}
