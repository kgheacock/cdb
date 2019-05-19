
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

RC IndexManager::createEmptyPage(IXFileHandle &index_file, void *page, bool isLeaf, PageNum &pageNumber, int leftSibling, int rightSibling)
{
    // isLeaf | number of entries | free space offset | ...
    memcpy((char *) page + POSITION_IS_LEAF, &isLeaf, SIZEOF_IS_LEAF);

    uint32_t numberOfEntries = 0;
    memcpy((char *)page + POSITION_NUM_ENTRIES, &numberOfEntries, SIZEOF_NUM_ENTRIES);

    uint32_t freeSpaceOffset = isLeaf ? SIZEOF_HEADER_LEAF : SIZEOF_HEADER_INTERIOR;
    memcpy((char *)page + POSITION_FREE_SPACE_OFFSET, &freeSpaceOffset, SIZEOF_FREE_SPACE_OFFSET);

    if (isLeaf)
    {
        memcpy((char *)page + POSITION_SIBLING_PAGENUM_LEFT, &leftSibling, SIZEOF_SIBLING_PAGENUM);
        memcpy((char *)page + POSITION_SIBLING_PAGENUM_RIGHT, &rightSibling, SIZEOF_SIBLING_PAGENUM);
    }

    //find page number to insert
    void *pageData = malloc(PAGE_SIZE);
    for (pageNumber = 0; pageNumber < index_file.ufh->getNumberOfPages(); ++pageNumber)
    {
        auto rc = index_file.ufh->readPage(pageNumber, pageData);
        if (rc != SUCCESS)
        {
            free(pageData);
            return rc;
        }

        bool isPageEmpty = findNumberOfEntries(pageData) == 0;
        if (isPageEmpty)
        {
            free(pageData);
            return SUCCESS;
        }
    }

    // No existing empty page was found.
    // This is still OK.  We just need to append a page instead of writing directly to some page.
    free(pageData);
    return SUCCESS;
}

int IndexManager::findNumberOfEntries(const void *page)
{
    int numberOfEntries = 0;
    memcpy(&numberOfEntries, (char *)page + POSITION_NUM_ENTRIES, SIZEOF_NUM_ENTRIES);
    return numberOfEntries;
}

RC IndexManager::createFile(const string &fileName)
{
    IndexManager::fileName = fileName;
    RC rc = _pf_manager->createFile(fileName.c_str());
    if (rc != SUCCESS)
        return rc;

    rc = updateRootPageNumber(fileName, 0);
    if (rc != SUCCESS)
        return rc;

    IXFileHandle fileHandle;
    rc = openFile(fileName, fileHandle);
    if (rc != SUCCESS)
        return rc;

    // Create the root page - a leaf page at first.
    void *page = calloc(PAGE_SIZE, 1);
    bool isLeaf = true;

    PageNum availablePage;
    rc = createEmptyPage(fileHandle, page, isLeaf, availablePage);
    if (rc != SUCCESS || availablePage != rootPage)
    {
        free(page);
        return rc;
    }

    rc = fileHandle.ufh->writePage(rootPage, page);
    free(page);

    return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    auto rc = getRootPageNumber(fileName);
    if (rc != SUCCESS)
        return rc;
    return _pf_manager->openFile(fileName.c_str(), *ixfileHandle.ufh);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    rootPage = -1;
    return _pf_manager->closeFile(*ixfileHandle.ufh);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void *newChild = nullptr;
    return insertToTree(ixfileHandle, attribute, key, rid, rootPage, newChild);
}

int IndexManager::findTrafficCop(const void *val, const Attribute attr, const void *pageData)
{
    //TODO: ???
    return -1; //delete
}

bool IndexManager::isRoot(PageNum pageNumber)
{
    return pageNumber == rootPage;
}

void IndexManager::insertEntryInPage(void *page, const void *key, const RID &rid, const Attribute &attr, bool isLeafNode)
{
    //TODO: search the given page using IXFile_Iterator and find the correct position to insert either a Leaf entry
    //      or TrafficCop entry into *page. Check to make sure it will fit first. If the position is in middle, make sure
    //      to not write over existing data. If iterator reaches EOF, the correct position is at the end
    if (isLeafNode)
    {
        //int offset = 0;
        if (findNumberOfEntries(page) == 0)
        {
            size_t insertPosition = SIZEOF_HEADER_LEAF;
            const size_t newFreeSpaceOffset = insertPosition + findLeafEntrySize(key, attr);

            const size_t keySize = findKeySize(key, attr);
            memcpy((char *)page + insertPosition, key, keySize);
            insertPosition += keySize;

            memcpy((char *)page + insertPosition, &(rid.pageNum), sizeof(uint32_t));
            insertPosition += sizeof(uint32_t);
            
            memcpy((char *)page + insertPosition, &(rid.slotNum), sizeof(uint32_t));
            insertPosition += sizeof(uint32_t);

            memcpy((char *)page + POSITION_FREE_SPACE_OFFSET, &newFreeSpaceOffset, SIZEOF_FREE_SPACE_OFFSET);
        }
        //int freeSpaceOffset = findFreeSpaceOffset(page);
    }
}

int IndexManager::findLeafEntrySize(const void *val, const Attribute attr)
{
    int keySize = findKeySize(val, attr);
    if (keySize < 0)
        return -1;

    int ridSize = sizeof(uint32_t) * 2; // pageNum + slotNum
    return keySize + ridSize;
}

int IndexManager::findInteriorEntrySize(const void *val, const Attribute attr)
{
    int keySize = findKeySize(val, attr);
    if (keySize < 0)
        return -1;

    int rightPageNumSize = sizeof(uint32_t); // Key must also have corresponding right pointer to child.
    return keySize + rightPageNumSize;
}

int IndexManager::findKeySize(const void *val, const Attribute attr)
{
    int keySize = -1;

    switch (attr.type)
    {
    case TypeInt:
    case TypeReal:
        keySize = 4;
        break;

    case TypeVarChar:
        memcpy(&keySize, val, sizeof(uint32_t)); // Get length of string.
        if (keySize <= 0)
            throw "Since values are never null, the VarChar length should never be 0. Check format of val";
        keySize += 4;  // Key also contains the length of the string.
        break;

    default:
        keySize = -1;
        break;
    }

    return keySize;
}

int IndexManager::findFreeSpaceOffset(const void *pageData)
{
    size_t freeSpaceOffset;
    memcpy(&freeSpaceOffset, (char *)pageData + POSITION_FREE_SPACE_OFFSET, SIZEOF_FREE_SPACE_OFFSET);

    size_t headerSize = isLeafPage(pageData) ? SIZEOF_HEADER_LEAF : SIZEOF_HEADER_INTERIOR;
    if (freeSpaceOffset < headerSize)
        throw "Page is corrupted. free_space_offset should never be less than the header size";

    return freeSpaceOffset;
}

RC IndexManager::getRootPageNumber(const string indexFileName)
{
    string rootFileName = indexFileName + ".root";
    FILE *rootFile = fopen(rootFileName.c_str(), "r");
    if (rootFile == nullptr)
        return -1;

    size_t expectedCount = 1;
    size_t actualCount = fread(&rootPage, sizeof(PageNum), expectedCount, rootFile);
    fclose(rootFile);

    if (expectedCount != actualCount)
        return -1;

    return SUCCESS;
}

RC IndexManager::updateRootPageNumber(const string indexFileName, const PageNum newRoot)
{
    string rootFileName = indexFileName + ".root";
    FILE *rootFile = fopen(rootFileName.c_str(), "wb");
    if (rootFile == nullptr)
        return -1;

    size_t expectedCount = 1;
    size_t actualCount = fwrite(&newRoot, sizeof(PageNum), expectedCount, rootFile);
    fclose(rootFile);
    
    rootPage = newRoot;

    if (expectedCount != actualCount)
        return -1;

    return SUCCESS;
}

RC IndexManager::updateRoot()
{
    return -1;
}

bool IndexManager::willEntryFit(const void *pageData, const void *val, const Attribute attr, bool isLeafValue)
{
    int freeSpaceOffset = IndexManager::findFreeSpaceOffset(pageData);
    int entrySize = 0;
    if (isLeafValue)
    {
        entrySize = findLeafEntrySize(val, attr);
    }
    else
    {
        entrySize = findInteriorEntrySize(val, attr);
    }
    return entrySize + freeSpaceOffset < PAGE_SIZE;
}


void IndexManager::splitPage(IXFileHandle &ixfileHandle, const Attribute &attribute, void *inPage, void *newChildEntry, int &newPageNumber)
{
}

void IndexManager::getNextEntry(void *page, int &currentOffset, void *fieldValue, const Attribute attr, bool isLeafPage)
{
    if (currentOffset == 0)
    {
        currentOffset = findFreeSpaceOffset(page);
    }
    if (isLeafPage)
    {
        switch (attr.type)
        {
        case TypeInt:
        case TypeReal:
            // memcpy(fieldValue, page + currentOffset);
            break;
        case TypeVarChar:
            break;
        }
    }
    else
    {
        switch (attr.type)
        {
        case TypeInt:
        case TypeReal:
            break;
        case TypeVarChar:
            break;
        }
    }
}
//IN PROGRESS
RC IndexManager::insertToTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, int nodePointer, void *newChild)
{
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == nullptr)
        return -1;

    auto rc = ixfileHandle.ufh->readPage(nodePointer, pageData);
    if (rc != SUCCESS)
    {
        free(pageData);
        return rc;
    }

    if (!isLeafPage(pageData))
    {
        int pagePointer = findTrafficCop(key, attribute, pageData); //if page is empty return -1
        //if pagePointer is -1
        insertToTree(ixfileHandle, attribute, key, rid, pagePointer, newChild);
        if (newChild == nullptr)
        {
            free(pageData);
            return SUCCESS;
        }

        if (willEntryFit(pageData, key, attribute, false))
        {
            insertEntryInPage(pageData, key, rid, attribute, false);
            free(pageData);
            return SUCCESS;
        }

        newChild = malloc(PAGE_SIZE);
        int newPageNumber = -1;
        splitPage(ixfileHandle, attribute, pageData, newChild, newPageNumber);
        if (isRoot(nodePointer))
        {
            updateRoot(); // Change the global pointer, set the root to now point to the current pages.
            insertEntryInPage(pageData, newChild, rid, attribute, false);
            //TODO:: UPDATE root pointer global variable
        }
        free(pageData);
        return SUCCESS;
    }
    else // Leaf page.
    {
        if (willEntryFit(pageData, key, attribute, true))
        {
            insertEntryInPage(pageData, key, rid, attribute, true);
            free(pageData);
            return SUCCESS;
        }

        newChild = malloc(PAGE_SIZE);
        //splitPage(pageData, newChild);
        int newPageNumber = -1;
        splitPage(ixfileHandle, attribute, pageData, newChild, newPageNumber);
        free(pageData);
        return SUCCESS;
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
    bool isLeaf;
    memcpy(&isLeaf, page, sizeof(bool));
    return isLeaf;
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
