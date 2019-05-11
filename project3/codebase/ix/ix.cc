
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
}

IndexManager::~IndexManager()
{
    //TODO: check for allocated member variables that need to be discarded and delete/free them
}

RC IndexManager::createFile(const string &fileName)
{
    IndexManager::fileName = fileName;
    //TODO: open underlying file and create root and first leaf node where both are empty
    //close underlying file
    //see project3 document for pseudo code
    return -1;
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
void IndexManager::splitPage(void *inPage, void *newChildEntry)
{
    //TODO: ???
}
bool IndexManager::isRoot(int pageNumber)
{
    //TODO: return wether a given page pageNumber is the root page. make sure that the global variable
    //      is being set properly by other methods if root is updated

    return false; //delete
}
void IndexManager::insertEntryInPage(void *page, const void *key, const RID &rid, const Attribute &attr, bool isLeafNode)
{
    //TODO: search the given page using IXFile_Iterator and find the correct position to insert either a Leaf entry
    //      or TrafficCop entry into *page. Check to make sure it will fit first. If the position is in middle, make sure
    //      to not write over existing data. If iterator reaches EOF, the correct position is at the end
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
int IndexManager::freeSpaceOffset(const void *pageData, bool isLeafPage)
{
    int header_size = isLeafPage ? LEAF_PAGE_HEADER_SIZE : INTERIOR_PAGE_HEADER_SIZE;
    int free_space_offset_position = header_size - sizeof(uint32_t);
    int free_space_offset = -1;
    memcpy(&free_space_offset, (char *)pageData + free_space_offset_position, sizeof(uint32_t));
    if (free_space_offset < header_size)
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
    return key_size;
}

//IN PROGRESS
void IndexManager::insertToTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, int nodePointer, void *newChild)
{
    void *pageData = malloc(PAGE_SIZE);
    ixfileHandle.ufh->readPage(nodePointer, pageData);
    if (!isLeafPage(pageData))
    {
        int pagePointer = findTrafficCop(key, attribute, pageData);
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
                splitPage(pageData, newChild);
                if (isRoot(nodePointer))
                {
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
//------------------------------------------------------------------------------------------
//-----------------------IXFILE_ScanIterator------------------------------------------------
//------------------------------------------------------------------------------------------
RC IXFile_ScanIterator::scanInit(bool isLeafNode, void *page, CompOp comparison, void *value, Attribute attr)
{
    IXFile_ScanIterator::isLeafNode = isLeafNode;
    IXFile_ScanIterator::page = page;
    switch (attr.type)
    {
    case TypeInt:
    case TypeReal:
        valueSize = sizeof(uint32_t);
        IXFile_ScanIterator::value = malloc(valueSize);
        break;
    case TypeVarChar:
        memcpy(&valueSize, value, sizeof(uint32_t));
        valueSize += sizeof(uint32_t);
        IXFile_ScanIterator::value = malloc(valueSize);
        memcpy(IXFile_ScanIterator::value, value, valueSize);
        break;
    }
    IXFile_ScanIterator::attr = attr;
    IXFile_ScanIterator::comparison = comparison;
    if (isLeafNode)
    {
        offset = LEAF_PAGE_HEADER_SIZE;
    }
    else
    {
        offset = INTERIOR_PAGE_HEADER_SIZE;
    }

    return SUCCESS;
}
RC IXFile_ScanIterator::close()
{
    free(page);
    free(value);
    return SUCCESS;
}
RC IXFile_ScanIterator::getNextEntry(void *key)
{
    int valueSize = 0;
    if (isLeafNode)
    {
        valueSize = IndexManager::findLeafEntrySize(value, attr);
    }
    else
    {
        valueSize = IndexManager::findInteriorNodeSize(value, attr);
    }
    if (offset + valueSize > PAGE_SIZE)
    {
        return IX_EOF;
    }
    key = malloc(valueSize);
    memcpy(value, (char *)page + offset, valueSize);
    offset += valueSize;
    return SUCCESS;
}