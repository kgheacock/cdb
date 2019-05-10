
#include "ix.h"

IndexManager *IndexManager::_index_manager = 0;

IndexManager *IndexManager::instance()
{
    if (!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    IndexManager::fileName = fileName;
    return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

    return -1;
}

//IN PROGRESS
void IndexManager::insertToTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, int nodePointer, void *newChild)
{
    void *pageData = malloc(PAGE_SIZE);
    ixfileHandle.fh.readPage(nodePointer, pageData);
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
                insertNode(pageData, key, attribute, false);
                free(pageData);
                return;
            }
            else
            {
                newChild = malloc(PAGE_SIZE);
                splitPage(pageData, newChild);
                if (isRoot(nodePointer))
                {
                    insertNode(pageData, newChild, attribute, false);
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
            insertNode(pageData, newChild, attribute, true);
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

IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}
