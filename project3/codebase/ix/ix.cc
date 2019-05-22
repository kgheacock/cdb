
#include "ix.h"
#include <iostream>
#include <tuple>
using namespace std;



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
    if (page == nullptr)
        return -1;

    // isLeaf | number of entries | free space offset | ...
    memcpy((char *)page + POSITION_IS_LEAF, &isLeaf, SIZEOF_IS_LEAF);

    if (isLeaf)
    {
        HeaderLeaf header;
        header.numEntries = 0;
        header.freeSpaceOffset = SIZEOF_HEADER_LEAF;
        header.leftSibling = leftSibling;
        header.rightSibling = rightSibling;
        setHeaderLeaf(page, header);
    }
    else
    {
        HeaderInterior header;
        header.numEntries = 0;
        header.freeSpaceOffset = SIZEOF_HEADER_INTERIOR;
        setHeaderInterior(page, header);
    }

    //find page number to insert
    void *currentPageData = malloc(PAGE_SIZE);
    for (uint32_t currentPageNumber = 0; currentPageNumber < index_file.ufh->getNumberOfPages(); ++currentPageNumber)
    {
        auto rc = index_file.readPage(currentPageNumber, currentPageData);
        if (rc != SUCCESS)
        {
            free(currentPageData);
            return rc;
        }

        int numEntries = findNumberOfEntries(currentPageData);
        if (numEntries < 0)
        {
            free(currentPageData);
            return -1;
        }

        bool isPageEmpty = numEntries == 0;
        if (isPageEmpty)
        {
            free(currentPageData);
            pageNumber = currentPageNumber;
            return SUCCESS;
        }
    }

    // No existing empty page was found.
    // This is still OK.  We just need to append a page instead of writing directly to some page.
    pageNumber = index_file.ufh->getNumberOfPages();
    memset(currentPageData, 0, PAGE_SIZE);
    auto rc = index_file.appendPage(currentPageData);
    free(currentPageData);
    return rc;
}

uint32_t IndexManager::findNumberOfEntries(const void *page)
{
    uint32_t numberOfEntries = 0;
    memcpy(&numberOfEntries, (char *)page + POSITION_NUM_ENTRIES, SIZEOF_NUM_ENTRIES);

    size_t headerSize = isLeafPage(page) ? SIZEOF_HEADER_LEAF : SIZEOF_HEADER_INTERIOR;
    if (numberOfEntries < 0 || numberOfEntries > PAGE_SIZE - headerSize)
        return -1;
    return numberOfEntries;
}

RC IndexManager::updateRoot(IXFileHandle &ixFileHandle, tuple<void *, int> newChild, int leftChild, const Attribute &attr)
{
    void *newRoot = calloc(PAGE_SIZE, sizeof(uint8_t));
    if (newRoot == nullptr)
        return -1;

    PageNum newRootPageNum = 0;
    auto rc = createEmptyPage(ixFileHandle, newRoot, false, newRootPageNum);
    if (rc != SUCCESS)
    {
        free(newRoot);
        return rc;
    }

    HeaderInterior newRootHeader;
    rc = getHeaderInterior(newRoot, newRootHeader);
    if (rc != SUCCESS)
    {
        free(newRoot);
        return rc;
    }

    int keySize = findKeySize(get<0>(newChild), attr);
    if (keySize < 0)
    {
        free(newRoot);
        return -1;
    }

    newRootHeader.numEntries = 1;

    int offset = SIZEOF_HEADER_INTERIOR;

    //left child pointer
    memcpy((char *)newRoot + offset, &leftChild, SIZEOF_CHILD_PAGENUM);
    offset += SIZEOF_CHILD_PAGENUM;

    //key value
    memcpy((char *)newRoot + offset, get<0>(newChild), keySize);
    offset += keySize;

    //right child pointer
    memcpy((char *)newRoot + offset, &(get<1>(newChild)), SIZEOF_CHILD_PAGENUM);
    offset += SIZEOF_CHILD_PAGENUM;

    newRootHeader.freeSpaceOffset = offset;

    setHeaderInterior(newRoot, newRootHeader);

    rc = ixFileHandle.writePage(newRootPageNum, newRoot);
    free(newRoot);
    if (rc != SUCCESS)
        return rc;

    rc = updateRootPageNumber(ixFileHandle.fileName, newRootPageNum);
    return rc;
}

RC IndexManager::createFile(const string &fileName)
{
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
    if (rc != SUCCESS)
    {
        free(page);
        closeFile(fileHandle);
        return rc;
    }
    if (availablePage != rootPage)
    {
        free(page);
        closeFile(fileHandle);
        return -1;
    }

    rc = fileHandle.writePage(rootPage, page);
    free(page);
    closeFile(fileHandle);
    return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
    string rootFile = fileName + ".root";
    RC rc = _pf_manager->destroyFile(rootFile);
    if (rc != SUCCESS)
        return rc;
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    auto rc = getRootPageNumber(fileName);
    if (rc != SUCCESS)
        return rc;
    ixfileHandle.fileName = fileName;
    return _pf_manager->openFile(fileName.c_str(), *ixfileHandle.ufh);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    rootPage = -1;
    return _pf_manager->closeFile(*ixfileHandle.ufh);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    tuple<void *, int> newChildTuple = std::make_tuple(nullptr, -1);
    return insertToTree(ixfileHandle, attribute, key, rid, rootPage, newChildTuple);
}

RC IndexManager::findTrafficCop(const void *val, const Attribute attr, const void *pageData, int &trafficCop)
{
    tuple<void *, int> targetTrafficCopWithSize = getKeyDataWithSize(attr, val);
    void *targetTrafficCop = get<0>(targetTrafficCopWithSize);
    if (targetTrafficCop == nullptr)
        return -1;

    vector<tuple<void *, int>> keysWithSizes = getKeysWithSizes_interior(attr, pageData);
    if (keysWithSizes.empty())
    {
        free(targetTrafficCop);
        return -1;
    }

    vector<int> children = getChildPointers_interior(attr, pageData);
    if (children.empty())
    {
        free(targetTrafficCop);
        freeKeysWithSizes(keysWithSizes);
        return -1;
    }

    for (auto it = children.begin(); it + 1 != children.end(); ++it)
    {
        int i = distance(children.begin(), it);

        int child = *it;
        tuple<void *, int> currentTrafficCopWithSize = getKeyDataWithSize(attr, get<0>(keysWithSizes[i]));
        void *currentTrafficCop = get<0>(currentTrafficCopWithSize);
        if (currentTrafficCop == nullptr)
        {
            free(targetTrafficCop);
            freeKeysWithSizes(keysWithSizes);
            return -1;
        }

        bool target_lt_current;
        bool target_eq_current;
        bool target_gt_current;
        auto rc = compareKeyData(attr, targetTrafficCop, currentTrafficCop, target_lt_current, target_eq_current, target_gt_current);

        free(currentTrafficCop);

        if (rc != SUCCESS)
        {
            free(targetTrafficCop);
            freeKeysWithSizes(keysWithSizes);
            return rc;
        }

        if (target_lt_current)
        {
            free(targetTrafficCop);
            freeKeysWithSizes(keysWithSizes);
            trafficCop = child;
            return SUCCESS;
        }
    }

    free(targetTrafficCop);
    freeKeysWithSizes(keysWithSizes);
    trafficCop = children.back();
    return SUCCESS;
}

//NOTE:: calling functions must free the memory in the first key of the tuple
vector<tuple<void *, int>> IndexManager::getKeysWithSizes_interior(const Attribute attribute, const void *pageData)
{
    vector<tuple<void *, int>> keysWithSizes;

    uint32_t numEntries = findNumberOfEntries(pageData);
    if (numEntries < 0)
        return keysWithSizes;

    size_t firstEntryPosition = SIZEOF_HEADER_INTERIOR + SIZEOF_CHILD_PAGENUM; // Skip over header and first child pointer.
    size_t offset = firstEntryPosition;

    for (uint32_t i = 0; i < numEntries; i++)
    {
        void *entry = (char *)pageData + offset;
        int entrySize = findInteriorEntrySize(entry, attribute);
        if (entrySize < 0)
            continue;

        int keySize = entrySize - SIZEOF_CHILD_PAGENUM;        // Each "entry" is key + pagenum for child with keys >= (i.e. the right child).
        void *savedKey = calloc(keySize + 1, sizeof(uint8_t)); // Safety (+1) is for reading null-term if attribute is VarChar.
        memcpy(savedKey, entry, keySize);
        tuple<void *, int> keyWithSize = make_tuple(savedKey, keySize);
        keysWithSizes.push_back(keyWithSize);
        offset += entrySize;
    }

    return keysWithSizes;
}

vector<int> IndexManager::getChildPointers_interior(const Attribute attribute, const void *pageData)
{
    vector<int> children;
    int child;

    uint32_t numEntries = findNumberOfEntries(pageData);
    if (numEntries < 0)
        return children;

    // Add leftmost child pointer initially.
    // We treat entries as <key, right child pointer>, so this keeps that convention for iterating later.
    if (numEntries > 0)
    {
        memcpy(&child, (char *)pageData + SIZEOF_HEADER_INTERIOR, SIZEOF_CHILD_PAGENUM);
        children.push_back(child);
    }

    size_t firstEntryPosition = SIZEOF_HEADER_INTERIOR + SIZEOF_CHILD_PAGENUM; // Skip over header and first child pointer.
    size_t offset = firstEntryPosition;

    for (uint32_t i = 0; i < numEntries; i++)
    {
        void *entry = (char *)pageData + offset;
        int entrySize = findInteriorEntrySize(entry, attribute);
        if (entrySize < 0)
            continue;

        int keySize = entrySize - SIZEOF_CHILD_PAGENUM; // Each "entry" is key + pagenum for child with keys >= (i.e. the right child).
        offset += keySize;                              // Skip over key so we're now on right child pointer.

        memcpy(&child, (char *)pageData + offset, SIZEOF_CHILD_PAGENUM);
        children.push_back(child);
        offset += entrySize;
    }

    return children;
}

RC IndexManager::getHeaderInterior(const void *pageData, HeaderInterior &header)
{
    memcpy(&(header.numEntries), (char *)pageData + POSITION_NUM_ENTRIES, SIZEOF_NUM_ENTRIES);
    memcpy(&(header.freeSpaceOffset), (char *)pageData + POSITION_FREE_SPACE_OFFSET, SIZEOF_FREE_SPACE_OFFSET);
    if (header.numEntries > PAGE_SIZE - SIZEOF_HEADER_INTERIOR || header.freeSpaceOffset > PAGE_SIZE)
        return -1;
    return SUCCESS;
}

void IndexManager::setHeaderInterior(void *pageData, const HeaderInterior header)
{
    memcpy((char *)pageData + POSITION_NUM_ENTRIES, &(header.numEntries), SIZEOF_NUM_ENTRIES);
    memcpy((char *)pageData + POSITION_FREE_SPACE_OFFSET, &(header.freeSpaceOffset), SIZEOF_FREE_SPACE_OFFSET);
}

RC IndexManager::getHeaderLeaf(const void *pageData, HeaderLeaf &header)
{
    memcpy(&(header.numEntries), (char *)pageData + POSITION_NUM_ENTRIES, SIZEOF_NUM_ENTRIES);
    memcpy(&(header.freeSpaceOffset), (char *)pageData + POSITION_FREE_SPACE_OFFSET, SIZEOF_FREE_SPACE_OFFSET);
    memcpy(&(header.leftSibling), (char *)pageData + POSITION_SIBLING_PAGENUM_LEFT, SIZEOF_SIBLING_PAGENUM);
    memcpy(&(header.rightSibling), (char *)pageData + POSITION_SIBLING_PAGENUM_RIGHT, SIZEOF_SIBLING_PAGENUM);
    if (header.numEntries > PAGE_SIZE - SIZEOF_HEADER_LEAF || header.freeSpaceOffset > PAGE_SIZE)
        return -1;
    return SUCCESS;
}

void IndexManager::setHeaderLeaf(void *pageData, const HeaderLeaf header)
{
    memcpy((char *)pageData + POSITION_NUM_ENTRIES, &(header.numEntries), SIZEOF_NUM_ENTRIES);
    memcpy((char *)pageData + POSITION_FREE_SPACE_OFFSET, &(header.freeSpaceOffset), SIZEOF_FREE_SPACE_OFFSET);
    memcpy((char *)pageData + POSITION_SIBLING_PAGENUM_LEFT, &(header.leftSibling), SIZEOF_SIBLING_PAGENUM);
    memcpy((char *)pageData + POSITION_SIBLING_PAGENUM_RIGHT, &(header.rightSibling), SIZEOF_SIBLING_PAGENUM);
}

RC IndexManager::insertEntryInPage(void *page, const void *key, const RID &rid, const Attribute &attr, bool isLeafNodeconst, int rightChild /*= -1*/)
{
    //TODO: search the given page using IXFile_Iterator and find the correct position to insert either a Leaf entry
    //      or TrafficCop entry into *page. Check to make sure it will fit first. If the position is in middle, make sure
    //      to not write over existing data. If iterator reaches EOF, the correct position is at the end
    int entrySize = isLeafNodeconst ? findLeafEntrySize(key, attr) : findInteriorEntrySize(key, attr);
    if (entrySize < 0)
        return -1;

    void *entryToInsert = calloc(entrySize, sizeof(uint8_t));
    if (entryToInsert == nullptr)
        return -1;

    int keySize = findKeySize(key, attr);
    if (keySize < 0)
        return -1;

    if (isLeafNodeconst)
    {
        int offset = 0;
        memcpy((char *)entryToInsert + offset, key, keySize);
        offset += keySize;
        memcpy((char *)entryToInsert + offset, &rid.pageNum, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy((char *)entryToInsert + offset, &rid.slotNum, sizeof(uint32_t));
    }
    else
    {
        int offset = 0;
        memcpy((char *)entryToInsert + offset, key, keySize);
        offset += keySize;
        memcpy((char *)entryToInsert + offset, &rightChild, sizeof(uint32_t));
    }

    uint32_t offset = 0;
    uint32_t numberOfEntries = 0;

    void *entry = malloc(PAGE_SIZE);
    if (entry == nullptr)
    {
        free(entryToInsert);
        return -1;
    }

    void *slotData = malloc(PAGE_SIZE);
    if (slotData == nullptr)
    {
        free(entryToInsert);
        free(entry);
        return -1;
    }

    tuple<void *, int> keyDataWithSize = getKeyDataWithSize(attr, key);
    void *keyData = get<0>(keyDataWithSize);
    if (keyData == nullptr)
    {
        free(entryToInsert);
        free(entry);
        free(slotData);
        return -1;
    }

    int previousOffset = isLeafNodeconst ? SIZEOF_HEADER_LEAF : SIZEOF_HEADER_INTERIOR + SIZEOF_CHILD_PAGENUM;
    //previous offset will contain the value of the offset that contains the greatest entry that is < than key
    RC rc;
    while (getNextEntry(page, offset, numberOfEntries, entry, slotData, attr, isLeafNodeconst) != IX_EOF)
    {
        tuple<void *, int> entryDataWithSize = getKeyDataWithSize(attr, entry);
        void *entryData = get<0>(entryDataWithSize);
        if (entryData == nullptr)
        {
            free(entryToInsert);
            free(entry);
            free(slotData);
            free(keyData);
            return -1;
        }

        bool entry_lt_key;
        bool entry_eq_key;
        bool entry_gt_key;
        rc = compareKeyData(attr, entryData, keyData, entry_lt_key, entry_eq_key, entry_gt_key);

        free(entryData);

        if (rc != SUCCESS)
        {
            free(entryToInsert);
            free(entry);
            free(slotData);
            free(keyData);
            return rc;
        }
        //Check for duplicates
        if (isLeafNodeconst && entry_eq_key)
        {
            RID ridFromPage;
            memcpy(&(ridFromPage.pageNum), slotData, sizeof(uint32_t));
            memcpy(&(ridFromPage.slotNum), (char *)slotData + sizeof(uint32_t), sizeof(uint32_t));
            if (areRIDsEqual(rid, ridFromPage))
            {
                free(entryToInsert);
                free(entry);
                free(slotData);
                free(keyData);
                free(entryData);
                return -1;
            }
        }
        else if (!isLeafNodeconst && entry_eq_key)
        {
            int slotNum;
            memcpy(&slotNum, slotData, sizeof(uint32_t));
            if (slotNum == rightChild)
            {
                free(entryToInsert);
                free(entry);
                free(slotData);
                free(keyData);
                free(entryData);
                return -1;
            }
        }
        //finish check for duplicates
        if (entry_eq_key || entry_gt_key)
            break;

        previousOffset = offset;
    }
    free(entry);
    free(slotData);
    free(keyData);

    size_t freeSpaceOffset;
    rc = findFreeSpaceOffset(page, freeSpaceOffset);
    if (rc != SUCCESS)
    {
        free(entryToInsert);
        return rc;
    }

    if (entrySize < 0)
    {
        free(entryToInsert);
        return -1;
    }
    freeSpaceOffset += entrySize;

    //Move everything from: previousOffset to freeSpaceOffset to: previousOffset + entrySize
    size_t partToMoveSize = freeSpaceOffset - previousOffset;
    void *partToMove = calloc(partToMoveSize, 1);
    if (partToMove == nullptr)
    {
        free(entryToInsert);
        return -1;
    }

    //Copy data that should be after our inserted entry.
    memcpy(partToMove, (char *)page + previousOffset, partToMoveSize);

    //Copy new entry to page
    memcpy((char *)page + previousOffset, entryToInsert, entrySize);

    //Recopy the previously saved part
    memcpy((char *)page + previousOffset + entrySize, partToMove, partToMoveSize);

    free(partToMove);
    free(entryToInsert);

    if (isLeafNodeconst)
    {
        HeaderLeaf header;
        rc = getHeaderLeaf(page, header);
        if (rc != SUCCESS)
            return rc;
        header.numEntries += 1;
        header.freeSpaceOffset = freeSpaceOffset;
        setHeaderLeaf(page, header);
    }
    else
    {
        HeaderInterior header;
        rc = getHeaderInterior(page, header);
        if (rc != SUCCESS)
            return rc;
        header.numEntries += 1;
        header.freeSpaceOffset = freeSpaceOffset;
        setHeaderInterior(page, header);
    }
    return SUCCESS;
}

int IndexManager::findLeafEntrySize(const void *val, const Attribute attr)
{
    int keySize = findKeySize(val, attr);
    if (keySize < 0)
        return -1;

    int ridSize = sizeof(uint32_t) * 2; // pageNum + slotNum
    return keySize + ridSize;
}
bool IndexManager::areRIDsEqual(const RID &rid1, const RID &rid2)
{
    if (rid1.pageNum == rid2.pageNum)
    {
        return rid1.slotNum == rid2.slotNum;
    }
    return false;
}
int IndexManager::findInteriorEntrySize(const void *val, const Attribute attr)
{
    int keySize = findKeySize(val, attr);
    if (keySize < 0)
        return -1;

    int rightPageNumSize = SIZEOF_CHILD_PAGENUM; // Key must also have corresponding right pointer to child.
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
        keySize += 4; // Key also contains the length of the string.
        break;

    default:
        keySize = -1;
        break;
    }

    return keySize;
}

RC IndexManager::findFreeSpaceOffset(const void *pageData, size_t &freeSpaceOffset)
{
    size_t headerSize;
    RC rc;
    if (isLeafPage(pageData))
    {
        headerSize = SIZEOF_HEADER_LEAF;
        HeaderLeaf header;
        rc = getHeaderLeaf(pageData, header);
        if (rc != SUCCESS)
            return rc;
        freeSpaceOffset = header.freeSpaceOffset;
    }
    else
    {
        headerSize = SIZEOF_HEADER_INTERIOR;
        HeaderInterior header;
        rc = getHeaderInterior(pageData, header);
        if (rc != SUCCESS)
            return rc;
        freeSpaceOffset = header.freeSpaceOffset;
    }
    if (freeSpaceOffset < headerSize || freeSpaceOffset > PAGE_SIZE)
        return -1;
    return SUCCESS;
    /*
    memcpy(&freeSpaceOffset, (char *)pageData + POSITION_FREE_SPACE_OFFSET, SIZEOF_FREE_SPACE_OFFSET);

    size_t headerSize = isLeafPage(pageData) ? SIZEOF_HEADER_LEAF : SIZEOF_HEADER_INTERIOR;
    if (freeSpaceOffset < headerSize || freeSpaceOffset > PAGE_SIZE)
        return -1;
    return SUCCESS;
    */
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

RC IndexManager::willEntryFit(const void *pageData, const void *val, const Attribute attr, bool isLeafValue, bool &willFit)
{
    size_t freeSpaceOffset;
    auto rc = findFreeSpaceOffset(pageData, freeSpaceOffset);
    if (rc != SUCCESS)
        return rc;

    int entrySize = 0;
    if (isLeafValue)
    {
        entrySize = findLeafEntrySize(val, attr);
    }
    else
    {
        entrySize = findInteriorEntrySize(val, attr);
    }
    if (entrySize < 0)
        return -1;

    willFit = entrySize + freeSpaceOffset < PAGE_SIZE;
    return SUCCESS;
}

RC IndexManager::splitPage(void *prevPage, void *newPage, int prevPageNumber, int newPageNumber, const Attribute &attribute, tuple<void *, int> &newChildEntry, bool isLeafPage)
{
    uint32_t middleOfPage = (PAGE_SIZE / 2) - 1;
    uint32_t currentOffset = isLeafPage ? SIZEOF_HEADER_LEAF : SIZEOF_HEADER_INTERIOR + SIZEOF_CHILD_PAGENUM;
    uint32_t previousOffset = currentOffset;
    uint32_t entryCount = 0;
    uint32_t splitPoint = 0;
    void *keyValue = malloc(PAGE_SIZE);
    if (keyValue == nullptr)
        return -1;

    void *slotData = malloc(PAGE_SIZE);
    if (slotData == nullptr)
    {
        free(keyValue);
        return -1;
    }

    int firstHalfEntries = 0;
    while (getNextEntry(prevPage, currentOffset, entryCount, keyValue, slotData, attribute, isLeafPage) != IX_EOF)
    {

        if (currentOffset > middleOfPage)
        {
            splitPoint = previousOffset;
            break;
        }
        previousOffset = currentOffset;
        firstHalfEntries++;
    }
    free(slotData);

    RC rc;
    if (isLeafPage)
    {
        int leftChildSize = findLeafEntrySize(keyValue, attribute);
        if (leftChildSize < 0)
        {
            free(keyValue);
            return -1;
        }

        memcpy(get<0>(newChildEntry), keyValue, leftChildSize);
        get<1>(newChildEntry) = newPageNumber;

        HeaderLeaf prevHeader;
        rc = getHeaderLeaf(prevPage, prevHeader);
        if (rc != SUCCESS)
        {
            free(keyValue);
            return rc;
        }

        HeaderLeaf nextHeader;
        rc = getHeaderLeaf(newPage, nextHeader);
        if (rc != SUCCESS)
        {
            free(keyValue);
            return rc;
        }

        size_t numBytesForSplitEntries = prevHeader.freeSpaceOffset - splitPoint;
        memcpy((char *)newPage + SIZEOF_HEADER_LEAF, (char *)prevPage + splitPoint, numBytesForSplitEntries);
        memset((char *)prevPage + splitPoint, 0, numBytesForSplitEntries);

        nextHeader.leftSibling = prevPageNumber;
        nextHeader.rightSibling = prevHeader.rightSibling;
        nextHeader.numEntries = prevHeader.numEntries - firstHalfEntries;
        nextHeader.freeSpaceOffset = SIZEOF_HEADER_LEAF + numBytesForSplitEntries;
        setHeaderLeaf(newPage, nextHeader);

        prevHeader.rightSibling = newPageNumber;
        prevHeader.numEntries = firstHalfEntries;
        prevHeader.freeSpaceOffset = splitPoint;
        setHeaderLeaf(prevPage, prevHeader);
    }
    else
    {
        /*                    0            splitPoint   splitPoint + leftEntrySize      prevHeader.offset
         * currentPageLayout: | prevPage       | newChild.key        | newPage                | 
         */
        HeaderInterior prevHeader;
        rc = getHeaderInterior(prevPage, prevHeader);
        if (rc != SUCCESS)
        {
            free(keyValue);
            return rc;
        }

        HeaderInterior newHeader;
        rc = getHeaderInterior(newPage, newHeader);
        if (rc != SUCCESS)
        {
            free(keyValue);
            return rc;
        }

        int leftEntrySize = findInteriorEntrySize(keyValue, attribute);
        if (leftEntrySize < 0)
        {
            free(keyValue);
            return -1;
        }

        memcpy(get<0>(newChildEntry), keyValue, leftEntrySize);
        get<1>(newChildEntry) = newPageNumber;
        int numBytesForSplitEntries = prevHeader.freeSpaceOffset - (splitPoint + leftEntrySize);
        memcpy((char *)newPage + SIZEOF_HEADER_INTERIOR, (char *)prevPage + splitPoint + leftEntrySize, numBytesForSplitEntries);
        memset((char *)prevPage + splitPoint, 0, prevHeader.freeSpaceOffset - splitPoint);

        newHeader.numEntries = prevHeader.numEntries - firstHalfEntries;
        newHeader.freeSpaceOffset = SIZEOF_HEADER_INTERIOR + numBytesForSplitEntries;
        setHeaderInterior(newPage, newHeader);

        prevHeader.numEntries = firstHalfEntries;
        prevHeader.freeSpaceOffset = splitPoint;
        setHeaderInterior(prevPage, prevHeader);
    }
    free(keyValue);
    return SUCCESS;
}
bool IndexManager::isRoot(PageNum pageNumber)
{
    return pageNumber == rootPage;
}
//NOTE:: fieldValue and slotData must be of size PAGE_SIZE so they can be zero-ed each time. bad design. my bad.
RC IndexManager::getNextEntry(void *page, uint32_t &currentOffset, uint32_t &entryCount, void *fieldValue, void *slotData, const Attribute attr, bool isLeafPage)
{
    memset(fieldValue, 0, PAGE_SIZE);
    memset(slotData, 0, PAGE_SIZE);
    if (currentOffset == 0)
    {
        currentOffset = isLeafPage ? SIZEOF_HEADER_LEAF : SIZEOF_HEADER_INTERIOR + SIZEOF_CHILD_PAGENUM;
    }
    uint32_t numEntries = findNumberOfEntries(page);
    if (numEntries < 0)
        return -1;
    if (entryCount == numEntries)
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
RC IndexManager::insertToTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, int nodePointer, tuple<void *, int> &newChild)
{
    void *pageData = malloc(PAGE_SIZE * 2);
    if (pageData == nullptr)
        return -1;

    auto rc = ixfileHandle.readPage(nodePointer, pageData);
    if (rc != SUCCESS)
    {
        free(pageData);
        return rc;
    }

    if (!isLeafPage(pageData))
    {
        int pagePointer = -1;
        rc = findTrafficCop(key, attribute, pageData, pagePointer);
        if (rc != SUCCESS)
        {
            free(pageData);
            return rc;
        }
        if (pagePointer == -1)
        {
            free(pageData);
            return -1;
        }

        auto rc = insertToTree(ixfileHandle, attribute, key, rid, pagePointer, newChild);
        if (rc != SUCCESS)
        {
            free(pageData);
            //propogate error
            return rc;
        }
        if (get<0>(newChild) == nullptr)
        {
            free(pageData);
            return SUCCESS;
        }
        else
        {
            bool willFit;
            rc = willEntryFit(pageData, get<0>(newChild), attribute, false, willFit);
            if (rc != SUCCESS)
            {
                free(pageData);
                free(get<0>(newChild));
                get<0>(newChild) = nullptr;
                return rc;
            }
            if (willFit)
            {
                RID temp;
                rc = insertEntryInPage(pageData, get<0>(newChild), temp, attribute, false, get<1>(newChild));
                if (rc != SUCCESS)
                    return rc;

                ixfileHandle.writePage(nodePointer, pageData);
                free(pageData);
                free(get<0>(newChild));
                get<0>(newChild) = nullptr;
                return SUCCESS;
            }
            else
            {
                void *newPage = malloc(PAGE_SIZE);
                if (newPage == nullptr)
                {
                    free(newPage);
                    free(pageData);
                    free(get<0>(newChild));
                    get<0>(newChild) = nullptr;
                    return -1;
                }

                RID temp;
                PageNum newPageNumber = 0;

                rc = insertEntryInPage(pageData, get<0>(newChild), temp, attribute, false, get<1>(newChild));
                if (rc != SUCCESS)
                    return rc;

                //Page is now overfull so it must be split
                rc = createEmptyPage(ixfileHandle, newPage, false, newPageNumber);
                if (rc != SUCCESS)
                {
                    free(newPage);
                    free(pageData);
                    free(get<0>(newChild));
                    get<0>(newChild) = nullptr;
                    return rc;
                }

                rc = splitPage(pageData, newPage, nodePointer, newPageNumber, attribute, newChild, false);
                if (rc != SUCCESS)
                    return rc;

                rc = ixfileHandle.writePage(nodePointer, pageData);
                if (rc != SUCCESS)
                    return rc;

                rc = ixfileHandle.writePage(newPageNumber, newPage);
                if (rc != SUCCESS)
                    return rc;

                if (isRoot(nodePointer))
                {
                    rc = updateRoot(ixfileHandle, newChild, nodePointer, attribute);
                    if (rc != SUCCESS)
                        return rc;
                }
                free(pageData);
                free(newPage);
                free(get<0>(newChild));
                get<0>(newChild) = nullptr;
                return SUCCESS;
            }
        }
    }
    else // Leaf page.
    {
        bool willFit;
        rc = willEntryFit(pageData, key, attribute, true, willFit);
        if (rc != SUCCESS)
            return rc;

        if (willFit)
        {
            rc = insertEntryInPage(pageData, key, rid, attribute, true);
            if (rc != SUCCESS)
            {
                free(pageData);
                return rc;
            }

            rc = ixfileHandle.writePage(nodePointer, pageData);
            free(pageData);
            return rc;
        }

        get<0>(newChild) = malloc(PAGE_SIZE);
        if (get<0>(newChild) == nullptr)
        {
            free(pageData);
            return -1;
        }

        rc = insertEntryInPage(pageData, key, rid, attribute, true); // This updated our numEntries and freeSpaceOffset.
        if (rc != SUCCESS)
        {
            free(get<0>(newChild));
            free(pageData);
            get<0>(newChild) = nullptr;
            return rc;
        }

        //page is now overfull
        PageNum newPageNumber = 0;
        void *newPage = calloc(PAGE_SIZE, sizeof(uint8_t));

        rc = createEmptyPage(ixfileHandle, newPage, true, newPageNumber);
        if (rc != SUCCESS)
        {
            free(get<0>(newChild));
            free(pageData);
            get<0>(newChild) = nullptr;
            return rc;
        }

        splitPage(pageData, newPage, nodePointer, newPageNumber, attribute, newChild, true);

        rc = ixfileHandle.writePage(nodePointer, pageData);
        if (rc != SUCCESS)
        {
            free(get<0>(newChild));
            free(pageData);
            free(newPage);
            get<0>(newChild) = nullptr;
            return rc;
        }

        rc = ixfileHandle.writePage(newPageNumber, newPage);
        if (rc != SUCCESS)
        {
            free(get<0>(newChild));
            free(pageData);
            free(newPage);
            get<0>(newChild) = nullptr;
            return rc;
        }

        if (isRoot(nodePointer))
        {
            rc = updateRoot(ixfileHandle, newChild, nodePointer, attribute);
            if (rc != SUCCESS)
            {
                free(get<0>(newChild));
                free(pageData);
                free(newPage);
                get<0>(newChild) = nullptr;
                return rc;
            }
        }
        free(get<0>(newChild));
        free(pageData);
        free(newPage);
        get<0>(newChild) = nullptr;
        return rc;
    }
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void *oldChildKey = nullptr;
    int parentNodePageNum = -1;
    int currentNodePageNum = rootPage;
    return deleteEntry_subtree(ixfileHandle, attribute, key, rid, oldChildKey, parentNodePageNum, currentNodePageNum);
}

RC IndexManager::deleteEntry_subtree(IXFileHandle &ixfileHandle,
                                     const Attribute attribute,
                                     const void *keyToDelete,
                                     const RID &ridToDelete,
                                     const void *oldChildKey,
                                     const int parentNodePageNum,
                                     const int currentNodePageNum)
{
    void *currentNodePageData = malloc(PAGE_SIZE);
    if (currentNodePageData == nullptr)
        return -1;

    auto rc = ixfileHandle.readPage(currentNodePageNum, currentNodePageData);
    if (rc != SUCCESS)
    {
        free(currentNodePageData);
        return rc;
    }

    bool isLeaf = isLeafPage(currentNodePageData);
    free(currentNodePageData);

    if (isLeaf)
    {
        return deleteEntry_leaf(ixfileHandle,
                                attribute,
                                keyToDelete,
                                ridToDelete,
                                oldChildKey,
                                parentNodePageNum,
                                currentNodePageNum);
    }

    return deleteEntry_interior(ixfileHandle,
                                attribute,
                                keyToDelete,
                                ridToDelete,
                                oldChildKey,
                                parentNodePageNum,
                                currentNodePageNum);
}

RC IndexManager::deleteEntry_leaf(IXFileHandle &ixfileHandle,
                                  const Attribute attribute,
                                  const void *keyToDelete,
                                  const RID &ridToDelete,
                                  const void *oldChildKey,
                                  const int parentNodePageNum,
                                  const int currentNodePageNum)
{
    void *currentNodePageData = malloc(PAGE_SIZE);
    if (currentNodePageData == nullptr)
        return -1;

    auto rc = ixfileHandle.readPage(currentNodePageNum, currentNodePageData);
    if (rc != SUCCESS)
    {
        free(currentNodePageData);
        return rc;
    }

    HeaderLeaf header;
    rc = getHeaderLeaf(currentNodePageData, header);
    if (rc != SUCCESS)
    {
        free(currentNodePageData);
        return rc;
    }

    vector<tuple<void *, int>> dataEntriesWithSizes = getDataEntriesWithSizes_leaf(attribute, currentNodePageData);
    if (dataEntriesWithSizes.empty())
    {
        free(currentNodePageData);
        return -1;
    }

    vector<tuple<void *, int>> keysWithSizes = getKeysWithSizes_leaf(attribute, dataEntriesWithSizes);
    if (keysWithSizes.empty())
    {
        free(currentNodePageData);
        freeDataEntriesWithSizes(dataEntriesWithSizes);
        return -1;
    }

    vector<RID> rids = getRIDs_leaf(attribute, dataEntriesWithSizes);

    bool isUnderfull;
    rc = isNodeUnderfull(currentNodePageData, isUnderfull);
    if (rc != SUCCESS)
    {
        free(currentNodePageData);
        freeDataEntriesWithSizes(dataEntriesWithSizes);
        freeKeysWithSizes(keysWithSizes);
        return rc;
    }

    if ((uint32_t) currentNodePageNum == rootPage || !isUnderfull)
    {
        int firstEntryPosition_start = SIZEOF_HEADER_LEAF;
        int entryIndexToDelete = findIndexOfKeyWithRID(attribute, keysWithSizes, rids, keyToDelete, ridToDelete);

        bool entryWasFound = entryIndexToDelete >= 0;
        bool entryIsInPage = entryIndexToDelete < (int)header.numEntries;
        bool canDeleteEntry = entryWasFound && entryIsInPage;
        if (!canDeleteEntry)
        {
            free(currentNodePageData);
            freeDataEntriesWithSizes(dataEntriesWithSizes);
            freeKeysWithSizes(keysWithSizes);
            return -1;
        }

        int offset = firstEntryPosition_start;
        for (int i = 0; i < entryIndexToDelete; i++)
        {
            int entrySize = get<1>(dataEntriesWithSizes[i]);
            offset += entrySize;
        }
        int entryPositionToDelete_start = offset;
        int entryPositionToDelete_size = get<1>(dataEntriesWithSizes[entryIndexToDelete]);
        int entryPositionToDelete_end = entryPositionToDelete_start + entryPositionToDelete_size;

        size_t freeSpaceOffset;
        rc = findFreeSpaceOffset(currentNodePageData, freeSpaceOffset);
        if (rc != SUCCESS)
        {
            free(currentNodePageData);
            freeDataEntriesWithSizes(dataEntriesWithSizes);
            freeKeysWithSizes(keysWithSizes);
            return rc;
        }
        int lastEntryPosition_end = freeSpaceOffset;

        int numBytesToShift = lastEntryPosition_end - entryPositionToDelete_end;

        // Shift bytes after the deleted entry into its place.
        memmove((char *)currentNodePageData + entryPositionToDelete_start,
                (char *)currentNodePageData + entryPositionToDelete_end,
                numBytesToShift);

        header.numEntries--;
        header.freeSpaceOffset -= entryPositionToDelete_size;
        setHeaderLeaf(currentNodePageData, header);
        rc = ixfileHandle.writePage(currentNodePageNum, currentNodePageData);
        free(currentNodePageData);
        freeDataEntriesWithSizes(dataEntriesWithSizes);
        freeKeysWithSizes(keysWithSizes);
        return rc;
    }
    free(currentNodePageData);
    freeDataEntriesWithSizes(dataEntriesWithSizes);
    freeKeysWithSizes(keysWithSizes);
    return -1;
}

RC IndexManager::deleteEntry_interior(IXFileHandle &ixfileHandle,
                                      const Attribute attribute,
                                      const void *keyToDelete,
                                      const RID &ridToDelete,
                                      const void *oldChildKey,
                                      const int parentNodePageNum,
                                      const int currentNodePageNum)
{
    return -1;
}

int IndexManager::findIndexOfKeyWithRID(const Attribute attribute,
                                        const vector<tuple<void *, int>> keysWithSizes,
                                        const vector<RID> rids,
                                        const void *targetKey,
                                        const RID targetRID)
{
    tuple<void *, int> targetKeyDataWithSize = getKeyDataWithSize(attribute, targetKey);
    void *targetKeyData = get<0>(targetKeyDataWithSize);

    for (auto it = keysWithSizes.begin(); it != keysWithSizes.end(); it++)
    {
        int i = distance(keysWithSizes.begin(), it);

        RID rid = rids[i];

        tuple<void *, int> keyDataWithSize = getKeyDataWithSize(attribute, get<0>(keysWithSizes[i]));
        void *keyData = get<0>(keyDataWithSize);
        if (keyData == nullptr)
        {
            free(targetKeyData);
            return -1;
        }

        bool key_lt_target;
        bool key_eq_target;
        bool key_gt_target;
        auto rc = compareKeyData(attribute, keyData, targetKeyData, key_lt_target, key_eq_target, key_gt_target);
        if (rc != SUCCESS)
        {
            free(keyData);
            continue;
        }

        if (key_eq_target && rid == targetRID)
        {
            free(targetKeyData);
            free(keyData);
            return i;
        }
    }
    free(targetKeyData);
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
    void *pageData = malloc(PAGE_SIZE);
    auto rc = ixfileHandle.readPage(rootPage, pageData);
    if (rc != SUCCESS)
    {
        free(pageData);
        return rc;
    }

    // Base case.
    if (isLeafPage(pageData))
        return ix_ScanIterator.open(ixfileHandle,
                                    attribute,
                                    const_cast<void *>(lowKey),
                                    const_cast<void *>(highKey),
                                    lowKeyInclusive,
                                    highKeyInclusive,
                                    const_cast<void *>(pageData));

    // Recursively on corresponding child.
    int childPage = -1;
    rc = findTrafficCop(lowKey, attribute, pageData, childPage);
    free(pageData);
    if (rc != SUCCESS)
        return rc;
    if (childPage == -1)
        return -1;

    if (childPage < 0)
        return -1;
    return scan(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator);
}

vector<tuple<void *, int>> IndexManager::getDataEntriesWithSizes_leaf(const Attribute attribute, const void *pageData)
{
    vector<tuple<void *, int>> entriesWithSizes;

    uint32_t numEntries = findNumberOfEntries(pageData);
    if (numEntries < 0)
        return entriesWithSizes;

    size_t firstEntryPosition = SIZEOF_HEADER_LEAF;
    size_t offset = firstEntryPosition;

    for (uint32_t i = 0; i < numEntries; i++)
    {
        void *entry = (char *)pageData + offset;
        int entrySize = findLeafEntrySize(entry, attribute);
        if (entrySize < 0)
            continue;

        void *savedEntry = calloc(entrySize + 1, sizeof(uint8_t)); // Safety (+1) is for reading null-term if attribute is VarChar.
        memcpy(savedEntry, entry, entrySize);
        entriesWithSizes.push_back(make_tuple(savedEntry, entrySize));
        offset += entrySize;
    }

    return entriesWithSizes;
}

vector<tuple<void *, int>> IndexManager::getKeysWithSizes_leaf(const Attribute attribute, vector<tuple<void *, int>> dataEntriesWithSizes)
{
    vector<tuple<void *, int>> keysWithSizes;
    for (auto entryWithSize : dataEntriesWithSizes)
    {
        void *entry = get<0>(entryWithSize);
        int keySize = findKeySize(entry, attribute);
        if (keySize < 0)
            continue;
        void *savedKey = malloc(keySize);
        memcpy(savedKey, entry, keySize);
        tuple<void *, int> keyWithSize = make_tuple(savedKey, keySize);
        keysWithSizes.push_back(keyWithSize);
    }
    return keysWithSizes;
}

vector<RID> IndexManager::getRIDs_leaf(const Attribute attribute, vector<tuple<void *, int>> dataEntriesWithSizes)
{
    vector<RID> rids;
    for (auto entryWithSize : dataEntriesWithSizes)
    {
        void *entry = get<0>(entryWithSize);

        int keySize = findKeySize(entry, attribute);
        if (keySize < 0)
            continue;
        int ridPosition = keySize;
        int pageNumPosition = ridPosition;
        int slotNumPosition = pageNumPosition + sizeof(uint32_t);

        uint32_t pageNumber;
        uint32_t slotNumber;
        memcpy(&pageNumber, (char *)entry + pageNumPosition, sizeof(uint32_t));
        memcpy(&slotNumber, (char *)entry + slotNumPosition, sizeof(uint32_t));

        RID rid;
        rid.pageNum = pageNumber;
        rid.slotNum = slotNumber;
        rids.push_back(rid);
    }
    return rids;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
    cout << '{' << endl;
    printBtree(ixfileHandle, attribute, 0, rootPage);
    cout << endl
         << '}' << endl;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, uint32_t depth, PageNum pageNumber) const
{
    void *pageData = calloc(PAGE_SIZE, 1);
    ixfileHandle.readPage(pageNumber, pageData);

    if (isLeafPage(pageData))
    {
        printLeaf(ixfileHandle, attribute, depth, pageData);
    }
    else
    {
        printInterior(ixfileHandle, attribute, depth, pageData);
    }

    free(pageData);
}

void IndexManager::printLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, uint32_t depth, const void *pageData) const
{
    string indent(depth * 4, ' ');
    cout << indent;

    if (depth != 0)
    {
        cout << '{'; // Open leaf.
    }

    cout << "\"keys\": ["; // Open "keys" field.

    void *prevKeyData = nullptr;
    int prevKeySize = -1;

    vector<tuple<void *, int>> dataEntriesWithSizes = getDataEntriesWithSizes_leaf(attribute, pageData);
    vector<tuple<void *, int>> keysWithSizes = getKeysWithSizes_leaf(attribute, dataEntriesWithSizes);
    vector<RID> rids = getRIDs_leaf(attribute, dataEntriesWithSizes);
    freeDataEntriesWithSizes(dataEntriesWithSizes);

    for (auto it = keysWithSizes.begin(); it != keysWithSizes.end(); ++it)
    {
        int i = distance(keysWithSizes.begin(), it);
        tuple<void *, int> keyDataWithSize = getKeyDataWithSize(attribute, get<0>(keysWithSizes[i]));
        void *currKeyData = get<0>(keyDataWithSize);
        int currKeySize = get<1>(keyDataWithSize);
        RID currRID = rids[i];

        if (prevKeyData == nullptr)
        {
            // Open key.
            cout << "\"";
            switch (attribute.type)
            {
            case TypeReal:
                cout << *(float *)currKeyData;
                break;
            case TypeInt:
                cout << *(int *)currKeyData;
                break;
            case TypeVarChar:
                cout << (char *)currKeyData;
                break;
            }
            cout << ":["; // Open key's values.
        }
        else
        {
            bool isNewKey = prevKeySize != currKeySize || memcmp(prevKeyData, currKeyData, currKeySize) != 0;
            if (isNewKey)
            {
                cout << "]\""; // Close previous key.
                cout << ',';   // Join previous with current key.

                // Open current key.
                cout << "\"";
                switch (attribute.type)
                {
                case TypeReal:
                    cout << *(float *)currKeyData;
                    break;
                case TypeInt:
                    cout << *(int *)currKeyData;
                    break;
                case TypeVarChar:
                    cout << (char *)currKeyData;
                    break;
                }
                cout << ":["; // Open current key's values.
            }
            else // Same key as last iteration.
            {
                cout << ','; // Continue with the current key's values.
            }
        }

        cout << '(' << currRID.pageNum << ',' << currRID.slotNum << ')'; // Output RID for current key.

        prevKeyData = currKeyData;
        prevKeySize = currKeySize;
        free(currKeyData);
    }

    freeKeysWithSizes(keysWithSizes);

    if (prevKeyData != nullptr)
    {
        cout << "]\""; // Close last key.
    }

    cout << "]"; // Close "keys" field.

    if (depth != 0)
    {
        cout << '}'; // Close leaf.
    }
}

void IndexManager::printInterior(IXFileHandle &ixfileHandle, const Attribute &attribute, uint32_t depth, const void *pageData) const
{
    string indent(depth * 4, ' ');
    cout << indent;

    if (depth != 0)
    {
        cout << '{'; // Open node.
    }

    cout << "\"keys\": ["; // Open "keys" field.

    bool hadPrevKey = false;

    vector<tuple<void *, int>> keysWithSizes = getKeysWithSizes_interior(attribute, pageData);

    for (auto it = keysWithSizes.begin(); it != keysWithSizes.end(); ++it)
    {
        tuple<void *, int> keyWithSize = *it;
        tuple<void *, int> keyDataWithSize = getKeyDataWithSize(attribute, get<0>(keyWithSize));
        void *currKeyData = get<0>(keyDataWithSize);

        if (hadPrevKey)
            cout << "\", "; // Join previous key and current key.

        // Open key.
        cout << "\"";
        switch (attribute.type)
        {
        case TypeReal:
            cout << *(float *)currKeyData;
            break;
        case TypeInt:
            cout << *(int *)currKeyData;
            break;
        case TypeVarChar:
            cout << (char *)currKeyData;
            break;
        default:
            cout << "BAD_ATTR_TYPE";
            break;
        }

        hadPrevKey = true;
    }
    freeKeysWithSizes(keysWithSizes);

    if (hadPrevKey)
    {
        cout << '\"'; // Close last key.
    }
    cout << "]," << endl; // Close "keys" field, join with "children" field.

    cout << indent;
    if (depth != 0)
    {
        cout << ' '; // Alignment of "children" under "keys".
    }

    vector<int> children = getChildPointers_interior(attribute, pageData);
    cout << "\"children\": [" << endl; // Open "children" field.
    bool hadPrevChild = false;
    for (auto child : children)
    {
        if (hadPrevChild)
            cout << ',' << endl;
        printBtree(ixfileHandle, attribute, depth + 1, child);
        hadPrevChild = true;
    }
    cout << endl
         << ']'; // Close "children" field.

    if (depth != 0)
    {
        cout << '}'; // Close node.
    }
}

tuple<void *, int> IndexManager::getKeyDataWithSize(const Attribute attribute, const void *key)
{
    if (key == nullptr)
        return make_tuple(nullptr, -1);

    void *keyData = calloc(attribute.length + 1, sizeof(uint8_t)); // Null-term if attr is varchar.
    if (keyData == nullptr)
        return make_tuple(nullptr, -1);

    int size = 0;

    int offset = 0;
    switch (attribute.type)
    {
    case TypeReal:
    case TypeInt:
        size = sizeof(uint32_t);
        break;
    case TypeVarChar:
        memcpy(&size, key, sizeof(uint32_t)); // Get the size of string.
        offset += sizeof(uint32_t);
        break;

    default:
        break;
    }
    memcpy(keyData, (char *)key + offset, size);
    return make_tuple(keyData, size);
}

bool IndexManager::isLeafPage(const void *page)
{
    bool isLeaf;
    memcpy(&isLeaf, page, sizeof(bool));
    return isLeaf;
}

RC IndexManager::isNodeUnderfull(const void *nodePageData, bool &isUnderfull)
{
    int headerSize = isLeafPage(nodePageData) ? SIZEOF_HEADER_LEAF : SIZEOF_HEADER_INTERIOR;
    int totalEntrySpace = PAGE_SIZE - headerSize;

    int minimumThreshold = totalEntrySpace / 2;

    int firstEntryPosition_start = headerSize;

    size_t freeSpaceOffset;
    auto rc = findFreeSpaceOffset(nodePageData, freeSpaceOffset);
    if (rc != SUCCESS)
        return rc;
    int lastEntryPosition_end = freeSpaceOffset;

    int usedEntrySpace = lastEntryPosition_end - firstEntryPosition_start;

    isUnderfull = usedEntrySpace < minimumThreshold;
    return SUCCESS;
}

// NOTE: if keyData1 == nullptr, it is interpreted as -inf.  Similarly, if keyData2 == nullptr, it is interpreted as +inf.
RC compareKeyData(const Attribute attr, const void *keyData1, const void *keyData2, bool &lt, bool &eq, bool &gt)
{
    bool negativeInf = keyData1 == nullptr;
    bool positiveInf = keyData2 == nullptr;
    if (negativeInf || positiveInf)
    {
        lt = true;  // -inf <  +inf, -inf <  x, x <  +inf
        eq = false; // -inf != +inf, -inf != x, x != +inf
        gt = false; // -inf !> +inf, -inf !> x, x !> +inf
        return SUCCESS;
    }

    int kd1_int;
    int kd2_int;
    float kd1_float;
    float kd2_float;
    string kd1_string;
    string kd2_string;
    int stringComp;

    switch (attr.type)
    {
    case TypeInt:
        kd1_int = *(int *)keyData1;
        kd2_int = *(int *)keyData2;
        lt = kd1_int < kd2_int;
        eq = kd1_int == kd2_int;
        gt = kd1_int > kd2_int;
        return SUCCESS;
    case TypeReal:
        kd1_float = *(float *)keyData1;
        kd2_float = *(float *)keyData2;
        lt = kd1_float < kd2_float;
        eq = kd1_float == kd2_float;
        gt = kd1_float > kd2_float;
        return SUCCESS;
    case TypeVarChar:
        kd1_string = (char *)keyData1;
        kd2_string = (char *)keyData2;
        stringComp = kd1_string.compare(kd2_string);
        lt = stringComp < 0;
        eq = stringComp == 0;
        gt = stringComp > 0;
        return SUCCESS;
    default:
        return -1;
    }
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
    if (closed_)
        return IX_SI_CLOSED;

    HeaderLeaf header;
    auto rc = IndexManager::getHeaderLeaf(currentPageData_, header);
    if (rc != SUCCESS)
        return rc;

    bool readAllEntriesInPage = numEntriesReadInPage_ >= header.numEntries;
    if (readAllEntriesInPage)
    {
        if (header.rightSibling < 0 || header.rightSibling >= (int)ixfileHandle_.ufh->getNumberOfPages())
            return IX_EOF;
        rc = ixfileHandle_.readPage(header.rightSibling, currentPageData_);
        if (rc != SUCCESS)
            return rc;
        numEntriesReadInPage_ = 0;
    }

    // More entries on page to read.

    vector<tuple<void *, int>> dataEntriesWithSizes = IndexManager::getDataEntriesWithSizes_leaf(attribute_, currentPageData_);
    vector<tuple<void *, int>> keysWithSizes = IndexManager::getKeysWithSizes_leaf(attribute_, dataEntriesWithSizes);
    vector<RID> rids = IndexManager::getRIDs_leaf(attribute_, dataEntriesWithSizes);
    freeDataEntriesWithSizes(dataEntriesWithSizes);

    int i = numEntriesReadInPage_; // if we've read entries [0, i), we must now read entry i.

    tuple<void *, int> current = IndexManager::getKeyDataWithSize(attribute_, get<0>(keysWithSizes[i]));
    tuple<void *, int> low = IndexManager::getKeyDataWithSize(attribute_, lowKey_);
    tuple<void *, int> high = IndexManager::getKeyDataWithSize(attribute_, highKey_);
    numEntriesReadInPage_++;

    bool low_lt_current;
    bool low_eq_current;
    bool low_gt_current;
    rc = compareKeyData(attribute_, get<0>(low), get<0>(current), low_lt_current, low_eq_current, low_gt_current);
    if (rc != SUCCESS)
        return rc;
    bool withinLowBound = lowKeyInclusive_ ? low_eq_current || low_lt_current : low_lt_current;

    bool current_lt_high;
    bool current_eq_high;
    bool current_gt_high;
    rc = compareKeyData(attribute_, get<0>(current), get<0>(high), current_lt_high, current_eq_high, current_gt_high);
    if (rc != SUCCESS)
        return rc;
    bool withinHighBound = highKeyInclusive_ ? current_eq_high || current_lt_high : current_lt_high;

    free(get<0>(current));
    free(get<0>(low));
    free(get<0>(high));

    /* Our search in index of ALL leaf nodes looks like:
     *     k1 | k2 | ... | kl | ... | ki | ... | kh | ... | k(n-2) | k(n-1)
     * where
     *   k1 = first key in index
     *   kl = low key of search
     *   ki = current key being scanned
     *   kh = high key of search
     *   k(n-1) = last key in index
     *
     * Recall that keys are in sorted order (by B+ tree properties, this is guaranteed).
     *   If we've gone past kh (not within bounds), then all keys after kh are also not within bounds, so our search is done.
     *   If we're not within bounds of kl, we haven't gone far enough, so keep searching.
     *   If we're within bounds of kl and kh, we have a matching entry to return to the caller.
     */
    if (!withinHighBound)
        return IX_EOF;
    if (!withinLowBound)
        return getNextEntry(rid, key);

    rid = rids[i];
    memcpy(key, get<0>(keysWithSizes[i]), get<1>(keysWithSizes[i]));
    freeKeysWithSizes(keysWithSizes);
    return SUCCESS;
}

// PageData must be a leaf which contains lowKey.
RC IX_ScanIterator::open(IXFileHandle &ixfileHandle,
                         Attribute attribute,
                         void *lowKey,
                         void *highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive,
                         void *pageData)
{
    closed_ = false;

    ixfileHandle_ = ixfileHandle;
    attribute_ = attribute;
    lowKey_ = lowKey;
    highKey_ = highKey;
    lowKeyInclusive_ = lowKeyInclusive;
    highKeyInclusive_ = highKeyInclusive;

    currentPageData_ = pageData;
    numEntriesReadInPage_ = 0;
    return SUCCESS;
}

RC IX_ScanIterator::close()
{
    closed_ = true;
    free(currentPageData_);
    return SUCCESS;
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
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    ixReadPageCounter++;
    RC rc = ufh->readPage(pageNum, data);
    return rc;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    ixWritePageCounter++;
    RC rc = ufh->writePage(pageNum, data);
    return rc;
}

RC IXFileHandle::appendPage(const void *data)
{
    ixAppendPageCounter++;
    RC rc = ufh->appendPage(data);
    return rc;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return ufh->collectCounterValues(readPageCount, writePageCount, appendPageCount);
}

void freeKeysWithSizes(vector<tuple<void *, int>> keysWithSizes)
{
    for (auto it = keysWithSizes.begin(); it != keysWithSizes.end(); it++)
    {
        tuple<void *, int> keyWithSize = *it;
        void *key = get<0>(keyWithSize);
        free(key);
    }
}

void freeDataEntriesWithSizes(vector<tuple<void *, int>> dataEntriesWithSizes)
{
    for (auto it = dataEntriesWithSizes.begin(); it != dataEntriesWithSizes.end(); it++)
    {
        tuple<void *, int> entryWithSize = *it;
        void *entry = get<0>(entryWithSize);
        free(entry);
    }
}

