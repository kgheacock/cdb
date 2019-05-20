
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
        auto rc = index_file.readPage(pageNumber, pageData);
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

uint32_t IndexManager::findNumberOfEntries(const void *page)
{
    uint32_t numberOfEntries = 0;
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

    rc = fileHandle.writePage(rootPage, page);
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
    tuple<void *, int> targetTrafficCopWithSize = getKeyDataWithSize(attr, val);
    void *targetTrafficCop = get<0>(targetTrafficCopWithSize);

    vector<tuple<void *, int>> keysWithSizes = getKeysWithSizes_interior(attr, pageData);
    vector<int> children = getChildPointers_interior(attr, pageData);

    // Give priority to first child on null value.
    if (val == nullptr)
        return children.empty() ? -1 : children.front();

    for (auto it = children.begin(); it != children.end(); ++it)
    {
        int i = distance(children.begin(), it);

        int child = *it;
        tuple<void *, int> currentTrafficCopWithSize = getKeyDataWithSize(attr, get<0>(keysWithSizes[i]));
        void *currentTrafficCop = get<0>(currentTrafficCopWithSize);

        bool lt;
        bool eq;
        bool gt;
        auto rc = compareKeyData(attr, targetTrafficCop, currentTrafficCop, lt, eq, gt);
        if (rc != SUCCESS)
            return rc;

        bool target_gteq_current = eq || gt; 
        if (!target_gteq_current)
            return child;
    }
    return children.empty() ? -1 : children.back();
}

vector<tuple<void *, int>> IndexManager::getKeysWithSizes_interior(const Attribute attribute, const void *pageData)
{
    vector<tuple<void *, int>> keysWithSizes;

    uint32_t numEntries = findNumberOfEntries(pageData);

    size_t firstEntryPosition = SIZEOF_HEADER_INTERIOR + SIZEOF_CHILD_PAGENUM; // Skip over header and first child pointer.
    size_t offset = firstEntryPosition;

    for (uint32_t i = 0; i < numEntries; i++)
    {
        void *entry = (char *)pageData + offset;
        int entrySize = findInteriorEntrySize(entry, attribute);
        int keySize = entrySize - SIZEOF_CHILD_PAGENUM; // Each "entry" is key + pagenum for child with keys >= (i.e. the right child).
        void *savedKey = calloc(keySize + 1, sizeof(uint8_t)); // Safety (+1) is for reading null-term if attribute is VarChar.
        memcpy(savedKey, entry, keySize);
        keysWithSizes.push_back(make_tuple(savedKey, keySize));
        offset += entrySize;
    }

    return keysWithSizes;
}

vector<int> IndexManager::getChildPointers_interior(const Attribute attribute, const void *pageData)
{
    vector<int> children;
    int child;

    uint32_t numEntries = findNumberOfEntries(pageData);

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
        int keySize = entrySize - SIZEOF_CHILD_PAGENUM; // Each "entry" is key + pagenum for child with keys >= (i.e. the right child).
        offset += keySize; // Skip over key so we're now on right child pointer.

        memcpy(&child, (char *)pageData + offset, SIZEOF_CHILD_PAGENUM);
        children.push_back(child);
        offset += entrySize;
    }

    return children;
}

bool IndexManager::isRoot(PageNum pageNumber)
{
    return pageNumber == rootPage;
}

HeaderInterior IndexManager::getHeaderInterior(const void *pageData)
{
    HeaderInterior header;
    memcpy(&(header.numEntries), (char *)pageData + POSITION_NUM_ENTRIES, SIZEOF_NUM_ENTRIES);
    memcpy(&(header.freeSpaceOffset), (char *)pageData + POSITION_FREE_SPACE_OFFSET, SIZEOF_FREE_SPACE_OFFSET);
    return header;
}

void IndexManager::setHeaderInterior(void *pageData, HeaderInterior header)
{
    memcpy((char *)pageData + POSITION_NUM_ENTRIES, &(header.numEntries), SIZEOF_NUM_ENTRIES);
    memcpy((char *)pageData + POSITION_FREE_SPACE_OFFSET, &(header.freeSpaceOffset), SIZEOF_FREE_SPACE_OFFSET);
}

HeaderLeaf IndexManager::getHeaderLeaf(const void *pageData)
{
    HeaderLeaf header;
    memcpy(&(header.numEntries), (char *)pageData + POSITION_NUM_ENTRIES, SIZEOF_NUM_ENTRIES);
    memcpy(&(header.freeSpaceOffset), (char *)pageData + POSITION_FREE_SPACE_OFFSET, SIZEOF_FREE_SPACE_OFFSET);
    memcpy(&(header.leftSibling), (char *)pageData + POSITION_SIBLING_PAGENUM_LEFT, SIZEOF_SIBLING_PAGENUM);
    memcpy(&(header.rightSibling), (char *)pageData + POSITION_SIBLING_PAGENUM_RIGHT, SIZEOF_SIBLING_PAGENUM);
    return header;
}

void IndexManager::setHeaderLeaf(void *pageData, HeaderLeaf header)
{
    memcpy((char *)pageData + POSITION_NUM_ENTRIES, &(header.numEntries), SIZEOF_NUM_ENTRIES);
    memcpy((char *)pageData + POSITION_FREE_SPACE_OFFSET, &(header.freeSpaceOffset), SIZEOF_FREE_SPACE_OFFSET);
    memcpy((char *)pageData + POSITION_SIBLING_PAGENUM_LEFT, &(header.leftSibling), SIZEOF_SIBLING_PAGENUM);
    memcpy((char *)pageData + POSITION_SIBLING_PAGENUM_RIGHT, &(header.rightSibling), SIZEOF_SIBLING_PAGENUM);
}

void IndexManager::insertEntryInPage(void *page, const void *key, const RID &rid, const Attribute &attr, bool isLeafNode)
{
    //TODO: search the given page using IXFile_Iterator and find the correct position to insert either a Leaf entry
    //      or TrafficCop entry into *page. Check to make sure it will fit first. If the position is in middle, make sure
    //      to not write over existing data. If iterator reaches EOF, the correct position is at the end
    uint32_t numEntries = findNumberOfEntries(page);
    if (isLeafNode)
    {
        //int offset = 0;
        HeaderLeaf header = getHeaderLeaf(page);
        size_t newFreeSpaceOffset;
        if (numEntries == 0)
        {
            size_t insertPosition = SIZEOF_HEADER_LEAF;
            newFreeSpaceOffset = insertPosition + findLeafEntrySize(key, attr);

            // Write key.
            const size_t keySize = findKeySize(key, attr);
            memcpy((char *)page + insertPosition, key, keySize);
            insertPosition += keySize;

            // Write RID pageNum.
            memcpy((char *)page + insertPosition, &(rid.pageNum), sizeof(uint32_t));
            insertPosition += sizeof(uint32_t);
            
            // Write RID slotNum.
            memcpy((char *)page + insertPosition, &(rid.slotNum), sizeof(uint32_t));
            insertPosition += sizeof(uint32_t);
        }
        else
        {
            //int freeSpaceOffset = findFreeSpaceOffset(page);
            newFreeSpaceOffset = header.freeSpaceOffset;
        }

        header.freeSpaceOffset = newFreeSpaceOffset;
        header.numEntries = numEntries + 1;
        setHeaderLeaf(page, header);
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
    size_t freeSpaceOffset = 0;
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

    auto rc = ixfileHandle.readPage(nodePointer, pageData);
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
            ixfileHandle.writePage(nodePointer, pageData);
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
            ixfileHandle.writePage(nodePointer, pageData);
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
            ixfileHandle.writePage(nodePointer, pageData);
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
    int childPage = findTrafficCop(lowKey, attribute, pageData);
    free(pageData);
    if (childPage == -1)
        return -1;
    return scan(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator);
}

vector<tuple<void *, int>> IndexManager::getDataEntriesWithSizes_leaf(const Attribute attribute, const void *pageData)
{
    vector<tuple<void *, int>> entriesWithSizes;

    uint32_t numEntries = findNumberOfEntries(pageData);

    size_t firstEntryPosition = SIZEOF_HEADER_LEAF;
    size_t offset = firstEntryPosition;

    for (uint32_t i = 0; i < numEntries; i++)
    {
        void *entry = (char *)pageData + offset;
        int entrySize = findLeafEntrySize(entry, attribute);
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
        void *savedKey = malloc(keySize);
        memcpy(savedKey, entry, keySize);
        keysWithSizes.push_back(make_tuple(savedKey, keySize));
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
    cout << endl << '}' << endl;
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
    string indent (depth * 4, ' ');
    cout << indent << "{\"keys\": ["; // Open leaf, open "keys" field.

    void *prevKeyData = nullptr;
    int prevKeySize = -1;

    vector<tuple<void *, int>> dataEntriesWithSizes = getDataEntriesWithSizes_leaf(attribute, pageData);
    vector<tuple<void *, int>> keysWithSizes = getKeysWithSizes_leaf(attribute, dataEntriesWithSizes);
    vector<RID> rids = getRIDs_leaf(attribute, dataEntriesWithSizes);

    for (auto it = dataEntriesWithSizes.begin(); it != dataEntriesWithSizes.end(); ++it)
    {
        int i = distance(dataEntriesWithSizes.begin(), it);
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
                cout << *(float *) currKeyData;
                break;
            case TypeInt:
                cout << *(int *) currKeyData;
                break;
            case TypeVarChar:
                cout << (char *) currKeyData;
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
                cout << ','; // Join previous with current key.

                // Open current key.
                cout << "\"";
                switch (attribute.type)
                {
                case TypeReal:
                    cout << *(float *) currKeyData;
                    break;
                case TypeInt:
                    cout << *(int *) currKeyData;
                    break;
                case TypeVarChar:
                    cout << (char *) currKeyData;
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

    }

    if (prevKeyData != nullptr)
    {
        cout << "]\""; // Close last key.
    }

    cout << indent << "]}"; // Close "keys" field, close leaf.
}

void IndexManager::printInterior(IXFileHandle &ixfileHandle, const Attribute &attribute, uint32_t depth, const void *pageData) const
{

}

tuple<void *, int> IndexManager::getKeyDataWithSize(const Attribute attribute, const void *key)
{
    if (key == nullptr)
        return make_tuple(nullptr, -1);

    void *keyData = calloc(attribute.length + 1, sizeof(uint8_t)); // Null-term if attr is varchar.
    int size = 0;

    int offset = 0;
    switch (attribute.type)
    {
    case TypeReal:
    case TypeInt:
        size = sizeof(uint32_t);
        break;
    case TypeVarChar:
        memcpy(&size, key, sizeof(uint32_t));  // Get the size of string.
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


// NOTE: if keyData1 == nullptr, it is interpreted as -inf.  Similarly, if keyData2 == nullptr, it is interpreted as +inf.
RC compareKeyData(const Attribute attr, const void *keyData1, const void *keyData2, bool &lt, bool &eq, bool &gt)
{
    bool negativeInf = keyData1 == nullptr;
    bool positiveInf = keyData2 == nullptr;
    if (negativeInf && positiveInf)
    {
        lt = true; // -inf < +inf
        eq = false; // -inf != +inf
        gt = false; // -inf !> +inf
        return SUCCESS;
    }
    else if (negativeInf)
    {
        lt = false; // -inf < x
        eq = false; // -inf != x
        gt = true; // -inf !> x
        return SUCCESS;
    }
    else if (positiveInf) // x ? +inf
    {
        lt = true; // x < +inf
        eq = false; // x != +inf
        gt = false; // x !> +inf
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
            kd1_int = * (int *) keyData1;
            kd2_int = * (int *) keyData2;
            lt = kd1_int < kd2_int;
            eq = kd1_int == kd2_int;
            gt = kd1_int > kd2_int;
            return SUCCESS;
        case TypeReal:
            kd1_float = * (float *) keyData1;
            kd2_float = * (float *) keyData2;
            lt = kd1_float < kd2_float;
            eq = kd1_float == kd2_float;
            gt = kd1_float > kd2_float;
            return SUCCESS;
        case TypeVarChar:
            kd1_string = (char *) keyData1;
            kd2_string = (char *) keyData2;
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
    RC rc;
    HeaderLeaf header = IndexManager::getHeaderLeaf(currentPageData_);
    bool readAllEntriesInPage = numEntriesReadInPage_ >= header.numEntries;
    if (readAllEntriesInPage)
    {
        if (header.rightSibling < 0 || header.rightSibling >= (int) ixfileHandle_.ufh->getNumberOfPages())
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
    delete (ufh);
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
