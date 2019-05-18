#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stdio.h>
#include <string.h>

#include "../rbf/rbfm.h"

const int LEAF_PAGE_HEADER_SIZE(17);    // bool isLeaf + int numEntries + int leftSibling + int rightSibling + int freeSpaceOffset
const int INTERIOR_PAGE_HEADER_SIZE(9); // bool isLeaf + int numEntries + int freeSpace
const int IX_EOF(-1);                   // end of the index scan

// Headers for leaf nodes and internal nodes
typedef struct
{
    uint32_t entries;   // entry number
    uint32_t freeSpace; // free space offset
} HeaderInternal;

typedef struct
{
    uint32_t entries;   // entry number
    uint32_t freeSpace; // free space offset
    uint32_t prev;      // left sibling
    uint32_t next;      // right sibling

} HeaderLeaf;


class IX_ScanIterator;
class IXFileHandle;
class IndexManager
{

public:
    static IndexManager *instance();

    // Create an index file.
    RC createFile(const string &fileName);

    // Delete an index file.
    RC destroyFile(const string &fileName);

    // Open an index and return an ixfileHandle.
    RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

    // Close an ixfileHandle for an index.
    RC closeFile(IXFileHandle &ixfileHandle);

    // Insert an entry into the given index that is indicated by the given ixfileHandle.
    RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixfileHandle.
    RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixfileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;


protected:
    IndexManager();
    ~IndexManager();

private:
    static IndexManager *_index_manager;
    static PagedFileManager *_pf_manager;
    string fileName;
    int rootPage;

    RC createEmptyPage(IXFileHandle &index_file, void *page, bool isLeafPage, int &pageNumber, int leftSibling = 0, int rightSibling = 0);

    //Pre: page is a pointer to page data of any type
    //Post: the truth value of whether the page parameter is a leaf
    static bool isLeafPage(const void *page);

    static int findNumberOfEntries(const void *page);

    //Pre: val contains a valid leaf entry to be inserted and attr corresponding to that entry
    //Post: return the total size of the entry including size of RID
    static int findLeafEntrySize(const void *val, const Attribute attr);

    //Pre: page is a pointer to a page data of any type
    //Post: the offset of the page’s free space is returned
    static int findFreeSpaceOffset(const void *page);

    //Pre: val contains a valid entry that will be inserted to a non-leaf page and attr corresponding to that entry
    //Post: return the total size of the entry which will be equal to the key size
    static int findInteriorNodeSize(const void *val, const Attribute &attr);

    static int freeSpaceOffset(const void *pageData);

    //Pre: val contains a valid entry to be inserted either to a Leaf or Non-Leaf page and attr corresponding to that entry.
    //     &Page is a pointer to a FileHandle of a page
    //Post: returns whether or not the given val will fit on the given page.
    static bool willEntryFit(const void *pageData, const void *val, const Attribute attr, bool isLeafValue);

    //Pre: val contains a valid entry to be inserted and attr coresponding to that entry. pageData is a
    //      page that contains traffic cops
    //Post: returns the page number of the next page to visit
    int findTrafficCop(const void *val, const Attribute attr, const void *pageData);

    //Pre: *page contains the page where *key will be written. attr corresponds to key and isLeafNode tells
    //      whether page is a leaf page
    //Post: *page will be searched and key (which is in the correct format) will be placed in the correct position
    void insertEntryInPage(void *page, const void *key, const RID &rid, const Attribute &attr, bool isLeafNode);

    //Post: a new node will be allocated with the minimum value in the right child as the traffic cop. The left pointer of that traffic cop will point to leftChild
    void updateRoot(IXFileHandle &IXFileHandle, const int leftChild, const int rightChild, void *rightChildValue);

    void splitPage(IXFileHandle &ixfileHandle, const Attribute &attribute, void *inPage, void *newChildEntry, int &newPageNumber);


    void getNextEntry(void *page, int &currentOffset, void *fieldValue, const Attribute attr, bool isLeafPage);

    void insertToTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, int nodePointer, void *newChild);

    bool isRoot(int pageNumber);

    RC updateRoot();
};

class IX_ScanIterator
{
public:
    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

private:
    IXFileHandle *ufh;
};

class IXFileHandle
{
public:
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    FileHandle *ufh;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
};

#endif
