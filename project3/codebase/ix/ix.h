#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

#define IX_EOF (-1) // end of the index scan

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
    string fileName;
    //Pre: page is a pointer to page data of any type
    //Post: the truth value of whether the page parameter is a leaf
    bool isLeafPage(const void *page);

    //Pre: val contains a valid leaf entry to be inserted and attr corresponding to that entry
    //Post: return the total size of the entry including size of RID
    int findLeafEntrySize(const void *val, const Attribute attr);

    //Pre: page is a pointer to a page data of any type
    //Post: the offset of the page’s free space is returned
    int freeSpaceOffset(void *page);

    //Pre: val contains a valid entry that will be inserted to a non-leaf page and attr corresponding to that entry
    //Post: return the total size of the entry which will be equal to the key size
    int findTrafficCopSize(const void *val, const Attribute attr);

    //Pre: val contains a valid entry to be inserted either to a Leaf or Non-Leaf page and attr corresponding to that entry.
    //     &Page is a pointer to a FileHandle of a page
    //Post: returns whether or not the given val will fit on the given page.
    bool willEntryFit(const void *pageData, const void *val, const Attribute attr, bool isLeafValue);

    //Pre: val contains a valid entry to be inserted and attr corresponding to that entry. &page is a pointer to a FileHandle of a
    //     Non-Leaf page
    //Post: returns the page number corresponding to either the next level down
    int findTrafficCop(const void *val, const Attribute attr, const void *pageData);

    void insertNode(void *page, const void *key, const Attribute &attr, bool isLeafNode);

    void splitPage(void *inPage, void *newChildEntry);

    void insertToTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, int nodePointer, void *newChild);

    bool isRoot(int pageNumber);
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
};

class IXFileHandle
{
public:
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    FileHandle fh;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
};

#endif
