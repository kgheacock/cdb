#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

int testCase_16()
{
    // Functions tested
    // 1. isLeafPage
    cerr << endl << "***** In IX Test Case 16 *****" << endl;

    void *leafPageData = calloc(PAGE_SIZE, sizeof(uint8_t));
    bool isLeaf = true;
    memcpy(leafPageData, &isLeaf, sizeof(bool));
    assert(IndexManager::isLeafPage(leafPageData) && "Should read leaf page");
    free(leafPageData);

    void *interiorPageData = calloc(PAGE_SIZE, sizeof(uint8_t));
    isLeaf = false;
    memcpy(interiorPageData, &isLeaf, sizeof(bool));
    assert(!IndexManager::isLeafPage(interiorPageData) && "Should read interior page");
    free(interiorPageData);

    return success;
}

int main()
{
    // Global Initialization
    indexManager = IndexManager::instance();

    RC result = testCase_16();
    if (result == success) {
        cerr << "***** IX Test Case 16 finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case 16 failed. *****" << endl;
        return fail;
    }

}

