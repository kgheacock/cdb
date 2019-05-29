#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe_test_util.h"

/* ASSUMES THAT THERE IS A TABLE AND INDEX CREATED ON table "left", attribute "B" */

int TestDestroyIndex() {
    // Mandatory for all
    // Destroy index

    RC rc = success;
    cerr << endl << "***** In QE Test Destroy Index *****" << endl;

    // Destroy index.
    rc = destroyIndexforLeftB();
    if (rc != success)
    {
        cerr << "***** destroyIndex() failed for: table \"left\", attribute \"B\".  *****" << endl;
        return rc;
    }

    // Try destroy again.  Should fail.
    rc = destroyIndexforLeftB();
    if (rc == success)
    {
        cerr << "***** Duplicate destroyIndex() should fail for: table \"left\", attribute \"B\".  *****" << endl;
        return -1;
    }

    // Try create index again.  Ensure it was previously deleted cleanly.
    rc = createIndexforLeftB();
    if (rc != success) {
        cerr << "***** createIndexforLeftB() failed.  *****" << endl;
        return rc;
    }

    // Destroy for final cleanup.
    rc = destroyIndexforLeftB();
    if (rc != success)
    {
        cerr << "***** destroyIndex() failed for: table \"left\", attribute \"B\".  *****" << endl;
        return -1;
    }

    // Try to destroy nonexistent table.  Should fail.
    rc = destroyIndexforNonexistentTable();
    if (rc == success)
    {
        cerr << "***** destroyIndexforNonexistentTable() should fail.  *****" << endl;
        return -1;
    }

    // Try to destroy attribute that's not an index.  Should fail.
    rc = destroyIndexforLeftNonexistentAttribute();
    if (rc == success)
    {
        cerr << "***** destroyIndexforNonexistentAttribute() should fail.  *****" << endl;
        return -1;
    }

    return SUCCESS;
}


int main() {
    if (TestDestroyIndex() != success) {
        cerr << "***** [FAIL] QE Test Destroy Index failed. *****" << endl;
        return fail;
    } else {
        cerr << "***** QE Test Destroy Index finished. The result will be examined. *****" << endl;
        return success;
    }
}
