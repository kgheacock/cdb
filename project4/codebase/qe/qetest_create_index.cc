#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe_test_util.h"

int TestCreateIndex() {
    // Mandatory for all
    // Create an Index
    // Load Data
    // Create an Index

    RC rc = success;
    cerr << endl << "***** In QE Test Create Index *****" << endl;

    // Create an index before inserting tuples.
    rc = createIndexforLeftB();
    if (rc != success) {
        cerr << "***** createIndexforLeftB() failed.  *****" << endl;
        return rc;
    }

    // Scan the index catalog for entries on table "left", get the attribute name.
    RM_ScanIterator rmsi;

    uint32_t compValueLength = 4; 
    string compValueString { "left" };
    void *compValue = calloc(sizeof(uint32_t) + 4, sizeof(uint8_t));
    memcpy(compValue, &compValueLength, sizeof(uint32_t));
    memcpy((char *) compValue + sizeof(uint32_t), compValueString.c_str(), compValueLength);

    vector<string> projectedAttributes { INDEXES_COL_COLUMN_NAME };
    rc = rm->scan(INDEXES_TABLE_NAME, INDEXES_COL_TABLE_NAME, EQ_OP, compValue, projectedAttributes, rmsi);
    if (rc != SUCCESS) {
        cerr << "***** scan() failed.  *****" << endl;
        return rc;
    }

    // For all indexes on table "left", try to find index on attribute "B".
    RID rid; 
    bool attributeFound = false;
    void *data = malloc(PAGE_SIZE);
    while (rmsi.getNextTuple(rid, data) == SUCCESS)
    {
        uint8_t SIZEOF_NULL_INDICATOR = 1;
        uint32_t SIZEOF_VARCHAR_LENGTH = sizeof(uint32_t);

        int tupleLength = * (int *) ((char *) data + SIZEOF_NULL_INDICATOR);

        char tupleString[tupleLength + 1];
        memcpy(tupleString, (char *) data + SIZEOF_NULL_INDICATOR + SIZEOF_VARCHAR_LENGTH, tupleLength),
        tupleString[tupleLength] = '\0';

        if (strcmp(tupleString, "B") == 0)
        {
            attributeFound = true;
            break;
        }
    }
    free(data);
    if (!attributeFound)
    {
        cerr << "***** Finding table & attribute in index catalog failed.  *****" << endl;
        return -1;
    }

    // Try to create index again.  Should fail.
    rc = createIndexforLeftB();
    if (rc == success) {
        cerr << "***** Duplicate createIndexforLeftB() should fail.  *****" << endl;
        return rc;
    }

    // Try to create index on nonexistent table.  Should fail.
    rc = createIndexforNonexistentTable();
    if (rc == success) {
        cerr << "***** createIndexforNonexistentTable() should fail.  *****" << endl;
        return rc;
    }

    // Try to create index on existent table but nonexistent attribute.
    rc = createIndexforLeftNonexistentAttribute();
    if (rc == success) {
        cerr << "***** createIndexforLeftNonexistentAttribute() should fail.  *****" << endl;
        return rc;
    }

    return SUCCESS;
}


int main() {
    // Tables created: left
    // Indexes created: left.B, left.C

    // Initialize the system catalog
    if (deleteAndCreateCatalog() != success) {
        cerr << "***** deleteAndCreateCatalog() failed." << endl;
        cerr << "***** [FAIL] QE Test Create Index failed. *****" << endl;
        return fail;
    }

    
    // Create the left table
    if (createLeftTable() != success) {
        cerr << "***** createLeftTable() failed." << endl;
        cerr << "***** [FAIL] QE Test Create Index failed. *****" << endl;
        return fail;
    }

    if (TestCreateIndex() != success) {
        cerr << "***** [FAIL] QE Test Create Index failed. *****" << endl;
        return fail;
    } else {
        cerr << "***** QE Test Create Index finished. The result will be examined. *****" << endl;
        return success;
    }
}
