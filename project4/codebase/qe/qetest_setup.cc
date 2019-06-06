#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe_test_util.h"
int main()
{
    deleteAndCreateCatalog();
    // Mandatory for all
    // Filter -- TableScan as input, on an Integer Attribute
    // SELECT * FROM LEFT WHERE B <= 30
    cerr << endl
         << "***** In QE Test Case TMP *****" << endl;
    RC rc;
    createLeftVarCharTable();
    rc = rm->createIndex("leftvarchar", "B");
    populateLeftVarCharTable();
    createRightVarCharTable();
    rc = rm->createIndex("rightvarchar", "B");
    populateRightVarCharTable();
    cout << "Table scan on table 'largeleft'" << endl;
    return rc;
}