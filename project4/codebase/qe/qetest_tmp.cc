#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe_test_util.h"


RC testCase_tmp() {
	// Mandatory for all
	// Filter -- TableScan as input, on an Integer Attribute
	// SELECT * FROM LEFT WHERE B <= 30
	cerr << endl << "***** In QE Test Case TMP *****" << endl;
	RC rc;

    cout << "Table scan on table 'right'" << endl;
    TableScan *tsRight = new TableScan(*rm, "right");
    vector<Attribute> attrs;
    tsRight->getAttributes(attrs);
    void *data = malloc(PAGE_SIZE);
	while ((rc = tsRight->getNextTuple(data)) == SUCCESS) {
        rm->printTuple(attrs, data);
	}
    return rc;
}


int main() {
	// Tables created: none
	// Indexes created: none

	if (testCase_tmp() != success) {
		cerr << "***** [FAIL] QE Test Case TMP failed. *****" << endl;
		return fail;
	} else {
		cerr << "***** QE Test Case TMP finished. The result will be examined. *****" << endl;
		return success;
	}
}
