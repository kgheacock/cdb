#include "rm.h"

RelationManager *RelationManager::_rm = 0;
RecordBasedFileManager *RelationManager::_rbfm = 0;

RelationManager *RelationManager::instance()
{
    if (!_rm)
    {
        _rm = new RelationManager();
        _rbfm = RecordBasedFileManager::instance();
        _rm->tableIndex = 0;
    }
    return _rm;
}

int RelationManager::getNextIndex()
{
    ++tableIndex;
    return tableIndex;
}

RelationManager::RelationManager()
{
    catalogCreated = -1;

    Attribute tableId_t = {.name = "table-id", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute tableName_t = {.name = "table-name", .type = TypeVarChar, .length = 50};
    Attribute fileName_t = {.name = "file-name", .type = TypeVarChar, .length = 50};
    tableCatalogAttributes = { tableId_t, tableName_t, fileName_t };

    Attribute tableId_c = {.name = "table-id", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute columnName_c = {.name = "column-name", .type = TypeVarChar, .length = 50};
    Attribute columnType_c = {.name = "column-type", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute columnLength_c = {.name = "column-length", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute columnPos_c = {.name = "column-position", .type = TypeInt, .length = sizeof(uint32_t)};
    columnCatalogAttributes = { tableId_c, columnName_c, columnType_c, columnLength_c, columnPos_c };

    tableTable = new Table(0, tableCatalogName, tableCatalogName + fileSuffix);
    columnTable = new Table(1, columnCatalogName, columnCatalogName + fileSuffix);
}

RelationManager::~RelationManager()
{
    delete tableTable;
    delete columnTable;
}
void RelationManager::addTableToCatalog(Table *table, const vector<Attribute> &attrs)
{
    FileHandle catalogFile;
    _rbfm->openFile(tableCatalogName + fileSuffix, catalogFile);
    int offset = 0;

    void *buffer = malloc(PAGE_SIZE);

    unsigned char nullFields = 0;
    memcpy((char *)buffer + offset, &nullFields, 1);
    offset += 1;

    // TableID
    uint32_t tableId = table->tableId;
    memcpy((char *)buffer + offset, &tableId, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // TableName size
    uint32_t tableNameSize = table->tableName.length();
    memcpy((char *)buffer + offset, &tableNameSize, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // TableName value
    const char *tableName = table->tableName.c_str();
    memcpy((char *)buffer + offset, tableName, tableNameSize);
    offset += tableNameSize;

    // FileName size
    uint32_t fileNameSize = table->fileName.length();
    memcpy((char *)buffer + offset, &fileNameSize, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // FileName value
    const char *fileName = table->fileName.c_str();
    memcpy((char *)buffer + offset, fileName, fileNameSize);

    RID temp;
    _rbfm->insertRecord(catalogFile, tableCatalogAttributes, buffer, temp);

    addColumnsToCatalog(attrs, tableId);
    free(buffer);
    _rbfm->closeFile(catalogFile);
}
bool RelationManager::catalogExists()
{
    if (catalogCreated == 0)
        return false;
    if (catalogCreated == 1)
        return true;
    FileHandle catalogFile;
    RC result = _rbfm->openFile(tableTable->fileName, catalogFile);
    if (result != SUCCESS)
    {
        catalogCreated = 0;
        return false;
    }
    _rbfm->closeFile(catalogFile);
    result = _rbfm->openFile(columnTable->fileName, catalogFile);
    if (result != SUCCESS)
    {
        catalogCreated = 0;
        return false;
    }
    _rbfm->closeFile(catalogFile);
    catalogCreated = 1;
    return true;
}
void RelationManager::addColumnsToCatalog(const vector<Attribute> &attrs, int tableId)
{
    FileHandle catalogFile;
    _rbfm->openFile(columnCatalogName + fileSuffix, catalogFile);

    int colPos = 1;
    for (Attribute attr : attrs)
    {
        void *buffer = malloc(PAGE_SIZE);
        int offset = 0;

        // Null fields.
        unsigned char nullFields = 0;
        memcpy((char *)buffer + offset, &nullFields, 1);
        offset += 1;

        // TableId
        memcpy((char *)buffer + offset, &tableId, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        // Column name length
        int fieldNameLength = attr.name.length();
        memcpy((char *)buffer + offset, &fieldNameLength, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        // Column name value
        const char *colName = attr.name.c_str();
        memcpy((char *)buffer + offset, colName, fieldNameLength);
        offset += fieldNameLength;

        // Column type
        memcpy((char *)buffer + offset, &attr.type, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        // Column length
        memcpy((char *)buffer + offset, &attr.length, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        // Column position
        memcpy((char *)buffer + offset, &colPos, sizeof(uint32_t));


        RID temp;
        _rbfm->insertRecord(catalogFile, columnCatalogAttributes, buffer, temp);
        colPos++;
        free(buffer);
    }
    _rbfm->closeFile(catalogFile);
}
RC RelationManager::createCatalog()
{
    //Check if Catalog files exist and if so return error, if not allocate
    RC result = _rbfm->createFile(tableCatalogName + fileSuffix);
    if (result != SUCCESS)
        return result; //propogate error
    result = _rbfm->createFile(columnCatalogName + fileSuffix);
    if (result != SUCCESS)
        return result; //propogate error

    //Create table objects
    tableTable = new Table(getNextIndex(), tableCatalogName, tableCatalogName + fileSuffix);
    columnTable = new Table(getNextIndex(), columnCatalogName, columnCatalogName + fileSuffix);

    addTableToCatalog(tableTable, tableCatalogAttributes);
    addTableToCatalog(columnTable, columnCatalogAttributes);
    catalogCreated = 1;
    return SUCCESS;
}

Table *RelationManager::getTableFromCatalog(const string &tableName, RID &rid)
{
    Table *returnTable = new Table();

    RBFM_ScanIterator tableCatalogIterator;
    vector<string> attrList;
    attrList.push_back("table-id");
    attrList.push_back("table-name");
    attrList.push_back("file-name");
    FileHandle tableCatalogFile;
    _rbfm->openFile(tableTable->fileName, tableCatalogFile);

    _rbfm->scan(tableCatalogFile, tableCatalogAttributes, "table-name", CompOp::EQ_OP, (void *) tableName.c_str(), attrList, tableCatalogIterator);
    void *data = malloc(PAGE_SIZE);
    auto rc = tableCatalogIterator.getNextRecord(rid, data);
    tableCatalogIterator.reset();
    if (rc == RBFM_EOF)
    {
        free(data);
        return nullptr;
    }

    int offset = 0;

    // Skip over initial null byte.
    offset += 1;

    // TableID
    int tableId = 0;
    memcpy(&tableId, (char *) data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // TableName size
    int tableNameSize = 0;
    memcpy(&tableNameSize, (char *) data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // TableName value
    offset += tableNameSize;

    // FileName size
    int sizeOfFileName = 0;
    memcpy(&sizeOfFileName, (char *)data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // FileName value
    char fileName[sizeOfFileName + 1];
    memcpy(fileName, (char *)data + offset, sizeOfFileName);
    offset += sizeOfFileName;
    fileName[sizeOfFileName] = '\0';


    returnTable->tableName = tableName;
    returnTable->fileName = fileName;
    returnTable->tableId = tableId;
    _rbfm->closeFile(tableCatalogFile);
    free(data);

    return returnTable;
}

RC RelationManager::deleteCatalog()
{
    RC result = _rbfm->destroyFile(tableCatalogName + fileSuffix);
    if (result != SUCCESS)
        return result; // Propogate error

    result = _rbfm->destroyFile(columnCatalogName + fileSuffix);
    if (result != SUCCESS)
        return result;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    if (!catalogExists())
        return CATALOG_DNE;
    RC result = _rbfm->createFile(tableName + fileSuffix);
    if (result != SUCCESS)
        return result;

    Table *newTable = new Table();
    newTable->tableId = getNextIndex();
    newTable->tableName = tableName;
    newTable->fileName = tableName + fileSuffix;
    addTableToCatalog(newTable, attrs);
    delete newTable;
    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    //TODO check to make sure column and table catalog aren't being deleted
    if (!catalogExists())
        return CATALOG_DNE;
    RID rid;
    Table *table = getTableFromCatalog(tableName, rid);
    if (table == nullptr)
        return -1;

    RC result = _rbfm->destroyFile(table->fileName);
    if (result != SUCCESS)
    {
        delete table;
        return result;
    }

    int tableId = table->tableId;
    if (tableId < 0)
        return -1;
    delete table;

    FileHandle tableCatalogFile;
    result = _rbfm->openFile(tableCatalogName + fileSuffix, tableCatalogFile);
    if (result != SUCCESS)
        return result;

    result = _rbfm->deleteRecord(tableCatalogFile, tableCatalogAttributes, rid);
    if (result != SUCCESS)
        return result;

    result = _rbfm->closeFile(tableCatalogFile);
    if (result != SUCCESS)
        return result;

    FileHandle columnCatalogFile;
    result = _rbfm->openFile(columnCatalogName + fileSuffix, columnCatalogFile);
    if (result != SUCCESS)
        return result;

    //We only care about RID so returned attributes isn't important
    RM_ScanIterator columnCatalogIterator;
    vector<string> returnAttrList;
    returnAttrList.push_back("column-position");
    result = scan(columnCatalogName, "table-id", CompOp::EQ_OP, &tableId, returnAttrList, columnCatalogIterator);
    if (result != SUCCESS && result != RBFM_EOF)
        return result;

    //int columnPosition;
    void *data = malloc(PAGE_SIZE);
    while (true)
    {
        result = columnCatalogIterator.getNextTuple(rid, data);
        if (result != SUCCESS && result != RBFM_EOF) // Some error.
        {
            free(data);
            columnCatalogIterator.close();
            return result;
        }

        if (result == RBFM_EOF) // Base case: no more attributes to delete.
        {
            free(data);
            columnCatalogIterator.close();
            return _rbfm->closeFile(columnCatalogFile);
        }

        auto deleted = _rbfm->deleteRecord(columnCatalogFile, columnCatalogAttributes, rid);
        if (deleted != SUCCESS)
        {
            free(data);
            columnCatalogIterator.close();
            return deleted;
        }
    }
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if (!catalogExists())
        return CATALOG_DNE;

    RID temp;
    Table *table = getTableFromCatalog(tableName, temp);
    if (table == nullptr)
    {
        return TABLE_DNE;
    }
    table->tableName = tableName;

    vector<string> columnAttributeNames;
    for (Attribute attr : columnCatalogAttributes)
    {
        columnAttributeNames.push_back(attr.name);
    }

    FileHandle columnCatalogFile;
    RC result = _rbfm->openFile(columnTable->fileName, columnCatalogFile);
    if (result != SUCCESS)
    {
        delete table;
        return result;
    }

    RBFM_ScanIterator rbfmi;
    int tableId = table->tableId;
    delete table;
    result = _rbfm->scan(columnCatalogFile, columnCatalogAttributes, "table-id", CompOp::EQ_OP, (void *) &tableId, columnAttributeNames, rbfmi);
    if (result != SUCCESS)
    {
        return result;
    }

    RID rid;
    void *data = malloc(PAGE_SIZE);
    while (true)
    {
        result = rbfmi.getNextRecord(rid, data); 
        if (result != SUCCESS && result != RBFM_EOF) // Error.
            return result;

        if (result == RBFM_EOF) // Base case: no more matching records.
        {
            _rbfm->closeFile(columnCatalogFile);
            free(data);
            if (attrs.empty())
                return -1;
            return SUCCESS;
        }

        Attribute toAdd;
        int offset = 0;

        // Skip null byte.
        offset += 1;

        // Skip table ID.
        offset += sizeof(uint32_t);

        // Column name length.
        int attrNameLength = 0;
        memcpy(&attrNameLength, (char *)data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        // Column name value.
        char attrName[attrNameLength + 1];
        memcpy(&attrName, (char *) data + offset, attrNameLength);
        offset += attrNameLength;
        attrName[attrNameLength] = '\0';
        toAdd.name = attrName;

        // Column type.
        int type = 0;
        memcpy(&type, (char *) data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        toAdd.type = (AttrType)type;

        // Column length.
        int length = 0;
        memcpy(&length, (char *) data + offset, sizeof(uint32_t));
        toAdd.length = length;
        attrs.push_back(toAdd);
    }
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    if (!catalogExists())
        return CATALOG_DNE;
    //TODO: if column catalog contains a dropped column, insert a null flag into the record before storing
    // Create a table object to check if the table exists and to get the file name
    RID temp;
    Table *table = getTableFromCatalog(tableName, temp);
    if (table == nullptr)
        return TABLE_DNE;

    FileHandle fileHandle;
    RC result = _rbfm->openFile(table->fileName, fileHandle);
    if (result != SUCCESS)
        return result;
    delete table;

    vector<Attribute> attributes;
    result = getAttributes(tableName, attributes);
    if (result != SUCCESS)
        return result;

    result = _rbfm->insertRecord(fileHandle, attributes, data, rid);
    return result;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{

    // Create a table object to check if the table exists and to get the file name
    if (!catalogExists())
        return CATALOG_DNE;
    RID temp;
    Table *table = getTableFromCatalog(tableName, temp);
    if (table == nullptr)
        return TABLE_DNE;

    FileHandle fileHandle;
    RC result = _rbfm->openFile(table->fileName, fileHandle);
    delete table;
    if (result != SUCCESS)
    {
        return result;
    }

    vector<Attribute> attributes;
    result = getAttributes(tableName, attributes);
    if (result != SUCCESS)
        return result;

    result = _rbfm->deleteRecord(fileHandle, attributes, rid);
    return result;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    //TODO: if column catalog contains a dropped column, insert a null flag into the record before storing
    // Create a table object to check if the table exists and to get the file name
    if (!catalogExists())
        return CATALOG_DNE;
    RID temp;
    Table *table = getTableFromCatalog(tableName, temp);
    if (table == nullptr)
        return TABLE_DNE;

    FileHandle fileHandle;
    RC result = _rbfm->openFile(table->fileName, fileHandle);
    delete table;
    if (result != SUCCESS)
        return result;

    vector<Attribute> attributes;
    result = getAttributes(tableName, attributes);
    if (result != SUCCESS)
        return result;

    result = _rbfm->updateRecord(fileHandle, attributes, data, rid);
    return result;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    //TODO: if there is a dropped column, it will show up as a nullField or DontCare value
    //take out that data before returning
    //TODO: if there are additional columns that don't appear in the record (because the column was added after the record was created)
    //set the null flag corrisponding to that position

    // Create a table object to check if the table exists and to get the file name
    RID temp;
    Table *table = getTableFromCatalog(tableName, temp);
    if (table == nullptr)
        return TABLE_DNE;

    FileHandle fileHandle;
    RC result = _rbfm->openFile(table->fileName, fileHandle);
    delete table;
    if (result != SUCCESS)
        return result;

    vector<Attribute> attributes;
    result = getAttributes(tableName, attributes);
    if (result != SUCCESS)
        return result;

    result = _rbfm->readRecord(fileHandle, attributes, rid, data);
    return result;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    //TODO: if there is a dropped column, it will show up as a nullField or DontCare value
    //take out that data before returning
    //TODO: if there are additional columns that don't appear in the record (because the column was added after the record was created)
    //set the null flag corrisponding to that position
    RC result = _rbfm->printRecord(attrs, data);
    return result;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RID temp;
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    Table *table = getTableFromCatalog(tableName, temp);
    if (table == nullptr)
        return TABLE_DNE;
    FileHandle tableFile;
    _rbfm->openFile(table->fileName, tableFile);
    delete table;
    RC result = _rbfm->readAttribute(tableFile, attrs, rid, attributeName, data);
    return result;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    RID temp;
    Table *tableToScan = getTableFromCatalog(tableName, temp);

    vector<Attribute> scannedTableAttributes;
    RC result = getAttributes(tableName, scannedTableAttributes);
    if (result != SUCCESS)
        return result;

    FileHandle tableToScanFile;
    _rbfm->openFile(tableToScan->fileName, tableToScanFile);
    delete tableToScan;

    RBFM_ScanIterator underlyingIterator;
    result = _rbfm->scan(tableToScanFile, scannedTableAttributes, conditionAttribute, compOp, value, attributeNames, underlyingIterator);
    if (result != SUCCESS)
        return result;
    rm_ScanIterator = RM_ScanIterator(underlyingIterator);
    return SUCCESS;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    RC result = underlyingIterator.getNextRecord(rid, data);
    if (result == RBFM_EOF)
        return RM_EOF;
    return result;
}

RC RM_ScanIterator::close()
{
    return underlyingIterator.close();
}

RC RM_ScanIterator::reset()
{
    return underlyingIterator.reset();
}

