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
}

RelationManager::~RelationManager()
{
}
void RelationManager::addTableToCatalog(Table *table, const vector<Attribute> &attrs)
{
    FileHandle catalogFile;
    _rbfm->openFile(tableCatalogName + fileSuffix, catalogFile);
    int offset = 0;

    unsigned char nullFields = 0;
    void *buffer = malloc(PAGE_SIZE);
    memcpy((char *)buffer + offset, &nullFields, 1);
    offset += 1;
    memcpy((char *)buffer + offset, &table->tableId, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    int tableNameSize = table->tableName.length();
    memcpy((char *)buffer + offset, &tableNameSize, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy((char *)buffer + offset, &table->tableName, tableNameSize);
    offset += tableNameSize;
    int fileNameSize = table->fileName.length();
    memcpy((char *)buffer + offset, &fileNameSize, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy((char *)buffer + offset, &table->fileName, fileNameSize);
    RID temp;
    _rbfm->insertRecord(catalogFile, tableCatalogAttributes, buffer, temp);

    addColumnsToCatalog(attrs, table->tableId);
    free(buffer);
    _rbfm->closeFile(catalogFile);
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
        unsigned char nullFields = 0;
        memcpy((char *)buffer + offset, &nullFields, 1);
        offset += 1;
        memcpy((char *)buffer + offset, &tableId, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        int fieldNameLength = attr.name.length();
        memcpy((char *)buffer + offset, &fieldNameLength, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy((char *)buffer + offset, &attr.name, fieldNameLength);
        offset += fieldNameLength;
        memcpy((char *)buffer + offset, &attr.type, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        memcpy((char *)buffer + offset, &attr.length, sizeof(uint32_t));
        offset += sizeof(uint32_t);
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

    //Prepare column attribute list
    columnCatalogAttributes.clear();
    Attribute tableId = {.name = "table-id", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute columnName = {.name = "column-name", .type = TypeVarChar, .length = 50};
    Attribute columnType = {.name = "column-type", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute columnLength = {.name = "column-length", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute columnPos = {.name = "column-position", .type = TypeInt, .length = sizeof(uint32_t)};
    columnCatalogAttributes.push_back(tableId);
    columnCatalogAttributes.push_back(columnName);
    columnCatalogAttributes.push_back(columnType);
    columnCatalogAttributes.push_back(columnLength);
    columnCatalogAttributes.push_back(columnPos);
    //Prepare table attribute list
    tableCatalogAttributes.clear();
    Attribute tableName = {.name = "table-name", .type = TypeVarChar, .length = 50};
    Attribute fileName = {.name = "file-name", .type = TypeVarChar, .length = 50};
    tableCatalogAttributes.push_back(tableId);
    tableCatalogAttributes.push_back(tableName);
    tableCatalogAttributes.push_back(fileName);

    //Create table objects
    tableTable = new Table(getNextIndex(), tableCatalogName, tableCatalogName + fileSuffix);
    columnTable = new Table(getNextIndex(), columnCatalogName, columnCatalogName + fileSuffix);

    addTableToCatalog(tableTable, tableCatalogAttributes);
    addTableToCatalog(columnTable, columnCatalogAttributes);

    return SUCCESS;
}

Table *RelationManager::getTableFromCatalog(const string &tableName, RID &rid)
{
    Table *returnTable = new Table();

    RBFM_ScanIterator tableCatalogIterator;
    vector<string> attrList;
    attrList.push_back("table-id");
    attrList.push_back("file-name");
    FileHandle tableCatalogFile;
    _rbfm->openFile(tableTable->fileName, tableCatalogFile);
    _rbfm->scan(tableCatalogFile, tableCatalogAttributes, "table-name", CompOp::EQ_OP, &tableName, attrList, tableCatalogIterator);
    void *data = malloc(PAGE_SIZE);
    if (tableCatalogIterator.getNextRecord(rid, data) == RBFM_EOF)
    {
        tableCatalogIterator.close();
        return nullptr;
    }
    tableCatalogIterator.close();
    int tableId = 0;
    int offset = 0;
    int sizeOfFileName = 0;
    memcpy(&tableId, data, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy(&sizeOfFileName, (char *)data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    char fileName[sizeOfFileName + 1];
    memcpy(&fileName, (char *)data + offset, sizeOfFileName);
    fileName[sizeOfFileName] = '\0';
    returnTable->fileName = fileName;
    returnTable->tableId = tableId;
    returnTable->tableName = tableName;
    _rbfm->closeFile(tableCatalogFile);
    free(data);

    return returnTable;
}

RC RelationManager::deleteCatalog()
{
    tableCatalogAttributes.clear();
    columnCatalogAttributes.clear();
    _rbfm->destroyFile(tableCatalogName + fileSuffix);
    _rbfm->destroyFile(columnCatalogName + fileSuffix);
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC result = _rbfm->createFile(tableName + fileSuffix);
    if (result != SUCCESS)
        return result; //propogate error

    Table *newTable = new Table();
    newTable->tableId = getNextIndex();
    newTable->tableName = tableName;
    newTable->fileName = tableName + fileSuffix;
    addTableToCatalog(newTable, attrs);

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    //TODO check to make sure column and table catalog aren't being deleted
    RID rid;
    Table *table = getTableFromCatalog(tableName, rid);
    int tableId = table->tableId;
    if (tableId < 0)
        return -1;
    FileHandle tableCatalogFile;
    _rbfm->openFile(tableCatalogName + fileSuffix, tableCatalogFile);
    _rbfm->deleteRecord(tableCatalogFile, tableCatalogAttributes, rid);
    _rbfm->closeFile(tableCatalogFile);
    RM_ScanIterator columnCatalogIterator;
    FileHandle columnCatalogFile;
    _rbfm->openFile(columnCatalogName + fileSuffix, columnCatalogFile);
    //We only care about RID so returned attributes isn't important
    vector<string> returnAttrList;
    returnAttrList.push_back("column-position");
    scan(columnCatalogName, "table-id", CompOp::EQ_OP, &tableId, returnAttrList, columnCatalogIterator);
    int columnPosition;
    while (columnCatalogIterator.getNextTuple(rid, &columnPosition) != RM_EOF)
    {
        _rbfm->deleteRecord(columnCatalogFile, columnCatalogAttributes, rid);
    }
    _rbfm->closeFile(columnCatalogFile);

    return SUCCESS;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
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
    RBFM_ScanIterator rbfmi;
    FileHandle columnCatalogFile;
    _rbfm->openFile(columnTable->fileName, columnCatalogFile);
    _rbfm->scan(columnCatalogFile, columnCatalogAttributes, "table-id", CompOp::EQ_OP, &table->tableId, columnAttributeNames, rbfmi);
    RID rid;
    void *data = malloc(PAGE_SIZE);
    while (rbfmi.getNextRecord(rid, data) != RBFM_EOF)
    {
        //TODO: check if data is in correct format
        Attribute toAdd;
        int offset = 0;
        //skip table-id and
        offset += sizeof(uint32_t);
        int attrNameLength = 0;
        memcpy(&attrNameLength, (char *)data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        char attrName[attrNameLength];
        memcpy(&attrName, data, attrNameLength);
        offset += attrNameLength;
        toAdd.name = attrName;
        int type = 0;
        memcpy(&type, data, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        toAdd.type = (AttrType)type;
        int length = 0;
        memcpy(&length, data, sizeof(uint32_t));
        toAdd.length = length;
        attrs.push_back(toAdd);
    }
    _rbfm->closeFile(columnCatalogFile);
    free(data);
    if (attrs.empty())
        return -1;
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
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
    RID temp;
    Table *table = getTableFromCatalog(tableName, temp);
    if (table == nullptr)
        return TABLE_DNE;

    FileHandle fileHandle;
    RC result = _rbfm->openFile(table->fileName, fileHandle);
    if (result != SUCCESS)
        return result;

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
    RID temp;
    Table *table = getTableFromCatalog(tableName, temp);
    if (table == nullptr)
        return TABLE_DNE;

    FileHandle fileHandle;
    RC result = _rbfm->openFile(table->fileName, fileHandle);
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
    RBFM_ScanIterator underlyingIterator;
    result = _rbfm->scan(tableToScanFile, scannedTableAttributes, conditionAttribute, compOp, value, attributeNames, underlyingIterator);
    if (result != SUCCESS)
        return result;
    RM_ScanIterator *returnIterator = new RM_ScanIterator(underlyingIterator);
    rm_ScanIterator = *returnIterator;
    return SUCCESS;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    RC result = underlyingIterator.getNextRecord(rid, data);
    if (result == RBFM_EOF)
        return RM_EOF;
    return result;
}
