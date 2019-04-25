
#include "rm.h"

RelationManager *RelationManager::_rm = 0;

RelationManager *RelationManager::instance()
{
    if (!_rm)
    {
        _rm = new RelationManager();
        _rbfm = RecordBasedFileManager::instance();
    }
    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}
void RelationManager::addTableToCatalog(Table table, const vector<Attribute> &attrs)
{
    FileHandle catalogFile;
    _rbfm->openFile(tableCatalogName + fileSuffix, catalogFile);
    int offset = 0;

    unsigned char nullFields = 0;
    void *buffer = malloc(PAGE_SIZE);
    memcpy((char *)buffer + offset, &nullFields, 1);
    offset += 1;
    memcpy((char *)buffer + offset, &table.tableId, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    int tableNameSize = table.tableName.length();
    memcpy((char *)buffer + offset, &tableNameSize, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy((char *)buffer + offset, &table.tableName, tableNameSize);
    offset += tableNameSize;
    int fileNameSize = table.fileName.length();
    memcpy((char *)buffer + offset, &fileNameSize, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy((char *)buffer + offset, &table.fileName, fileNameSize);
    RID temp;
    _rbfm->insertRecord(catalogFile, tableCatalogAttributes, buffer, temp);

    addColumnsToCatalog(attrs, table.tableId);
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
    columnCatalogAttributes.empty();
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
    tableCatalogAttributes.empty();
    Attribute tableName = {.name = "table-name", .type = TypeVarChar, .length = 50};
    Attribute fileName = {.name = "file-name", .type = TypeVarChar, .length = 50};
    tableCatalogAttributes.push_back(tableId);
    tableCatalogAttributes.push_back(tableName);
    tableCatalogAttributes.push_back(fileName);

    //Create table objects
    Table tableTable;
    tableTable.tableId = 1;
    tableTable.tableName = tableCatalogName;
    tableTable.fileName = tableCatalogName + fileSuffix;
    Table columnTable;
    columnTable.tableId = 2;
    columnTable.tableName = columnCatalogName;
    columnTable.fileName = columnCatalogName + fileSuffix;

    addTableToCatalog(tableTable, tableCatalogAttributes);
    addTableToCatalog(columnTable, columnCatalogAttributes);

    return SUCCESS;
}

int RelationManager::getTableId(const string &tableName, RID &rid)
{

    RM_ScanIterator tableCatalogIterator;
    vector<string> attrList;
    attrList.push_back("table-id");
    scan(tableCatalogName, "table-name", CompOp::EQ_OP, &tableName, attrList, tableCatalogIterator);
    int tableId;
    if (tableCatalogIterator.getNextTuple(rid, (void *)&tableId) == RM_EOF)
    {
        tableCatalogIterator.close();
        return -1;
    }
    tableCatalogIterator.close();
    return tableId;
}

RC RelationManager::deleteCatalog()
{
    tableCatalogAttributes.empty();
    columnCatalogAttributes.empty();
    _rbfm->destroyFile(tableCatalogName + fileSuffix);
    _rbfm->destroyFile(columnCatalogName + fileSuffix);
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC result = _rbfm->createFile(tableName + fileSuffix);
    if (result != SUCCESS)
        return result; //propogate error

    //TODO: Get next tableId
    Table newTable;
    newTable.tableId = 0;
    newTable.tableName = tableName;
    newTable.fileName = tableName + fileSuffix;
    addTableToCatalog(newTable, attrs);

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RID rid;
    int tableId = getTableId(tableName, rid);
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
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{

    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}
