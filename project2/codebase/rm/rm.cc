#include "rm.h"
#include <sys/stat.h>

RelationManager *RelationManager::_rm = 0;
RecordBasedFileManager *RelationManager::_rbfm = 0;

RelationManager *RelationManager::instance()
{
    if (!_rm)
    {
        _rm = new RelationManager();
        _rbfm = RecordBasedFileManager::instance();
    }
    return _rm;
}

int RelationManager::getCurrentIndex()
{
    struct stat buf;
    bool fileExists = stat(_fIndexName.c_str(), &buf) == 0;
    bool mustCreateIndex = !fileExists;

    RC rc;
    int tableIndex;
    if (mustCreateIndex)
    {
        tableIndex = 2;
        _fIndex = fopen(_fIndexName.c_str(), "w+b");
        rc = writeTableIndex(tableIndex);
    }
    else
    {
        _fIndex = fopen(_fIndexName.c_str(), "r+b");
        rc = fread(&tableIndex, sizeof(tableIndex), 1, _fIndex) == 1 ? SUCCESS : -1;
    }

    if (rc != SUCCESS)
        return -1;

    bool isValidTableIndex = tableIndex >= 2;
    return isValidTableIndex ? tableIndex : -1;
}

RC RelationManager::writeTableIndex(int newIndex)
{
    RC rc;

    rc = fseek(_fIndex, 0, SEEK_SET);
    if (rc != 0)
        return -1;

    rc = fwrite(&newIndex, sizeof(newIndex), 1, _fIndex);
    if (rc != 1)
        return -1;

    rc = fflush(_fIndex);
    if (rc != 0)
        return -1;

    rc = fseek(_fIndex, 0, SEEK_SET);
    if (rc != 0)
        return -1;

    return SUCCESS;
}

int RelationManager::getNextIndex()
{
    ++_tableIndex;
    writeTableIndex(_tableIndex);
    return _tableIndex;
}

RelationManager::RelationManager()
{
    catalogCreated = -1;

    Attribute tableId_t = {.name = "table-id", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute tableName_t = {.name = "table-name", .type = TypeVarChar, .length = 50};
    Attribute fileName_t = {.name = "file-name", .type = TypeVarChar, .length = 50};
    tableCatalogAttributes = {tableId_t, tableName_t, fileName_t};

    Attribute tableId_c = {.name = "table-id", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute columnName_c = {.name = "column-name", .type = TypeVarChar, .length = 50};
    Attribute columnType_c = {.name = "column-type", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute columnLength_c = {.name = "column-length", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute columnPos_c = {.name = "column-position", .type = TypeInt, .length = sizeof(uint32_t)};
    Attribute isDropped_c = {.name = "is-dropped", .type = TypeInt, .length = sizeof(uint32_t)};
    columnCatalogAttributes = {tableId_c, columnName_c, columnType_c, columnLength_c, columnPos_c, isDropped_c};

    tableTable = new Table(0, tableCatalogName, tableCatalogName + fileSuffix);
    columnTable = new Table(1, columnCatalogName, columnCatalogName + fileSuffix);

    _tableIndex = getCurrentIndex();
}

RelationManager::~RelationManager()
{
}
int getNullIndicatorSize(int fieldCount)
{
    return int(ceil((double)fieldCount / CHAR_BIT));
}
bool fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}
void setFieldToNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    nullIndicator[indicatorIndex] |= indicatorMask;
}
unsigned int getRecordSize(const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char *)data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof(RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned)recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
            size += INT_SIZE;
            offset += INT_SIZE;
            break;
        case TypeReal:
            size += REAL_SIZE;
            offset += REAL_SIZE;
            break;
        case TypeVarChar:
            uint32_t varcharSize;
            // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
            memcpy(&varcharSize, (char *)data + offset, VARCHAR_LENGTH_SIZE);
            size += varcharSize;
            offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
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
        addColumnToCatalog(attr, tableId, colPos, catalogFile);
        colPos++;
    }
    _rbfm->closeFile(catalogFile);
}

// Assumes that the caller opens and manages the FileHandle to the catalog.
void RelationManager::addColumnToCatalog(const Attribute attr, const int tableId, const int columnPosition, FileHandle &columnCatalogFile)
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
    memcpy((char *)buffer + offset, &columnPosition, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // isDropped
    int isDropped = (int)false;
    memcpy((char *)buffer + offset, &isDropped, sizeof(uint32_t));

    RID temp;
    _rbfm->insertRecord(columnCatalogFile, columnCatalogAttributes, buffer, temp);
    free(buffer);
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

    _rbfm->scan(tableCatalogFile, tableCatalogAttributes, "table-name", CompOp::EQ_OP, (void *)tableName.c_str(), attrList, tableCatalogIterator);
    void *data = malloc(PAGE_SIZE);
    auto rc = tableCatalogIterator.getNextRecord(rid, data);
    tableCatalogIterator.reset();
    if (rc == RBFM_EOF)
    {
        free(data);
        _rbfm->closeFile(tableCatalogFile);
        delete returnTable;
        return nullptr;
    }

    int offset = 0;

    // Skip over initial null byte.
    offset += 1;

    // TableID
    int tableId = 0;
    memcpy(&tableId, (char *)data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // TableName size
    int tableNameSize = 0;
    memcpy(&tableNameSize, (char *)data + offset, sizeof(uint32_t));
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
    if (tableName.compare(tableCatalogName) == 0 || tableName.compare(columnCatalogName) == 0)
        return INVALID_PERMISSIONS;

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
            result = _rbfm->closeFile(columnCatalogFile);
            return result;
        }

        if (result == RBFM_EOF) // Base case: no more attributes to delete.
        {
            free(data);
            columnCatalogIterator.close();
            result = _rbfm->closeFile(columnCatalogFile);
            return result;
        }

        auto deleted = _rbfm->deleteRecord(columnCatalogFile, columnCatalogAttributes, rid);
        if (deleted != SUCCESS)
        {
            free(data);
            columnCatalogIterator.close();
            result = _rbfm->closeFile(columnCatalogFile);
            return deleted;
        }
    }
}
RC RelationManager::getAttributes(const string &tableName, vector<tuple<Attribute, bool>> &attrs)
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
    result = _rbfm->scan(columnCatalogFile, columnCatalogAttributes, "table-id", CompOp::EQ_OP, (void *)&tableId, columnAttributeNames, rbfmi);
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
        {
            _rbfm->closeFile(columnCatalogFile);
            return result;
        }

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
        memcpy(&attrName, (char *)data + offset, attrNameLength);
        offset += attrNameLength;
        attrName[attrNameLength] = '\0';
        toAdd.name = attrName;

        // Column type.
        int type = 0;
        memcpy(&type, (char *)data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        toAdd.type = (AttrType)type;

        // Column length.
        int length = 0;
        memcpy(&length, (char *)data + offset, sizeof(uint32_t));
        toAdd.length = length;

        int isDropped = 0;
        memcpy(&isDropped, (char *)data + offset, sizeof(uint32_t));
        attrs.push_back({toAdd, isDropped == true});
    }
}
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    vector<tuple<Attribute, bool>> tempVector;
    RC result = getAttributes(tableName, tempVector);
    for (auto attr : tempVector)
    {
        attrs.push_back(std::get<0>(attr));
    }
    return result;
}

RC RelationManager::unPadNulls(void *out, const void *data, const vector<tuple<Attribute, bool>> attrs)
{
    int nullCount = 0;
    vector<Attribute> strippedAttrs;
    for (auto attr : attrs)
    {
        bool isNull = std::get<1>(attr);
        strippedAttrs.push_back(std::get<0>(attr));
        nullCount += isNull;
    }
    int oldNullIndicatorSize = getNullIndicatorSize(attrs.size());
    int newNullIndicatorSize = getNullIndicatorSize(attrs.size() - nullCount);
    unsigned int oldRecordSize = getRecordSize(strippedAttrs, data);
    char *newNullIndicator = (char *)malloc(newNullIndicatorSize);
    char *oldNullIndicator = (char *)malloc(oldNullIndicatorSize);
    memcpy(oldNullIndicator, data, oldNullIndicatorSize);
    int newNullOffset = 0;
    int oldNullOffset = 0;
    for (auto attr : attrs)
    {
        bool isDropped = std::get<1>(attr);
        if (isDropped)
        {
            ++oldNullOffset;
            continue;
        }
        if (fieldIsNull(oldNullIndicator, oldNullOffset))
        {
            setFieldToNull(newNullIndicator, newNullOffset);
        }
        ++newNullOffset;
        ++oldNullOffset;
    }
    memcpy(out, newNullIndicator, newNullIndicatorSize);
    memcpy((char *)out + newNullIndicatorSize, (char *)data + oldNullIndicatorSize, oldRecordSize - newNullIndicatorSize);
    free(newNullIndicator);
    free(oldNullIndicator);
    return SUCCESS;
}
RC RelationManager::padNulls(void *out, const void *data, const vector<tuple<Attribute, bool>> attrs)
{
    int nullCount = 0;
    vector<Attribute> strippedAttrs;
    for (auto attr : attrs)
    {
        bool isNull = std::get<1>(attr);
        if (!isNull)
        {
            strippedAttrs.push_back(std::get<0>(attr));
        }
        nullCount += isNull;
    }
    int oldNullIndicatorSize = getNullIndicatorSize(attrs.size() - nullCount);
    int newNullIndicatorSize = getNullIndicatorSize(attrs.size());
    char *newNullIndicator = (char *)malloc(newNullIndicatorSize);
    char *oldNullIndicator = (char *)malloc(oldNullIndicatorSize);
    memcpy(oldNullIndicator, data, oldNullIndicatorSize);
    int newNullOffset = 0;
    int oldNullOffset = 0;
    for (auto attr : attrs)
    {
        bool isDropped = std::get<1>(attr);
        if (isDropped)
        {
            setFieldToNull(newNullIndicator, newNullOffset);
            ++newNullOffset;
            continue;
        }
        if (fieldIsNull(oldNullIndicator, oldNullOffset))
        {
            setFieldToNull(newNullIndicator, newNullOffset);
        }
        ++newNullOffset;
        ++oldNullOffset;
    }
    unsigned int oldRecordSize = getRecordSize(strippedAttrs, data);
    memcpy(out, newNullIndicator, newNullIndicatorSize);
    memcpy((char *)out + newNullIndicatorSize, (char *)data + oldNullIndicatorSize, oldRecordSize - oldNullIndicatorSize);
    free(oldNullIndicator);
    free(newNullIndicator);
    return SUCCESS;
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
    string fileName = table->fileName;
    delete table;

    FileHandle fileHandle;
    RC result = _rbfm->openFile(fileName, fileHandle);
    if (result != SUCCESS)
        return result;

    vector<Attribute> attributes;
    vector<tuple<Attribute, bool>> attributesAndDropped;
    void *updatedData = malloc(PAGE_SIZE);
    result = getAttributes(tableName, attributesAndDropped);
    for (auto attr : attributesAndDropped)
    {
        attributes.push_back(std::get<0>(attr));
    }
    if (result != SUCCESS)
    {
        _rbfm->closeFile(fileHandle);
        return result;
    }
    padNulls(updatedData, data, attributesAndDropped);
    result = _rbfm->insertRecord(fileHandle, attributes, updatedData, rid);
    free(updatedData);
    if (result != SUCCESS)
    {
        _rbfm->closeFile(fileHandle);
        return result;
    }

    result = _rbfm->closeFile(fileHandle);
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
        return result;

    vector<Attribute> attributes;
    result = getAttributes(tableName, attributes);
    if (result != SUCCESS)
    {
        _rbfm->closeFile(fileHandle);
        return result;
    }

    result = _rbfm->deleteRecord(fileHandle, attributes, rid);
    if (result != SUCCESS)
    {
        _rbfm->closeFile(fileHandle);
        return result;
    }

    result = _rbfm->closeFile(fileHandle);
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
    {
        _rbfm->closeFile(fileHandle);
        return result;
    }

    result = _rbfm->updateRecord(fileHandle, attributes, data, rid);
    if (result != SUCCESS)
    {
        _rbfm->closeFile(fileHandle);
        return result;
    }

    result = _rbfm->closeFile(fileHandle);
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
    string fileName = table->fileName;
    delete table;

    FileHandle fileHandle;
    RC result = _rbfm->openFile(fileName, fileHandle);
    if (result != SUCCESS)
        return result;

    vector<Attribute> attributes;
    result = getAttributes(tableName, attributes);
    if (result != SUCCESS)
    {
        _rbfm->closeFile(fileHandle);
        return result;
    }

    result = _rbfm->readRecord(fileHandle, attributes, rid, data);
    if (result != SUCCESS)
    {
        _rbfm->closeFile(fileHandle);
        return result;
    }
    result = _rbfm->closeFile(fileHandle);
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
    RC result = _rbfm->openFile(table->fileName, tableFile);
    if (result != SUCCESS)
        return result;

    delete table;

    result = _rbfm->readAttribute(tableFile, attrs, rid, attributeName, data);
    if (result != SUCCESS)
    {
        _rbfm->closeFile(tableFile);
        return result;
    }

    result = _rbfm->closeFile(tableFile);
    return result;
}
RC RelationManager::deleteAttribute(const string &tableName, const string &attributeName)
{
    FileHandle columnCatalog;
    RC result = _rbfm->openFile(columnCatalogName + fileSuffix, columnCatalog);
    vector<tuple<Attribute, bool>> attributesWithDroppedIndicator;
    vector<Attribute> attributes;
    result = getAttributes(tableName, attributesWithDroppedIndicator);
    for (auto attr : attributesWithDroppedIndicator)
    {
        attributes.push_back(std::get<0>(attr));
    }
    RID temp;
    Table *tableToUpdate = getTableFromCatalog(tableName, temp);
    if (result != SUCCESS)
    {
        delete tableToUpdate;
        return result;
    }

    RBFM_ScanIterator rbfmi;
    int tableId = tableToUpdate->tableId;
    delete tableToUpdate;
    _rbfm->openFile(columnCatalogName + fileSuffix, columnCatalog);
    vector<string> retAttr = {"column-name"};
    FileHandle columnCatalogFile;
    _rbfm->openFile(columnTable->fileName, columnCatalogFile);
    result = _rbfm->scan(columnCatalogFile, columnCatalogAttributes, "table-id", CompOp::EQ_OP, (void *)&tableId, retAttr, rbfmi);
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
        {
            _rbfm->closeFile(columnCatalogFile);
            return result;
        }

        if (result == RBFM_EOF) // Base case: no more matching records.
        {
            _rbfm->closeFile(columnCatalogFile);
            free(data);
            return SUCCESS;
        }
        int colNameLength = 0;
        int offset = 1;
        memcpy(&colNameLength, (char *)data + 1, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        char *colName = (char *)malloc(colNameLength);
        string colNameString = string(colName);
        free(colName);
        if (colNameString.compare(attributeName) == 0)
        {
            int recordSize = getRecordSize(columnCatalogAttributes, data);
            int dropped = (int)false;
            memcpy((char *)data + recordSize - sizeof(uint32_t), &dropped, sizeof(uint32_t));
            updateTuple(columnCatalogName, data, rid);
        }
    }

    _rbfm->closeFile(columnCatalog);
    return result;
}
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    RID temp;
    Table *table = getTableFromCatalog(tableName, temp);
    if (table == nullptr)
        return -1;
    const int tableId = table->tableId;
    delete table;

    vector<Attribute> existingAttrs;
    RC result = getAttributes(tableName, existingAttrs);
    if (result != SUCCESS)
        return result;
    const int nextColumnPosition = existingAttrs.size() + 1;

    FileHandle columnCatalogFile;
    result = _rbfm->openFile(columnCatalogName + fileSuffix, columnCatalogFile);
    if (result != SUCCESS)
        return result;

    addColumnToCatalog(attr, tableId, nextColumnPosition, columnCatalogFile);

    result = _rbfm->closeFile(columnCatalogFile);
    if (result != SUCCESS)
        return result;

    return SUCCESS;
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

    vector<tuple<Attribute, bool>> scannedTableAttributesWithDropped;
    vector<Attribute> scannedTableAttributes;
    RC result = getAttributes(tableName, scannedTableAttributesWithDropped);
    for (auto attr : scannedTableAttributesWithDropped)
    {
        scannedTableAttributes.push_back(std::get<0>(attr));
    }
    if (result != SUCCESS)
        return result;

    FileHandle tableToScanFile;
    result = _rbfm->openFile(tableToScan->fileName, tableToScanFile);
    if (result != SUCCESS)
        return result;

    delete tableToScan;

    RBFM_ScanIterator underlyingIterator;
    result = _rbfm->scan(tableToScanFile, scannedTableAttributes, conditionAttribute, compOp, value, attributeNames, underlyingIterator);
    if (result != SUCCESS)
        return result;

    rm_ScanIterator = RM_ScanIterator(underlyingIterator);
    return result;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    void *temp = malloc(PAGE_SIZE);
    RC result = underlyingIterator.getNextRecord(rid, temp);
    RelationManager::unPadNulls(data, temp, attrsWithDropped);
    free(temp);
    if (result == RBFM_EOF)
        return RM_EOF;
    return result;
}

RC RM_ScanIterator::close()
{
    RC result = underlyingIterator.rbfm_->closeFile(underlyingIterator.fileHandle_);
    if (result != SUCCESS)
        return result;
    return underlyingIterator.close();
}

RC RM_ScanIterator::reset()
{
    return underlyingIterator.reset();
}
