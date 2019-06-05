
#include "qe.h"
#include <string.h>
#include <algorithm>
#include <unordered_map>

int Value::compare(const int key, const int value)
{
    if (key == value)
        return 0;
    if (key > value)
        return 1;
    if (key < value)
        return -1;
    return 0; // suppress warnings
}

int Value::compare(const float key, const float value)
{
    if (key == value)
        return 0;
    if (key > value)
        return 1;
    if (key < value)
        return -1;
    return 0;
}
int Value::compare(const char *key, const char *value)
{
    return strcmp(key, value);
}
int Value::compare(const void *key, const void *value, const AttrType attrType)
{
    switch (attrType)
    {
    case TypeInt:
    {
        int key_int = *(int *)key;
        int value_int = *(int *)value;
        return compare(key_int, value_int);
    }
    case TypeReal:
    {
        float key_float = *(float *)key;
        float value_float = *(float *)value;
        return compare(key_float, value_float);
    }
    case TypeVarChar:
    {
        size_t key_size;
        size_t value_size;
        memcpy(&key_size, key, sizeof(uint32_t));
        memcpy(&value_size, value, sizeof(uint32_t));
        char key_array[key_size + 1];
        char value_array[value_size + 1];
        memcpy(key_array, (char *)key + sizeof(uint32_t), key_size);
        memcpy(value_array, (char *)value + sizeof(uint32_t), value_size);
        key_array[key_size] = '\0';
        value_array[value_size] = '\0';
        return compare(key_array, value_array);
    }
    }
    throw "Attribute is malformed";
    return -2;
}
int Value::compare(const Value *rhs)
{
    if (rhs->type != type)
    {
        throw "Mismatched attributes";
    }
    return compare(data, rhs->data, type);
}
bool Value::compare(const Value *rhs, const CompOp op)
{
    int result = compare(rhs);
    switch (op)
    {
    case LT_OP:
        return result == -1;
    case GT_OP:
        return result == 1;
    case LE_OP:
        return result <= 0;
    case GE_OP:
        return result >= 0;
    case EQ_OP:
        return result == 0;
    case NE_OP:
        return result != 0;
    case NO_OP:
        return true;
    default:
        throw "Comparison Operator is invalid";
        return false;
    }
}
// ... the rest of your implementations go here
RC Filter::getNextTuple(void *data)
{
    vector<Attribute> attrs;
    iter_->getAttributes(attrs);

    void *iter_tuple = calloc(PAGE_SIZE, sizeof(uint8_t));
    if (iter_tuple == nullptr)
        return -1;

    while (iter_->getNextTuple(iter_tuple) != QE_EOF) {

        /* For simplified Filter, we only compare with:
         *   - the tuple we just got, or
         *   - some predefined value in condition.
         */
        bool result;
        auto rc = evalPredicate(result, iter_tuple, cond_, iter_tuple, attrs, attrs);
        if (rc != SUCCESS)
        {
            free(iter_tuple);
            return rc;
        }

        if (result)
        {
            RelationManager *rm = RelationManager::instance();
            memcpy(data, iter_tuple, rm->getTupleSize(attrs, iter_tuple));
            free(iter_tuple);
            return SUCCESS;
        }
    }

    free(iter_tuple);
    return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
    iter_->getAttributes(attrs);
}

RC Project::getNextTuple(void *data)
{
    RC rc;

    void *dataBefore = malloc(PAGE_SIZE);
    rc = iter_->getNextTuple(dataBefore);
    if (rc != SUCCESS)
    {
        free(dataBefore);
        return rc;
    }

    int dataBefore_NullIndSize = RecordBasedFileManager::getNullIndicatorSize(attrsBeforeProjection_.size());
    void *dataBefore_NullInd = dataBefore;
    uint32_t dataBefore_Offset = dataBefore_NullIndSize;

    unordered_map<string, AttrData> projectedAttrData;

    int i = 0;
    for (auto attr : attrsBeforeProjection_)
    {
        uint32_t attrSize;
        if (RecordBasedFileManager::fieldIsNull((char *) dataBefore_NullInd, i))
        {
            attrSize = 0;
        }
        else
        {
            // Get size of attribute.
            int varCharLength;
            switch (attr.type)
            {
                case TypeInt:
                case TypeReal:
                    attrSize = sizeof(uint32_t);
                    break;
                case TypeVarChar:
                    memcpy(&varCharLength, (char *) dataBefore + dataBefore_Offset, sizeof(uint32_t));
                    attrSize = sizeof(uint32_t) + varCharLength; // Word of length + length itself.
                    break;
                default:
                    free(dataBefore);
                    for (auto it = projectedAttrData.begin(); it != projectedAttrData.end(); it++)
                    {
                        AttrData ad = it->second;
                        if (!ad.isNull)
                            free(ad.data);
                    }
                    return QE_NO_SUCH_ATTR_TYPE;
            }
        }

        auto isProjectedAttr = [attr](string aname) { return aname == attr.name; };
        if (find_if(attrNames_.begin(), attrNames_.end(), isProjectedAttr) != attrNames_.end())
        {
            AttrData ad;
            ad.attr = attr;
            ad.size = attrSize;
            ad.isNull = attrSize == 0;
            if (!ad.isNull)
            {
                ad.data = calloc(ad.size, sizeof(uint8_t));
                memcpy(ad.data, (char *) dataBefore + dataBefore_Offset, ad.size);
            }

            projectedAttrData[attr.name] = ad;
        }

        dataBefore_Offset += attrSize;
        i++;
    }
    free(dataBefore);

    int dataAfter_NullIndSize = RecordBasedFileManager::getNullIndicatorSize(attrNames_.size());
    uint32_t dataAfter_Offset = dataAfter_NullIndSize; // Start copying data after where the null indicator will be.

    int j = 0;
    for (auto attrName : attrNames_) // Go through in projected order and copy into new tuple.
    {
        AttrData pa = projectedAttrData[attrName];
        if (pa.isNull)
        {
            int indicatorIndex = j / CHAR_BIT;
            char indicatorMask = 1 << (CHAR_BIT - 1 - (j % CHAR_BIT));
            ((char *) data)[indicatorIndex] |= indicatorMask;
        }
        else
        {
            memcpy((char *) data + dataAfter_Offset, pa.data, pa.size);
            dataAfter_Offset += pa.size;
            free(pa.data);
        }
        j++;
    }

    return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
    attrs = attrs_;
}

RC evalPredicate(bool &result,
                 const void *leftTuple, const Condition condition, const void *rightTuple,
                 const vector<Attribute> leftAttrs,
                 const vector<Attribute> rightAttrs)
{
    RC rc;

    // Get leftValue.
    auto matchingAttrName_left = [condition](Attribute a) { return a.name == condition.lhsAttr; };
    auto iterPos_left = find_if(leftAttrs.begin(), leftAttrs.end(), matchingAttrName_left);
    unsigned index_left = distance(leftAttrs.begin(), iterPos_left);
    if (index_left == leftAttrs.size())
        return QE_NO_SUCH_ATTR;
    const Attribute leftAttr = leftAttrs[index_left];

    void *leftKey;
    rc = RecordBasedFileManager::getColumnFromTuple(leftTuple, leftAttrs, leftAttr, leftKey);
    if (rc != SUCCESS)
        return rc;

    Value leftValue = { leftAttr.type, leftKey };

    // Get rightValue.
    Value rightValue;
    void *rightKey = nullptr;
    if (condition.bRhsIsAttr)
    {
        auto matchingAttrName_right = [condition](Attribute a) { return a.name == condition.rhsAttr; };
        auto iterPos_right = find_if(rightAttrs.begin(), rightAttrs.end(), matchingAttrName_right);
        unsigned index_right = distance(rightAttrs.begin(), iterPos_right);
        if (index_right == rightAttrs.size())
            return QE_NO_SUCH_ATTR;
        const Attribute rightAttr = rightAttrs[index_right];

        rc = RecordBasedFileManager::getColumnFromTuple(rightTuple, rightAttrs, rightAttr, rightKey);
        if (rc != SUCCESS)
        {
            free(leftKey);
            return rc;
        }

        rightValue = { rightAttr.type, rightKey };
    }
    else
    {
        rightValue = condition.rhsValue;
    }
    
    result = leftValue.compare(&rightValue, condition.op);

    free(leftKey);
    if (rightKey != nullptr)
        free(rightKey);

    return SUCCESS;
}

