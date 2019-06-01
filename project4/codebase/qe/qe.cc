
#include "qe.h"
#include <string.h>
Filter::Filter(Iterator *input, const Condition &condition)
{
}
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
        break;
    case GT_OP:
        return result == 1;
        break;
    case LE_OP:
        return result <= 0;
        break;
    case GE_OP:
        return result >= 0;
        break;
    case EQ_OP:
        return result == 0;
        break;
    }
    throw "Comparison Operator is invalid";
    return false;
}
// ... the rest of your implementations go here
