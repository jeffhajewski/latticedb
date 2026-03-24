#include "helpers.h"

void lattice_go_value_set_null(lattice_value* value) {
    memset(value, 0, sizeof(*value));
    value->type = LATTICE_VALUE_NULL;
}

void lattice_go_value_set_bool(lattice_value* value, bool bool_val) {
    memset(value, 0, sizeof(*value));
    value->type = LATTICE_VALUE_BOOL;
    value->data.bool_val = bool_val;
}

void lattice_go_value_set_int(lattice_value* value, int64_t int_val) {
    memset(value, 0, sizeof(*value));
    value->type = LATTICE_VALUE_INT;
    value->data.int_val = int_val;
}

void lattice_go_value_set_float(lattice_value* value, double float_val) {
    memset(value, 0, sizeof(*value));
    value->type = LATTICE_VALUE_FLOAT;
    value->data.float_val = float_val;
}

void lattice_go_value_set_string(lattice_value* value, const char* ptr, size_t len) {
    memset(value, 0, sizeof(*value));
    value->type = LATTICE_VALUE_STRING;
    value->data.string_val.ptr = ptr;
    value->data.string_val.len = len;
}

void lattice_go_value_set_bytes(lattice_value* value, const uint8_t* ptr, size_t len) {
    memset(value, 0, sizeof(*value));
    value->type = LATTICE_VALUE_BYTES;
    value->data.bytes_val.ptr = ptr;
    value->data.bytes_val.len = len;
}

void lattice_go_value_set_vector(lattice_value* value, const float* ptr, uint32_t dimensions) {
    memset(value, 0, sizeof(*value));
    value->type = LATTICE_VALUE_VECTOR;
    value->data.vector_val.ptr = ptr;
    value->data.vector_val.dimensions = dimensions;
}

void lattice_go_value_set_list(lattice_value* value, lattice_list* list_val) {
    memset(value, 0, sizeof(*value));
    value->type = LATTICE_VALUE_LIST;
    value->data.list_val = list_val;
}

void lattice_go_value_set_map(lattice_value* value, lattice_map* map_val) {
    memset(value, 0, sizeof(*value));
    value->type = LATTICE_VALUE_MAP;
    value->data.map_val = map_val;
}

lattice_value_type lattice_go_value_type(const lattice_value* value) {
    return value->type;
}

bool lattice_go_value_bool(const lattice_value* value) {
    return value->data.bool_val;
}

int64_t lattice_go_value_int(const lattice_value* value) {
    return value->data.int_val;
}

double lattice_go_value_float(const lattice_value* value) {
    return value->data.float_val;
}

const char* lattice_go_value_string_ptr(const lattice_value* value) {
    return value->data.string_val.ptr;
}

size_t lattice_go_value_string_len(const lattice_value* value) {
    return value->data.string_val.len;
}

const uint8_t* lattice_go_value_bytes_ptr(const lattice_value* value) {
    return value->data.bytes_val.ptr;
}

size_t lattice_go_value_bytes_len(const lattice_value* value) {
    return value->data.bytes_val.len;
}

const float* lattice_go_value_vector_ptr(const lattice_value* value) {
    return value->data.vector_val.ptr;
}

uint32_t lattice_go_value_vector_dimensions(const lattice_value* value) {
    return value->data.vector_val.dimensions;
}

lattice_list* lattice_go_value_list(const lattice_value* value) {
    return value->data.list_val;
}

lattice_map* lattice_go_value_map(const lattice_value* value) {
    return value->data.map_val;
}

lattice_value* lattice_go_list_items(lattice_list* list_val) {
    return list_val->items;
}

size_t lattice_go_list_len(lattice_list* list_val) {
    return list_val->len;
}

lattice_map_entry* lattice_go_map_entries(lattice_map* map_val) {
    return map_val->entries;
}

size_t lattice_go_map_len(lattice_map* map_val) {
    return map_val->len;
}

const char* lattice_go_map_entry_key(const lattice_map_entry* entry) {
    return entry->key;
}

size_t lattice_go_map_entry_key_len(const lattice_map_entry* entry) {
    return entry->key_len;
}

lattice_value* lattice_go_map_entry_value(lattice_map_entry* entry) {
    return &entry->value;
}
