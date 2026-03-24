#ifndef LATTICE_GO_HELPERS_H
#define LATTICE_GO_HELPERS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "lattice.h"

void lattice_go_value_set_null(lattice_value* value);
void lattice_go_value_set_bool(lattice_value* value, bool bool_val);
void lattice_go_value_set_int(lattice_value* value, int64_t int_val);
void lattice_go_value_set_float(lattice_value* value, double float_val);
void lattice_go_value_set_string(lattice_value* value, const char* ptr, size_t len);
void lattice_go_value_set_bytes(lattice_value* value, const uint8_t* ptr, size_t len);
void lattice_go_value_set_vector(lattice_value* value, const float* ptr, uint32_t dimensions);
void lattice_go_value_set_list(lattice_value* value, lattice_list* list_val);
void lattice_go_value_set_map(lattice_value* value, lattice_map* map_val);

lattice_value_type lattice_go_value_type(const lattice_value* value);
bool lattice_go_value_bool(const lattice_value* value);
int64_t lattice_go_value_int(const lattice_value* value);
double lattice_go_value_float(const lattice_value* value);
const char* lattice_go_value_string_ptr(const lattice_value* value);
size_t lattice_go_value_string_len(const lattice_value* value);
const uint8_t* lattice_go_value_bytes_ptr(const lattice_value* value);
size_t lattice_go_value_bytes_len(const lattice_value* value);
const float* lattice_go_value_vector_ptr(const lattice_value* value);
uint32_t lattice_go_value_vector_dimensions(const lattice_value* value);
lattice_list* lattice_go_value_list(const lattice_value* value);
lattice_map* lattice_go_value_map(const lattice_value* value);

lattice_value* lattice_go_list_items(lattice_list* list_val);
size_t lattice_go_list_len(lattice_list* list_val);
lattice_map_entry* lattice_go_map_entries(lattice_map* map_val);
size_t lattice_go_map_len(lattice_map* map_val);
const char* lattice_go_map_entry_key(const lattice_map_entry* entry);
size_t lattice_go_map_entry_key_len(const lattice_map_entry* entry);
lattice_value* lattice_go_map_entry_value(lattice_map_entry* entry);

#endif
