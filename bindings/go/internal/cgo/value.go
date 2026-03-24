package cgobridge

/*
#include "helpers.h"
*/
import "C"

import (
	"fmt"
	"math"
	"reflect"
	"unsafe"
)

type cAlloc struct {
	ptrs []unsafe.Pointer
}

func (a *cAlloc) malloc(size uintptr) (unsafe.Pointer, error) {
	if size == 0 {
		return nil, nil
	}
	ptr := C.malloc(C.size_t(size))
	if ptr == nil {
		return nil, &Error{Code: ErrorOutOfMemory, Message: "out of memory"}
	}
	a.ptrs = append(a.ptrs, ptr)
	return ptr, nil
}

func (a *cAlloc) bytes(data []byte) (unsafe.Pointer, error) {
	if len(data) == 0 {
		return nil, nil
	}
	ptr := C.CBytes(data)
	if ptr == nil {
		return nil, &Error{Code: ErrorOutOfMemory, Message: "out of memory"}
	}
	a.ptrs = append(a.ptrs, ptr)
	return ptr, nil
}

func (a *cAlloc) freeAll() {
	for i := len(a.ptrs) - 1; i >= 0; i-- {
		C.free(a.ptrs[i])
	}
	a.ptrs = nil
}

func encodeValue(value any) (*C.lattice_value, func(), error) {
	normalized, err := normalizeValue(value)
	if err != nil {
		return nil, nil, err
	}

	alloc := &cAlloc{}
	rootPtr, err := alloc.malloc(C.sizeof_struct_lattice_value)
	if err != nil {
		return nil, nil, err
	}
	root := (*C.lattice_value)(rootPtr)

	if err := fillValue(root, normalized, alloc); err != nil {
		alloc.freeAll()
		return nil, nil, err
	}

	return root, alloc.freeAll, nil
}

func fillValue(dst *C.lattice_value, value any, alloc *cAlloc) error {
	switch v := value.(type) {
	case nil:
		C.lattice_go_value_set_null(dst)
		return nil
	case bool:
		C.lattice_go_value_set_bool(dst, C.bool(v))
		return nil
	case int64:
		C.lattice_go_value_set_int(dst, C.int64_t(v))
		return nil
	case float64:
		C.lattice_go_value_set_float(dst, C.double(v))
		return nil
	case string:
		ptr, err := alloc.bytes([]byte(v))
		if err != nil {
			return err
		}
		C.lattice_go_value_set_string(dst, (*C.char)(ptr), C.size_t(len(v)))
		return nil
	case []byte:
		ptr, err := alloc.bytes(v)
		if err != nil {
			return err
		}
		C.lattice_go_value_set_bytes(dst, (*C.uint8_t)(ptr), C.size_t(len(v)))
		return nil
	case []float32:
		var ptr unsafe.Pointer
		var err error
		if len(v) > 0 {
			ptr, err = alloc.malloc(uintptr(len(v)) * unsafe.Sizeof(float32(0)))
			if err != nil {
				return err
			}
			copy(unsafe.Slice((*float32)(ptr), len(v)), v)
		}
		C.lattice_go_value_set_vector(dst, (*C.float)(ptr), C.uint32_t(len(v)))
		return nil
	case []any:
		listPtr, err := alloc.malloc(C.sizeof_struct_lattice_list)
		if err != nil {
			return err
		}
		list := (*C.lattice_list)(listPtr)
		if len(v) > 0 {
			itemsPtr, err := alloc.malloc(uintptr(len(v)) * C.sizeof_struct_lattice_value)
			if err != nil {
				return err
			}
			items := unsafe.Slice((*C.lattice_value)(itemsPtr), len(v))
			for i, item := range v {
				if err := fillValue(&items[i], item, alloc); err != nil {
					return err
				}
			}
			list.items = (*C.lattice_value)(itemsPtr)
		}
		list.len = C.size_t(len(v))
		C.lattice_go_value_set_list(dst, list)
		return nil
	case map[string]any:
		mapPtr, err := alloc.malloc(C.sizeof_struct_lattice_map)
		if err != nil {
			return err
		}
		cMap := (*C.lattice_map)(mapPtr)
		if len(v) > 0 {
			entriesPtr, err := alloc.malloc(uintptr(len(v)) * C.sizeof_struct_lattice_map_entry)
			if err != nil {
				return err
			}
			entries := unsafe.Slice((*C.lattice_map_entry)(entriesPtr), len(v))
			i := 0
			for key, item := range v {
				keyPtr, err := alloc.bytes([]byte(key))
				if err != nil {
					return err
				}
				entries[i].key = (*C.char)(keyPtr)
				entries[i].key_len = C.size_t(len(key))
				if err := fillValue(C.lattice_go_map_entry_value(&entries[i]), item, alloc); err != nil {
					return err
				}
				i++
			}
			cMap.entries = (*C.lattice_map_entry)(entriesPtr)
		}
		cMap.len = C.size_t(len(v))
		C.lattice_go_value_set_map(dst, cMap)
		return nil
	default:
		return fmt.Errorf("unsupported property value type %T", value)
	}
}

func normalizeValue(value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return v, nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return nil, fmt.Errorf("integer value %d overflows int64", v)
		}
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("integer value %d overflows int64", v)
		}
		return int64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return v, nil
	case []byte:
		return append([]byte(nil), v...), nil
	case []float32:
		return append([]float32(nil), v...), nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			normalized, err := normalizeValue(item)
			if err != nil {
				return nil, err
			}
			out[i] = normalized
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			normalized, err := normalizeValue(item)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, nil
	}

	for rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
	}

	switch rv.Kind() {
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := rv.Uint()
		if u > math.MaxInt64 {
			return nil, fmt.Errorf("integer value %d overflows int64", u)
		}
		return int64(u), nil
	case reflect.Float32, reflect.Float64:
		return rv.Convert(reflect.TypeOf(float64(0))).Float(), nil
	case reflect.String:
		return rv.String(), nil
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			out := make([]byte, rv.Len())
			reflect.Copy(reflect.ValueOf(out), rv)
			return out, nil
		}
		if rv.Type().Elem().Kind() == reflect.Float32 {
			out := make([]float32, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				out[i] = float32(rv.Index(i).Float())
			}
			return out, nil
		}
		fallthrough
	case reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			out := make([]byte, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				out[i] = byte(rv.Index(i).Uint())
			}
			return out, nil
		}
		if rv.Type().Elem().Kind() == reflect.Float32 {
			out := make([]float32, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				out[i] = float32(rv.Index(i).Float())
			}
			return out, nil
		}
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			normalized, err := normalizeValue(rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			out[i] = normalized
		}
		return out, nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map keys must be strings, got %s", rv.Type().Key())
		}
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			normalized, err := normalizeValue(iter.Value().Interface())
			if err != nil {
				return nil, err
			}
			out[iter.Key().String()] = normalized
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported property value type %T", value)
	}
}

func decodeValue(value *C.lattice_value) (any, error) {
	switch ErrorCode(C.lattice_go_value_type(value)) {
	case ErrorCode(C.LATTICE_VALUE_NULL):
		return nil, nil
	case ErrorCode(C.LATTICE_VALUE_BOOL):
		return bool(C.lattice_go_value_bool(value)), nil
	case ErrorCode(C.LATTICE_VALUE_INT):
		return int64(C.lattice_go_value_int(value)), nil
	case ErrorCode(C.LATTICE_VALUE_FLOAT):
		return float64(C.lattice_go_value_float(value)), nil
	case ErrorCode(C.LATTICE_VALUE_STRING):
		ptr := C.lattice_go_value_string_ptr(value)
		length := int(C.lattice_go_value_string_len(value))
		return cStringN((*C.char)(unsafe.Pointer(ptr)), length), nil
	case ErrorCode(C.LATTICE_VALUE_BYTES):
		ptr := C.lattice_go_value_bytes_ptr(value)
		length := int(C.lattice_go_value_bytes_len(value))
		if ptr == nil || length == 0 {
			return []byte{}, nil
		}
		return C.GoBytes(unsafe.Pointer(ptr), C.int(length)), nil
	case ErrorCode(C.LATTICE_VALUE_VECTOR):
		ptr := C.lattice_go_value_vector_ptr(value)
		length := int(C.lattice_go_value_vector_dimensions(value))
		if ptr == nil || length == 0 {
			return []float32{}, nil
		}
		out := make([]float32, length)
		copy(out, unsafe.Slice((*float32)(unsafe.Pointer(ptr)), length))
		return out, nil
	case ErrorCode(C.LATTICE_VALUE_LIST):
		list := C.lattice_go_value_list(value)
		if list == nil {
			return nil, &Error{Code: ErrorInvalidArg, Message: "invalid native LIST value"}
		}
		length := int(C.lattice_go_list_len(list))
		out := make([]any, length)
		if length == 0 {
			return out, nil
		}
		items := unsafe.Slice(C.lattice_go_list_items(list), length)
		for i := range items {
			decoded, err := decodeValue(&items[i])
			if err != nil {
				return nil, err
			}
			out[i] = decoded
		}
		return out, nil
	case ErrorCode(C.LATTICE_VALUE_MAP):
		cMap := C.lattice_go_value_map(value)
		if cMap == nil {
			return nil, &Error{Code: ErrorInvalidArg, Message: "invalid native MAP value"}
		}
		length := int(C.lattice_go_map_len(cMap))
		out := make(map[string]any, length)
		if length == 0 {
			return out, nil
		}
		entries := unsafe.Slice(C.lattice_go_map_entries(cMap), length)
		for i := range entries {
			key := cStringN((*C.char)(unsafe.Pointer(C.lattice_go_map_entry_key(&entries[i]))), int(C.lattice_go_map_entry_key_len(&entries[i])))
			decoded, err := decodeValue(C.lattice_go_map_entry_value(&entries[i]))
			if err != nil {
				return nil, err
			}
			out[key] = decoded
		}
		return out, nil
	default:
		return nil, &Error{
			Code:    ErrorUnsupported,
			Message: fmt.Sprintf("unsupported native value type %d", int(C.lattice_go_value_type(value))),
		}
	}
}
