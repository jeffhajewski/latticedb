/*
 * LatticeDB Java bindings - JNI bridge.
 *
 * Wraps the stable C API (include/lattice.h) for the io.latticedb Java
 * package. Mirrors the Go cgo bridge (bindings/go/internal/cgo) in behavior:
 * same ownership rules, same error propagation, same value conversions.
 */

#include <jni.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "lattice.h"

/* ------------------------------------------------------------------ */
/* Cached JNI references                                               */
/* ------------------------------------------------------------------ */

static jclass CLS_LATTICE_EXCEPTION = NULL;
static jmethodID MID_LATTICE_INIT = NULL;
static jclass CLS_QUERY_EXCEPTION = NULL;
static jmethodID MID_QUERY_EXCEPTION_INIT = NULL;
static jclass CLS_STRING = NULL;
static jmethodID MID_GET_BYTES_UTF8 = NULL;
static jmethodID MID_STRING_NEW_UTF8 = NULL;
static jclass CLS_ARRAYLIST = NULL;
static jmethodID MID_ARRAYLIST_INIT = NULL;
static jmethodID MID_ARRAYLIST_ADD = NULL;
static jclass CLS_LINKED_HASH_MAP = NULL;
static jmethodID MID_LHM_INIT = NULL;
static jmethodID MID_LHM_PUT = NULL;
static jclass CLS_BOOL = NULL;
static jmethodID MID_BOOL_INIT = NULL;
static jmethodID MID_BOOL_VALUE = NULL;
static jclass CLS_LONG = NULL;
static jmethodID MID_LONG_INIT = NULL;
static jclass CLS_DOUBLE = NULL;
static jmethodID MID_DOUBLE_INIT = NULL;
static jclass CLS_FLOAT = NULL;
static jclass CLS_INTEGER = NULL;
static jmethodID MID_INT_INIT = NULL;
static jclass CLS_NUMBER = NULL;
static jmethodID MID_NUMBER_DOUBLE_VALUE = NULL;
static jmethodID MID_NUMBER_LONG_VALUE = NULL;
static jclass CLS_BYTE_ARRAY = NULL;
static jclass CLS_FLOAT_ARRAY = NULL;
static jclass CLS_LIST = NULL;
static jclass CLS_COLLECTION = NULL;
static jmethodID MID_COLLECTION_ITERATOR = NULL;
static jclass CLS_ITERATOR = NULL;
static jmethodID MID_ITERATOR_HAS_NEXT = NULL;
static jmethodID MID_ITERATOR_NEXT = NULL;
static jclass CLS_MAP = NULL;
static jmethodID MID_MAP_ENTRY_SET = NULL;
static jclass CLS_MAP_ENTRY = NULL;
static jmethodID MID_MAP_ENTRY_GET_KEY = NULL;
static jmethodID MID_MAP_ENTRY_GET_VALUE = NULL;
static jclass CLS_OBJECT = NULL;

static void init_cache(JNIEnv *env) {
    if (CLS_LATTICE_EXCEPTION != NULL) {
        return;
    }
    jclass le = (*env)->FindClass(env, "io/latticedb/LatticeException");
    if (le == NULL) return;
    CLS_LATTICE_EXCEPTION = (*env)->NewGlobalRef(env, le);
    MID_LATTICE_INIT = (*env)->GetMethodID(env, le, "<init>", "(ILjava/lang/String;)V");

    jclass qe = (*env)->FindClass(env, "io/latticedb/QueryException");
    if (qe == NULL) return;
    CLS_QUERY_EXCEPTION = (*env)->NewGlobalRef(env, qe);
    MID_QUERY_EXCEPTION_INIT = (*env)->GetMethodID(env, qe, "<init>",
        "(IILjava/lang/String;Ljava/lang/String;ZIII)V");

    jclass str = (*env)->FindClass(env, "java/lang/String");
    if (str == NULL) return;
    CLS_STRING = (*env)->NewGlobalRef(env, str);
    MID_GET_BYTES_UTF8 = (*env)->GetMethodID(env, str, "getBytes",
        "(Ljava/lang/String;)[B");
    MID_STRING_NEW_UTF8 = (*env)->GetMethodID(env, str, "<init>",
        "([BLjava/lang/String;)V");

    jclass al = (*env)->FindClass(env, "java/util/ArrayList");
    if (al == NULL) return;
    CLS_ARRAYLIST = (*env)->NewGlobalRef(env, al);
    MID_ARRAYLIST_INIT = (*env)->GetMethodID(env, al, "<init>", "(I)V");
    MID_ARRAYLIST_ADD = (*env)->GetMethodID(env, al, "add", "(Ljava/lang/Object;)Z");

    jclass lhm = (*env)->FindClass(env, "java/util/LinkedHashMap");
    if (lhm == NULL) return;
    CLS_LINKED_HASH_MAP = (*env)->NewGlobalRef(env, lhm);
    MID_LHM_INIT = (*env)->GetMethodID(env, lhm, "<init>", "()V");
    MID_LHM_PUT = (*env)->GetMethodID(env, lhm, "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

#define CACHE_BOX(cls_var, mid_var, name, sig)                              \
    do {                                                                    \
        jclass c = (*env)->FindClass(env, name);                            \
        if (c == NULL) return;                                              \
        cls_var = (*env)->NewGlobalRef(env, c);                             \
        mid_var = (*env)->GetMethodID(env, c, "<init>", sig);               \
    } while (0)
    CACHE_BOX(CLS_BOOL, MID_BOOL_INIT, "java/lang/Boolean", "(Z)V");
    CACHE_BOX(CLS_LONG, MID_LONG_INIT, "java/lang/Long", "(J)V");
    CACHE_BOX(CLS_DOUBLE, MID_DOUBLE_INIT, "java/lang/Double", "(D)V");
    CACHE_BOX(CLS_INTEGER, MID_INT_INIT, "java/lang/Integer", "(I)V");
#undef CACHE_BOX

#define CACHE_CLASS(cls_var, name)                                        \
    do {                                                                   \
        jclass c = (*env)->FindClass(env, name);                            \
        if (c == NULL) return;                                              \
        cls_var = (*env)->NewGlobalRef(env, c);                             \
    } while (0)
    CACHE_CLASS(CLS_FLOAT, "java/lang/Float");
    CACHE_CLASS(CLS_NUMBER, "java/lang/Number");
    CACHE_CLASS(CLS_BYTE_ARRAY, "[B");
    CACHE_CLASS(CLS_FLOAT_ARRAY, "[F");
    CACHE_CLASS(CLS_LIST, "java/util/List");
    CACHE_CLASS(CLS_COLLECTION, "java/util/Collection");
    CACHE_CLASS(CLS_ITERATOR, "java/util/Iterator");
    CACHE_CLASS(CLS_MAP, "java/util/Map");
    CACHE_CLASS(CLS_MAP_ENTRY, "java/util/Map$Entry");
    CACHE_CLASS(CLS_OBJECT, "java/lang/Object");
#undef CACHE_CLASS

    MID_BOOL_VALUE = (*env)->GetMethodID(env, CLS_BOOL, "booleanValue", "()Z");
    MID_NUMBER_DOUBLE_VALUE = (*env)->GetMethodID(env, CLS_NUMBER,
        "doubleValue", "()D");
    MID_NUMBER_LONG_VALUE = (*env)->GetMethodID(env, CLS_NUMBER,
        "longValue", "()J");
    MID_COLLECTION_ITERATOR = (*env)->GetMethodID(env, CLS_COLLECTION,
        "iterator", "()Ljava/util/Iterator;");
    MID_ITERATOR_HAS_NEXT = (*env)->GetMethodID(env, CLS_ITERATOR,
        "hasNext", "()Z");
    MID_ITERATOR_NEXT = (*env)->GetMethodID(env, CLS_ITERATOR,
        "next", "()Ljava/lang/Object;");
    MID_MAP_ENTRY_SET = (*env)->GetMethodID(env, CLS_MAP, "entrySet",
        "()Ljava/util/Set;");
    MID_MAP_ENTRY_GET_KEY = (*env)->GetMethodID(env, CLS_MAP_ENTRY,
        "getKey", "()Ljava/lang/Object;");
    MID_MAP_ENTRY_GET_VALUE = (*env)->GetMethodID(env, CLS_MAP_ENTRY,
        "getValue", "()Ljava/lang/Object;");
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    (void)reserved;
    JNIEnv *env = NULL;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_8) != JNI_OK) {
        return JNI_ERR;
    }
    init_cache(env);
    if ((*env)->ExceptionCheck(env)) {
        return JNI_ERR;
    }
    return JNI_VERSION_1_8;
}

/* Throw LatticeException(code, message). Always returns a default value
 * suitable for the caller's return type after use. */
static void throw_lattice(JNIEnv *env, lattice_error code) {
    const char *msg = lattice_error_message(code);
    jstring jmsg = (*env)->NewStringUTF(env, msg ? msg : "");
    jobject ex = (*env)->NewObject(env, CLS_LATTICE_EXCEPTION, MID_LATTICE_INIT,
                                   (jint)code, jmsg);
    if (ex != NULL) (*env)->Throw(env, ex);
    if (jmsg != NULL) (*env)->DeleteLocalRef(env, jmsg);
}

static int check(JNIEnv *env, lattice_error code) {
    if (code != LATTICE_OK) {
        throw_lattice(env, code);
        return 0;
    }
    return 1;
}

/* Build and throw a QueryException from the prepared-query diagnostics. */
static void throw_query_error(JNIEnv *env, lattice_query *q) {
    jint stage = (jint)lattice_query_last_error_stage(q);
    const char *msg = lattice_query_last_error_message(q);
    const char *diag = lattice_query_last_error_code(q);
    jint code = -1; /* generic */
    jboolean has_loc = lattice_query_last_error_has_location(q) ? JNI_TRUE : JNI_FALSE;
    jint line = (jint)lattice_query_last_error_line(q);
    jint column = (jint)lattice_query_last_error_column(q);
    jint length = (jint)lattice_query_last_error_length(q);
    jstring jmsg = (*env)->NewStringUTF(env, msg ? msg : "");
    jstring jdiag = diag ? (*env)->NewStringUTF(env, diag) : NULL;
    jobject ex = (*env)->NewObject(env, CLS_QUERY_EXCEPTION, MID_QUERY_EXCEPTION_INIT,
        code, stage, jmsg, jdiag, has_loc, line, column, length);
    if (ex != NULL) (*env)->Throw(env, ex);
    if (jmsg) (*env)->DeleteLocalRef(env, jmsg);
    if (jdiag) (*env)->DeleteLocalRef(env, jdiag);
}

/* ------------------------------------------------------------------ */
/* UTF-8 string helpers                                                */
/* ------------------------------------------------------------------ */

/* Convert a jstring to a NUL-terminated UTF-8 buffer (malloc'd).
 * Returns NULL (with an exception pending) on failure.
 * The returned length excludes the NUL terminator. */
static char *jstring_to_utf8(JNIEnv *env, jstring str, size_t *out_len) {
    *out_len = 0;
    if (str == NULL) return NULL;
    jstring charset = (*env)->NewStringUTF(env, "UTF-8");
    if (charset == NULL) return NULL;
    jbyteArray bytes = (jbyteArray)(*env)->CallObjectMethod(env, str,
                                                            MID_GET_BYTES_UTF8,
                                                            charset);
    (*env)->DeleteLocalRef(env, charset);
    if (bytes == NULL || (*env)->ExceptionCheck(env)) return NULL;
    jsize len = (*env)->GetArrayLength(env, bytes);
    char *buf = (char *)malloc((size_t)len + 1);
    if (buf == NULL) {
        (*env)->DeleteLocalRef(env, bytes);
        throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
        return NULL;
    }
    if (len > 0) {
        (*env)->GetByteArrayRegion(env, bytes, 0, len, (jbyte *)buf);
    }
    buf[len] = '\0';
    (*env)->DeleteLocalRef(env, bytes);
    *out_len = (size_t)len;
    return buf;
}

/* Convert a UTF-8 buffer to a jstring without going through modified UTF-8. */
static jstring utf8_to_jstring(JNIEnv *env, const char *ptr, size_t len) {
    if (ptr == NULL) return NULL;
    jbyteArray bytes = (*env)->NewByteArray(env, (jsize)len);
    if (bytes == NULL) return NULL;
    if (len > 0) {
        (*env)->SetByteArrayRegion(env, bytes, 0, (jsize)len, (const jbyte *)ptr);
    }
    jstring charset = (*env)->NewStringUTF(env, "UTF-8");
    jstring result = (jstring)(*env)->NewObject(env, CLS_STRING,
                                                MID_STRING_NEW_UTF8, bytes, charset);
    (*env)->DeleteLocalRef(env, charset);
    (*env)->DeleteLocalRef(env, bytes);
    return result;
}

/* ------------------------------------------------------------------ */
/* Value conversion: Java Object -> lattice_value tree                 */
/*                                                                     */
/* Values passed to the native layer are borrowed for the call, so we   */
/* build a self-contained tree with malloc and free it after the call.  */
/* ------------------------------------------------------------------ */

static int fill_value_inner(JNIEnv *env, lattice_value *dst, jobject value);

/* Give each recursive conversion its own local-reference frame. Large nested
 * collections would otherwise retain several JNI class/object references per
 * element until the native entry point returned. */
static int fill_value(JNIEnv *env, lattice_value *dst, jobject value) {
    if ((*env)->PushLocalFrame(env, 24) < 0) return 0;
    int ok = fill_value_inner(env, dst, value);
    (*env)->PopLocalFrame(env, NULL);
    return ok;
}

static void free_value_tree(lattice_value *v);

static void free_list(lattice_list *list) {
    if (list == NULL) return;
    for (size_t i = 0; i < list->len; i++) free_value_tree(&list->items[i]);
    free(list->items);
    free(list);
}

static void free_map(lattice_map *map) {
    if (map == NULL) return;
    for (size_t i = 0; i < map->len; i++) {
        free((void *)map->entries[i].key);
        free_value_tree(&map->entries[i].value);
    }
    free(map->entries);
    free(map);
}

static void free_value_tree(lattice_value *v) {
    if (v == NULL) return;
    switch (v->type) {
        case LATTICE_VALUE_STRING: free((void *)v->data.string_val.ptr); break;
        case LATTICE_VALUE_BYTES:  free((void *)v->data.bytes_val.ptr); break;
        case LATTICE_VALUE_VECTOR: free((void *)v->data.vector_val.ptr); break;
        case LATTICE_VALUE_LIST:   free_list(v->data.list_val); break;
        case LATTICE_VALUE_MAP:    free_map(v->data.map_val); break;
        default: break;
    }
}

static int fill_string(JNIEnv *env, lattice_value *dst, jstring str) {
    memset(dst, 0, sizeof(*dst));
    size_t len = 0;
    char *buf = jstring_to_utf8(env, str, &len);
    if (buf == NULL && !(*env)->ExceptionCheck(env)) {
        /* null string maps to empty */
    }
    if ((*env)->ExceptionCheck(env)) { free(buf); return 0; }
    dst->type = LATTICE_VALUE_STRING;
    dst->data.string_val.ptr = buf;
    dst->data.string_val.len = buf ? len : 0;
    return 1;
}

static int fill_value_inner(JNIEnv *env, lattice_value *dst, jobject value) {
    if (value == NULL) {
        memset(dst, 0, sizeof(*dst));
        dst->type = LATTICE_VALUE_NULL;
        return 1;
    }

    /* Boolean */
    if ((*env)->IsInstanceOf(env, value, CLS_BOOL)) {
        memset(dst, 0, sizeof(*dst));
        dst->type = LATTICE_VALUE_BOOL;
        dst->data.bool_val = (*env)->CallBooleanMethod(env, value, MID_BOOL_VALUE);
        return 1;
    }

    /* Numbers: integral types -> INT, floating types -> FLOAT */
    if ((*env)->IsInstanceOf(env, value, CLS_NUMBER)) {
        memset(dst, 0, sizeof(*dst));
        if ((*env)->IsInstanceOf(env, value, CLS_DOUBLE) ||
            (*env)->IsInstanceOf(env, value, CLS_FLOAT)) {
            dst->type = LATTICE_VALUE_FLOAT;
            dst->data.float_val = (*env)->CallDoubleMethod(env, value,
                                                            MID_NUMBER_DOUBLE_VALUE);
        } else {
            dst->type = LATTICE_VALUE_INT;
            dst->data.int_val = (*env)->CallLongMethod(env, value,
                                                        MID_NUMBER_LONG_VALUE);
        }
        return 1;
    }

    /* String */
    if ((*env)->IsInstanceOf(env, value, CLS_STRING)) {
        return fill_string(env, dst, (jstring)value);
    }

    /* byte[] -> BYTES */
    if ((*env)->IsInstanceOf(env, value, CLS_BYTE_ARRAY)) {
        jbyteArray arr = (jbyteArray)value;
        jsize len = (*env)->GetArrayLength(env, arr);
        uint8_t *buf = NULL;
        if (len > 0) {
            buf = (uint8_t *)malloc((size_t)len);
            if (buf == NULL) { throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY); return 0; }
            (*env)->GetByteArrayRegion(env, arr, 0, len, (jbyte *)buf);
        }
        memset(dst, 0, sizeof(*dst));
        dst->type = LATTICE_VALUE_BYTES;
        dst->data.bytes_val.ptr = buf;
        dst->data.bytes_val.len = (size_t)(len > 0 ? len : 0);
        return 1;
    }

    /* float[] -> VECTOR */
    if ((*env)->IsInstanceOf(env, value, CLS_FLOAT_ARRAY)) {
        jfloatArray arr = (jfloatArray)value;
        jsize len = (*env)->GetArrayLength(env, arr);
        float *buf = NULL;
        if (len > 0) {
            buf = (float *)malloc(sizeof(float) * (size_t)len);
            if (buf == NULL) { throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY); return 0; }
            (*env)->GetFloatArrayRegion(env, arr, 0, len, buf);
        }
        memset(dst, 0, sizeof(*dst));
        dst->type = LATTICE_VALUE_VECTOR;
        dst->data.vector_val.ptr = buf;
        dst->data.vector_val.dimensions = (uint32_t)(len > 0 ? len : 0);
        return 1;
    }

    /* java.util.List -> LIST */
    if ((*env)->IsInstanceOf(env, value, CLS_LIST)) {
        jobject iter = NULL;
        iter = (*env)->CallObjectMethod(env, value, MID_COLLECTION_ITERATOR);

        lattice_list *list = (lattice_list *)calloc(1, sizeof(lattice_list));
        if (list == NULL) {
            throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
            return 0;
        }
        size_t cap = 8;
        list->items = (lattice_value *)calloc(cap, sizeof(lattice_value));
        if (list->items == NULL) {
            free(list);
            throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
            return 0;
        }
        while ((*env)->CallBooleanMethod(env, iter, MID_ITERATOR_HAS_NEXT)) {
            jobject item = (*env)->CallObjectMethod(env, iter, MID_ITERATOR_NEXT);
            if (list->len == cap) {
                cap *= 2;
                lattice_value *grown = (lattice_value *)
                    realloc(list->items, cap * sizeof(lattice_value));
                if (grown == NULL) {
                    free_list(list);
                    throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
                    return 0;
                }
                list->items = grown;
            }
            if (!fill_value(env, &list->items[list->len], item)) {
                free_value_tree(&list->items[list->len]);
                free_list(list);
                return 0;
            }
            list->len++;
            if (item != NULL) (*env)->DeleteLocalRef(env, item);
        }
        memset(dst, 0, sizeof(*dst));
        dst->type = LATTICE_VALUE_LIST;
        dst->data.list_val = list;
        return 1;
    }

    /* java.util.Map -> MAP */
    if ((*env)->IsInstanceOf(env, value, CLS_MAP)) {
        jobject entrySet = (*env)->CallObjectMethod(env, value, MID_MAP_ENTRY_SET);
        jobject iter = (*env)->CallObjectMethod(env, entrySet, MID_COLLECTION_ITERATOR);

        lattice_map *map = (lattice_map *)calloc(1, sizeof(lattice_map));
        if (map == NULL) {
            throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
            return 0;
        }
        size_t cap = 8, count = 0;
        map->entries = (lattice_map_entry *)calloc(cap, sizeof(lattice_map_entry));
        if (map->entries == NULL) {
            free(map);
            throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
            return 0;
        }
        while ((*env)->CallBooleanMethod(env, iter, MID_ITERATOR_HAS_NEXT)) {
            jobject entry = (*env)->CallObjectMethod(env, iter, MID_ITERATOR_NEXT);
            jobject key_obj = (*env)->CallObjectMethod(env, entry, MID_MAP_ENTRY_GET_KEY);
            jobject val = (*env)->CallObjectMethod(env, entry, MID_MAP_ENTRY_GET_VALUE);
            if (key_obj == NULL || !(*env)->IsInstanceOf(env, key_obj, CLS_STRING)) {
                map->len = count;
                free_map(map);
                throw_lattice(env, LATTICE_ERROR_INVALID_ARG);
                return 0;
            }
            jstring key = (jstring)key_obj;
            if (count == cap) {
                cap *= 2;
                lattice_map_entry *grown = (lattice_map_entry *)
                    realloc(map->entries, cap * sizeof(lattice_map_entry));
                if (grown == NULL) {
                    map->len = count;
                    free_map(map);
                    throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
                    return 0;
                }
                map->entries = grown;
            }
            size_t keyLen = 0;
            char *keyBuf = jstring_to_utf8(env, key, &keyLen);
            if (keyBuf == NULL) {
                map->len = count;
                free_map(map);
                return 0;
            }
            map->entries[count].key = keyBuf;
            map->entries[count].key_len = keyLen;
            if (!fill_value(env, &map->entries[count].value, val)) {
                free((void *)map->entries[count].key);
                free_value_tree(&map->entries[count].value);
                map->len = count;
                free_map(map);
                return 0;
            }
            count++;
            if (entry) (*env)->DeleteLocalRef(env, entry);
            (*env)->DeleteLocalRef(env, key_obj);
            if (val) (*env)->DeleteLocalRef(env, val);
        }
        map->len = count;
        memset(dst, 0, sizeof(*dst));
        dst->type = LATTICE_VALUE_MAP;
        dst->data.map_val = map;
        return 1;
    }

    throw_lattice(env, LATTICE_ERROR_INVALID_ARG);
    return 0;
}

/* Free a borrowed-call tree built by fill_value. */
static void release_value_tree(lattice_value *v) {
    free_value_tree(v);
}

/* ------------------------------------------------------------------ */
/* Value conversion: lattice_value -> Java Object                      */
/* ------------------------------------------------------------------ */

static jobject value_to_java(JNIEnv *env, const lattice_value *v) {
    switch (v->type) {
        case LATTICE_VALUE_NULL:
            return NULL;
        case LATTICE_VALUE_BOOL:
            return (*env)->NewObject(env, CLS_BOOL, MID_BOOL_INIT,
                (jboolean)(v->data.bool_val ? JNI_TRUE : JNI_FALSE));
        case LATTICE_VALUE_INT:
            return (*env)->NewObject(env, CLS_LONG, MID_LONG_INIT,
                (jlong)v->data.int_val);
        case LATTICE_VALUE_FLOAT:
            return (*env)->NewObject(env, CLS_DOUBLE, MID_DOUBLE_INIT,
                (jdouble)v->data.float_val);
        case LATTICE_VALUE_STRING:
            return utf8_to_jstring(env, v->data.string_val.ptr, v->data.string_val.len);
        case LATTICE_VALUE_BYTES: {
            jsize len = (jsize)v->data.bytes_val.len;
            jbyteArray arr = (*env)->NewByteArray(env, len);
            if (arr == NULL) return NULL;
            if (len > 0 && v->data.bytes_val.ptr != NULL) {
                (*env)->SetByteArrayRegion(env, arr, 0, len,
                                           (const jbyte *)v->data.bytes_val.ptr);
            }
            return arr;
        }
        case LATTICE_VALUE_VECTOR: {
            jsize dims = (jsize)v->data.vector_val.dimensions;
            jfloatArray arr = (*env)->NewFloatArray(env, dims);
            if (arr == NULL) return NULL;
            if (dims > 0 && v->data.vector_val.ptr != NULL) {
                (*env)->SetFloatArrayRegion(env, arr, 0, dims, v->data.vector_val.ptr);
            }
            return arr;
        }
        case LATTICE_VALUE_LIST: {
            lattice_list *list = v->data.list_val;
            jobject out = (*env)->NewObject(env, CLS_ARRAYLIST, MID_ARRAYLIST_INIT,
                                            (jint)(list ? list->len : 0));
            if (out == NULL) return NULL;
            if (list != NULL) {
                for (size_t i = 0; i < list->len; i++) {
                    jobject item = value_to_java(env, &list->items[i]);
                    if (item != NULL) {
                        (*env)->CallBooleanMethod(env, out, MID_ARRAYLIST_ADD, item);
                        (*env)->DeleteLocalRef(env, item);
                    } else {
                        (*env)->CallBooleanMethod(env, out, MID_ARRAYLIST_ADD, NULL);
                    }
                }
            }
            return out;
        }
        case LATTICE_VALUE_MAP: {
            lattice_map *map = v->data.map_val;
            jobject out = (*env)->NewObject(env, CLS_LINKED_HASH_MAP, MID_LHM_INIT);
            if (out == NULL) return NULL;
            if (map != NULL) {
                for (size_t i = 0; i < map->len; i++) {
                    jstring key = utf8_to_jstring(env, map->entries[i].key,
                                                  map->entries[i].key_len);
                    jobject val = value_to_java(env, &map->entries[i].value);
                    (*env)->CallObjectMethod(env, out, MID_LHM_PUT,
                                             key ? (jobject)key : NULL,
                                             val ? val : NULL);
                    if (key) (*env)->DeleteLocalRef(env, key);
                    if (val) (*env)->DeleteLocalRef(env, val);
                }
            }
            return out;
        }
        default:
            throw_lattice(env, LATTICE_ERROR_UNSUPPORTED);
            return NULL;
    }
}

/* Convert an owned lattice_value tree to Java and then free it. */
static jobject take_owned_value(JNIEnv *env, lattice_value *v) {
    jobject out = value_to_java(env, v);
    lattice_value_free(v);
    return out;
}

/* ------------------------------------------------------------------ */
/* Database lifecycle                                                  */
/* ------------------------------------------------------------------ */

JNIEXPORT jstring JNICALL
Java_io_latticedb_Native_version(JNIEnv *env, jclass cls) {
    init_cache(env);
    (void)cls;
    return (*env)->NewStringUTF(env, lattice_version());
}

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_open(JNIEnv *env, jclass cls, jstring path,
        jboolean create, jboolean read_only, jint cache_size_mb, jint page_size,
        jboolean enable_vector, jint vector_dimensions, jboolean enable_wal,
        jboolean enable_adjacency_cache, jboolean lock) {
    init_cache(env);
    (void)cls;
    lattice_open_options_v4 opts;
    memset(&opts, 0, sizeof(opts));
    opts.struct_size = sizeof(opts);
    opts.create = create == JNI_TRUE;
    opts.read_only = read_only == JNI_TRUE;
    opts.cache_size_mb = cache_size_mb <= 0 ? 100 : (uint32_t)cache_size_mb;
    opts.page_size = page_size <= 0 ? 4096 : (uint32_t)page_size;
    opts.enable_vector = enable_vector == JNI_TRUE;
    opts.vector_dimensions = vector_dimensions <= 0 ? 128 : (uint16_t)vector_dimensions;
    opts.enable_wal = enable_wal == JNI_TRUE;
    opts.enable_adjacency_cache = enable_adjacency_cache == JNI_TRUE;
    opts.lock = lock == JNI_TRUE;

    size_t path_len = 0;
    char *path_buf = jstring_to_utf8(env, path, &path_len);
    if (path_buf == NULL && (*env)->ExceptionCheck(env)) return 0;

    lattice_database *db = NULL;
    lattice_error rc = lattice_open_v4(path_buf, &opts, &db);
    free(path_buf);
    if (!check(env, rc)) return 0;
    return (jlong)(uintptr_t)db;
}

JNIEXPORT jbyteArray JNICALL
Java_io_latticedb_Native_serialize(JNIEnv *env, jclass cls, jlong db_handle) {
    init_cache(env);
    (void)cls;
    uint8_t *bytes = NULL;
    size_t len = 0;
    lattice_error rc = lattice_serialize(
        (lattice_database *)(uintptr_t)db_handle, &bytes, &len);
    if (!check(env, rc)) return NULL;
    if (len > INT32_MAX) {
        lattice_free_bytes(bytes, len);
        throw_lattice(env, LATTICE_ERROR_VALUE_TOO_LARGE);
        return NULL;
    }

    jbyteArray out = (*env)->NewByteArray(env, (jsize)len);
    if (out == NULL) {
        lattice_free_bytes(bytes, len);
        return NULL;
    }
    if (len > 0) {
        (*env)->SetByteArrayRegion(env, out, 0, (jsize)len, (const jbyte *)bytes);
    }
    lattice_free_bytes(bytes, len);
    return out;
}

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_deserialize(JNIEnv *env, jclass cls, jbyteArray bytes,
        jint cache_size_mb, jint page_size, jboolean enable_vector,
        jint vector_dimensions, jboolean enable_wal,
        jboolean enable_adjacency_cache, jboolean lock) {
    init_cache(env);
    (void)cls;
    if (bytes == NULL) {
        throw_lattice(env, LATTICE_ERROR_INVALID_ARG);
        return 0;
    }

    lattice_open_options_v4 opts;
    memset(&opts, 0, sizeof(opts));
    opts.struct_size = sizeof(opts);
    opts.create = false;
    opts.read_only = false;
    opts.cache_size_mb = cache_size_mb <= 0 ? 100 : (uint32_t)cache_size_mb;
    opts.page_size = page_size <= 0 ? 4096 : (uint32_t)page_size;
    opts.enable_vector = enable_vector == JNI_TRUE;
    opts.vector_dimensions = vector_dimensions <= 0 ? 128 : (uint16_t)vector_dimensions;
    opts.enable_wal = enable_wal == JNI_TRUE;
    opts.enable_adjacency_cache = enable_adjacency_cache == JNI_TRUE;
    opts.lock = lock == JNI_TRUE;

    jsize len = (*env)->GetArrayLength(env, bytes);
    jbyte *data = len > 0 ? (*env)->GetByteArrayElements(env, bytes, NULL) : NULL;
    if (len > 0 && data == NULL) return 0;

    lattice_database *db = NULL;
    lattice_error rc = lattice_deserialize(
        (const uint8_t *)data, (size_t)len, &opts, &db);
    if (data != NULL) {
        (*env)->ReleaseByteArrayElements(env, bytes, data, JNI_ABORT);
    }
    if (!check(env, rc)) return 0;
    return (jlong)(uintptr_t)db;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_close(JNIEnv *env, jclass cls, jlong db_handle) {
    init_cache(env);
    (void)cls;
    lattice_database *db = (lattice_database *)(uintptr_t)db_handle;
    if (db == NULL) return;
    check(env, lattice_close(db));
}

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_begin(JNIEnv *env, jclass cls, jlong db_handle,
                               jboolean read_only) {
    init_cache(env);
    (void)cls;
    lattice_txn *txn = NULL;
    lattice_error rc = lattice_begin((lattice_database *)(uintptr_t)db_handle,
        read_only == JNI_TRUE ? LATTICE_TXN_READ_ONLY : LATTICE_TXN_READ_WRITE,
        &txn);
    if (!check(env, rc)) return 0;
    return (jlong)(uintptr_t)txn;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_commit(JNIEnv *env, jclass cls, jlong txn_handle) {
    init_cache(env);
    (void)cls;
    check(env, lattice_commit((lattice_txn *)(uintptr_t)txn_handle));
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_rollback(JNIEnv *env, jclass cls, jlong txn_handle) {
    init_cache(env);
    (void)cls;
    check(env, lattice_rollback((lattice_txn *)(uintptr_t)txn_handle));
}

/* ------------------------------------------------------------------ */
/* Node operations                                                     */
/* ------------------------------------------------------------------ */

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_nodeCreate(JNIEnv *env, jclass cls, jlong txn_handle,
                                    jstring label) {
    init_cache(env);
    (void)cls;
    size_t len = 0;
    char *buf = label ? jstring_to_utf8(env, label, &len)
                      : (char *)calloc(1, 1);
    if ((*env)->ExceptionCheck(env)) { free(buf); return 0; }
    lattice_node_id id = 0;
    lattice_error rc = lattice_node_create((lattice_txn *)(uintptr_t)txn_handle,
                                           buf ? buf : "", &id);
    free(buf);
    if (!check(env, rc)) return 0;
    return (jlong)id;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_nodeAddLabel(JNIEnv *env, jclass cls, jlong txn_handle,
                                      jlong node_id, jstring label) {
    init_cache(env);
    (void)cls;
    char *buf = jstring_to_utf8(env, label, &(size_t){0});
    if ((*env)->ExceptionCheck(env)) return;
    lattice_error rc = lattice_node_add_label((lattice_txn *)(uintptr_t)txn_handle,
                                              (lattice_node_id)node_id, buf);
    free(buf);
    check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_nodeRemoveLabel(JNIEnv *env, jclass cls, jlong txn_handle,
                                         jlong node_id, jstring label) {
    init_cache(env);
    (void)cls;
    char *buf = jstring_to_utf8(env, label, &(size_t){0});
    if ((*env)->ExceptionCheck(env)) return;
    lattice_error rc = lattice_node_remove_label((lattice_txn *)(uintptr_t)txn_handle,
                                                 (lattice_node_id)node_id, buf);
    free(buf);
    check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_nodeDelete(JNIEnv *env, jclass cls, jlong txn_handle,
                                    jlong node_id) {
    init_cache(env);
    (void)cls;
    check(env, lattice_node_delete((lattice_txn *)(uintptr_t)txn_handle,
                                   (lattice_node_id)node_id));
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_nodeSetProperty(JNIEnv *env, jclass cls, jlong txn_handle,
                                         jlong node_id, jstring key, jobject value) {
    init_cache(env);
    (void)cls;
    lattice_value v;
    if (!fill_value(env, &v, value)) return;
    char *key_buf = key ? jstring_to_utf8(env, key, &(size_t){0}) : NULL;
    if ((*env)->ExceptionCheck(env)) { release_value_tree(&v); return; }
    lattice_error rc = lattice_node_set_property((lattice_txn *)(uintptr_t)txn_handle,
        (lattice_node_id)node_id, key_buf, &v);
    free(key_buf);
    release_value_tree(&v);
    check(env, rc);
}

JNIEXPORT jobject JNICALL
Java_io_latticedb_Native_nodeGetProperty(JNIEnv *env, jclass cls, jlong txn_handle,
                                         jlong node_id, jstring key) {
    init_cache(env);
    (void)cls;
    char *key_buf = key ? jstring_to_utf8(env, key, &(size_t){0}) : NULL;
    if ((*env)->ExceptionCheck(env)) return NULL;
    lattice_value v;
    memset(&v, 0, sizeof(v));
    lattice_error rc = lattice_node_get_property((lattice_txn *)(uintptr_t)txn_handle,
        (lattice_node_id)node_id, key_buf, &v);
    free(key_buf);
    if (rc == LATTICE_ERROR_NOT_FOUND) return NULL; /* missing key -> null */
    if (!check(env, rc)) return NULL;
    return take_owned_value(env, &v);
}

JNIEXPORT jboolean JNICALL
Java_io_latticedb_Native_nodeExists(JNIEnv *env, jclass cls, jlong txn_handle,
                                    jlong node_id) {
    init_cache(env);
    (void)cls;
    bool exists = false;
    lattice_error rc = lattice_node_exists((lattice_txn *)(uintptr_t)txn_handle,
        (lattice_node_id)node_id, &exists);
    if (!check(env, rc)) return JNI_FALSE;
    return exists ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_nodeGetLabels(JNIEnv *env, jclass cls, jlong txn_handle,
                                       jlong node_id) {
    init_cache(env);
    (void)cls;
    char *joined = NULL;
    lattice_error rc = lattice_node_get_labels((lattice_txn *)(uintptr_t)txn_handle,
                                               (lattice_node_id)node_id, &joined);
    if (!check(env, rc)) return NULL;
    /* Labels come back comma-separated. Split on commas; labels themselves
     * cannot contain commas in this engine (mirrors Go bridge behavior). */
    jclass strCls = CLS_STRING;
    jobjectArray arr;
    size_t count = 0;
    if (joined != NULL && joined[0] != '\0') {
        count = 1;
        for (const char *p = joined; *p; p++) if (*p == ',') count++;
    }
    arr = (*env)->NewObjectArray(env, (jsize)count, strCls, NULL);
    size_t idx = 0;
    char *cursor = joined;
    while (cursor != NULL && idx < count) {
        char *comma = strchr(cursor, ',');
        size_t seg = comma ? (size_t)(comma - cursor) : strlen(cursor);
        jstring s = utf8_to_jstring(env, cursor, seg);
        (*env)->SetObjectArrayElement(env, arr, (jsize)idx++, s);
        if (s) (*env)->DeleteLocalRef(env, s);
        cursor = comma ? comma + 1 : NULL;
    }
    lattice_free_string(joined);
    return arr;
}

JNIEXPORT jlongArray JNICALL
Java_io_latticedb_Native_getNodesByLabelTxn(JNIEnv *env, jclass cls, jlong txn_handle,
                                            jstring label) {
    init_cache(env);
    (void)cls;
    size_t len = 0;
    char *buf = jstring_to_utf8(env, label, &len);
    if ((*env)->ExceptionCheck(env)) return NULL;
    lattice_node_id *ids = NULL;
    size_t count = 0;
    lattice_error rc = lattice_get_nodes_by_label_txn(
        (lattice_txn *)(uintptr_t)txn_handle, buf, buf ? len : 0, &ids, &count);
    free(buf);
    if (!check(env, rc)) return NULL;
    jlongArray arr = (*env)->NewLongArray(env, (jsize)count);
    if (arr != NULL && count > 0) {
        (*env)->SetLongArrayRegion(env, arr, 0, (jsize)count, (const jlong *)ids);
    }
    lattice_free_node_ids(ids, count);
    return arr;
}

JNIEXPORT jlongArray JNICALL
Java_io_latticedb_Native_getAllNodesTxn(JNIEnv *env, jclass cls, jlong txn_handle) {
    init_cache(env);
    (void)cls;
    lattice_node_id *ids = NULL;
    size_t count = 0;
    lattice_error rc = lattice_get_all_nodes_txn(
        (lattice_txn *)(uintptr_t)txn_handle, &ids, &count);
    if (!check(env, rc)) return NULL;
    jlongArray arr = (*env)->NewLongArray(env, (jsize)count);
    if (arr != NULL && count > 0) {
        (*env)->SetLongArrayRegion(env, arr, 0, (jsize)count, (const jlong *)ids);
    }
    lattice_free_node_ids(ids, count);
    return arr;
}

JNIEXPORT jlongArray JNICALL
Java_io_latticedb_Native_getNodesByLabel(JNIEnv *env, jclass cls, jlong db_handle,
                                         jstring label) {
    init_cache(env);
    (void)cls;
    size_t len = 0;
    char *buf = jstring_to_utf8(env, label, &len);
    if ((*env)->ExceptionCheck(env)) return NULL;
    lattice_node_id *ids = NULL;
    size_t count = 0;
    lattice_error rc = lattice_get_nodes_by_label(
        (lattice_database *)(uintptr_t)db_handle, buf, buf ? len : 0, &ids, &count);
    free(buf);
    if (!check(env, rc)) return NULL;
    jlongArray arr = (*env)->NewLongArray(env, (jsize)count);
    if (arr != NULL && count > 0) {
        (*env)->SetLongArrayRegion(env, arr, 0, (jsize)count, (const jlong *)ids);
    }
    lattice_free_node_ids(ids, count);
    return arr;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_createNodePropertyIndex(JNIEnv *env, jclass cls,
        jlong db_handle, jstring label, jstring property) {
    init_cache(env);
    (void)cls;
    char *lb = jstring_to_utf8(env, label, &(size_t){0});
    char *pb = jstring_to_utf8(env, property, &(size_t){0});
    if ((*env)->ExceptionCheck(env)) { free(lb); free(pb); return; }
    lattice_error rc = lattice_node_property_index_create(
        (lattice_database *)(uintptr_t)db_handle, lb, pb);
    free(lb); free(pb);
    check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_dropNodePropertyIndex(JNIEnv *env, jclass cls,
        jlong db_handle, jstring label, jstring property) {
    init_cache(env);
    (void)cls;
    char *lb = jstring_to_utf8(env, label, &(size_t){0});
    char *pb = jstring_to_utf8(env, property, &(size_t){0});
    if ((*env)->ExceptionCheck(env)) { free(lb); free(pb); return; }
    lattice_error rc = lattice_node_property_index_drop(
        (lattice_database *)(uintptr_t)db_handle, lb, pb);
    free(lb); free(pb);
    check(env, rc);
}

JNIEXPORT jlongArray JNICALL
Java_io_latticedb_Native_findNodesByLabelProperty(JNIEnv *env, jclass cls,
        jlong txn_handle, jstring label, jstring property, jobject value, jint limit) {
    init_cache(env);
    (void)cls;
    lattice_value v;
    if (!fill_value(env, &v, value)) return NULL;
    size_t lb_len = 0, pb_len = 0;
    char *lb = jstring_to_utf8(env, label, &lb_len);
    char *pb = jstring_to_utf8(env, property, &pb_len);
    if ((*env)->ExceptionCheck(env)) {
        release_value_tree(&v); free(lb); free(pb); return NULL;
    }
    lattice_node_id *ids = NULL;
    size_t count = 0;
    lattice_error rc = lattice_nodes_find_by_label_property(
        (lattice_txn *)(uintptr_t)txn_handle, lb, pb, &v, (size_t)limit, &ids, &count);
    release_value_tree(&v); free(lb); free(pb);
    if (!check(env, rc)) return NULL;
    jlongArray arr = (*env)->NewLongArray(env, (jsize)count);
    if (arr != NULL && count > 0) {
        (*env)->SetLongArrayRegion(env, arr, 0, (jsize)count, (const jlong *)ids);
    }
    lattice_free_node_ids(ids, count);
    return arr;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_nodeSetVector(JNIEnv *env, jclass cls, jlong txn_handle,
                                       jlong node_id, jstring key,
                                       jfloatArray vector) {
    init_cache(env);
    (void)cls;
    char *key_buf = key ? jstring_to_utf8(env, key, &(size_t){0}) : NULL;
    if ((*env)->ExceptionCheck(env)) return;
    jsize dims = vector ? (*env)->GetArrayLength(env, vector) : 0;
    const float *ptr = NULL;
    float *copy = NULL;
    if (dims > 0) {
        copy = (float *)malloc(sizeof(float) * (size_t)dims);
        if (copy == NULL) { free(key_buf); throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY); return; }
        (*env)->GetFloatArrayRegion(env, vector, 0, dims, copy);
        ptr = copy;
    }
    lattice_error rc = lattice_node_set_vector((lattice_txn *)(uintptr_t)txn_handle,
        (lattice_node_id)node_id, key_buf, ptr, (uint32_t)dims);
    free(copy);
    free(key_buf);
    check(env, rc);
}

/* ------------------------------------------------------------------ */
/* Batch insert                                                        */
/* ------------------------------------------------------------------ */

JNIEXPORT jlongArray JNICALL
Java_io_latticedb_Native_batchInsert(JNIEnv *env, jclass cls, jlong txn_handle,
                                     jstring label, jobjectArray vectors) {
    init_cache(env);
    (void)cls;
    size_t lb_len = 0;
    char *lb = jstring_to_utf8(env, label, &lb_len);
    if ((*env)->ExceptionCheck(env)) return NULL;
    jsize count = (*env)->GetArrayLength(env, vectors);
    lattice_node_with_vector *nodes = NULL;
    float **copies = NULL;
    if (count > 0) {
        nodes = (lattice_node_with_vector *)calloc((size_t)count, sizeof(*nodes));
        copies = (float **)calloc((size_t)count, sizeof(float *));
        if (nodes == NULL || copies == NULL) {
            free(nodes); free(copies); free(lb);
            throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
            return NULL;
        }
        for (jsize i = 0; i < count; i++) {
            nodes[i].label = lb;
            jfloatArray vec = (jfloatArray)(*env)->GetObjectArrayElement(env, vectors, i);
            jsize dims = vec ? (*env)->GetArrayLength(env, vec) : 0;
            if (dims > 0 && vec != NULL) {
                copies[i] = (float *)malloc(sizeof(float) * (size_t)dims);
                if (copies[i] == NULL) {
                    for (jsize j = 0; j < i; j++) free(copies[j]);
                    free(copies); free(nodes); free(lb);
                    throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
                    return NULL;
                }
                (*env)->GetFloatArrayRegion(env, vec, 0, dims, copies[i]);
                nodes[i].vector = copies[i];
                nodes[i].dimensions = (uint32_t)dims;
            }
            if (vec) (*env)->DeleteLocalRef(env, vec);
        }
    }
    lattice_node_id *ids = (lattice_node_id *)
        calloc(count > 0 ? (size_t)count : 1, sizeof(lattice_node_id));
    uint32_t created = 0;
    lattice_error rc = lattice_batch_insert((lattice_txn *)(uintptr_t)txn_handle,
        nodes, (uint32_t)(count > 0 ? count : 0), ids, &created);
    for (jsize i = 0; i < count; i++) free(copies ? copies[i] : NULL);
    free(copies); free(nodes); free(lb);
    if (!check(env, rc)) { free(ids); return NULL; }
    jlongArray arr = (*env)->NewLongArray(env, (jsize)created);
    if (arr != NULL && created > 0) {
        (*env)->SetLongArrayRegion(env, arr, 0, (jsize)created, (const jlong *)ids);
    }
    free(ids);
    return arr;
}

/* ------------------------------------------------------------------ */
/* Edge operations                                                     */
/* ------------------------------------------------------------------ */

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_edgeCreate(JNIEnv *env, jclass cls, jlong txn_handle,
                                    jlong source, jlong target, jstring edge_type) {
    init_cache(env);
    (void)cls;
    char *buf = edge_type ? jstring_to_utf8(env, edge_type, &(size_t){0})
                          : (char *)calloc(1, 1);
    if ((*env)->ExceptionCheck(env)) { free(buf); return 0; }
    lattice_edge_id id = 0;
    lattice_error rc = lattice_edge_create((lattice_txn *)(uintptr_t)txn_handle,
        (lattice_node_id)source, (lattice_node_id)target, buf, &id);
    free(buf);
    if (!check(env, rc)) return 0;
    return (jlong)id;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_edgeDelete(JNIEnv *env, jclass cls, jlong txn_handle,
                                    jlong source, jlong target, jstring edge_type) {
    init_cache(env);
    (void)cls;
    char *buf = edge_type ? jstring_to_utf8(env, edge_type, &(size_t){0}) : NULL;
    if ((*env)->ExceptionCheck(env)) return;
    lattice_error rc = lattice_edge_delete((lattice_txn *)(uintptr_t)txn_handle,
        (lattice_node_id)source, (lattice_node_id)target, buf);
    free(buf);
    check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_edgeSetProperty(JNIEnv *env, jclass cls, jlong txn_handle,
                                         jlong edge_id, jstring key, jobject value) {
    init_cache(env);
    (void)cls;
    lattice_value v;
    if (!fill_value(env, &v, value)) return;
    char *key_buf = key ? jstring_to_utf8(env, key, &(size_t){0}) : NULL;
    if ((*env)->ExceptionCheck(env)) { release_value_tree(&v); return; }
    lattice_error rc = lattice_edge_set_property((lattice_txn *)(uintptr_t)txn_handle,
        (lattice_edge_id)edge_id, key_buf, &v);
    free(key_buf);
    release_value_tree(&v);
    check(env, rc);
}

JNIEXPORT jobject JNICALL
Java_io_latticedb_Native_edgeGetProperty(JNIEnv *env, jclass cls, jlong txn_handle,
                                         jlong edge_id, jstring key) {
    init_cache(env);
    (void)cls;
    char *key_buf = key ? jstring_to_utf8(env, key, &(size_t){0}) : NULL;
    if ((*env)->ExceptionCheck(env)) return NULL;
    lattice_value v;
    memset(&v, 0, sizeof(v));
    lattice_error rc = lattice_edge_get_property((lattice_txn *)(uintptr_t)txn_handle,
        (lattice_edge_id)edge_id, key_buf, &v);
    free(key_buf);
    if (rc == LATTICE_ERROR_NOT_FOUND) return NULL; /* missing key -> null */
    if (!check(env, rc)) return NULL;
    return take_owned_value(env, &v);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_edgeRemoveProperty(JNIEnv *env, jclass cls, jlong txn_handle,
                                            jlong edge_id, jstring key) {
    init_cache(env);
    (void)cls;
    char *key_buf = key ? jstring_to_utf8(env, key, &(size_t){0}) : NULL;
    if ((*env)->ExceptionCheck(env)) return;
    lattice_error rc = lattice_edge_remove_property((lattice_txn *)(uintptr_t)txn_handle,
        (lattice_edge_id)edge_id, key_buf);
    free(key_buf);
    check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_createEdgePropertyIndex(JNIEnv *env, jclass cls,
        jlong db_handle, jstring edge_type, jstring property) {
    init_cache(env);
    (void)cls;
    char *tb = jstring_to_utf8(env, edge_type, &(size_t){0});
    char *pb = jstring_to_utf8(env, property, &(size_t){0});
    if ((*env)->ExceptionCheck(env)) { free(tb); free(pb); return; }
    lattice_error rc = lattice_edge_property_index_create(
        (lattice_database *)(uintptr_t)db_handle, tb, pb);
    free(tb); free(pb);
    check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_dropEdgePropertyIndex(JNIEnv *env, jclass cls,
        jlong db_handle, jstring edge_type, jstring property) {
    init_cache(env);
    (void)cls;
    char *tb = jstring_to_utf8(env, edge_type, &(size_t){0});
    char *pb = jstring_to_utf8(env, property, &(size_t){0});
    if ((*env)->ExceptionCheck(env)) { free(tb); free(pb); return; }
    lattice_error rc = lattice_edge_property_index_drop(
        (lattice_database *)(uintptr_t)db_handle, tb, pb);
    free(tb); free(pb);
    check(env, rc);
}

JNIEXPORT jlongArray JNICALL
Java_io_latticedb_Native_findEdgesByTypeProperty(JNIEnv *env, jclass cls,
        jlong txn_handle, jstring edge_type, jstring property, jobject value, jint limit) {
    init_cache(env);
    (void)cls;
    lattice_value v;
    if (!fill_value(env, &v, value)) return NULL;
    size_t tb_len = 0, pb_len = 0;
    char *tb = jstring_to_utf8(env, edge_type, &tb_len);
    char *pb = jstring_to_utf8(env, property, &pb_len);
    if ((*env)->ExceptionCheck(env)) {
        release_value_tree(&v); free(tb); free(pb); return NULL;
    }
    lattice_edge_id *ids = NULL;
    size_t count = 0;
    lattice_error rc = lattice_edges_find_by_type_property(
        (lattice_txn *)(uintptr_t)txn_handle, tb, pb, &v, (size_t)limit, &ids, &count);
    release_value_tree(&v); free(tb); free(pb);
    if (!check(env, rc)) return NULL;
    jlongArray arr = (*env)->NewLongArray(env, (jsize)count);
    if (arr != NULL && count > 0) {
        (*env)->SetLongArrayRegion(env, arr, 0, (jsize)count, (const jlong *)ids);
    }
    lattice_free_edge_ids(ids, count);
    return arr;
}

/* Fetch an edge result set. which: 0=outgoing,1=incoming,2=outgoing-by-type,
 * 3=incoming-by-type,4=scan.
 * Returns Object[2] = { long[] {id, source, target} * n, String[] types }. */
static jobjectArray fetch_edges(JNIEnv *env, jlong txn_handle, jint which,
                                jlong node_id, jstring edge_type, jint limit) {
    char *type_buf = NULL;
    if (which == 2 || which == 3 || which == 4) {
        type_buf = edge_type ? jstring_to_utf8(env, edge_type, &(size_t){0}) : NULL;
        if ((*env)->ExceptionCheck(env)) return NULL;
    } else if (edge_type != NULL) {
        type_buf = jstring_to_utf8(env, edge_type, &(size_t){0});
        if ((*env)->ExceptionCheck(env)) return NULL;
    }
    lattice_txn *txn = (lattice_txn *)(uintptr_t)txn_handle;
    lattice_edge_result *res = NULL;
    lattice_error rc = LATTICE_OK;
    switch (which) {
        case 0: rc = lattice_edge_get_outgoing(txn, (lattice_node_id)node_id, &res); break;
        case 1: rc = lattice_edge_get_incoming(txn, (lattice_node_id)node_id, &res); break;
        case 2: rc = lattice_edge_get_outgoing_by_type(txn, (lattice_node_id)node_id,
                    type_buf ? type_buf : "", (size_t)(limit < 0 ? 0 : limit), &res); break;
        case 3: rc = lattice_edge_get_incoming_by_type(txn, (lattice_node_id)node_id,
                    type_buf ? type_buf : "", (size_t)(limit < 0 ? 0 : limit), &res); break;
        case 4: rc = lattice_edge_scan(txn, type_buf, (size_t)(limit < 0 ? 0 : limit), &res); break;
        default: rc = LATTICE_ERROR_INVALID_ARG; break;
    }
    free(type_buf);
    if (!check(env, rc)) return NULL;

    uint32_t n = res ? lattice_edge_result_count(res) : 0;
    jlong *triples = (jlong *)malloc(sizeof(jlong) * 3 * (n > 0 ? n : 1));
    jobjectArray types = (*env)->NewObjectArray(env, (jsize)n, CLS_STRING, NULL);
    if (triples == NULL || types == NULL) {
        free(triples);
        lattice_edge_result_free(res);
        throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
        return NULL;
    }
    for (uint32_t i = 0; i < n; i++) {
        lattice_node_id src = 0, tgt = 0;
        lattice_edge_id eid = 0;
        const char *type = NULL;
        uint32_t type_len = 0;
        lattice_edge_result_get_id(res, i, &eid);
        lattice_edge_result_get(res, i, &src, &tgt, &type, &type_len);
        triples[3 * i + 0] = (jlong)eid;
        triples[3 * i + 1] = (jlong)src;
        triples[3 * i + 2] = (jlong)tgt;
        jstring jt = utf8_to_jstring(env, type, type_len);
        (*env)->SetObjectArrayElement(env, types, (jsize)i, jt);
        if (jt) (*env)->DeleteLocalRef(env, jt);
    }
    lattice_edge_result_free(res);

    jlongArray idsArr = (*env)->NewLongArray(env, (jsize)(3 * n));
    if (idsArr == NULL) {
        free(triples);
        throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
        return NULL;
    }
    if (n > 0) (*env)->SetLongArrayRegion(env, idsArr, 0, (jsize)(3 * n), triples);
    free(triples);

    jobjectArray out = (*env)->NewObjectArray(env, 2, CLS_OBJECT, NULL);
    if (out == NULL) return NULL;
    (*env)->SetObjectArrayElement(env, out, 0, idsArr);
    (*env)->SetObjectArrayElement(env, out, 1, types);
    return out;
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_edgesOutgoing(JNIEnv *env, jclass cls, jlong txn_handle,
                                       jlong node_id) {
    init_cache(env); (void)cls;
    return fetch_edges(env, txn_handle, 0, node_id, NULL, 0);
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_edgesIncoming(JNIEnv *env, jclass cls, jlong txn_handle,
                                       jlong node_id) {
    init_cache(env); (void)cls;
    return fetch_edges(env, txn_handle, 1, node_id, NULL, 0);
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_edgesOutgoingByType(JNIEnv *env, jclass cls, jlong txn_handle,
                                             jlong node_id, jstring edge_type, jint limit) {
    init_cache(env); (void)cls;
    return fetch_edges(env, txn_handle, 2, node_id, edge_type, limit);
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_edgesIncomingByType(JNIEnv *env, jclass cls, jlong txn_handle,
                                             jlong node_id, jstring edge_type, jint limit) {
    init_cache(env); (void)cls;
    return fetch_edges(env, txn_handle, 3, node_id, edge_type, limit);
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_edgesScan(JNIEnv *env, jclass cls, jlong txn_handle,
                                   jstring edge_type, jint limit) {
    init_cache(env); (void)cls;
    return fetch_edges(env, txn_handle, 4, 0, edge_type, limit);
}

/* ------------------------------------------------------------------ */
/* Vector search                                                       */
/* ------------------------------------------------------------------ */

/* Returns Object[2] = { long[] nodeIds, float[] distances }.
 * use_txn selects the txn/db variant. */
static jobject search_vectors(JNIEnv *env, jlong db_handle, jlong txn_handle,
                              jint use_txn, jfloatArray vector, jint k, jint ef_search) {
    jsize dims = vector ? (*env)->GetArrayLength(env, vector) : 0;
    float *copy = NULL;
    if (dims > 0) {
        copy = (float *)malloc(sizeof(float) * (size_t)dims);
        if (copy == NULL) { throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY); return NULL; }
        (*env)->GetFloatArrayRegion(env, vector, 0, dims, copy);
    }
    lattice_vector_result *res = NULL;
    lattice_error rc;
    if (use_txn) {
        rc = lattice_vector_search_txn((lattice_txn *)(uintptr_t)txn_handle,
            copy, (uint32_t)dims, (uint32_t)k, (uint16_t)ef_search, &res);
    } else {
        rc = lattice_vector_search((lattice_database *)(uintptr_t)db_handle,
            copy, (uint32_t)dims, (uint32_t)k, (uint16_t)ef_search, &res);
    }
    free(copy);
    if (!check(env, rc)) return NULL;

    uint32_t n = res ? lattice_vector_result_count(res) : 0;
    jlong *ids = (jlong *)malloc(sizeof(jlong) * (n > 0 ? n : 1));
    float *dists = (float *)malloc(sizeof(float) * (n > 0 ? n : 1));
    if (ids == NULL || dists == NULL) {
        free(ids); free(dists); lattice_vector_result_free(res);
        throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
        return NULL;
    }
    for (uint32_t i = 0; i < n; i++) {
        lattice_node_id id = 0;
        float d = 0.0f;
        lattice_vector_result_get(res, i, &id, &d);
        ids[i] = (jlong)id;
        dists[i] = d;
    }
    lattice_vector_result_free(res);

    jlongArray idsArr = (*env)->NewLongArray(env, (jsize)n);
    jfloatArray distsArr = (*env)->NewFloatArray(env, (jsize)n);
    if (idsArr == NULL || distsArr == NULL) {
        free(ids); free(dists);
        throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
        return NULL;
    }
    if (n > 0) {
        (*env)->SetLongArrayRegion(env, idsArr, 0, (jsize)n, ids);
        (*env)->SetFloatArrayRegion(env, distsArr, 0, (jsize)n, dists);
    }
    free(ids); free(dists);
    jobjectArray out = (*env)->NewObjectArray(env, 2, CLS_OBJECT, NULL);
    if (out == NULL) return NULL;
    (*env)->SetObjectArrayElement(env, out, 0, idsArr);
    (*env)->SetObjectArrayElement(env, out, 1, distsArr);
    return out;
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_vectorSearch(JNIEnv *env, jclass cls, jlong db_handle,
                                      jfloatArray vector, jint k, jint ef_search) {
    init_cache(env); (void)cls;
    return search_vectors(env, db_handle, 0, 0, vector, k, ef_search);
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_vectorSearchTxn(JNIEnv *env, jclass cls, jlong txn_handle,
                                         jfloatArray vector, jint k, jint ef_search) {
    init_cache(env); (void)cls;
    return search_vectors(env, 0, txn_handle, 1, vector, k, ef_search);
}

/* ------------------------------------------------------------------ */
/* Full-text search                                                    */
/* ------------------------------------------------------------------ */

JNIEXPORT void JNICALL
Java_io_latticedb_Native_createNodeFtsIndex(JNIEnv *env, jclass cls, jlong db_handle,
                                            jstring label, jstring property) {
    init_cache(env);
    (void)cls;
    size_t l_len = 0, p_len = 0;
    char *l = jstring_to_utf8(env, label, &l_len);
    if ((*env)->ExceptionCheck(env)) { free(l); return; }
    char *pr = jstring_to_utf8(env, property, &p_len);
    if ((*env)->ExceptionCheck(env)) { free(l); free(pr); return; }
    lattice_error rc = lattice_node_fts_index_create(
        (lattice_database *)(uintptr_t)db_handle, l ? l : "", pr ? pr : "");
    free(l); free(pr);
    check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_dropNodeFtsIndex(JNIEnv *env, jclass cls, jlong db_handle,
                                          jstring label, jstring property) {
    init_cache(env);
    (void)cls;
    size_t l_len = 0, p_len = 0;
    char *l = jstring_to_utf8(env, label, &l_len);
    if ((*env)->ExceptionCheck(env)) { free(l); return; }
    char *pr = jstring_to_utf8(env, property, &p_len);
    if ((*env)->ExceptionCheck(env)) { free(l); free(pr); return; }
    lattice_error rc = lattice_node_fts_index_drop(
        (lattice_database *)(uintptr_t)db_handle, l ? l : "", pr ? pr : "");
    free(l); free(pr);
    check(env, rc);
}

JNIEXPORT jboolean JNICALL
Java_io_latticedb_Native_hasNodeFtsIndex(JNIEnv *env, jclass cls, jlong db_handle,
                                         jstring label, jstring property) {
    init_cache(env);
    (void)cls;
    size_t l_len = 0, p_len = 0;
    char *l = jstring_to_utf8(env, label, &l_len);
    if ((*env)->ExceptionCheck(env)) { free(l); return JNI_FALSE; }
    char *pr = jstring_to_utf8(env, property, &p_len);
    if ((*env)->ExceptionCheck(env)) { free(l); free(pr); return JNI_FALSE; }
    bool exists = false;
    lattice_error rc = lattice_node_fts_index_exists(
        (lattice_database *)(uintptr_t)db_handle, l ? l : "", pr ? pr : "", &exists);
    free(l); free(pr);
    if (!check(env, rc)) return JNI_FALSE;
    return exists ? JNI_TRUE : JNI_FALSE;
}

/* Returns Object[2] = { long[] nodeIds, float[] scores }. fuzzy=1 uses the
 * fuzzy variant with max_distance/min_term_length. */
static jobject fts_search(JNIEnv *env, jlong db_handle, jlong txn_handle, jint use_txn,
                          jstring label, jstring property,
                          jstring query, jint limit, jint fuzzy,
                          jint max_distance, jint min_term_length) {
    size_t qlen = 0, l_len = 0, p_len = 0;
    char *q = query ? jstring_to_utf8(env, query, &qlen) : NULL;
    if ((*env)->ExceptionCheck(env)) { free(q); return NULL; }
    char *l = jstring_to_utf8(env, label, &l_len);
    if ((*env)->ExceptionCheck(env)) { free(q); free(l); return NULL; }
    char *pr = jstring_to_utf8(env, property, &p_len);
    if ((*env)->ExceptionCheck(env)) { free(q); free(l); free(pr); return NULL; }

    lattice_fts_result *res = NULL;
    lattice_error rc;
    if (fuzzy) {
        if (use_txn) {
            rc = lattice_fts_search_fuzzy_txn((lattice_txn *)(uintptr_t)txn_handle,
                l, pr, q ? q : "", qlen, (uint32_t)limit, (uint32_t)max_distance,
                (uint32_t)min_term_length, &res);
        } else {
            rc = lattice_fts_search_fuzzy((lattice_database *)(uintptr_t)db_handle,
                l, pr, q ? q : "", qlen, (uint32_t)limit, (uint32_t)max_distance,
                (uint32_t)min_term_length, &res);
        }
    } else {
        if (use_txn) {
            rc = lattice_fts_search_txn((lattice_txn *)(uintptr_t)txn_handle,
                l, pr, q ? q : "", qlen, (uint32_t)limit, &res);
        } else {
            rc = lattice_fts_search((lattice_database *)(uintptr_t)db_handle,
                l, pr, q ? q : "", qlen, (uint32_t)limit, &res);
        }
    }
    free(q); free(l); free(pr);
    if (!check(env, rc)) return NULL;

    uint32_t n = res ? lattice_fts_result_count(res) : 0;
    jlong *ids = (jlong *)malloc(sizeof(jlong) * (n > 0 ? n : 1));
    float *scores = (float *)malloc(sizeof(float) * (n > 0 ? n : 1));
    if (ids == NULL || scores == NULL) {
        free(ids); free(scores); lattice_fts_result_free(res);
        throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
        return NULL;
    }
    for (uint32_t i = 0; i < n; i++) {
        lattice_node_id id = 0;
        float score = 0.0f;
        lattice_fts_result_get(res, i, &id, &score);
        ids[i] = (jlong)id;
        scores[i] = score;
    }
    lattice_fts_result_free(res);

    jlongArray idsArr = (*env)->NewLongArray(env, (jsize)n);
    jfloatArray scoresArr = (*env)->NewFloatArray(env, (jsize)n);
    if (idsArr == NULL || scoresArr == NULL) {
        free(ids); free(scores);
        throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
        return NULL;
    }
    if (n > 0) {
        (*env)->SetLongArrayRegion(env, idsArr, 0, (jsize)n, ids);
        (*env)->SetFloatArrayRegion(env, scoresArr, 0, (jsize)n, scores);
    }
    free(ids); free(scores);
    jobjectArray out = (*env)->NewObjectArray(env, 2, CLS_OBJECT, NULL);
    if (out == NULL) return NULL;
    (*env)->SetObjectArrayElement(env, out, 0, idsArr);
    (*env)->SetObjectArrayElement(env, out, 1, scoresArr);
    return out;
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_ftsSearch(JNIEnv *env, jclass cls, jlong db_handle,
                                   jstring label, jstring property,
                                   jstring query, jint limit) {
    init_cache(env); (void)cls;
    return fts_search(env, db_handle, 0, 0, label, property, query, limit, 0, 0, 0);
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_ftsSearchFuzzy(JNIEnv *env, jclass cls, jlong db_handle,
                                        jstring label, jstring property,
                                        jstring query, jint limit, jint max_distance,
                                        jint min_term_length) {
    init_cache(env); (void)cls;
    return fts_search(env, db_handle, 0, 0, label, property, query, limit, 1,
                      max_distance, min_term_length);
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_ftsSearchTxn(JNIEnv *env, jclass cls, jlong txn_handle,
                                      jstring label, jstring property,
                                      jstring query, jint limit) {
    init_cache(env); (void)cls;
    return fts_search(env, 0, txn_handle, 1, label, property, query, limit, 0, 0, 0);
}

JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_ftsSearchFuzzyTxn(JNIEnv *env, jclass cls, jlong txn_handle,
                                           jstring label, jstring property,
                                           jstring query, jint limit, jint max_distance,
                                           jint min_term_length) {
    init_cache(env); (void)cls;
    return fts_search(env, 0, txn_handle, 1, label, property, query, limit, 1,
                      max_distance, min_term_length);
}

/* ------------------------------------------------------------------ */
/* Streams                                                             */
/* ------------------------------------------------------------------ */

static int publish_stream_common(JNIEnv *env, jlong txn_handle, jstring stream,
                                 jstring kind, jobject payload, jboolean want_seq,
                                 jlong *seq_out) {
    size_t s_len = 0, k_len = 0;
    char *s = jstring_to_utf8(env, stream, &s_len);
    char *k = kind ? jstring_to_utf8(env, kind, &k_len)
                   : (char *)calloc(1, 1); /* empty -> default kind "message" */
    lattice_value v;
    int filled = fill_value(env, &v, payload);
    if ((*env)->ExceptionCheck(env) || !filled) {
        free(s); free(k); release_value_tree(&v);
        return 0;
    }
    lattice_txn *txn = (lattice_txn *)(uintptr_t)txn_handle;
    lattice_error rc;
    if (want_seq == JNI_TRUE) {
        uint64_t sequence = 0;
        rc = lattice_stream_publish_get_sequence(txn, s, s_len, k, k ? k_len : 0, &v, &sequence);
        if (rc == LATTICE_OK && seq_out != NULL) {
            *seq_out = (jlong)sequence;
        }
    } else {
        rc = lattice_stream_publish(txn, s, s_len, k, k ? k_len : 0, &v);
    }
    free(s); free(k);
    release_value_tree(&v);
    return check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_streamPublish(JNIEnv *env, jclass cls, jlong txn_handle,
                                       jstring stream, jstring kind, jobject payload) {
    init_cache(env); (void)cls;
    publish_stream_common(env, txn_handle, stream, kind, payload, JNI_FALSE, NULL);
}

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_streamPublishGetSequence(JNIEnv *env, jclass cls,
        jlong txn_handle, jstring stream, jstring kind, jobject payload) {
    init_cache(env); (void)cls;
    jlong seq[1] = {0};
    if (!publish_stream_common(env, txn_handle, stream, kind, payload, JNI_TRUE, seq))
        return 0;
    return seq[0];
}

/* Returns Object[3] = { long[] sequences, String[] kinds, Object[] payloads }. */
JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_streamRead(JNIEnv *env, jclass cls, jlong db_handle,
                                    jstring stream, jlong after_sequence, jint limit,
                                    jint timeout_ms) {
    init_cache(env); (void)cls;
    size_t s_len = 0;
    char *s = jstring_to_utf8(env, stream, &s_len);
    if ((*env)->ExceptionCheck(env)) return NULL;
    lattice_stream_batch *batch = NULL;
    lattice_error rc = lattice_stream_read((lattice_database *)(uintptr_t)db_handle,
        s, s_len, (uint64_t)after_sequence, (size_t)(limit < 0 ? 0 : limit),
        (uint32_t)timeout_ms, &batch);
    free(s);
    if (!check(env, rc)) return NULL;

    size_t n = batch ? lattice_stream_batch_count(batch) : 0;
    jlong *seqs = (jlong *)malloc(sizeof(jlong) * (n > 0 ? n : 1));
    jobjectArray kinds = (*env)->NewObjectArray(env, (jsize)n, CLS_STRING, NULL);
    jobjectArray payloads = (*env)->NewObjectArray(env, (jsize)n, CLS_OBJECT, NULL);
    if (seqs == NULL || kinds == NULL || payloads == NULL) {
        free(seqs); lattice_stream_batch_free(batch);
        throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
        return NULL;
    }
    for (size_t i = 0; i < n; i++) {
        uint64_t seq = 0;
        const char *kind = NULL;
        size_t kind_len = 0;
        const lattice_value *payload = NULL;
        lattice_stream_batch_get(batch, i, &seq, &kind, &kind_len, &payload);
        seqs[i] = (jlong)seq;
        jstring jk = utf8_to_jstring(env, kind, kind_len);
        (*env)->SetObjectArrayElement(env, kinds, (jsize)i, jk);
        if (jk) (*env)->DeleteLocalRef(env, jk);
        jobject jp = payload ? value_to_java(env, payload) : NULL;
        (*env)->SetObjectArrayElement(env, payloads, (jsize)i, jp);
        if (jp) (*env)->DeleteLocalRef(env, jp);
    }
    lattice_stream_batch_free(batch);

    jlongArray seqsArr = (*env)->NewLongArray(env, (jsize)n);
    if (seqsArr == NULL) {
        free(seqs);
        throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY);
        return NULL;
    }
    if (n > 0) (*env)->SetLongArrayRegion(env, seqsArr, 0, (jsize)n, seqs);
    free(seqs);

    jobjectArray out = (*env)->NewObjectArray(env, 3, CLS_OBJECT, NULL);
    if (out == NULL) return NULL;
    (*env)->SetObjectArrayElement(env, out, 0, seqsArr);
    (*env)->SetObjectArrayElement(env, out, 1, kinds);
    (*env)->SetObjectArrayElement(env, out, 2, payloads);
    return out;
}

/* Returns Object[2] = { Long offset or null when !exists, Boolean exists }. */
JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_streamGetOffset(JNIEnv *env, jclass cls, jlong db_handle,
                                         jstring stream, jstring consumer) {
    init_cache(env); (void)cls;
    size_t s_len = 0, c_len = 0;
    char *s = jstring_to_utf8(env, stream, &s_len);
    char *c = consumer ? jstring_to_utf8(env, consumer, &c_len) : (char *)calloc(1, 1);
    if ((*env)->ExceptionCheck(env)) { free(s); free(c); return NULL; }
    bool offset_exists = false;
    uint64_t sequence = 0;
    lattice_error rc = lattice_stream_get_offset((lattice_database *)(uintptr_t)db_handle,
        s, s_len, c, c ? c_len : 0, &offset_exists, &sequence);
    free(s); free(c);
    if (!check(env, rc)) return NULL;

    jobjectArray out = (*env)->NewObjectArray(env, 2, CLS_OBJECT, NULL);
    if (out == NULL) return NULL;
    jobject off = (*env)->NewObject(env, CLS_LONG, MID_LONG_INIT, (jlong)sequence);
    jobject bex = (*env)->NewObject(env, CLS_BOOL, MID_BOOL_INIT,
                                    offset_exists ? JNI_TRUE : JNI_FALSE);
    (*env)->SetObjectArrayElement(env, out, 0, off);
    (*env)->SetObjectArrayElement(env, out, 1, bex);
    return out;
}

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_streamGetLastSequence(JNIEnv *env, jclass cls,
        jlong db_handle, jstring stream) {
    init_cache(env); (void)cls;
    size_t s_len = 0;
    char *s = jstring_to_utf8(env, stream, &s_len);
    if ((*env)->ExceptionCheck(env)) return 0;
    uint64_t seq = 0;
    lattice_error rc = lattice_stream_get_last_sequence(
        (lattice_database *)(uintptr_t)db_handle, s, s_len, &seq);
    free(s);
    if (!check(env, rc)) return 0;
    return (jlong)seq;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_streamSetOffset(JNIEnv *env, jclass cls, jlong txn_handle,
                                         jstring stream, jstring consumer, jlong sequence) {
    init_cache(env); (void)cls;
    size_t s_len = 0, c_len = 0;
    char *s = jstring_to_utf8(env, stream, &s_len);
    char *c = consumer ? jstring_to_utf8(env, consumer, &c_len) : (char *)calloc(1, 1);
    if ((*env)->ExceptionCheck(env)) { free(s); free(c); return; }
    lattice_error rc = lattice_stream_set_offset((lattice_txn *)(uintptr_t)txn_handle,
        s, s_len, c, c ? c_len : 0, (uint64_t)sequence);
    free(s); free(c);
    check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_streamTrim(JNIEnv *env, jclass cls, jlong txn_handle,
                                    jstring stream, jlong through_sequence) {
    init_cache(env); (void)cls;
    size_t s_len = 0;
    char *s = jstring_to_utf8(env, stream, &s_len);
    if ((*env)->ExceptionCheck(env)) return;
    lattice_error rc = lattice_stream_trim((lattice_txn *)(uintptr_t)txn_handle,
        s, s_len, (uint64_t)through_sequence);
    free(s);
    check(env, rc);
}

/* ------------------------------------------------------------------ */
/* Query engine                                                        */
/* ------------------------------------------------------------------ */

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_queryPrepare(JNIEnv *env, jclass cls, jlong db_handle,
                                      jstring cypher) {
    init_cache(env); (void)cls;
    size_t len = 0;
    char *buf = jstring_to_utf8(env, cypher, &len);
    if ((*env)->ExceptionCheck(env)) return 0;
    lattice_query *q = NULL;
    lattice_error rc = lattice_query_prepare((lattice_database *)(uintptr_t)db_handle,
        buf ? buf : "", &q);
    free(buf);
    if (!check(env, rc)) return 0;
    return (jlong)(uintptr_t)q;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_queryBind(JNIEnv *env, jclass cls, jlong query_handle,
                                   jstring name, jobject value) {
    init_cache(env); (void)cls;
    lattice_value v;
    if (!fill_value(env, &v, value)) return;
    char *nb = name ? jstring_to_utf8(env, name, &(size_t){0}) : NULL;
    if ((*env)->ExceptionCheck(env)) { release_value_tree(&v); return; }
    lattice_error rc = lattice_query_bind((lattice_query *)(uintptr_t)query_handle,
        nb ? nb : "", &v);
    free(nb);
    release_value_tree(&v);
    check(env, rc);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_queryBindVector(JNIEnv *env, jclass cls, jlong query_handle,
                                         jstring name, jfloatArray vector) {
    init_cache(env); (void)cls;
    char *nb = name ? jstring_to_utf8(env, name, &(size_t){0}) : NULL;
    if ((*env)->ExceptionCheck(env)) return;
    jsize dims = vector ? (*env)->GetArrayLength(env, vector) : 0;
    float *copy = NULL;
    if (dims > 0) {
        copy = (float *)malloc(sizeof(float) * (size_t)dims);
        if (copy == NULL) { free(nb); throw_lattice(env, LATTICE_ERROR_OUT_OF_MEMORY); return; }
        (*env)->GetFloatArrayRegion(env, vector, 0, dims, copy);
    }
    lattice_error rc = lattice_query_bind_vector((lattice_query *)(uintptr_t)query_handle,
        nb ? nb : "", copy, (uint32_t)dims);
    free(copy);
    free(nb);
    check(env, rc);
}

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_queryExecute(JNIEnv *env, jclass cls, jlong query_handle,
                                      jlong txn_handle) {
    init_cache(env); (void)cls;
    lattice_result *result = NULL;
    lattice_error rc = lattice_query_execute((lattice_query *)(uintptr_t)query_handle,
        (lattice_txn *)(uintptr_t)txn_handle, &result);
    if (rc != LATTICE_OK) {
        throw_query_error(env, (lattice_query *)(uintptr_t)query_handle);
        return 0;
    }
    return (jlong)(uintptr_t)result;
}

JNIEXPORT jboolean JNICALL
Java_io_latticedb_Native_queryWrites(JNIEnv *env, jclass cls, jlong db_handle,
                                     jstring cypher) {
    init_cache(env); (void)cls;
    /* Prepare a scratch query to ask whether it writes. */
    size_t len = 0;
    char *buf = jstring_to_utf8(env, cypher, &len);
    if ((*env)->ExceptionCheck(env)) return JNI_FALSE;
    lattice_query *q = NULL;
    lattice_error rc = lattice_query_prepare((lattice_database *)(uintptr_t)db_handle,
        buf ? buf : "", &q);
    free(buf);
    if (rc != LATTICE_OK || q == NULL) return JNI_FALSE; /* parse fails: weaker tx is fine */
    jboolean writes = lattice_query_writes(q) ? JNI_TRUE : JNI_FALSE;
    lattice_query_free(q);
    return writes;
}

JNIEXPORT jboolean JNICALL
Java_io_latticedb_Native_resultNext(JNIEnv *env, jclass cls, jlong result_handle) {
    init_cache(env); (void)cls;
    return lattice_result_next((lattice_result *)(uintptr_t)result_handle)
               ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jint JNICALL
Java_io_latticedb_Native_resultColumnCount(JNIEnv *env, jclass cls, jlong result_handle) {
    init_cache(env); (void)cls;
    return (jint)lattice_result_column_count((lattice_result *)(uintptr_t)result_handle);
}

JNIEXPORT jstring JNICALL
Java_io_latticedb_Native_resultColumnName(JNIEnv *env, jclass cls, jlong result_handle,
                                          jint index) {
    init_cache(env); (void)cls;
    const char *name = lattice_result_column_name(
        (lattice_result *)(uintptr_t)result_handle, (uint32_t)index);
    return name ? (*env)->NewStringUTF(env, name) : NULL;
}

JNIEXPORT jobject JNICALL
Java_io_latticedb_Native_resultGet(JNIEnv *env, jclass cls, jlong result_handle,
                                   jint index) {
    init_cache(env); (void)cls;
    lattice_value v;
    memset(&v, 0, sizeof(v));
    lattice_error rc = lattice_result_get((lattice_result *)(uintptr_t)result_handle,
        (uint32_t)index, &v);
    if (!check(env, rc)) return NULL;
    /* Borrowed from the result handle: convert only. */
    jobject out = value_to_java(env, &v);
    return out;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_resultFree(JNIEnv *env, jclass cls, jlong result_handle) {
    init_cache(env); (void)cls;
    if (result_handle == 0) return;
    lattice_result_free((lattice_result *)(uintptr_t)result_handle);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_queryFree(JNIEnv *env, jclass cls, jlong query_handle) {
    init_cache(env); (void)cls;
    if (query_handle == 0) return;
    lattice_query_free((lattice_query *)(uintptr_t)query_handle);
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_cacheClear(JNIEnv *env, jclass cls, jlong db_handle) {
    init_cache(env); (void)cls;
    check(env, lattice_query_cache_clear((lattice_database *)(uintptr_t)db_handle));
}

/* Returns Object[3] = { Integer entries, Long hits, Long misses }. */
JNIEXPORT jobjectArray JNICALL
Java_io_latticedb_Native_cacheStats(JNIEnv *env, jclass cls, jlong db_handle) {
    init_cache(env); (void)cls;
    uint32_t entries = 0;
    uint64_t hits = 0, misses = 0;
    lattice_error rc = lattice_query_cache_stats(
        (lattice_database *)(uintptr_t)db_handle, &entries, &hits, &misses);
    if (!check(env, rc)) return NULL;
    jobjectArray out = (*env)->NewObjectArray(env, 3, CLS_OBJECT, NULL);
    if (out == NULL) return NULL;
    jobject e = (*env)->NewObject(env, CLS_INTEGER, MID_INT_INIT, (jint)entries);
    jobject h = (*env)->NewObject(env, CLS_LONG, MID_LONG_INIT, (jlong)hits);
    jobject m = (*env)->NewObject(env, CLS_LONG, MID_LONG_INIT, (jlong)misses);
    (*env)->SetObjectArrayElement(env, out, 0, e);
    (*env)->SetObjectArrayElement(env, out, 1, h);
    (*env)->SetObjectArrayElement(env, out, 2, m);
    return out;
}

/* ------------------------------------------------------------------ */
/* Embedding helpers                                                   */
/* ------------------------------------------------------------------ */

JNIEXPORT jfloatArray JNICALL
Java_io_latticedb_Native_hashEmbed(JNIEnv *env, jclass cls, jstring text,
                                   jint dimensions) {
    init_cache(env); (void)cls;
    size_t len = 0;
    char *buf = jstring_to_utf8(env, text, &len);
    if ((*env)->ExceptionCheck(env)) return NULL;
    float *vector = NULL;
    uint32_t dims = 0;
    lattice_error rc = lattice_hash_embed(buf ? buf : "", len,
                                          (uint16_t)dimensions, &vector, &dims);
    free(buf);
    if (!check(env, rc)) return NULL;
    jfloatArray arr = (*env)->NewFloatArray(env, (jsize)dims);
    if (arr != NULL && dims > 0) {
        (*env)->SetFloatArrayRegion(env, arr, 0, (jsize)dims, vector);
    }
    lattice_hash_embed_free(vector, dims);
    return arr;
}

JNIEXPORT jlong JNICALL
Java_io_latticedb_Native_embeddingClientCreate(JNIEnv *env, jclass cls,
        jstring endpoint, jstring model, jint api_format, jstring api_key,
        jint timeout_ms) {
    init_cache(env); (void)cls;
    lattice_embedding_config config;
    memset(&config, 0, sizeof(config));
    config.endpoint = NULL;
    config.model = NULL;
    config.api_format = (lattice_embedding_api_format)api_format;
    config.api_key = NULL;
    config.timeout_ms = (uint32_t)(timeout_ms <= 0 ? 0 : timeout_ms);

    size_t dummy = 0;
    char *ep = endpoint ? jstring_to_utf8(env, endpoint, &dummy) : NULL;
    char *md = model ? jstring_to_utf8(env, model, &dummy) : NULL;
    char *ak = api_key ? jstring_to_utf8(env, api_key, &dummy) : NULL;
    if ((*env)->ExceptionCheck(env)) { free(ep); free(md); free(ak); return 0; }
    config.endpoint = ep;
    config.model = md;
    config.api_key = ak;

    lattice_embedding_client *client = NULL;
    lattice_error rc = lattice_embedding_client_create(&config, &client);
    free(ep); free(md); free(ak);
    if (!check(env, rc)) return 0;
    return (jlong)(uintptr_t)client;
}

JNIEXPORT jfloatArray JNICALL
Java_io_latticedb_Native_embeddingClientEmbed(JNIEnv *env, jclass cls,
        jlong client_handle, jstring text) {
    init_cache(env); (void)cls;
    size_t len = 0;
    char *buf = jstring_to_utf8(env, text, &len);
    if ((*env)->ExceptionCheck(env)) return NULL;
    float *vector = NULL;
    uint32_t dims = 0;
    lattice_error rc = lattice_embedding_client_embed(
        (lattice_embedding_client *)(uintptr_t)client_handle, buf ? buf : "", len,
        &vector, &dims);
    free(buf);
    if (!check(env, rc)) return NULL;
    jfloatArray arr = (*env)->NewFloatArray(env, (jsize)dims);
    if (arr != NULL && dims > 0) {
        (*env)->SetFloatArrayRegion(env, arr, 0, (jsize)dims, vector);
    }
    lattice_hash_embed_free(vector, dims);
    return arr;
}

JNIEXPORT void JNICALL
Java_io_latticedb_Native_embeddingClientFree(JNIEnv *env, jclass cls,
                                             jlong client_handle) {
    init_cache(env); (void)cls;
    if (client_handle == 0) return;
    lattice_embedding_client_free((lattice_embedding_client *)(uintptr_t)client_handle);
}
