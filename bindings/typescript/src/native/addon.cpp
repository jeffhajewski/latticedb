/**
 * N-API native addon for Lattice database.
 *
 * This file provides the C++ bridge between Node.js and the Lattice C API.
 */

#include <napi.h>
#include <vector>
#include <string>

extern "C" {
#include "lattice.h"
}

// Forward declarations
class TransactionWrapper;

/**
 * Helper to throw a JavaScript error from a Lattice error code.
 */
static void ThrowLatticeError(Napi::Env env, lattice_error err) {
    const char* msg = lattice_error_message(err);
    Napi::Error::New(env, msg ? msg : "Unknown error").ThrowAsJavaScriptException();
}

/**
 * Convert a JavaScript value to a lattice_value.
 * Returns true on success, false on error (exception will be pending).
 */
static bool JsToLatticeValue(Napi::Env env, Napi::Value jsVal, lattice_value* out, std::string& strHolder) {
    if (jsVal.IsNull() || jsVal.IsUndefined()) {
        out->type = LATTICE_VALUE_NULL;
        return true;
    }
    if (jsVal.IsBoolean()) {
        out->type = LATTICE_VALUE_BOOL;
        out->data.bool_val = jsVal.As<Napi::Boolean>().Value();
        return true;
    }
    if (jsVal.IsNumber()) {
        double num = jsVal.As<Napi::Number>().DoubleValue();
        // Check if it's an integer
        if (num == static_cast<int64_t>(num) && num >= INT64_MIN && num <= INT64_MAX) {
            out->type = LATTICE_VALUE_INT;
            out->data.int_val = static_cast<int64_t>(num);
        } else {
            out->type = LATTICE_VALUE_FLOAT;
            out->data.float_val = num;
        }
        return true;
    }
    if (jsVal.IsBigInt()) {
        bool lossless;
        int64_t val = jsVal.As<Napi::BigInt>().Int64Value(&lossless);
        if (!lossless) {
            Napi::Error::New(env, "BigInt value out of range").ThrowAsJavaScriptException();
            return false;
        }
        out->type = LATTICE_VALUE_INT;
        out->data.int_val = val;
        return true;
    }
    if (jsVal.IsString()) {
        strHolder = jsVal.As<Napi::String>().Utf8Value();
        out->type = LATTICE_VALUE_STRING;
        out->data.string_val.ptr = strHolder.c_str();
        out->data.string_val.len = strHolder.length();
        return true;
    }
    if (jsVal.IsTypedArray()) {
        Napi::TypedArray arr = jsVal.As<Napi::TypedArray>();
        if (arr.TypedArrayType() == napi_float32_array) {
            Napi::Float32Array f32 = arr.As<Napi::Float32Array>();
            out->type = LATTICE_VALUE_VECTOR;
            out->data.vector_val.ptr = f32.Data();
            out->data.vector_val.dimensions = static_cast<uint32_t>(f32.ElementLength());
            return true;
        }
        if (arr.TypedArrayType() == napi_uint8_array) {
            Napi::Uint8Array u8 = arr.As<Napi::Uint8Array>();
            out->type = LATTICE_VALUE_BYTES;
            out->data.bytes_val.ptr = u8.Data();
            out->data.bytes_val.len = u8.ElementLength();
            return true;
        }
    }
    Napi::TypeError::New(env, "Unsupported value type").ThrowAsJavaScriptException();
    return false;
}

/**
 * Convert a lattice_value to a JavaScript value.
 */
static Napi::Value LatticeValueToJs(Napi::Env env, const lattice_value* val) {
    switch (val->type) {
        case LATTICE_VALUE_NULL:
            return env.Null();
        case LATTICE_VALUE_BOOL:
            return Napi::Boolean::New(env, val->data.bool_val);
        case LATTICE_VALUE_INT:
            return Napi::BigInt::New(env, val->data.int_val);
        case LATTICE_VALUE_FLOAT:
            return Napi::Number::New(env, val->data.float_val);
        case LATTICE_VALUE_STRING:
            if (val->data.string_val.ptr && val->data.string_val.len > 0) {
                return Napi::String::New(env, val->data.string_val.ptr, val->data.string_val.len);
            }
            return Napi::String::New(env, "");
        case LATTICE_VALUE_BYTES:
            if (val->data.bytes_val.ptr && val->data.bytes_val.len > 0) {
                auto buf = Napi::Uint8Array::New(env, val->data.bytes_val.len);
                memcpy(buf.Data(), val->data.bytes_val.ptr, val->data.bytes_val.len);
                return buf;
            }
            return Napi::Uint8Array::New(env, 0);
        case LATTICE_VALUE_VECTOR:
            if (val->data.vector_val.ptr && val->data.vector_val.dimensions > 0) {
                auto buf = Napi::Float32Array::New(env, val->data.vector_val.dimensions);
                memcpy(buf.Data(), val->data.vector_val.ptr, val->data.vector_val.dimensions * sizeof(float));
                return buf;
            }
            return Napi::Float32Array::New(env, 0);
        default:
            return env.Null();
    }
}

/**
 * Wrapper for lattice_txn handle.
 */
class TransactionWrapper : public Napi::ObjectWrap<TransactionWrapper> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    static Napi::Object NewInstance(Napi::Env env, lattice_txn* txn);
    TransactionWrapper(const Napi::CallbackInfo& info);
    ~TransactionWrapper();

    lattice_txn* GetHandle() { return txn_; }
    void ClearHandle() { txn_ = nullptr; }

private:
    static Napi::FunctionReference constructor;
    lattice_txn* txn_ = nullptr;

    Napi::Value Commit(const Napi::CallbackInfo& info);
    Napi::Value Rollback(const Napi::CallbackInfo& info);
    Napi::Value CreateNode(const Napi::CallbackInfo& info);
    Napi::Value DeleteNode(const Napi::CallbackInfo& info);
    Napi::Value NodeExists(const Napi::CallbackInfo& info);
    Napi::Value GetLabels(const Napi::CallbackInfo& info);
    Napi::Value SetProperty(const Napi::CallbackInfo& info);
    Napi::Value GetProperty(const Napi::CallbackInfo& info);
    Napi::Value SetVector(const Napi::CallbackInfo& info);
    Napi::Value FtsIndex(const Napi::CallbackInfo& info);
    Napi::Value CreateEdge(const Napi::CallbackInfo& info);
    Napi::Value DeleteEdge(const Napi::CallbackInfo& info);
    Napi::Value GetOutgoingEdges(const Napi::CallbackInfo& info);
    Napi::Value GetIncomingEdges(const Napi::CallbackInfo& info);
};

Napi::FunctionReference TransactionWrapper::constructor;

Napi::Object TransactionWrapper::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "Transaction", {
        InstanceMethod("commit", &TransactionWrapper::Commit),
        InstanceMethod("rollback", &TransactionWrapper::Rollback),
        InstanceMethod("createNode", &TransactionWrapper::CreateNode),
        InstanceMethod("deleteNode", &TransactionWrapper::DeleteNode),
        InstanceMethod("nodeExists", &TransactionWrapper::NodeExists),
        InstanceMethod("getLabels", &TransactionWrapper::GetLabels),
        InstanceMethod("setProperty", &TransactionWrapper::SetProperty),
        InstanceMethod("getProperty", &TransactionWrapper::GetProperty),
        InstanceMethod("setVector", &TransactionWrapper::SetVector),
        InstanceMethod("ftsIndex", &TransactionWrapper::FtsIndex),
        InstanceMethod("createEdge", &TransactionWrapper::CreateEdge),
        InstanceMethod("deleteEdge", &TransactionWrapper::DeleteEdge),
        InstanceMethod("getOutgoingEdges", &TransactionWrapper::GetOutgoingEdges),
        InstanceMethod("getIncomingEdges", &TransactionWrapper::GetIncomingEdges),
    });

    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();

    exports.Set("Transaction", func);
    return exports;
}

Napi::Object TransactionWrapper::NewInstance(Napi::Env env, lattice_txn* txn) {
    Napi::Object obj = constructor.New({});
    TransactionWrapper* wrapper = Napi::ObjectWrap<TransactionWrapper>::Unwrap(obj);
    wrapper->txn_ = txn;
    return obj;
}

TransactionWrapper::TransactionWrapper(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<TransactionWrapper>(info) {}

TransactionWrapper::~TransactionWrapper() {
    if (txn_ != nullptr) {
        lattice_rollback(txn_);
        txn_ = nullptr;
    }
}

Napi::Value TransactionWrapper::Commit(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction already closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    lattice_error err = lattice_commit(txn_);
    txn_ = nullptr;
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
    }
    return env.Undefined();
}

Napi::Value TransactionWrapper::Rollback(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        return env.Undefined();
    }
    lattice_error err = lattice_rollback(txn_);
    txn_ = nullptr;
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
    }
    return env.Undefined();
}

Napi::Value TransactionWrapper::CreateNode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::string label = "";
    if (info.Length() > 0 && info[0].IsString()) {
        label = info[0].As<Napi::String>().Utf8Value();
    }

    lattice_node_id node_id;
    lattice_error err = lattice_node_create(txn_, label.c_str(), &node_id);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    return Napi::BigInt::New(env, static_cast<uint64_t>(node_id));
}

Napi::Value TransactionWrapper::DeleteNode(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 1 || !info[0].IsBigInt()) {
        Napi::TypeError::New(env, "Node ID must be a BigInt").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t node_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);

    lattice_error err = lattice_node_delete(txn_, node_id);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
    }
    return env.Undefined();
}

Napi::Value TransactionWrapper::NodeExists(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 1 || !info[0].IsBigInt()) {
        Napi::TypeError::New(env, "Node ID must be a BigInt").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t node_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);

    bool exists = false;
    lattice_error err = lattice_node_exists(txn_, node_id, &exists);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    return Napi::Boolean::New(env, exists);
}

Napi::Value TransactionWrapper::GetLabels(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 1 || !info[0].IsBigInt()) {
        Napi::TypeError::New(env, "Node ID must be a BigInt").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t node_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);

    char* labels = nullptr;
    lattice_error err = lattice_node_get_labels(txn_, node_id, &labels);
    if (err == LATTICE_ERROR_NOT_FOUND) {
        return env.Null();
    }
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    Napi::Array result = Napi::Array::New(env);
    if (labels && labels[0] != '\0') {
        std::string labelsStr(labels);
        size_t pos = 0;
        size_t idx = 0;
        std::string token;
        while ((pos = labelsStr.find(',')) != std::string::npos) {
            token = labelsStr.substr(0, pos);
            result.Set(idx++, Napi::String::New(env, token));
            labelsStr.erase(0, pos + 1);
        }
        if (!labelsStr.empty()) {
            result.Set(idx, Napi::String::New(env, labelsStr));
        }
    }

    if (labels) {
        lattice_free_string(labels);
    }

    return result;
}

Napi::Value TransactionWrapper::SetProperty(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 3) {
        Napi::TypeError::New(env, "Expected (nodeId, key, value)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (!info[0].IsBigInt()) {
        Napi::TypeError::New(env, "Node ID must be a BigInt").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    if (!info[1].IsString()) {
        Napi::TypeError::New(env, "Key must be a string").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t node_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);
    std::string key = info[1].As<Napi::String>().Utf8Value();

    lattice_value val;
    std::string strHolder;
    if (!JsToLatticeValue(env, info[2], &val, strHolder)) {
        return env.Undefined();
    }

    lattice_error err = lattice_node_set_property(txn_, node_id, key.c_str(), &val);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
    }
    return env.Undefined();
}

Napi::Value TransactionWrapper::GetProperty(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 2) {
        Napi::TypeError::New(env, "Expected (nodeId, key)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (!info[0].IsBigInt()) {
        Napi::TypeError::New(env, "Node ID must be a BigInt").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    if (!info[1].IsString()) {
        Napi::TypeError::New(env, "Key must be a string").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t node_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);
    std::string key = info[1].As<Napi::String>().Utf8Value();

    lattice_value val;
    val.type = LATTICE_VALUE_NULL;
    lattice_error err = lattice_node_get_property(txn_, node_id, key.c_str(), &val);
    if (err == LATTICE_ERROR_NOT_FOUND) {
        return env.Null();
    }
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    return LatticeValueToJs(env, &val);
}

Napi::Value TransactionWrapper::SetVector(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 3) {
        Napi::TypeError::New(env, "Expected (nodeId, key, vector)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (!info[0].IsBigInt()) {
        Napi::TypeError::New(env, "Node ID must be a BigInt").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    if (!info[1].IsString()) {
        Napi::TypeError::New(env, "Key must be a string").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    if (!info[2].IsTypedArray() || info[2].As<Napi::TypedArray>().TypedArrayType() != napi_float32_array) {
        Napi::TypeError::New(env, "Vector must be a Float32Array").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t node_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);
    std::string key = info[1].As<Napi::String>().Utf8Value();
    Napi::Float32Array vec = info[2].As<Napi::Float32Array>();

    lattice_error err = lattice_node_set_vector(txn_, node_id, key.c_str(), vec.Data(), static_cast<uint32_t>(vec.ElementLength()));
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
    }
    return env.Undefined();
}

Napi::Value TransactionWrapper::FtsIndex(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 2) {
        Napi::TypeError::New(env, "Expected (nodeId, text)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (!info[0].IsBigInt()) {
        Napi::TypeError::New(env, "Node ID must be a BigInt").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    if (!info[1].IsString()) {
        Napi::TypeError::New(env, "Text must be a string").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t node_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);
    std::string text = info[1].As<Napi::String>().Utf8Value();

    lattice_error err = lattice_fts_index(txn_, node_id, text.c_str(), text.length());
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
    }
    return env.Undefined();
}

Napi::Value TransactionWrapper::CreateEdge(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 3) {
        Napi::TypeError::New(env, "Expected (sourceId, targetId, edgeType)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (!info[0].IsBigInt() || !info[1].IsBigInt()) {
        Napi::TypeError::New(env, "Source and target IDs must be BigInts").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    if (!info[2].IsString()) {
        Napi::TypeError::New(env, "Edge type must be a string").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t source_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);
    uint64_t target_id = info[1].As<Napi::BigInt>().Uint64Value(&lossless);
    std::string edge_type = info[2].As<Napi::String>().Utf8Value();

    lattice_edge_id edge_id;
    lattice_error err = lattice_edge_create(txn_, source_id, target_id, edge_type.c_str(), &edge_id);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    return Napi::BigInt::New(env, static_cast<uint64_t>(edge_id));
}

Napi::Value TransactionWrapper::DeleteEdge(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 3) {
        Napi::TypeError::New(env, "Expected (sourceId, targetId, edgeType)").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (!info[0].IsBigInt() || !info[1].IsBigInt()) {
        Napi::TypeError::New(env, "Source and target IDs must be BigInts").ThrowAsJavaScriptException();
        return env.Undefined();
    }
    if (!info[2].IsString()) {
        Napi::TypeError::New(env, "Edge type must be a string").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t source_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);
    uint64_t target_id = info[1].As<Napi::BigInt>().Uint64Value(&lossless);
    std::string edge_type = info[2].As<Napi::String>().Utf8Value();

    lattice_error err = lattice_edge_delete(txn_, source_id, target_id, edge_type.c_str());
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
    }
    return env.Undefined();
}

Napi::Value TransactionWrapper::GetOutgoingEdges(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 1 || !info[0].IsBigInt()) {
        Napi::TypeError::New(env, "Node ID must be a BigInt").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t node_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);

    lattice_edge_result* result = nullptr;
    lattice_error err = lattice_edge_get_outgoing(txn_, node_id, &result);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    Napi::Array edges = Napi::Array::New(env);
    uint32_t count = lattice_edge_result_count(result);
    for (uint32_t i = 0; i < count; i++) {
        lattice_node_id source, target;
        const char* type;
        uint32_t type_len;
        err = lattice_edge_result_get(result, i, &source, &target, &type, &type_len);
        if (err == LATTICE_OK) {
            Napi::Object edge = Napi::Object::New(env);
            edge.Set("sourceId", Napi::BigInt::New(env, static_cast<uint64_t>(source)));
            edge.Set("targetId", Napi::BigInt::New(env, static_cast<uint64_t>(target)));
            edge.Set("type", Napi::String::New(env, type, type_len));
            edges.Set(i, edge);
        }
    }

    lattice_edge_result_free(result);
    return edges;
}

Napi::Value TransactionWrapper::GetIncomingEdges(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (txn_ == nullptr) {
        Napi::Error::New(env, "Transaction closed").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 1 || !info[0].IsBigInt()) {
        Napi::TypeError::New(env, "Node ID must be a BigInt").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    bool lossless;
    uint64_t node_id = info[0].As<Napi::BigInt>().Uint64Value(&lossless);

    lattice_edge_result* result = nullptr;
    lattice_error err = lattice_edge_get_incoming(txn_, node_id, &result);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    Napi::Array edges = Napi::Array::New(env);
    uint32_t count = lattice_edge_result_count(result);
    for (uint32_t i = 0; i < count; i++) {
        lattice_node_id source, target;
        const char* type;
        uint32_t type_len;
        err = lattice_edge_result_get(result, i, &source, &target, &type, &type_len);
        if (err == LATTICE_OK) {
            Napi::Object edge = Napi::Object::New(env);
            edge.Set("sourceId", Napi::BigInt::New(env, static_cast<uint64_t>(source)));
            edge.Set("targetId", Napi::BigInt::New(env, static_cast<uint64_t>(target)));
            edge.Set("type", Napi::String::New(env, type, type_len));
            edges.Set(i, edge);
        }
    }

    lattice_edge_result_free(result);
    return edges;
}

/**
 * Wrapper for lattice_database handle.
 */
class DatabaseWrapper : public Napi::ObjectWrap<DatabaseWrapper> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    DatabaseWrapper(const Napi::CallbackInfo& info);
    ~DatabaseWrapper();

private:
    static Napi::FunctionReference constructor;
    lattice_database* db_ = nullptr;

    Napi::Value Open(const Napi::CallbackInfo& info);
    Napi::Value Close(const Napi::CallbackInfo& info);
    Napi::Value Begin(const Napi::CallbackInfo& info);
    Napi::Value IsOpen(const Napi::CallbackInfo& info);
    Napi::Value Query(const Napi::CallbackInfo& info);
    Napi::Value VectorSearch(const Napi::CallbackInfo& info);
    Napi::Value FtsSearch(const Napi::CallbackInfo& info);
    Napi::Value CacheClear(const Napi::CallbackInfo& info);
    Napi::Value CacheStats(const Napi::CallbackInfo& info);
};

Napi::FunctionReference DatabaseWrapper::constructor;

Napi::Object DatabaseWrapper::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "Database", {
        InstanceMethod("open", &DatabaseWrapper::Open),
        InstanceMethod("close", &DatabaseWrapper::Close),
        InstanceMethod("begin", &DatabaseWrapper::Begin),
        InstanceMethod("isOpen", &DatabaseWrapper::IsOpen),
        InstanceMethod("query", &DatabaseWrapper::Query),
        InstanceMethod("vectorSearch", &DatabaseWrapper::VectorSearch),
        InstanceMethod("ftsSearch", &DatabaseWrapper::FtsSearch),
        InstanceMethod("cacheClear", &DatabaseWrapper::CacheClear),
        InstanceMethod("cacheStats", &DatabaseWrapper::CacheStats),
    });

    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();

    exports.Set("Database", func);
    return exports;
}

DatabaseWrapper::DatabaseWrapper(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<DatabaseWrapper>(info) {}

DatabaseWrapper::~DatabaseWrapper() {
    if (db_ != nullptr) {
        lattice_close(db_);
        db_ = nullptr;
    }
}

Napi::Value DatabaseWrapper::Open(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (info.Length() < 1 || !info[0].IsString()) {
        Napi::TypeError::New(env, "Path must be a string").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::string path = info[0].As<Napi::String>().Utf8Value();

    lattice_open_options options = LATTICE_OPEN_OPTIONS_DEFAULT;

    if (info.Length() > 1 && info[1].IsObject()) {
        Napi::Object opts = info[1].As<Napi::Object>();
        if (opts.Has("create") && opts.Get("create").IsBoolean()) {
            options.create = opts.Get("create").As<Napi::Boolean>().Value();
        }
        if (opts.Has("readOnly") && opts.Get("readOnly").IsBoolean()) {
            options.read_only = opts.Get("readOnly").As<Napi::Boolean>().Value();
        }
        if (opts.Has("cacheSizeMb") && opts.Get("cacheSizeMb").IsNumber()) {
            options.cache_size_mb = opts.Get("cacheSizeMb").As<Napi::Number>().Uint32Value();
        }
        if (opts.Has("enableVector") && opts.Get("enableVector").IsBoolean()) {
            options.enable_vector = opts.Get("enableVector").As<Napi::Boolean>().Value();
        }
        if (opts.Has("vectorDimensions") && opts.Get("vectorDimensions").IsNumber()) {
            options.vector_dimensions = static_cast<uint16_t>(opts.Get("vectorDimensions").As<Napi::Number>().Uint32Value());
        }
    }

    lattice_error err = lattice_open(path.c_str(), &options, &db_);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    return env.Undefined();
}

Napi::Value DatabaseWrapper::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (db_ != nullptr) {
        lattice_error err = lattice_close(db_);
        db_ = nullptr;
        if (err != LATTICE_OK) {
            ThrowLatticeError(env, err);
        }
    }

    return env.Undefined();
}

Napi::Value DatabaseWrapper::Begin(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (db_ == nullptr) {
        Napi::Error::New(env, "Database not open").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    lattice_txn_mode mode = LATTICE_TXN_READ_WRITE;
    if (info.Length() > 0 && info[0].IsBoolean() && info[0].As<Napi::Boolean>().Value()) {
        mode = LATTICE_TXN_READ_ONLY;
    }

    lattice_txn* txn = nullptr;
    lattice_error err = lattice_begin(db_, mode, &txn);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    return TransactionWrapper::NewInstance(env, txn);
}

Napi::Value DatabaseWrapper::IsOpen(const Napi::CallbackInfo& info) {
    return Napi::Boolean::New(info.Env(), db_ != nullptr);
}

Napi::Value DatabaseWrapper::Query(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (db_ == nullptr) {
        Napi::Error::New(env, "Database not open").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 1 || !info[0].IsString()) {
        Napi::TypeError::New(env, "Query must be a string").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::string cypher = info[0].As<Napi::String>().Utf8Value();

    // Prepare query
    lattice_query* query = nullptr;
    lattice_error err = lattice_query_prepare(db_, cypher.c_str(), &query);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    // Bind parameters if provided
    std::vector<std::string> strHolders;
    if (info.Length() > 1 && info[1].IsObject()) {
        Napi::Object params = info[1].As<Napi::Object>();
        Napi::Array keys = params.GetPropertyNames();
        for (uint32_t i = 0; i < keys.Length(); i++) {
            std::string key = keys.Get(i).As<Napi::String>().Utf8Value();
            Napi::Value val = params.Get(key);

            // Check if it's a Float32Array (vector)
            if (val.IsTypedArray() && val.As<Napi::TypedArray>().TypedArrayType() == napi_float32_array) {
                Napi::Float32Array vec = val.As<Napi::Float32Array>();
                err = lattice_query_bind_vector(query, key.c_str(), vec.Data(), static_cast<uint32_t>(vec.ElementLength()));
            } else {
                lattice_value lval;
                strHolders.push_back("");
                if (!JsToLatticeValue(env, val, &lval, strHolders.back())) {
                    lattice_query_free(query);
                    return env.Undefined();
                }
                err = lattice_query_bind(query, key.c_str(), &lval);
            }

            if (err != LATTICE_OK) {
                lattice_query_free(query);
                ThrowLatticeError(env, err);
                return env.Undefined();
            }
        }
    }

    // Begin read-only transaction for query
    lattice_txn* txn = nullptr;
    err = lattice_begin(db_, LATTICE_TXN_READ_ONLY, &txn);
    if (err != LATTICE_OK) {
        lattice_query_free(query);
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    // Execute
    lattice_result* result = nullptr;
    err = lattice_query_execute(query, txn, &result);
    if (err != LATTICE_OK) {
        lattice_rollback(txn);
        lattice_query_free(query);
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    // Collect columns
    uint32_t col_count = lattice_result_column_count(result);
    Napi::Array columns = Napi::Array::New(env, col_count);
    for (uint32_t i = 0; i < col_count; i++) {
        const char* name = lattice_result_column_name(result, i);
        columns.Set(i, Napi::String::New(env, name ? name : ""));
    }

    // Collect rows
    Napi::Array rows = Napi::Array::New(env);
    uint32_t row_idx = 0;
    while (lattice_result_next(result)) {
        Napi::Object row = Napi::Object::New(env);
        for (uint32_t i = 0; i < col_count; i++) {
            lattice_value val;
            val.type = LATTICE_VALUE_NULL;
            lattice_result_get(result, i, &val);
            const char* col_name = lattice_result_column_name(result, i);
            row.Set(col_name ? col_name : "", LatticeValueToJs(env, &val));
        }
        rows.Set(row_idx++, row);
    }

    // Cleanup
    lattice_result_free(result);
    lattice_rollback(txn);
    lattice_query_free(query);

    // Return result object
    Napi::Object qresult = Napi::Object::New(env);
    qresult.Set("columns", columns);
    qresult.Set("rows", rows);
    return qresult;
}

Napi::Value DatabaseWrapper::VectorSearch(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (db_ == nullptr) {
        Napi::Error::New(env, "Database not open").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 1 || !info[0].IsTypedArray() ||
        info[0].As<Napi::TypedArray>().TypedArrayType() != napi_float32_array) {
        Napi::TypeError::New(env, "Vector must be a Float32Array").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    Napi::Float32Array vec = info[0].As<Napi::Float32Array>();
    uint32_t k = 10;
    uint16_t ef_search = 0;

    if (info.Length() > 1 && info[1].IsObject()) {
        Napi::Object opts = info[1].As<Napi::Object>();
        if (opts.Has("k") && opts.Get("k").IsNumber()) {
            k = opts.Get("k").As<Napi::Number>().Uint32Value();
        }
        if (opts.Has("efSearch") && opts.Get("efSearch").IsNumber()) {
            ef_search = static_cast<uint16_t>(opts.Get("efSearch").As<Napi::Number>().Uint32Value());
        }
    }

    lattice_vector_result* result = nullptr;
    lattice_error err = lattice_vector_search(db_, vec.Data(), static_cast<uint32_t>(vec.ElementLength()), k, ef_search, &result);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    Napi::Array results = Napi::Array::New(env);
    uint32_t count = lattice_vector_result_count(result);
    for (uint32_t i = 0; i < count; i++) {
        lattice_node_id node_id;
        float distance;
        err = lattice_vector_result_get(result, i, &node_id, &distance);
        if (err == LATTICE_OK) {
            Napi::Object item = Napi::Object::New(env);
            item.Set("nodeId", Napi::BigInt::New(env, static_cast<uint64_t>(node_id)));
            item.Set("distance", Napi::Number::New(env, distance));
            results.Set(i, item);
        }
    }

    lattice_vector_result_free(result);
    return results;
}

Napi::Value DatabaseWrapper::FtsSearch(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (db_ == nullptr) {
        Napi::Error::New(env, "Database not open").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    if (info.Length() < 1 || !info[0].IsString()) {
        Napi::TypeError::New(env, "Query must be a string").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    std::string query = info[0].As<Napi::String>().Utf8Value();
    uint32_t limit = 10;

    if (info.Length() > 1 && info[1].IsObject()) {
        Napi::Object opts = info[1].As<Napi::Object>();
        if (opts.Has("limit") && opts.Get("limit").IsNumber()) {
            limit = opts.Get("limit").As<Napi::Number>().Uint32Value();
        }
    }

    lattice_fts_result* result = nullptr;
    lattice_error err = lattice_fts_search(db_, query.c_str(), query.length(), limit, &result);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    Napi::Array results = Napi::Array::New(env);
    uint32_t count = lattice_fts_result_count(result);
    for (uint32_t i = 0; i < count; i++) {
        lattice_node_id node_id;
        float score;
        err = lattice_fts_result_get(result, i, &node_id, &score);
        if (err == LATTICE_OK) {
            Napi::Object item = Napi::Object::New(env);
            item.Set("nodeId", Napi::BigInt::New(env, static_cast<uint64_t>(node_id)));
            item.Set("score", Napi::Number::New(env, score));
            results.Set(i, item);
        }
    }

    lattice_fts_result_free(result);
    return results;
}

Napi::Value DatabaseWrapper::CacheClear(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (db_ == nullptr) {
        Napi::Error::New(env, "Database not open").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    lattice_error err = lattice_query_cache_clear(db_);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
    }
    return env.Undefined();
}

Napi::Value DatabaseWrapper::CacheStats(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (db_ == nullptr) {
        Napi::Error::New(env, "Database not open").ThrowAsJavaScriptException();
        return env.Undefined();
    }

    uint32_t entries = 0;
    uint64_t hits = 0;
    uint64_t misses = 0;
    lattice_error err = lattice_query_cache_stats(db_, &entries, &hits, &misses);
    if (err != LATTICE_OK) {
        ThrowLatticeError(env, err);
        return env.Undefined();
    }

    Napi::Object result = Napi::Object::New(env);
    result.Set("entries", Napi::Number::New(env, entries));
    result.Set("hits", Napi::Number::New(env, static_cast<double>(hits)));
    result.Set("misses", Napi::Number::New(env, static_cast<double>(misses)));
    return result;
}

/**
 * Get library version.
 */
Napi::Value GetVersion(const Napi::CallbackInfo& info) {
    return Napi::String::New(info.Env(), lattice_version());
}

/**
 * Module initialization.
 */
Napi::Object Init(Napi::Env env, Napi::Object exports) {
    TransactionWrapper::Init(env, exports);
    DatabaseWrapper::Init(env, exports);
    exports.Set("version", Napi::Function::New(env, GetVersion));
    return exports;
}

NODE_API_MODULE(lattice_native, Init)
