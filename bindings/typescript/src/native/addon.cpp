/**
 * N-API native addon for Lattice database.
 *
 * This file provides the C++ bridge between Node.js and the Lattice C API.
 */

#include <napi.h>

extern "C" {
#include "lattice.h"
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
};

Napi::FunctionReference DatabaseWrapper::constructor;

Napi::Object DatabaseWrapper::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "Database", {
        InstanceMethod("open", &DatabaseWrapper::Open),
        InstanceMethod("close", &DatabaseWrapper::Close),
        InstanceMethod("begin", &DatabaseWrapper::Begin),
        InstanceMethod("isOpen", &DatabaseWrapper::IsOpen),
    });

    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();

    exports.Set("Database", func);
    return exports;
}

DatabaseWrapper::DatabaseWrapper(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<DatabaseWrapper>(info) {
    // Constructor implementation
}

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
    }

    lattice_error err = lattice_open(path.c_str(), &options, &db_);
    if (err != LATTICE_OK) {
        Napi::Error::New(env, lattice_error_message(err)).ThrowAsJavaScriptException();
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
            Napi::Error::New(env, lattice_error_message(err)).ThrowAsJavaScriptException();
        }
    }

    return env.Undefined();
}

Napi::Value DatabaseWrapper::Begin(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    // TODO: Implement transaction creation
    return env.Undefined();
}

Napi::Value DatabaseWrapper::IsOpen(const Napi::CallbackInfo& info) {
    return Napi::Boolean::New(info.Env(), db_ != nullptr);
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
    DatabaseWrapper::Init(env, exports);
    exports.Set("version", Napi::Function::New(env, GetVersion));
    return exports;
}

NODE_API_MODULE(lattice_native, Init)
