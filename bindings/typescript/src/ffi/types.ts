/**
 * TypeScript types and constants for the FFI layer.
 *
 * Note: koffi type registrations are done in bindings.ts to avoid
 * circular dependencies and ensure proper initialization order.
 */

/**
 * Error codes from lattice.h
 */
export const enum LatticeErrorCode {
  Ok = 0,
  Error = -1,
  IoError = -2,
  Corruption = -3,
  NotFound = -4,
  AlreadyExists = -5,
  InvalidArg = -6,
  TxnAborted = -7,
  LockTimeout = -8,
  ReadOnly = -9,
  Full = -10,
  VersionMismatch = -11,
  Checksum = -12,
  OutOfMemory = -13,
}

/**
 * Transaction modes
 */
export const enum LatticeTxnMode {
  ReadOnly = 0,
  ReadWrite = 1,
}

/**
 * Value types for properties
 */
export const enum LatticeValueType {
  Null = 0,
  Bool = 1,
  Int = 2,
  Float = 3,
  String = 4,
  Bytes = 5,
  Vector = 6,
  List = 7,
  Map = 8,
}

/**
 * Default open options matching LATTICE_OPEN_OPTIONS_DEFAULT
 */
export function defaultOpenOptions(): {
  create: boolean;
  read_only: boolean;
  cache_size_mb: number;
  page_size: number;
  enable_vector: boolean;
  vector_dimensions: number;
} {
  return {
    create: false,
    read_only: false,
    cache_size_mb: 100,
    page_size: 4096,
    enable_vector: false,
    vector_dimensions: 128,
  };
}
