/**
 * TypeScript types and constants for the FFI layer.
 *
 * Note: koffi type registrations are done in bindings.ts to avoid
 * circular dependencies and ensure proper initialization order.
 */

/**
 * Error codes from lattice.h. A plain enum, not a const one, so that the
 * package exports it as a value: JavaScript callers, and TypeScript compiled
 * with isolatedModules, cannot read a const enum.
 */
export enum LatticeErrorCode {
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
  Unsupported = -14,
  ValueTooLarge = -15,
  DatabaseLocked = -16,
  /** A plain node delete was refused: the node still has relationships. */
  HasRelationships = -17,
  /** Open options contradict the vector index the database file records. */
  ConfigMismatch = -18,
  /**
   * The file was written by a newer release that does not let this one write
   * it. It opens with readOnly: true; a writable open writes nothing.
   */
  NeedsNewerWriter = -19,
}

/**
 * lattice_open_options_v5 enum values. 0 means "not set" for each.
 */
export const enum LatticeSyncMode {
  Default = 0,
  Full = 1,
  Normal = 2,
}

export const enum LatticeFullFsync {
  Default = 0,
  Off = 1,
  On = 2,
}

export const enum LatticeVectorMetric {
  Default = 0,
  Cosine = 1,
  Euclidean = 2,
  DotProduct = 3,
}

/**
 * lattice_open_options_v5.changefeed_retention values. 0 keeps the file's policy.
 */
export const enum LatticeChangefeedRetention {
  Default = 0,
  All = 1,
  Last = 2,
  Age = 3,
  Off = 4,
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
 * Query diagnostic stages from the C API.
 */
export enum QueryErrorStage {
  None = 0,
  Parse = 1,
  Semantic = 2,
  Plan = 3,
  Execution = 4,
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
  enable_wal: boolean;
} {
  return {
    create: false,
    read_only: false,
    cache_size_mb: 100,
    page_size: 4096,
    enable_vector: false,
    vector_dimensions: 128,
    enable_wal: true,
  };
}
