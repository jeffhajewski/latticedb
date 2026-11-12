/**
 * What the package root exports at run time, for JavaScript callers and for
 * TypeScript callers compiled one file at a time, who cannot read a const enum.
 */

// eslint-disable-next-line @typescript-eslint/no-require-imports
const root = require('../src') as Record<string, unknown>;

describe('package root', () => {
  test('exports the error codes and query error stages as values', () => {
    const codes = root.LatticeErrorCode as Record<string, number>;
    expect(codes).toBeDefined();
    expect(codes.NotFound).toBe(-4);
    expect(codes.VersionMismatch).toBe(-11);
    expect(codes.HasRelationships).toBe(-17);
    expect(codes.ConfigMismatch).toBe(-18);
    expect(codes.NeedsNewerWriter).toBe(-19);

    const stages = root.QueryErrorStage as Record<string, number>;
    expect(stages).toBeDefined();
    expect(stages.Parse).toBe(1);
    expect(stages.Execution).toBe(4);
  });

  test('exports the error classes', () => {
    expect(typeof root.LatticeError).toBe('function');
    expect(typeof root.LatticeQueryError).toBe('function');
  });
});
