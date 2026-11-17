# LatticeDB 0.18.1 Release Notes

## Summary

0.18.1 is a patch release. It stops the repository tracking the TypeScript
binding's coverage report.

## Fixes

- **The TypeScript coverage report is no longer tracked.**
  `bindings/typescript/coverage/`, which `npm run test:coverage` rewrites on
  every run, was committed, so a coverage run left the working tree dirty. It is
  now removed from the repository and ignored. Neither CI's coverage gate, which
  measures a fresh run, nor the npm package, which ships only `dist` and `lib`,
  read those files.

## API Notes

- None.

## Upgrade Notes

- None.

## Validation Notes

- `git status` stays clean after `npm run test:coverage` in
  `bindings/typescript`.
