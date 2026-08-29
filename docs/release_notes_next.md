# LatticeDB Next Release Notes

Use this file as the draft for the next release after `0.15.0`.

## Summary

- Two full-text predicates joined by `AND` now start from the rarer one, so the
  order they are written in no longer decides what the query costs.

## Highlights

- **Selectivity chooses the access path.** `d.title @@ "the" AND d.body @@
  "sourdough"` can be answered by reading either index. Reading the common one
  means materialising most of the corpus to discard it; reading the rare one means
  reading almost nothing. The dictionary already counts documents per term, so the
  cheaper way in is known before either is read.

  Measured on eight thousand documents returning one row:

  | query | before | after |
  |---|---:|---:|
  | rare predicate written first | 4.3 ms | 4.3 ms |
  | common predicate written first | 244 ms | 4.2 ms |

  The 59x matters less than the two rows agreeing. Query cost no longer depends on
  which predicate the author happened to type first.

  This also covers `$param` queries, because the choice is made when the query
  runs rather than when it is planned.

## API Notes

- No API changes. This is a planner improvement; queries and results are
  unchanged.

## Known limits

- The estimate is per term. It uses the rarest term's document frequency, which
  bounds an AND query correctly, but says nothing about how often two terms occur
  *together*: predicates whose terms are individually common and jointly rare
  still estimate as common.

## Upgrade Notes

- Nothing to do.
