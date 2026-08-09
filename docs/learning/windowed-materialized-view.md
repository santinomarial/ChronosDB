# Windowed materialized-view state

`WindowedMaterializedView` is a single-owner incremental state machine driven only by consecutive
committed tablet/WAL positions. A logical row identity has at most one current contribution.
Corrections remove its prior contribution, insert the replacement, and update every tumbling or
sliding window selected by event time. Commit order controls application; watermark and allowed
lateness control finalization. They are deliberately separate.

Each window owns removable count, sum, min, max, VWAP, OHLC, and Welford variance state. Ordered
value/event indexes keep extrema and endpoints correct after corrections or tombstones. The global
row map locates a row's prior event time so a correction can move it between windows. Window and row
counts are configuration-bounded; the owner returns resource exhaustion instead of silently
dropping committed input.

The logical checkpoint captures source progress, definition, watermark, current rows, historical
window metadata, and each aggregate's exact running numeric fields. Restore reconstructs ordered
indexes, verifies window membership from event times, preserves revisions/finalization, and resumes
at the next consecutive committed position. Exact running fields matter because rebuilding floating
aggregates in a different order can change later output bits.

Materialized View Checkpoint v1 now encodes that state with fixed little-endian fields, exact
floating bits, authenticated counts, header/file CRC32C, and complete logical validation. It still
has no installation owner. External delivery remains at least once, and the subscription manager
remains a separate single-source retention/handoff owner. Durable checkpoint installation must
order source retention release after the checkpoint is fully validated and synchronized.

Update and correction work is proportional to the number of overlapping windows plus ordered-map
costs. Checkpoint size includes global rows and per-window contribution rows; this favors a simple
auditable recovery contract over compact bytes until measurement justifies another representation.

Useful review questions are: why must commit order and event-time windows remain distinct, why are
running Welford fields checkpointed, what makes a late correction observable, and which durable
ordering must precede release of the committed source suffix?
