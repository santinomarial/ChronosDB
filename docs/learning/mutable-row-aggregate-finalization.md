# Mutable-row aggregate finalization

## Why this bridge exists

ChronosDB has two independently implemented paths: a sufficient-state aggregate exchange over
immutable files, and a replicated mutable-row query path over current TabletSnapshots. The latter
already owns the production read proof, routing, mutual TLS, retry, cancellation, and complete
all-tablet result. This bridge lets global aggregate SQL use that authority without pretending the
older aggregate endpoint is packaged there.

The compromise is visible: workers send the exact aggregate input columns as rows, then the
coordinator accumulates them. That is correct and bounded, but network work grows with matching
rows. Worker-side sufficient-state pushdown remains the scale-oriented successor.

## Public contract and data flow

The input execution owns an identity row Plan Intent, exact result schema, and complete exchange
messages. The aggregate Plan Intent and result schema are separate authority-neutral products from
SQL lowering.

```text
complete tablet row streams
          |
          v
identity/schema/sequence/limit validation
          |
          v
decode one Native batch at a time
          |
          v
optional checked Boolean predicate (TRUE only)
          |
          v
shared MergeableVectorAggregateState instances
          |
          v
optional checked final aggregate projection
          |
          v
global Native aggregate finalizer
```

The input row plan must have outputs `0..N-1`, no hidden visibility mapping, no order keys, no
grouping or aggregates, and no LIMIT. This matters because sorting is irrelevant work, hidden
columns would obscure the authoritative input mapping, and an early limit would change aggregate
truth.

## Canonical cells and aggregate state

Native batches carry canonical little-endian scalar bytes. For each cell the finalizer builds a
stack-owned one-row `PhysicalColumnView`: one validity byte, two 32-bit offsets for variable-width
types, or at most sixteen fixed bytes. `PhysicalColumnView::create` independently checks the
physical representation before the borrowed cell enters the aggregate kernel.

When present, the immutable coordinator predicate is validated against the exact input schema and
evaluated over a reusable vector of borrowed canonical cells. SQL FALSE and NULL skip the row before
COUNT(*) or any input aggregate advances. Runtime expression failure aborts without publishing a
partial scalar.

One `MergeableVectorAggregateState` exists per SQL output. Reusing that state avoids a second COUNT,
SUM, AVG, extrema, or variance implementation. Variable extrema retain their bytes under both a
per-value ceiling and `QueryResourceContext` reservation. Batches and their decoded cell vectors
are temporary; only aggregate state survives between batches.

After every state produces its exact owned scalar, an optional coordinator projection canonicalizes
that raw single row and evaluates visible checked expressions. Fixed-width results own scalar
storage; transformed variable results are copied into owned STRING/SYMBOL/BINARY values before raw
state is released. Source leaves are revalidated against the internal aggregate schema. Projection
evaluation occurs even for LIMIT zero so SQL runtime errors are not hidden.

## Stream and resource invariants

Messages for each tablet form one contiguous sequence starting at one and ending exactly once. All
messages share a nonnil query ID, tablet segments are unique, and even an empty tablet contributes
one canonical terminal message. No result can be finalized while any segment remains open.

The caller separately bounds input rows, messages, encoded exchange bytes, decoded batch shape,
predicate and final-expression configuration/canonical row views, transformed payload ownership,
conservative working memory, query-accounted state, variable extrema, and final Native output.
Header shapes are checked before full decoding or cell-vector allocation. Complexity is
`O(rows * aggregates)` time and `O(max batch cells + aggregate states + retained extrema)` extra
memory.

## Failure and ownership

The call consumes its input execution and synchronously borrows decoded cell bytes only while each
batch view is alive. Returned schema and payload own their memory. Every failure destroys partial
states and reservations through RAII; no partial Native response escapes. This code is
single-thread-affine, so it has no shared-memory publication or memory-ordering requirement.

## Replicated Native composition

After SQL binding, the service chooses the aggregate product's `input_rows` for the same correlated
snapshot preparation used by direct SELECT rows. Self-led fragments execute through the local
proof-revalidating worker and remote fragments through the mutual-TLS scheduler. Both enter one
all-tablet result coordinator. A retryable authority failure discards that entire coordinator and
installs only a freshly barrier-covered fragment set with identical logical identity; cancellation
and the original deadline cover both modes.

The service invokes this finalizer only after the coordinator closes. It then applies Native
response row/name/payload limits and emits QUERY_RESULT plus QUERY_END. LIMIT zero is not an empty
response: it is one zero-row schema-bearing QUERY_RESULT followed by QUERY_END.

## Tradeoffs and interview questions

**Why not calculate aggregates directly from Native bytes?** The physical column adapter lets one
well-tested kernel remain the semantic authority and reproves canonical encoding at the boundary.

**Why forbid an input LIMIT even when SQL has LIMIT 1?** SQL LIMIT applies to the one global
aggregate result, not to source rows. Pushing it into the scan changes COUNT, SUM, AVG, extrema, and
variance.

**Why validate terminal streams before accumulation?** It prevents expensive work from producing a
tempting partial result. Publication still happens only after complete authority has been proved.

**Why filter at the coordinator?** It closes the SQL correctness surface using the shared checked
expression engine and existing authenticated row carrier. The cost is transferring rows that a
future versioned worker program could reject earlier.

**Why project before LIMIT zero?** Local SQL evaluates the output expression stage before LIMIT. A
checked division or cast failure must therefore remain observable rather than disappearing because
the final row count is zero.

**What should replace this at scale?** Bind the existing sufficient-state exchange to current
TabletSnapshot/Raft proof authority and package it in the same authenticated service. The authority,
retry, cancellation, and final Native scalar stages can remain unchanged.
