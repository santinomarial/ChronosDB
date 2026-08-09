# ADR 0097: Schema-bound subscription plan identity

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB SQL and live-query maintainers
- **Extends:** [ADR 0008](0008-custom-sql-and-vectorized-execution.md),
  [ADR 0068](0068-live-handoff-and-resume-token-v1.md), and
  [ADR 0096](0096-plan-bound-subscription-snapshot-execution.md)

## Context

Resume Token v1 reserves a 32-byte plan fingerprint, but the service previously relied on callers
to invent it. That cannot prove that a resumed subscription uses the same SQL semantics and bound
schema. The single-tablet manager also admitted unrelated plan fingerprints even though one manager
receives one already-derived logical result stream.

## Accepted decision

`prepare_subscription_plan` accepts bounded UTF-8 SQL and an immutable catalog snapshot. It requires
`SUBSCRIBE SELECT`, rejects historical `FOR SYSTEM_TIME` and multi-source ASOF for the current live
surface, binds exact schema identities, and lowers the supported single-source vector SQL subset.
The resulting `PreparedSubscriptionPlan` owns the executable physical plan, output names/shapes,
bound schema, and one deterministic SHA-256 identity.

The fingerprint hashes a versioned domain, explicit SQL byte length and source count, the exact SQL
bytes, and every bound source's table ID, schema ID, and schema version in source order. Textually
distinct SQL intentionally has a distinct identity even if an optimizer might prove it equivalent.
Catalog generation is excluded because the bound stable identities, not unrelated catalog edits,
define compatibility. Integer fields use explicit little-endian bytes; native object
representations are never hashed.

The prepared owner constructs exact `SubscriptionSource` and `SubscriptionRequest` values. A
single-tablet manager is now fixed to that plan/schema at creation and rejects registration or
resume tokens for any other plan/schema. This matches the already plan-bound multi-tablet manager.

## Consequences and alternatives

Plan lookup across process restart must persist or deterministically reconstruct the SQL/catalog
binding associated with a fingerprint; that registry remains separate work. This fingerprint is an
identity and compatibility guard, not a signature. Resume Token authentication continues to use its
MAC key.

Hashing only raw SQL was rejected because a name can bind to a new schema. Hashing catalog
generation was rejected because unrelated catalog changes would invalidate a stable binding.
Normalizing or optimizer-canonicalizing SQL was deferred because it would create another versioned
semantic format without a current need.

## Affected invariants and validation

Invariants 10, 12, 14, 15, and 17 apply. Focused tests prove repeatable identity, different identity
for textually different SQL, exact output/schema ownership, rejection of non-subscription,
historical, and multi-source statements, and manager rejection of a mismatched plan. Cross-process
goldens, hostile allocation sweeps, catalog migration, and durable plan registry recovery remain in
the Phase 18 ledger.
