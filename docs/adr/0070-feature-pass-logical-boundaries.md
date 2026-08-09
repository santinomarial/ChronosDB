# ADR 0070: Feature-pass logical boundaries for temporal, distributed, and cold paths

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB query, storage, networking, and runtime maintainers

## Context

Phases 12–17 need working semantic boundaries while frozen CSEG v1, Manifest v1, and native Protocol
v1 do not assign correction, cold-location, distributed-exchange, or Raft-wire bytes.

## Accepted decision

- Keep those v1 formats unchanged.
- Provide an in-memory committed temporal version store behind the already implemented scalar
  `FOR SYSTEM_TIME AS OF` provider interface. Corrections append; tombstones hide rather than erase;
  retention fails closed for expired history.
- Provide bounded tablet pruning, mergeable partial aggregates, exchange backpressure/cancellation,
  and a coordinator that rejects incomplete results.
- Model movement as add learner, checksummed snapshot, catch-up, promote, then remove source, with
  placement-epoch checks at both membership changes.
- Keep object storage behind S3-compatible immutable put/stat/range semantics. Verify whole-object
  SHA-256 before the caller's atomic manifest-install callback; only then may local source release.
- Keep epoll as the reference connection engine. The optional liburing build is a Linux-isolated
  readiness pilot and receives no performance claim. CPU/NUMA hooks are optional and fail explicitly
  when unsupported.

## Consequences and alternatives

This accepts real logical implementations without pretending the missing durable/network adapters
exist. CSEG v2 correction bytes, Manifest v2 cold descriptors, a Raft wire protocol, production S3
transport, Arrow/Parquet providers, and a packaged three-node server require separate versioned
contracts. Reusing reserved v1 bytes or returning partial distributed results was rejected.

## Affected invariants and validation

Invariants 2–8, 10–14, 17, and 18 apply. Focused tests cover temporal visibility/retention, aggregate
merge, pruning, exchange failure, movement order/checksums, immutable object identity/cache/ranges,
and one cross-module smoke. All production durability, network, cloud, compatibility, sanitizer,
chaos, and performance evidence is deferred in the ledger.

