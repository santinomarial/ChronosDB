# Restart remote garbage discovery

## Purpose and public interface

`TieredRestartRemoteGarbageCoordinator::reclaim_unreachable` completes an interrupted remote
reclamation after process restart. It runs after `TieredPairCommitStorage::recover` selects and
authenticates one Manifest/cold pair, but before that pair is published or queries are admitted.
This timing matters: all pre-crash reader pins are gone, while no new reader can acquire a route
that startup is about to classify.

The call receives the recovered pair, the three durable storage owners, exact per-Manifest catalog
bindings, the configured object store, and finite history/object limits. It returns counts for
validated generations, candidates, metadata checks, deletions, and already-absent retry outcomes.

## Durable reference graph and invariants

Every installed Cold Location Manifest is an immutable full generation. Consecutive local history
therefore records routes that used to be authoritative even after a selected successor omits them.
The coordinator never uses bucket listing. For each generation it exact-loads the referenced base
Manifest using the schema and tablet-source bindings for that Manifest generation, validates the
cold binding, and validates the adjacent cold transition.

A historical object becomes a candidate only if the selected Manifest no longer names its logical
part and the selected cold generation names neither its part ID nor its key. A surviving route must
retain exactly the same part, key, length, and SHA-256. Reusing an immutable key or changing route
identity is corruption, not garbage.

## Ownership, lifetime, and synchronization

The coordinator borrows storage owners and the recovered pair for one synchronous startup call.
It owns decoded historical generations and copied candidate descriptors. The caller owns the
exclusive lifecycle rule: do not publish the recovered pair or admit readers until the call
returns. This version is a single-process control-plane operation and makes no concurrent-startup
claim.

The highest pair record is compared with the recovered record before discovery and again after all
remote metadata preflight. Thus a changed authority aborts before deletion. Every candidate is
preflighted before the first delete so a mismatch cannot produce a partially deleted batch.

## Failure and retry behavior

Missing generations, missing exact Manifest bindings, corrupt bytes, invalid transitions, changed
identity, reused keys, and wrong object metadata fail closed. Limits fail with
`RESOURCE_EXHAUSTED` before unsafe work. Remote absence is not corruption: a previous attempt may
already have removed the object. Exact conditional deletion protects the interval between metadata
preflight and mutation.

If a remote error occurs after one candidate is removed, selected authority still references none
of the candidates. Repeating recovery rediscovers the same set, treats removed objects as already
absent, and continues. This makes recovery idempotent without a separate mutable deletion receipt.

## Complexity and tradeoffs

Discovery reads every retained cold generation and each referenced historical Manifest, then scans
their route tables. It uses `O(candidate objects)` memory and performs one metadata request plus one
exact deletion per present candidate. Full retained generations increase local metadata and startup
work, but keep the durable proof simple and avoid trusting remote enumeration.

This implementation requires the embedding catalog to reconstruct exact schema/source bindings for
old Manifest generations. Pruning those catalog versions must therefore be coordinated with cold
history retention or a later durable compaction/checkpoint format.

## Verification and likely interview questions

The focused lifecycle test covers evolving catalog coverage, bounded-history rejection, missing
historical bindings, remote metadata mismatch, exact deletion, and absent-object retry. Broader
subprocess crash points, multiple-object partial failures, and live S3-compatible providers remain
qualification work.

Useful questions include:

- Why is cold history authoritative while bucket listing is not?
- Why must startup supply per-Manifest bindings instead of one current binding set?
- Why is the pair record checked twice?
- Why does absence mean idempotent progress rather than corruption?
- What must happen before old cold generations can be pruned?
