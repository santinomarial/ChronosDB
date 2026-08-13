# Distributed Read Admission

## Purpose and interface

Distributed planning selects tablets and a named `DistributedReadPolicy`. Before a coordinator can
accept partial results, each selected tablet supplies one `DistributedReadAdmission`. Coordinator
creation exact-validates the whole set, so a missing or weaker fragment fails before merge.

## Consistency contracts

- Leader linearizable routes to the planned leader, completes a nonzero Raft read barrier, and waits
  until that tablet's applied position covers the returned read index.
- Follower bounded stale names a maximum log-position lag. A fresh observed leader commit position
  minus the follower applied position must not exceed it; a follower ahead of the observation has
  zero measured lag.
- Local eventual names the serving node and accepts its applied state without leadership or lag
  evidence. It is deliberately distinct, not a fallback.

Raft commit and apply indexes are per group. A production caller must preserve the tablet-to-group
mapping when constructing an admission; the value API does not make a proof transferable between
tablets.
The metadata-backed aggregate binder now performs that production mapping from one committed
catalog snapshot, requires stable membership equal to placement, and derives each admission from
the matching plan-ordered group observation. Callers still acquire read barriers and fresh leader
commit observations because those are asynchronous runtime proofs rather than catalog data.

## Ownership, failure, and complexity

Plans and admissions are owned values. Coordinator creation stores only the plan after validation.
There is no background work or synchronization in this boundary. Validation is `O(fragments log
fragments)` with bounded vectors and ordered identity sets. Any duplicate, missing, foreign, stale,
or unapplied proof rejects the complete coordinator; partial results are never returned as success.

## Tradeoffs and interview questions

Position lag is deterministic and clock-free but is not a direct time-duration promise when write
rates vary. A later protocol may expose both after defining clock and sampling contracts. The
compatible aggregate snapshot binder now pins one Manifest v2 generation across all admitted
tablets. A retryably failed TCP execution may be rebound only as a whole after the caller reacquires
every admission, placement proof, and compatible snapshot; logical query identity and nonregressing
generation are checked before any new socket opens.

- Why is a committed read index insufficient without applied coverage?
- Why must bounded stale name a number rather than an enum alone?
- Why is a leader commit observation part of follower admission?
- Why does local eventual reject an attached barrier?
- What additional identity must production routing preserve?
