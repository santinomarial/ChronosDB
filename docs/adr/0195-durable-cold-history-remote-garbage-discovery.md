# ADR 0195: Durable cold-history remote garbage discovery

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB tiering, recovery, query, and operations maintainers
- **Extends:** [ADR 0193](0193-reader-pinned-remote-object-reclamation.md)

## Context

Live remote reclamation retains enough in-memory publication history to prove that an object route
is absent from current authority and no older reader can still use it. A process crash destroys
those reader pins and that volatile history. If the process stopped after committing a route-free
pair but before deleting every retired object, recovery needs a durable and deterministic way to
rediscover the unreachable objects. Object-store listings cannot provide that authority: they may
be incomplete or delayed, and they do not prove which historical route was ever committed.

The local Cold Location Manifest registry already retains immutable, consecutive full generations.
Those generations contain every historically committed route and bind each route to an exact
Manifest generation, object-store identity, length, and SHA-256.

## Decision

Cold Location Manifest generation history is the durable garbage journal. Startup may run
`TieredRestartRemoteGarbageCoordinator` only after recovering the selected tiered pair and before
publishing that pair or admitting readers. No reader pin can survive a process crash, so this
startup boundary replaces the live reader-wait proof.

The caller supplies a strictly ordered exact catalog/source binding for every historical Manifest
generation referenced by cold history. This is required because tablet and schema coverage can
change between generations; validating old metadata with only the current catalog would either
reject valid history or weaken ownership checks. The coordinator then:

1. authenticates the recovered owners and their digests against the highest selected pair record;
2. exact-decodes every cold generation from one through the selected generation;
3. loads and validates each exact historical Manifest, rebinds its cold generation, and validates
   every adjacent cold successor transition;
4. identifies historical routes absent from both current logical and cold authority, rejecting
   part identity changes and object-key reuse;
5. preflights every present candidate's exact key, length, and SHA-256 before any deletion;
6. reloads and rechecks the selected pair; and
7. invokes exact conditional deletion for each candidate.

An already absent object is successful idempotent progress. A failure after some deletions leaves a
safe partial set because every candidate was unreachable before the first mutation; rerunning the
same startup pass converges. The coordinator never lists the remote store and never treats an
uncommitted component final as authority.

## Consequences and validation

Startup work is linear in retained cold generations plus historical route count, with Manifest
decode and binding costs for each referenced base generation and one metadata request per present
candidate. Explicit generation and object limits bound memory and work. Retaining full cold history
costs local metadata space but avoids a new mutable intent log and its own cross-directory commit
protocol.

Focused tests recover a route-free selected pair, authenticate two cold generations whose base
Manifests require different catalog bindings, reject a history-limit breach and missing historical
binding without mutation, reject wrong remote metadata, delete the exact retired object, and report
the object already absent on retry.

Invariants 2, 3, 6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **List the bucket and subtract current routes:** rejected because listings are not durable
  metadata authority and unrelated objects may share the prefix.
- **Persist a separate deletion-intent log:** rejected because immutable consecutive cold history
  already contains the required reference graph; another log would add a crash-consistency join.
- **Validate all history with the current catalog:** rejected because legitimate tablet retirement
  changes the exact bindings needed by older Manifest generations.
- **Delete while normal readers are active:** rejected because the restart proof depends on the
  fact that no pre-crash reader survived and must run before new readers are admitted.

## Migration and rollback

No durable format changes. Older binaries ignore retained cold history and may leak unreachable
objects but cannot make them query-visible. Operators may disable startup reclamation while keeping
all objects. Rolling back after deletion requires restoring exact bytes under every immutable key
before selecting any pair that references those routes.

## References

- [Reader-pinned remote object reclamation](0193-reader-pinned-remote-object-reclamation.md)
- [Durable cold-location manifests](../learning/cold-location-manifest-storage.md)
- [Restart remote garbage discovery](../learning/restart-remote-garbage-discovery.md)
- [Architecture invariants](../architecture/invariants.md)
