# ADR 0565: Locked Empty WAL Identity Authority

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB WAL and storage maintainers

## Context

WAL v1 requires every independently created history to carry a nonzero, collision-resistant
128-bit identity. ADR 0404 assigns entropy-level nil retry to `SystemUuidGenerator` and requires
each owning domain to reject collisions visible in its authority. CREATE and live Manifest-v1 flush
have durable namespaces containing existing identities, so their owners scan those namespaces and
retry collisions under finite limits.

New WAL creation has a different authority. `WalWriter::create_new` accepts only an already durable
directory that is empty or contains one regular `LOCK`. It rejects installed segments, recognized
temporaries, malformed reserved names, and unrelated entries before identity generation, then
rechecks after acquiring the exclusive writer lock. Existing or interrupted history must use
recovery or fail closed; creation is never a reset path.

The accepted directory therefore contains no prior WAL identity against which a generated candidate
can be compared. Adding an owner-level retry limit would retry only nil values or generator errors,
duplicating the entropy generator's policy without detecting another collision class.

## Decision

The locked empty/`LOCK`-only WAL directory is the complete durable authority for initial WAL
identity allocation. The writer consumes exactly one `WalLogIdGenerator` result per creation
attempt. Generation failure is propagated immediately. A nil result violates the generator
contract and is rejected before `LOCK` or a segment is created. A valid result is installed in the
first segment only after the directory is reclassified under the exclusive lock.

`SystemWalLogIdGenerator` continues to delegate to `SystemUuidGenerator`, whose production entropy
boundary already makes at most eight attempts to skip nil candidates and propagates an entropy
failure immediately. The WAL owner does not add a configurable identity-attempt count.

An existing final segment or temporary is namespace evidence, not a reason to generate another WAL
identity. Creation rejects it without consuming a candidate. Opening preserves the installed
history identity. ChronosDB does not claim to detect reuse of an identity from a history whose
entire directory was deleted outside the storage protocol. Any future authorized history deletion
or reset that requires no-reuse must first define a separately durable tombstone or registry.

## Detailed rationale

Identity collision handling is useful only when an authoritative set can reject a candidate. The
new-history precondition deliberately makes that set empty, while the directory lock prevents a
cooperating creator from racing installation. Retrying a contract-invalid nil from a test generator
would hide the caller error and duplicate the bounded production entropy logic. Retrying a
generator failure could also turn a precise provider failure into an arbitrary later result.

This preserves the stricter creation rule: no identity is generated for a directory that already
contains history, and no filesystem mutation occurs after identity generation fails. It also avoids
a configuration knob whose exhaustion semantics would provide no additional collision proof.

## Alternatives considered

- **Add a finite writer-level candidate loop:** bounded, but there is no existing identity in the
  accepted namespace to compare. It would only duplicate nil handling and obscure generator
  failures.
- **Compare against every WAL on the host:** there is no authoritative host-wide database registry,
  and directory scanning would be racy, incomplete, and outside WAL ownership.
- **Persist a global history tombstone registry now:** could prove no-reuse after authorized deletion,
  but no WAL reset/deletion protocol currently exists. Adding that state before its lifecycle is
  specified would be speculative.
- **Accept nil because the directory is empty:** rejected because nil is reserved as invalid in the
  durable WAL v1 header and weakens corruption detection.

## Consequences

New WAL creation remains one-shot, deterministic, and mutation-free on identity failure. The
production path retains finite nil behavior through the common UUID generator. Tests can inject
fixed valid, nil, or failing generators without a hidden retry count.

This decision does not provide mathematical global uniqueness or detect identities from externally
deleted histories. Those are not properties random generation alone can establish. A future reset,
clone, restore, or deletion feature must define its authority and migration rules before changing
this contract.

## Affected invariants

- [Invariant 8](../architecture/invariants.md#8-recovery-is-idempotent): existing history is reopened
  with its installed identity rather than silently replaced.
- [Invariant 10](../architecture/invariants.md#10-durable-records-and-pages-have-integrity-coverage):
  nil remains invalid inside the checksummed segment header.
- [Invariant 14](../architecture/invariants.md#14-durable-and-network-formats-are-versioned-from-release-one):
  the decision changes no WAL v1 bytes or interpretation.
- [Invariant 18](../architecture/invariants.md#18-optimization-cannot-weaken-guarantees): an apparent
  retry improvement cannot manufacture collision evidence outside the owning durable namespace.

## Validation plan

Existing focused tests require invalid configuration and nonempty directories to reject without
calling the generator. Real-filesystem tests require one injected generation failure or nil result
to return the exact error class, call the generator once, and leave the directory empty. Creation
tests require a valid candidate to appear exactly in the installed first-segment header. Recovery,
corruption, sanitizer, and subprocess suites continue to prove that existing history retains and
validates the same nonzero identity.

## Migration or rollback considerations

There is no durable-format or network change. Existing WAL directories reopen unchanged. Rolling
back the documentation/public contract leaves current executable behavior intact. Introducing a
future owner-level retry requires a new decision naming the durable collision authority; adding a
deletion/reset registry requires an explicit migration and recovery design.

## Unresolved questions

- The authority and retention period for identities after a future authorized history deletion,
  clone, restore, or reset remain deferred until one of those operations is specified.
- Cross-database uniqueness remains probabilistic under the OS entropy contract; no global registry
  is planned in the current single-node phase.

## References

- [WAL v1](../formats/wal-v1.md)
- [WAL recovery architecture](../architecture/wal-recovery.md)
- [ADR 0013](0013-wal-v1-format-and-recovery.md)
- [ADR 0404](0404-injectable-system-uuid-entropy.md)
- [ADR 0563](0563-authoritative-bounded-create-identity-allocation.md)
- [ADR 0564](0564-authoritative-bounded-live-flush-identity-allocation.md)
- [WAL design](../learning/wal-design.md)
