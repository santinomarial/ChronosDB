# ADR 0404: Injectable System UUID Entropy

- **Status:** accepted
- **Date:** 2026-08-15
- **Owners:** ChronosDB common-runtime, catalog, and storage maintainers

## Context

ADR 0014 requires collision-resistant, nonzero opaque 128-bit durable identifiers and separates
identity assignment from schema/storage semantics. `SystemUuidGenerator` already reads Linux or
macOS operating-system entropy and retries an all-zero value, but its entropy boundary is embedded
inside `generate()`. Tests can replace the entire UUID generator at outer services, yet cannot
deterministically execute this generator's response to an entropy failure or repeated-nil input.

ADR 0224 originally named OpenSSL `RAND_bytes` for daemon bootstrap. The implemented common adapter
instead uses the operating-system interfaces directly so UUID generation does not add a second
cryptographic-library dependency to the common target. This decision records and supersedes only
that provider-selection sentence; Bootstrap v1 identity and restart rules remain unchanged.

## Decision

`UuidEntropySource` returns exactly one owned 16-byte candidate or a classified failure.
`SystemUuidEntropySource` is the production implementation: macOS `arc4random_buf` fills the whole
candidate, while Linux `getrandom` retries `EINTR`, accepts partial progress, and fails on zero
progress or another error. `system_uuid_entropy_source()` returns one stateless thread-safe
process-lifetime adapter.

`SystemUuidGenerator` defaults to that adapter and may instead borrow an injected typed source. It
propagates an entropy failure immediately, rejects nil candidates, and makes at most eight attempts
before returning `INTERNAL`. Successful bytes remain uninterpreted: ChronosDB does not set or claim
RFC UUID version/variant bits, derive identity from names, or serialize a native integer.

An injected source must outlive the generator and all overlapping calls. Mutable injected sources
provide their own synchronization. The owning catalog, WAL, storage, or request operation still
decides the identity domain, rejects collisions visible in its authority, and durably installs the
chosen value before exposure.

## Alternatives considered

- **Inject only complete UUID generators:** preserves outer deterministic tests but cannot validate
  the production generator's nil retry or reaction to an entropy failure.
- **Use OpenSSL solely for UUID bytes:** is secure but adds an unnecessary common-target dependency
  when supported operating systems already provide maintained entropy APIs.
- **Force RFC version 4 bits:** would describe a standard UUID subtype that current formats and APIs
  do not require, while discarding entropy bits and changing existing opaque-byte behavior.
- **Keep retrying until nonnil:** makes a broken source an unbounded startup or request hang.
- **Guarantee global uniqueness in the generator:** requires durable namespace state and belongs to
  the authority assigning a table, tablet, part, WAL, or request identity.

## Consequences

The actual generator's failure and retry policy is deterministic and directly testable without
weakening the OS-backed default. Success remains allocation-free. The entropy interface is a narrow
security boundary and does not become a general pseudo-random-number API, deterministic ID scheme,
or global collision registry. The Linux completion loop has a private getrandom-shaped reader seam;
the production adapter and deterministic provider tests use the same loop without exposing syscall
injection in the installed API.

## Affected invariants

This decision supports invariants 1, 8, 10, 14, 16, and 18 by keeping durable identities opaque and
nonzero, bounding failed entropy behavior, preserving explicit byte order, and making the secure
provider contract executable.

## Validation

- Existing system tests generate 32 distinct nonnil candidates through the real OS adapter.
- Injected tests prove nil retry to the first valid value, the exact eight-attempt ceiling, and
  immediate error propagation.
- Provider tests prove exact remaining-length requests after partial progress, `EINTR` retry without
  losing that progress, zero-progress rejection as `EIO`, exact terminal-errno reporting, and
  oversized-provider-result rejection. The Linux `getrandom` adapter delegates to this exact loop.
- Native CREATE composition fails the fifth generated candidate through both an injected entropy
  source and a Linux test-only link wrapper. It returns one execution failure before durable table
  creation; a clean restart observes no table or metadata prefix and a fresh CREATE is non-resumed.
- Header self-containment, installed-consumer, static-analysis, sanitizer, and full-suite gates cover
  the refactored public boundary and unchanged service/WAL consumers.

## Migration or rollback considerations

No durable byte changes. Existing `SystemUuidGenerator` default construction and `UuidGenerator`
injection remain source compatible. Removing the seam requires retaining equivalent executable
coverage for generator error propagation, nil bounds, provider ownership, and the Linux partial-read
and error completion contract.

## References

- [ADR 0014](0014-logical-types-schema-identity-and-evolution.md)
- [ADR 0011](0011-dependency-and-build-versus-buy-policy.md)
- [ADR 0216](0216-durable-database-root-bootstrap.md)
- [ADR 0224](0224-configured-single-node-chronosd.md)
- [Schema foundation](../learning/schema-foundation.md)
