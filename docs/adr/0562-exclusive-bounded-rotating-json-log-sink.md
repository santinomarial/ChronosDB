# ADR 0562: Exclusive bounded rotating JSON log sink

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB common-foundation, operations, and daemon maintainers

## Context

ChronosDB can encode and synchronously write one bounded structured diagnostic, but `chronosd`
depends on shell redirection for file collection. There is no owned file lifetime, size bound,
rotation policy, competing-process exclusion, or explicit terminal failure state. This leaves an
early Phase 1 operational gate incomplete and allows an unattended daemon to grow one file without
bound.

Logs are observability evidence, not database authority. Their storage policy must not be confused
with WAL/Raft durability or make daemon service depend on a collector.

## Decision

Add `RotatingJsonLogSink` to `chronos_common`. Its factory owns:

- one configured append-only regular active file opened without following a final symlink;
- one adjacent `.lock` regular file with a nonblocking exclusive `flock` held for the owner lifetime;
- a byte bound for the active generation; and
- zero through 64 retained archives, with `.1` newest.

The sink encodes before taking its mutex. Invalid or individually oversized records do not mutate or
poison it. Under the mutex, it checks the exact next size. Before a nonempty active file would exceed
the bound, it removes the oldest retained archive, renames older suffixes upward, renames the active
file to `.1`, and opens a new append-only active file. Zero retained files removes the old active
generation. It then writes and flushes exactly one complete JSON line.

Any file write, flush, close, removal, rename, or replacement-open failure makes the sink terminal;
later writes return the same failure. The mutex supplies the happens-before relationship for the
stream, current-size counter, archive transition, and terminal status. No lock-free algorithm or
atomic memory ordering is involved.

`chronosd` exposes `--log-file`, `--log-max-bytes`, and `--log-retained-files`. File output requires
`--log-format json`. Sink setup failure prevents startup; a runtime sink failure emits a fixed
critical fallback to the event's original stdout/stderr stream. The configured parent directory must
already exist.

## Detailed rationale

Opening with append semantics prevents ordinary seek races from overwriting prior bytes. Rotating
before the next line keeps every accepted generation within its configured bound, except an
oversized pre-existing active file which is rotated before the next acceptable write. A complete
line larger than the bound is rejected rather than split.

The adjacent lock prevents two cooperating processes from independently rotating one namespace.
Keeping the lock file separate lets the lock survive active-file rename. `flock` is available on the
declared Linux and macOS targets and, unlike a process-scoped record lock, rejects a second separately
opened owner in the same process as well.

## Alternatives considered

- **Require shell redirection/logrotate only:** simple, but leaves ownership and bounded behavior
  outside the executable and provides no startup conflict signal.
- **Synchronize every line with `fsync`:** rejected because diagnostics are not acknowledged data;
  this would add foreground latency without upgrading any database durability contract.
- **Compress archives in-process:** deferred because it adds CPU scheduling, temporary-file, and
  crash-recovery policy without evidence of a current need.
- **Continue after a partial rotation failure:** rejected because the sink can no longer prove its
  archive ordering or active-file ownership.
- **Delete an existing parent or create it implicitly:** rejected because directory provisioning and
  permissions are deployment authority.

## Consequences

Operators can bound `chronosd` JSON log storage without an external rotation race, and startup
reports conflicting ownership. Existing stdout/stderr text and JSON behavior remains the default;
no current invocation changes meaning.

Rotation uses filename replacement inside the configured directory but is not a crash-durable
transaction. A process or filesystem failure between renames may leave missing archive ordinals;
reopening preserves the files it sees and the next rotation continues the bounded suffix policy.
The sink does not directory-sync, compress, ship, parse, or authenticate archives.

## Affected invariants

- [Invariant 11](../architecture/invariants.md): one RAII owner retains the lock descriptor and
  stream; destruction releases both after the final use.
- [Invariant 15](../architecture/invariants.md): line, file-size, and retained-generation bounds are
  explicit and terminal I/O failure cannot be reported as success.
- [Invariant 18](../architecture/invariants.md): bounded structured operational evidence becomes
  available from the packaged daemon without weakening data correctness checks.

## Validation plan

Eight focused common tests cover exact archive order/eviction, zero-retention replacement,
same-process lock exclusion and reopen, concurrent writers, oversized existing-file rotation,
terminal rotation failure, configuration limits, non-poisoning oversized-record rejection, and
active-file symlink refusal.
All 68 common tests pass on Apple arm64 and under Ubuntu GCC 13 with repository warnings-as-errors;
the focused suite passes under ASan/UBSan and TSan. All 22 Linux daemon-process tests pass. The new
real-process case also passes under ASan/UBSan: `chronosd` writes a startup event, shuts down, reopens
the same sink with the first generation as its bound, rotates it exactly, and preserves the prior
bytes. CLI help and invalid-option exit behavior, formatting, workflow-pinning, whitespace, and
final-diff checks pass.

Pinned clang-tidy 18 is blocked by the known macOS 26 libc++ unsupported-builtin compiler errors. A
focused rerun after correcting its one new diagnostic reports no project-source diagnostic before
those system-header errors. Existing diagnostics in `chronosd` remain outside this change.

## Migration or rollback considerations

No migration exists. Existing stdout/stderr logging remains unchanged unless all file options are
explicitly supplied. Rollback leaves diagnostic `.jsonl`, numbered archives, and `.lock` files that
operators may remove after confirming no process owns the path.

## Unresolved questions

- Add an external collector/export boundary and its backpressure/drop policy.
- Decide whether archive compression has measured operational value.
- Add deterministic syscall injection for every rotation transition if the sink becomes release-
  critical evidence rather than best-effort diagnostics.
- Complete durable identity-domain collision policy and the broader Phase 1 test-utility gate.

## References

- [Common binary foundations](../learning/common-binary-foundations.md)
- [Native server operations](../operations/native-server.md)
- [Implementation roadmap](../roadmap.md)
- [Correctness testing and performance evidence](0012-correctness-testing-and-performance-evidence.md)
