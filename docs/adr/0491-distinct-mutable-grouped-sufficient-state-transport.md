# ADR 0491: Distinct mutable grouped sufficient-state transport

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster, query, and replicated-service maintainers
- **Extends:** [ADR 0430](0430-distinct-mutable-vector-query-transport.md),
  [ADR 0477](0477-distinct-grouped-sufficient-state-response-v2.md), and
  [ADR 0490](0490-proof-revalidated-mutable-grouped-sufficient-state-worker.md)

## Context

The mutable grouped worker has exact applied-head authority, but the existing grouped transport
accepts only Manifest/CSEG Fragment-v2 requests. Reusing that endpoint would either discard the
mutable fragment's applied position, commit observation, and barrier or reinterpret them as a
Manifest generation. A new response format is unnecessary because grouped sufficient-state
responses contain route/query/tablet correlation and schema-bound state, not storage authority.

## Decision

Add a distinct mutable grouped endpoint policy. Its request is the existing exact, versioned, and
checksummed `CHDMREQ1` mutable-fragment frame. Its responses are the existing exact `CHDVGRP2`
grouped sufficient-state frames. This pairing is valid only through the new mutable grouped worker
interface; the Fragment-v2 grouped endpoint continues to accept only `CHDVREQ2`.

The receiver authenticates before decoding and authorizing the claimed source, validates the local
target and grouped mode, binds fresh mutable authority, validates it against the fragment's plan and
raw result schema, then executes the worker. Bound and executed authority must remain identical.
The receiver exact-decodes every worker frame under request-local memory, requires one canonical
empty terminal or a complete contiguous group stream, and builds the entire bounded response
vector before publication. A worker failure becomes one correlated payload-free response;
`UNAVAILABLE` alone may include a fresh advisory leader hint.

The sender owns one immutable encoded `CHDMREQ1` request, grouped authority, query resources, and a
finite exponential retry budget. Attempts copy byte-identical request authority. It accepts only a
complete correlated `CHDVGRP2` vector, canonicalizes every outer and nested frame, and retains no
prefix after malformed, partial, over-limit, or allocation-failed input. Hints never rewrite the
fragment's serving node or snapshot proof.

Both owners are synchronous and single-thread-affine. They add no shared-memory publication or
memory-ordering rule. TLS, TCP, scheduler, cancellation, and Native finalization remain enclosing
lifecycle work.

**Retrospective (2026-08-25):** [ADR 0492](0492-bounded-mutable-grouped-sufficient-state-mutual-tls.md)
adds the connected mutual-TLS lifecycle while preserving this endpoint's exact mutable request and
grouped response types. TCP acquisition/listening and scheduler ownership remain outside it.

## Consequences

Mutable applied-head grouped states can cross a process boundary without weakening their proof or
inventing duplicate response bytes. Wire discrimination remains unambiguous: `CHDMREQ1` names a
mutable fragment, `CHDVREQ2` names Fragment-v2, and `CHDVGRP2` carries storage-authority-agnostic
grouped state only after endpoint-specific admission.

The endpoint duplicates a small amount of receiver/sender policy rather than creating a generic
request variant. That duplication preserves closed authority types and keeps accidental mutable/
Manifest conversion impossible at compile time.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): source, target, mutable snapshot proof, grouped plan,
  and response authority are rebound and correlated before publication.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 14](../architecture/invariants.md): both reused wire formats retain their versions,
  checksums, exact decoding, and distinct application magic.
- [Invariant 15](../architecture/invariants.md): decode memory, response count/bytes, retries, and
  backoff are finite; allocation failures expose no partial stream.
- [Invariant 18](../architecture/invariants.md): mutable and Manifest-backed grouped endpoints share
  state semantics while retaining disjoint snapshot authority.

## Validation

Focused receiver tests prove authentication-before-binding, exact mutable authority binding,
complete canonical publication, authority-drift and incomplete-stream rejection, and correlated
leader-hint failure encoding. Sender tests prove exact `CHDMREQ1` decoding, byte-identical retry,
partial-vector rejection, and complete result retention. Deterministic allocation injection walks
authority/execution/publication and canonical reconstruction until success, requiring prior
failures to classify as resource exhaustion and decoded key credit to return to zero.

The complete cluster, cluster-allocation, and service suites pass 250, 33, and 108 tests
respectively when loopback permission is available. The five focused receiver/sender and allocation
cases pass under ASan/UBSan with leak detection disabled. The warning-as-error build, changed-file
LLVM 18 formatting, and whitespace validation pass. LLVM 18 static analysis found and prompted one
project-local automatic-move fix; after that fix it reports no project-local finding but cannot
complete because the installed macOS 26 libc++ requires compiler builtins unavailable to LLVM 18.
The repository-wide format check reports only the pre-existing violation in the unchanged grouped
TLS header self-containment test; every changed C++ file passes the same pinned formatter.

## Migration and rollback

This is an additive pre-alpha API and introduces no new wire bytes. Rollback removes the mutable
grouped endpoint policy and restores the worker as an in-process-only boundary; the independent
`CHDMREQ1` and `CHDVGRP2` codecs remain valid.

## Unresolved questions

- Deadline-bound outbound TCP and bounded inbound listener ownership.
- All-tablet mutable grouped scheduling and atomic Native path selection.
- Computed pre-group programs and partitioned shuffle/skew policy.

## References

- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
