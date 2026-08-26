# ADR 0499: Shared mutable grouped query-control endpoint

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster, service, networking, and security maintainers
- **Extends:** [ADR 0468](0468-packaged-shared-query-control-service.md),
  [ADR 0494](0494-bounded-mutable-grouped-sufficient-state-tcp-server.md), and
  [ADR 0495](0495-owned-mutable-grouped-sufficient-state-tcp-service.md)

## Context

The production mutable grouped server was bounded and authenticated, but it owned a standalone
listener. Committed node metadata advertises one private query-control endpoint. That endpoint
already multiplexed mutable row requests and Raft read-authority requests, so a coordinator could
not both acquire remote authority and send grouped sufficient-state work through the advertised
route. Deriving an uncommitted second port would escape metadata authority.

Mutable row and grouped requests intentionally share the exact `CHDMREQ1` request envelope. Their
response envelopes differ, so the eight-byte request magic alone cannot select the response codec.

## Decision

The shared query-control TLS owner exact-decodes `CHDMREQ1`, then selects the mutable row receiver or
the mutable grouped receiver from the bound plan mode. Grouped requests retain the distinct
`CHDVGRP2` response envelope. Before writing any byte, the shared owner revalidates complete grouped
authority, route correlation, empty-or-contiguous terminal closure, response count and bytes, and
nested decode memory under explicit grouped limits.

The TCP owner retains a distinct completed-grouped metric. The packaged service owner constructs
both request-local `TabletState` workers, both authenticated receivers, and the read-authority
service in dependency-safe order behind the one committed listener. Certificate authentication and
claimed-source node authorization still occur once before protocol dispatch.

This decision only makes the inbound advertised endpoint capable of serving grouped work. It does
not yet cause Native SQL preparation to select the mutable grouped outbound scheduler.

## Consequences

One committed endpoint can now serve remote authority, mutable rows, and mutable grouped
sufficient-state work without port inference or a second listener. The shared event-loop thread
still serializes receiver execution; listener sharding remains measurement-driven future work.

Memory is bounded independently for row responses, grouped response frames and bytes, grouped
nested decode state, read-authority framing, admitted connections, and per-poll accepts. Grouped
decode or validation failure publishes no response prefix.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only proof-revalidated applied mutable state can
  enter the selected worker.
- [Invariant 6](../architecture/invariants.md): request plan mode, exact fragment authority, and
  complete terminal response correlation are retained through dispatch.
- [Invariant 10](../architecture/invariants.md): every grouped payload is exact-decoded under the
  bound key and aggregate authority before response publication.
- [Invariant 11](../architecture/invariants.md): worker, receiver, carrier, socket, and listener
  destruction order is explicit.
- [Invariant 14](../architecture/invariants.md): `CHDMREQ1`, `CHDVRES2`, `CHDVGRP2`, and the Raft
  authority protocol remain distinct frozen formats.
- [Invariant 15](../architecture/invariants.md): authentication and source-node authorization
  precede request dispatch, and all buffers and admissions remain bounded.
- [Invariant 18](../architecture/invariants.md): multiplexing changes no snapshot, authority, or
  response-validation guarantee.

## Validation

The focused loopback test sends a mutable row request, a grouped sufficient-state request, and a
Raft read-authority request through one mutual-TLS listener. It verifies receiver isolation, exact
grouped completion, three authenticated sessions, and distinct completed metrics. The denial case
still rejects an unauthenticated principal before any receiver call. The packaged production-owner
authority test continues to pass with both workers installed. The complete cluster suite passes
263 tests, the complete service suite passes 109 tests, the complete cluster allocation-failure
suite passes 39 tests, and the complete service allocation-failure suite passes 6 tests. The
focused shared endpoint and production-owner cases pass under ASan/UBSan with leak detection
disabled. Changed-file LLVM 18 formatting, the warning-as-error build, and whitespace validation
pass. Repository-wide formatting still reports only the pre-existing violation in the unchanged
grouped Fragment-v2 TLS header self-containment test. Static-analysis availability is reported
separately because the installed LLVM 18 analyzer cannot parse the macOS 26 libc++ builtin set.

## Migration and rollback

No durable or wire format changes. Pre-alpha embedding configuration gains the grouped receiver and
its finite limits. Rollback must first ensure no Native or other caller depends on grouped requests
through the committed query-control endpoint; the standalone grouped server remains independently
available for focused testing.

## Unresolved questions

- Select the mutable grouped scheduler from replicated Native SQL preparation.
- Measure whether grouped worker execution warrants bounded listener sharding.
- Partition grouped sufficient-state by destination and define skew policy.

## References

- [Packaged shared query-control service](0468-packaged-shared-query-control-service.md)
- [Owned mutable grouped sufficient-state TCP service](0495-owned-mutable-grouped-sufficient-state-tcp-service.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)

**Retrospective (2026-08-25):**
[ADR 0500](0500-packaged-mutable-grouped-native-execution.md) selects this endpoint from replicated
Native direct grouped SQL and adds the matching in-process worker path for self-led fragments.

**Retrospective (2026-08-26):**
[ADR 0541](0541-shared-grouped-reducer-job-control-endpoint.md) adds the distinct `CHDVGJC1` reducer-
job protocol without changing `CHDMREQ1` row/grouped plan-mode dispatch.
