# Phase 16 Lightweight Query/Movement Gate

## Status

The focused feature gate passes for the currently supported projected Float64 global-aggregate
path. This is not the full Phase 16 roadmap exit and is not a packaged multi-process cluster claim.

## Executed path

`DistributedQueryMovementGateTest.QueryResultIsStableAcrossCompletedTabletMovement` performs the
following in one deterministic test:

1. binds two tablet dispatches to one compatible Manifest v2 generation;
2. starts three real loopback query listeners with mutual TLS and node-principal authorization;
3. executes the two-tablet aggregate against source node 11 and stable node 12;
4. begins movement of the first tablet from source 11 to target 13 with target 13 as a learner;
5. transfers two sequential CRC-checked snapshot chunks and verifies the complete snapshot CRC;
6. records catch-up through the snapshot boundary;
7. records externally committed target promotion at placement epoch 13;
8. records externally committed source removal at placement epoch 14;
9. rebinds a new compatible two-tablet execution to target node 13 and stable node 12; and
10. executes the aggregate again and exact-compares count, sum, minimum, maximum, mean, and M2.

Both queries use the maintained TCP/TLS client/server carriers and complete scheduler rather than
in-process response injection. The stable tablet is contacted in both executions, the old source
only before movement, and the target only afterward.

## Correctness boundary

The movement state machine's `promote_target` and `remove_source` calls record milestones that the
production contract requires an externally committed Raft/metadata proof to establish. This gate
does not duplicate the separate durable reconfiguration, checkpoint, physical CSEG ownership, and
source-reclamation tests, and it does not substitute direct calls for those production owners.

The worker services return deterministic aggregate states; they do not scan an actual remotely
installed CSEG in this gate. The two compatible Manifest fixtures retain the same database, schema,
tablet, group, projection, read policy, and logical data, while the moved tablet changes its serving
node and placement epoch. Thus the test proves query routing/result continuity around the movement
contract, not a full storage/process deployment.

## Remaining Phase 16 exit work

- general vector physical-fragment and grouped exchange semantics beyond the supported global
  aggregate;
- automatic metadata acquisition and service-level rebinding;
- packaged multi-process failure/partition validation and real remote CSEG execution;
- broader deterministic failure, stale-route, skew, and movement-state matrices; and
- declared scale-out, exchange, coordination, movement, and consistency-cost measurements.

These remain in the deferred-validation ledger and must not be inferred as passed from this focused
gate.
