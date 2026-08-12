# ADR 0263: Immediate Outbound Raft Terminal Reconnect

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft transport maintainers

## Context

A composed poll loop receives explicit terminal descriptor events. Waiting for a later TLS frame or
connect deadline after `POLLERR`, `POLLHUP`, or invalidation can spin on an always-ready descriptor
and delays retry despite already knowing the transport is unusable.

## Decision

Outbound TLS carriers retain terminal transport closure as their ordinary failed state. The peer
pool exposes that transition by exact node ID. The multi-peer manager handles a terminal event by
finishing an in-progress TCP attempt through authoritative `SO_ERROR`, or by failing an established
carrier, taking every complete retry frame, and transferring them immediately into the existing
capped reconnect policy at the event's monotonic time.

Fresh durable results remain outside reconnect custody and are still rejected without consumption
while the route is unavailable. Only frames already admitted to the failed carrier move into retry.

## Consequences and validation

A unified event loop can react to terminal poll events without deadline spin or suffix retry. The
same descriptor/carrier destruction order and duplicate-safe whole-frame policy remain. Focused
peer-manager coverage proves immediate connected-peer removal and subsequent capped reconnect.
Partial-write terminal matrices and reconnect storms remain Phase 18 work.

## References

- [ADR 0248](0248-persistent-outbound-raft-mtls-carrier.md)
- [ADR 0254](0254-capped-raft-peer-reconnect-policy.md)
- [ADR 0255](0255-bounded-raft-outbound-peer-manager.md)
