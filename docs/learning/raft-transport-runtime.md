# Unified Raft Transport Runtime

## Purpose and public interface

`RaftTransportRuntime` is the production-facing single-thread owner that turns the deterministic and
durable Multi-Raft layers into a live node transport. It owns inbound TCP/TLS sessions, outbound
peer connections and reconnect policy, election/heartbeat timing, one poll table, and a bounded FIFO
of completed results. It borrows `AsyncDurableMultiRaftRuntime`; that owner and all authentication,
authorization, TLS-context, receiver, and deadline-source dependencies must outlive it.

`add_group` arms a recovered owning observation. `poll_once` blocks no longer than the caller's
limit or the earliest exact monotonic deadline. `try_submit_application` lets that same poll owner
admit one bounded application transition plus an automatically ordered group observation.
`take_completed` moves one FIFO result only after its network output is queued; application,
snapshot installation, and read-barrier handling remain with the embedding. A returned result
identifies inbound, timer, or application origin; remote source or timer action; exact group;
ordered observation; durable transition; and runtime-lifetime submission sequence.

`service::ReplicatedRaftTransportRuntime` is the address-stable production composition above this
poll owner. It owns the immutable certificate/address/node authority, receiver, one TLS client
context per remote identity, randomized election source, and the unified runtime in destruction-safe
order. Creation obtains each initial observation from the asynchronous durable owner and rejects a
group/local-node mismatch before returning. Its completed-result API intentionally remains visible;
the service layer cannot discard snapshot-install or read-barrier work merely because ordinary
election traffic needs no additional handling.

## Data structures and invariants

The poll table is rebuilt into pre-reserved storage from four exact owner kinds: durable wakeup,
listener, stable inbound connection ID, and fixed outbound node ID. No pointer or compacting vector
index crosses the poll call. Its configured bound includes all four categories.

The result ring and application-completion slots are fixed at construction. Inbound, timer, and
application owners each preserve durable FIFO submission identity; intake compares all three heads.
Results are routed in that same order and stop at the first missing/full route. A routed entry stays
owned until pickup, but does not prevent later FIFO entries from being queued. This separates
network liveness from potentially slower tablet application without creating another unbounded
queue. Focused real mutual-TLS coverage retains application, timer, and inbound completions together
and observes their exact consecutive durable-submission order at pickup.

Timer rearming uses the observation executed immediately after each inbound receive or timer action
in the same durable batch. A newer inbound activity observation advances the timer generation, so a
stale timer completion may still be returned and routed but cannot rewrite the newer deadline.
Application submission also obtains an observation in its exact batch, but does not count as peer
activity and therefore cannot postpone a follower election. Operations already owned by the timer,
receiver, or observation API are rejected from the application lane.

## Ownership, lifetime, and synchronization

One event-loop thread owns every method; no runtime field is concurrently accessed. Cross-thread
publication occurs only inside `AsyncDurableRaftCompletion`: the durable worker installs the owning
result under its completion mutex and then signals the nonblocking completion pipe. Poll readability
therefore follows result publication. Draining coalesced pipe bytes is safe because the loop scans
all completion owners before blocking again.

The runtime is the sole pipe consumer. After any durable wake, `poll_once` completes internal
progress and returns, so its caller can inspect additional completion owners sharing the same durable
worker. A second independent drainer would violate the wakeup contract.

The service owner must be created while consensus admission is quiescent, then polled by exactly one
thread. It must be shut down before `ReplicatedIngestRuntime` closes the borrowed durable worker.
Moving the outer owner moves only its PIMPL pointer; borrowed addresses inside that allocation remain
stable.

## Failure and backpressure

Disconnected or full outbound routes retain the first unrouted result. Established terminal peers
transfer complete original frames to capped reconnect; accepted inbound work survives disconnect
until pickup. A full result ring leaves component completions owned where they are. Ordinary durable
operation errors are returned as results. Corrupt sequencing, timer inconsistency, listener/poll
failure, descriptor-bound overflow, and unexpected routing errors fail the aggregate owner closed.
Application admission returns `RESOURCE_EXHAUSTED` before durable submission when its fixed slot
bound is full.

Focused saturation coverage leaves one voter destination without a configured route, retains that
timer result at the FIFO head, fills the result ring with a later application completion, and proves
that the next completed application remains in its fixed slot while further admission is rejected.

## Complexity, tradeoffs, and interview questions

Each poll-table rebuild is linear in active inbound plus configured outbound peers. Deadline scans
are linear in groups and sessions. Completion intake and routing are bounded by configured result
counts and transport queue capacity. This favors auditable bounded ownership over a more complex
incremental registration structure; profiling is required before changing it.

- Why is FIFO submission sequence different from physical persistence sequence?
- Why does result pickup wait for outbound routing, but not for socket delivery?
- Why must accepted inbound work outlive peer disconnect?
- What release/acquire edge publishes a durable completion before pipe readiness?
- Why does routing stop at the first backpressured result?
- Which dependencies must outlive the aggregate owner?
