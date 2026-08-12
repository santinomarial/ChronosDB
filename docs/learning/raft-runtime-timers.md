# Raft Runtime Timers

`RaftTimerRuntime` turns injected monotonic deadlines and current group observations into bounded
election or heartbeat actions. It does not call a clock or random generator: production supplies a
randomized future election deadline, while a simulator supplies the exact deterministic deadline it
wants to explore.

Each group stores an owning observation, one deadline, a nonzero generation, and an in-flight bit.
Polling at the deadline emits at most the configured action count and prevents duplicate emission.
Admission rejection clears only the in-flight bit, so overload does not fabricate a later deadline.
Activity and successful action completion both rearm from a current observation and advance the
generation; therefore an older completion cannot replace newer timing state.

Leader observations schedule the fixed heartbeat interval. Followers and candidates require a
strictly future caller-selected election deadline. Saturating heartbeat arithmetic avoids time-point
overflow. Group storage is reserved at construction, deadline scans are linear in configured group
count, and returned work is bounded by `maximum_actions_per_poll`.

The scheduler is single-thread-affine. A production event loop owns every call; asynchronous durable
results cross their existing completion mutex acquire edge before becoming observations here. This
keeps timing policy outside `RaftNode` and avoids adding atomics without a sharing requirement.

Focused tests cover boundary firing, backpressure retry, heartbeat rearm, stale generations, and
bounds. End-to-end event-loop composition, randomized timeout selection, trace replay, clock-change
tests, and scheduling benchmarks remain future validation.
