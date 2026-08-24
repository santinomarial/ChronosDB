# Replicated Ingest Coordinator

The coordinator turns many reactor-routed QUORUM_SYNC requests into one finite service-owner set.
It validates negotiated task authority and payload shape, owns each move-only replicated operation,
polls round-robin without blocking, and releases at most one exact response per call.

The ordered routing observation may terminally redirect a negotiated client when this node is a
stable follower, the named remote leader is inside the exact committed placement, the tablet/group
binding is unchanged, and membership is not transitioning. The response carries the current
placement epoch and observed term. Missing leaders, candidates, divergent membership, or
unnegotiated clients receive an error; metadata leader hints never authorize a redirect.

The connection/request pair is the cancellation and correlation key. A cancel drops response
ownership but does not pretend to roll back Raft. Deadlines create a correlated CANCELLED error;
the same command remains safely retryable through its canonical client identity.

`poll(observer)` borrows one progress observer only for that synchronous call. On the coordinator
thread, a successful write reports route validated, proposal admitted, application proved, and
response ready in exact order with the same connection/request key. Errors and redirects report no
write stage. The callback cannot throw, reenter, or mutate the coordinator, and blocking it blocks
polling. The observer is a test/embedding boundary; it changes no durable or network format.

The packaged `SIGKILL` matrix uses those stages as recovery contracts. Route validation is before
proposal submission and must recover the prior state. Admission may recover prior or committed
state because the worker is concurrent. Application proof and response readiness both require the
committed rows and retry identity. An exact retry resolves either allowed outcome without duplicate
rows, and another reopen preserves it.

Pending storage is pre-reserved and bounded. Admission and polling are `O(P)` in the worst case;
operation work retains its own Raft/application costs. Measure hot pending counts, completion skew,
poll scan cost, timeout bursts, and response-queue stalls before replacing the vector.

Review questions include why cancellation emits no response, why negotiated context must survive
the reactor handoff, why one response per poll helps backpressure, and why timeout cannot undo an
admitted entry.

The production lifetime is supplied by `ReplicatedIngestRuntime`. It fixes the asynchronous runtime
at its final address before constructing this coordinator, then destroys the coordinator before
worker shutdown. A coordinator must never borrow a stack-local runtime that will later be moved.

`ReplicatedIngestService` is the queue-facing layer. It consumes reactor tasks, converts admission
failures to correlated errors, forwards exact cancellations, polls this coordinator, and retains
one owning response across SPSC backpressure. It also owns at most one joined Native query thread,
which leaves the queue owner available to consume an exact connection/request cancellation while a
synchronous distributed query advances. A matching cancel publishes a sticky cooperative token and
suppresses the whole result; a mismatched cancel still targets ingest. Its poll result tells the
embedding when a response was actually enqueued and the reactor should be woken.
