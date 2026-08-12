# Replicated Ingest Coordinator

The coordinator turns many reactor-routed QUORUM_SYNC requests into one finite service-owner set.
It validates negotiated task authority and payload shape, owns each move-only replicated operation,
polls round-robin without blocking, and releases at most one exact response per call.

The connection/request pair is the cancellation and correlation key. A cancel drops response
ownership but does not pretend to roll back Raft. Deadlines create a correlated CANCELLED error;
the same command remains safely retryable through its canonical client identity.

Pending storage is pre-reserved and bounded. Admission and polling are `O(P)` in the worst case;
operation work retains its own Raft/application costs. Measure hot pending counts, completion skew,
poll scan cost, timeout bursts, and response-queue stalls before replacing the vector.

Review questions include why cancellation emits no response, why negotiated context must survive
the reactor handoff, why one response per poll helps backpressure, and why timeout cannot undo an
admitted entry.

The production lifetime is supplied by `ReplicatedIngestRuntime`. It fixes the asynchronous runtime
at its final address before constructing this coordinator, then destroys the coordinator before
worker shutdown. A coordinator must never borrow a stack-local runtime that will later be moved.
