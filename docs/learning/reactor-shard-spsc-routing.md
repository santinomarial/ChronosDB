# Reactor-to-Shard SPSC Routing

`SpscNetworkTaskQueue` is the sole ownership handoff from one reactor producer to one shard
consumer. It owns a finite ring, allocates only at creation, preserves FIFO, and reports saturation
without blocking or overwriting work.

The producer initializes a cell before release-publishing its index; the consumer acquire-loads
that index before reading. The consumer clears the cell before release-publishing its index; the
producer acquire-loads that index before reuse. Relaxed accesses apply only to the index exclusively
written by the caller. Destruction occurs after both threads join.

Push/pop are constant time; retained memory is linear in capacity and maximum task size. Tests
cover wraparound, full/empty behavior, 100,000 concurrent frames, allocation failure, TSan, and
public consumption.

The subscription worker also needs to retry an already-encoded response. `try_push_preserving`
checks capacity before moving from its lvalue task, so a full ring leaves the exact payload owned by
the worker. A successful call follows the same cell initialization and release publication as
ordinary push; the consumer path is unchanged.
