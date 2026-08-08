# Native Protocol Request Lifecycle

The portable `ServerConnectionState` owns a pre-reserved finite set of active request IDs and the
negotiated Protocol v1 payload ceiling. It accepts already integrity-checked frames, validates the
message payload and connection phase, and emits a small dispatch action. It never performs socket
I/O or tablet/query work.

The connection moves `awaiting hello -> active -> closed`. Positive ingest/query IDs strictly
increase, preventing a late completion from being confused with reused work. Cancellation removes
active ownership immediately and is idempotent for an issued ID. Completion also removes exactly
one active ID. Close clears all active state and is terminal.

Message decoders borrow canonical command/SQL/diagnostic bytes from an owned frame. Encoders own
their bytes and classify allocation failure. Ingest acknowledgements always expose requested and
effective durability; matching retry outcomes carry no fabricated WAL position.

Lifecycle operations are linear in the configured active-request count, currently capped at
65,536 and normally 64. This simple bounded vector is deliberate; no profile yet justifies a more
complex hash table.

Review questions: request IDs cannot be reused because cancellation and completion race; hello
negotiates resources rather than changing frame compatibility; and a retry acknowledgement lacks a
new WAL position because the retry performed no new durable operation.
