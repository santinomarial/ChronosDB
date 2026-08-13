# Native Protocol Request Lifecycle

The portable `ServerConnectionState` owns a pre-reserved finite set of active request IDs and the
negotiated Protocol v1 payload ceiling. It accepts already integrity-checked frames, validates the
message payload and connection phase, and emits a small dispatch action. It never performs socket
I/O or tablet/query work.

The connection moves `awaiting hello -> active -> closed`. Positive ingest/query/subscription IDs
strictly increase, preventing a late completion from being confused with reused work. Cancellation
removes finite ingest/query ownership immediately. For a negotiated subscription it marks one
terminal transition but retains ownership until `SUBSCRIPTION_END` or `ERROR`, so the last safe
resume token can be delivered. Repeated cancellation is idempotent. Close clears all active state.

Message decoders borrow canonical command/SQL/diagnostic bytes from an owned frame. Encoders own
their bytes and classify allocation failure. Ingest acknowledgements always expose requested and
effective durability; matching retry outcomes carry no fabricated WAL position.

Protocol 2 may negotiate a structured leader redirect. The state owner accepts it only for ingest
or finite query work before any query batch, then releases the request exactly once. The payload
names a group, observed leader node/term, and placement epoch but deliberately omits an endpoint and
does not grant a lease. Reactor-side validation repeats canonical decoding before enqueueing bytes.

Protocol 1.0 remains the default. A client explicitly requests minor 1 and subscription feature bit
0; the server intersects supported features, and both sides then require the exact selected minor
on every post-handshake frame. Subscription state enforces finite snapshot `QUERY_RESULT` completion
before READY, increasing live delivery sequences, acknowledgements no later than delivered state,
checkpoint confirmation no later than acknowledged state, and one terminal end.

Lifecycle operations are linear in the configured active-request count, currently capped at
65,536 and normally 64. This simple bounded vector is deliberate; no profile yet justifies a more
complex hash table.

Review questions: request IDs cannot be reused because cancellation and completion race; hello
negotiates resources rather than changing frame compatibility; and a retry acknowledgement lacks a
new WAL position because the retry performed no new durable operation.
