# Replicated Ingest Operation

`ReplicatedIngestOperation` owns one nonblocking COLUMNAR_APPEND from canonical command admission
through a protocol-v2 QUORUM_SYNC result. It exact-decodes before admission, proposes under a
required leader term, validates the post-sync persistence result and command identity, waits for the
exact worker-applied receipt, and verifies the tablet retry publication before returning success.

The operation has proposal, receipt, and complete phases. `poll` never waits: the service event loop
retains the move-only owner and calls it only when coordinating completions. Destruction abandons
response ownership but cannot roll back a durable proposal. Runtime and application owners must
outlive it.

The published retry outcome identifies the first logical application index. Equality with the
attempt receipt index means `APPLIED`; a larger receipt index with the identical retry/mutation
identity means `MATCHING_RETRY`. A missing, foreign, WAL-sourced, or future outcome is corruption.

Proposal/result validation is `O(retained log bytes)` today because the durable transition owns a
complete persistent state, while receipt and retry lookup follow their existing bounded owners. No
performance claim is made. Measure retained-log copy cost, proposal-to-application latency, polling
overhead, retries, and response-queue delay before optimizing the result contract.

Likely review questions: why bind the leader term, why decode the persisted command again, why can
destruction not cancel Raft, and why does the first retry record sequence distinguish outcomes?
