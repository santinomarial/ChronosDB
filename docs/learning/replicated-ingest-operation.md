# Replicated Ingest Operation

`ReplicatedIngestOperation` owns one nonblocking COLUMNAR_APPEND from canonical command admission
through a protocol-v2 QUORUM_SYNC result. It exact-decodes before admission, proposes under a
required leader term, validates the post-sync persistence result and command identity, waits for the
exact worker-applied receipt, and verifies the tablet retry publication before returning success.

`ReplicatedIngestCoordinator` supplies its production-facing route. It decodes the tablet and table
from the same canonical command, joins committed placement with the immutable tablet-group binding,
and queues an ordered observation of that derived group. Polling revalidates the metadata snapshot,
requires stable voters to exactly equal placement replicas, and admits only when this node is the
current leader. The proposal is fenced by the exact observed term. Leader hints never authorize a
write, and a joint or placement-divergent group fails closed until its authorities converge.
Before observation admission, the coordinator also requires the command's schema identity and
version to be the committed active definition and validates its complete columnar shape. It repeats
the active identity check after observation, preventing a concurrent schema activation from
allowing an old-schema command into the data log.

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

Likely review questions: why bind the leader term, why validate metadata both before and after the
ordered observation, why reject writes during joint membership, why decode the persisted command
again, why can destruction not cancel Raft, and why does the first retry record sequence
distinguish outcomes?
