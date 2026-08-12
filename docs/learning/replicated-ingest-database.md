# Replicated-Ingest Database Recovery

`ReplicatedIngestDatabase` bridges the durable database root and the address-stable replicated
runtime. Deployment configuration says which Raft groups reside on this node and their voter
configuration. Committed metadata says which tablet each data group owns, its table, active and
retained schemas, placement, and retry policy. Keeping those roles separate prevents a daemon flag
from silently overriding consensus state.

Startup has two log-owner stages. A temporary synchronous durable runtime recovers only the metadata
catalog and then closes. That owning projection is used to build bounded tablet/retry owners. The
final asynchronous runtime reopens the same log, recovers metadata and every selected tablet on its
worker, and only then exposes the coordinator. The two log owners never overlap.

```text
Database root lock
  -> temporary Raft-log owner -> committed metadata projection -> close
  -> build resident tablet owners
  -> asynchronous ReplicatedIngestRuntime -> native service
```

The committed catalog is cluster-global, so remote bindings are skipped when their placement does
not include this node and their group is absent from the resident configuration. A locally placed
group cannot be omitted. A configured data group cannot lack one committed binding. Placement and
current membership are deliberately not forced equal during startup because a durable
reconfiguration can be in progress; write admission performs the stable-membership proof.

Projection is linear in catalog definitions, bindings, and configured groups with straightforward
searches favored for correctness at the present scale. Recovery cost is dominated by reading the
Raft log/application snapshots twice and replaying tablet suffixes. A later profile can justify a
single-pass discovery format or indexed catalog projection without weakening authority.

Failure closes temporary owners through RAII and never exposes a partial runtime. Shutdown drains
the asynchronous owner before releasing the root lock. Likely review questions include why group
membership remains external, why the log is reopened, why remote catalog entries are not local
owners, and why placement equality is checked at admission rather than recovery.
