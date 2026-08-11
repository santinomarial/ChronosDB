#ifndef CHRONOS_CLUSTER_TABLET_PHYSICAL_SNAPSHOT_OWNERSHIP_HPP_
#define CHRONOS_CLUSTER_TABLET_PHYSICAL_SNAPSHOT_OWNERSHIP_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/manifest/temporal_validation.hpp"
#include "chronos/raft/types.hpp"

#include <functional>
#include <span>

namespace chronos::cluster {

struct TabletPhysicalSnapshotOwnershipRequest {
  common::ByteView physical_snapshot;
  raft::GroupId group_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  std::reference_wrapper<const raft::SnapshotMetadata> raft_snapshot;
  std::span<const manifest::TabletSchemaBinding> schema_bindings;
  std::span<const manifest::TemporalTabletSourceBinding> source_bindings;
  common::Uuid manifest_nonce;
  manifest::ManifestDecodeLimits decode_limits;
  manifest::TemporalPartValidationLimits part_validation_limits;
};

struct PublishedTabletPhysicalSnapshot {
  manifest::RaftTabletPhysicalSnapshotReport authority;
  manifest::TemporalDatabaseStorageSnapshot destination;
  bool manifest_already_durable{};
};

// Requires every projected CSEG final to be installed already. Under external single-writer
// serialization this builds the exact local successor, installs/reloads it, and release-publishes
// one owning epoch. It also resumes an exact successor already durable after an interrupted prior
// attempt. No success advances Raft movement readiness or authorizes source reclamation.
[[nodiscard]] common::Result<PublishedTabletPhysicalSnapshot>
install_and_publish_tablet_physical_snapshot(manifest::ManifestStorage& storage,
                                             manifest::TemporalDatabaseStoragePublisher& publisher,
                                             const TabletPhysicalSnapshotOwnershipRequest& request);

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_TABLET_PHYSICAL_SNAPSHOT_OWNERSHIP_HPP_
