#ifndef CHRONOS_CLUSTER_TABLET_PHYSICAL_RECEIPT_RECLAMATION_HPP_
#define CHRONOS_CLUSTER_TABLET_PHYSICAL_RECEIPT_RECLAMATION_HPP_

#include "chronos/cluster/tablet_physical_movement_readiness.hpp"
#include "chronos/cluster/tablet_physical_part_chunk_storage.hpp"
#include "chronos/common/result.hpp"
#include "chronos/manifest/temporal_publication.hpp"

#include <cstdint>

namespace chronos::cluster {

struct TabletPhysicalReceiptReclamationReport {
  ReclaimedTabletPhysicalPartReceipt receipt;
  std::uint64_t destination_manifest_generation{};
  ingest::Sha256Digest part_set_checksum;
};

// Reclaims one completed physical-part receipt only while the movement is durably ready and the
// supplied immutable destination publication still owns that exact part at the RTAS/Raft boundary.
// The storage owner installs its session-bound durable marker before deleting any chunk.
[[nodiscard]] common::Result<TabletPhysicalReceiptReclamationReport>
reclaim_tablet_physical_part_receipt(TabletPhysicalPartChunkStorage& receipt,
                                     const manifest::TemporalDatabaseStorageSnapshot& destination,
                                     const TabletPhysicalMovementReadinessReport& readiness);

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_TABLET_PHYSICAL_RECEIPT_RECLAMATION_HPP_
