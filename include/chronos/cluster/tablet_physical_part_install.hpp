#ifndef CHRONOS_CLUSTER_TABLET_PHYSICAL_PART_INSTALL_HPP_
#define CHRONOS_CLUSTER_TABLET_PHYSICAL_PART_INSTALL_HPP_

#include "chronos/cluster/tablet_physical_part_chunk_storage.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstdint>
#include <functional>

namespace chronos::cluster {

inline constexpr std::uint64_t kDefaultMaximumPhysicalPartMaterializedBytes = 256U * 1024U * 1024U;

struct TabletPhysicalPartInstallRequest {
  std::uint64_t expected_manifest_generation{};
  manifest::TemporalPartDescriptor descriptor;
  manifest::TemporalTabletDescriptor owner;
  std::reference_wrapper<const schema::TableSchema> schema;
  common::Uuid nonce;
  std::uint64_t maximum_materialized_bytes{kDefaultMaximumPhysicalPartMaterializedBytes};
  manifest::TemporalPartValidationLimits validation_limits;
};

struct InstalledTabletPhysicalPart {
  CompletedTabletPhysicalPartTransfer transfer;
  manifest::InstalledTemporalPart part;
};

// The transfer and destination owners must remain alive and externally serialized for this call.
// The complete received object is materialized only below the explicit request cap, exact-adopted
// as canonical CSEG v2, and then installed through ManifestStorage's existing fsync/rename
// protocol. Success does not publish a Manifest generation, advance movement readiness, or remove
// chunks.
[[nodiscard]] common::Result<InstalledTabletPhysicalPart>
install_tablet_physical_part(const TabletPhysicalPartChunkStorage& transfer,
                             manifest::ManifestStorage& destination,
                             const TabletPhysicalPartInstallRequest& request);

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_TABLET_PHYSICAL_PART_INSTALL_HPP_
