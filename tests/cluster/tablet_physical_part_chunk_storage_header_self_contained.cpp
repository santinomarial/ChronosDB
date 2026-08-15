#include "chronos/cluster/tablet_physical_part_chunk_storage.hpp"

static_assert(chronos::cluster::kDefaultMaximumTabletPhysicalPartChunkFiles ==
              std::size_t{256U} * 1024U);

[[maybe_unused]] auto* const kTabletPhysicalPartChunkStorageCreate =
    &chronos::cluster::TabletPhysicalPartChunkStorage::create;
