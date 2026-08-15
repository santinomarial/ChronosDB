#include "chronos/cluster/tablet_physical_part_chunk.hpp"

static_assert(chronos::cluster::kMaximumTabletPhysicalPartChunkSize ==
              std::size_t{16U} * 1024U * 1024U);
static_assert(chronos::cluster::TabletPhysicalPartChunkCodecLimits{}.maximum_chunk_bytes ==
              std::size_t{4U} * 1024U * 1024U);

[[maybe_unused]] auto* const kEncodeTabletPhysicalPartChunk =
    &chronos::cluster::encode_tablet_physical_part_chunk_v1;
[[maybe_unused]] auto* const kDecodeTabletPhysicalPartChunk =
    &chronos::cluster::decode_tablet_physical_part_chunk_v1;
