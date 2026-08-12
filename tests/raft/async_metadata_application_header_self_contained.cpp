#include "chronos/raft/async_metadata_application.hpp"

[[maybe_unused]] constexpr auto kMetadataSnapshotSize =
    sizeof(chronos::raft::MetadataCatalogSnapshot);
