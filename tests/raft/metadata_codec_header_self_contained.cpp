#include "chronos/raft/metadata_codec.hpp"

static_assert(chronos::raft::kMaximumMetadataCommandSize == std::size_t{64U} * 1024U);
