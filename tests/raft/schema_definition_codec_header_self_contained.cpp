#include "chronos/raft/schema_definition_codec.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    &chronos::raft::kRaftSchemaDefinitionEntryType;
}

static_assert(chronos::raft::kMaximumSchemaDefinitionSize == std::size_t{16U} * 1024U * 1024U);
