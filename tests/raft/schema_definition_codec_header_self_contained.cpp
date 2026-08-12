#include "chronos/raft/schema_definition_codec.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    &chronos::raft::kRaftSchemaDefinitionEntryType;
}
