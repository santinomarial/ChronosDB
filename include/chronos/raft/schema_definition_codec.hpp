#ifndef CHRONOS_RAFT_SCHEMA_DEFINITION_CODEC_HPP_
#define CHRONOS_RAFT_SCHEMA_DEFINITION_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/metadata.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kSchemaDefinitionHeaderSize = 48U;
inline constexpr std::size_t kSchemaDefinitionTrailerSize = 4U;
inline constexpr std::size_t kMaximumSchemaDefinitionSize = 16U * 1024U * 1024U;
inline constexpr std::uint8_t kRaftSchemaDefinitionEntryType = 3U;

struct SchemaDefinitionCodecLimits {
  std::size_t maximum_definition_bytes{kMaximumSchemaDefinitionSize};
  std::size_t maximum_table_name_bytes{1024U};
  std::size_t maximum_column_name_bytes{1024U};
  std::size_t maximum_columns{schema::kMaximumSchemaColumnCount};
  std::size_t maximum_role_columns{schema::kMaximumSchemaColumnCount};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_schema_definition_v1(const CatalogTableDefinition& definition,
                            SchemaDefinitionCodecLimits limits = {});

[[nodiscard]] common::Result<CatalogTableDefinition>
decode_schema_definition_v1(common::ByteView bytes, SchemaDefinitionCodecLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_SCHEMA_DEFINITION_CODEC_HPP_
