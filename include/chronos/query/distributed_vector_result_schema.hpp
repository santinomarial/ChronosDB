#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_RESULT_SCHEMA_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_RESULT_SCHEMA_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_plan.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace chronos::query {

namespace distributed_vector_result_schema_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 48U;
inline constexpr std::size_t kDescriptorFixedLength = 16U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::uint32_t kMaximumColumns = 4096U;
inline constexpr std::uint32_t kMaximumNameLength = 1024U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength +
    static_cast<std::size_t>(kMaximumColumns) * (kDescriptorFixedLength + kMaximumNameLength) +
    kTrailerLength;
} // namespace distributed_vector_result_schema_format

struct DistributedVectorResultColumn {
  std::string name;
  schema::LogicalType type;
  bool nullable{};

  friend bool operator==(const DistributedVectorResultColumn&,
                         const DistributedVectorResultColumn&) = default;
};

struct DistributedVectorResultSchema {
  std::vector<DistributedVectorResultColumn> columns;

  friend bool operator==(const DistributedVectorResultSchema&,
                         const DistributedVectorResultSchema&) = default;
};

struct DistributedVectorResultSchemaDecodeLimits {
  std::size_t maximum_frame_length{distributed_vector_result_schema_format::kMaximumFrameLength};
  std::uint32_t maximum_columns{distributed_vector_result_schema_format::kMaximumColumns};
  std::uint32_t maximum_name_length{distributed_vector_result_schema_format::kMaximumNameLength};
};

class EncodedDistributedVectorResultSchema {
public:
  EncodedDistributedVectorResultSchema() = delete;
  EncodedDistributedVectorResultSchema(const EncodedDistributedVectorResultSchema&) = delete;
  EncodedDistributedVectorResultSchema&
  operator=(const EncodedDistributedVectorResultSchema&) = delete;
  EncodedDistributedVectorResultSchema(EncodedDistributedVectorResultSchema&&) noexcept = default;
  EncodedDistributedVectorResultSchema&
  operator=(EncodedDistributedVectorResultSchema&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorResultSchema(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorResultSchema>
  encode_distributed_vector_result_schema(const DistributedVectorResultSchema&);
};

[[nodiscard]] common::Result<EncodedDistributedVectorResultSchema>
encode_distributed_vector_result_schema(const DistributedVectorResultSchema& schema);

[[nodiscard]] common::Result<DistributedVectorResultSchema>
decode_distributed_vector_result_schema_exact(
    common::ByteView bytes, DistributedVectorResultSchemaDecodeLimits limits = {});

// Proves that ordered result descriptors have the exact physical shape produced by the intent over
// the projected inputs. Names remain caller-bound SQL identities and are validated but not derived.
[[nodiscard]] common::Status
validate_distributed_vector_result_schema(const DistributedVectorPlanIntent& intent,
                                          std::span<const PhysicalColumnShape> projected_inputs,
                                          const DistributedVectorResultSchema& result_schema);

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_RESULT_SCHEMA_HPP_
