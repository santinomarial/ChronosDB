#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_PLAN_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_PLAN_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/aggregate.hpp"
#include "chronos/query/sort.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::query {

namespace distributed_vector_plan_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kLegacyMinor = 0U;
inline constexpr std::uint16_t kMinor = 1U;
inline constexpr std::size_t kHeaderLength = 48U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::uint32_t kMaximumInputColumns = 4096U;
inline constexpr std::uint32_t kMaximumOutputColumns = 4096U;
inline constexpr std::uint32_t kMaximumRowOutputs = 4096U;
inline constexpr std::uint32_t kMaximumVisibleRowOutputs = 4096U;
inline constexpr std::uint32_t kMaximumGroupKeys = 4096U;
inline constexpr std::uint32_t kMaximumAggregates = 4096U;
inline constexpr std::uint32_t kMaximumOrderKeys = 256U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + static_cast<std::size_t>(kMaximumRowOutputs) * 4U +
    static_cast<std::size_t>(kMaximumVisibleRowOutputs) * 4U +
    static_cast<std::size_t>(kMaximumGroupKeys) * 4U +
    static_cast<std::size_t>(kMaximumAggregates) * 8U +
    static_cast<std::size_t>(kMaximumOrderKeys) * 8U + 8U + kTrailerLength;
} // namespace distributed_vector_plan_format

enum class DistributedVectorPlanMode : std::uint8_t {
  kRows = 1U,
  kUngroupedAggregate = 2U,
  kGroupedAggregate = 3U,
};

struct DistributedVectorAggregateIntent {
  VectorAggregateOperation operation;
  std::optional<std::uint32_t> input_index;

  friend bool operator==(const DistributedVectorAggregateIntent&,
                         const DistributedVectorAggregateIntent&) = default;
};

struct DistributedVectorOrderKey {
  std::uint32_t output_index{};
  PhysicalSortDirection direction{PhysicalSortDirection::kAscending};
  ScalarNullPlacement null_placement{ScalarNullPlacement::kLast};

  friend bool operator==(const DistributedVectorOrderKey&,
                         const DistributedVectorOrderKey&) = default;
};

// Schema-neutral intent over the projected input of a later authority-bound fragment. Row output
// indices retain worker-output order and may repeat. An empty visible-row vector means every row
// output is visible; otherwise it selects unique worker outputs in final client order. Group keys
// and order keys are canonical and unique. Grouped output is keys followed by aggregates;
// ungrouped output is aggregates only.
struct DistributedVectorPlanIntent {
  DistributedVectorPlanMode mode{DistributedVectorPlanMode::kRows};
  std::vector<std::uint32_t> row_output_indices;
  std::vector<std::uint32_t> visible_row_output_indices;
  std::vector<std::uint32_t> group_key_input_indices;
  std::vector<DistributedVectorAggregateIntent> aggregates;
  std::vector<DistributedVectorOrderKey> order_keys;
  std::optional<std::uint64_t> limit;

  friend bool operator==(const DistributedVectorPlanIntent&,
                         const DistributedVectorPlanIntent&) = default;
};

struct DistributedVectorPlanDecodeLimits {
  std::uint32_t maximum_input_columns{distributed_vector_plan_format::kMaximumInputColumns};
  std::uint32_t maximum_output_columns{distributed_vector_plan_format::kMaximumOutputColumns};
  std::uint32_t maximum_row_outputs{distributed_vector_plan_format::kMaximumRowOutputs};
  std::uint32_t maximum_visible_row_outputs{
      distributed_vector_plan_format::kMaximumVisibleRowOutputs};
  std::uint32_t maximum_group_keys{distributed_vector_plan_format::kMaximumGroupKeys};
  std::uint32_t maximum_aggregates{distributed_vector_plan_format::kMaximumAggregates};
  std::uint32_t maximum_order_keys{distributed_vector_plan_format::kMaximumOrderKeys};
};

// Validates one intent against the exact projected input width and a caller output-width bound.
[[nodiscard]] common::Status validate_distributed_vector_plan_intent(
    const DistributedVectorPlanIntent& intent, std::uint32_t input_columns,
    std::uint32_t maximum_output_columns = distributed_vector_plan_format::kMaximumOutputColumns);

class EncodedDistributedVectorPlanIntent {
public:
  EncodedDistributedVectorPlanIntent() = delete;
  EncodedDistributedVectorPlanIntent(const EncodedDistributedVectorPlanIntent&) = delete;
  EncodedDistributedVectorPlanIntent& operator=(const EncodedDistributedVectorPlanIntent&) = delete;
  EncodedDistributedVectorPlanIntent(EncodedDistributedVectorPlanIntent&&) noexcept = default;
  EncodedDistributedVectorPlanIntent&
  operator=(EncodedDistributedVectorPlanIntent&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorPlanIntent(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorPlanIntent>
  encode_distributed_vector_plan_intent(const DistributedVectorPlanIntent&);
};

[[nodiscard]] common::Result<EncodedDistributedVectorPlanIntent>
encode_distributed_vector_plan_intent(const DistributedVectorPlanIntent& intent);

[[nodiscard]] common::Result<DistributedVectorPlanIntent>
decode_distributed_vector_plan_intent_exact(common::ByteView bytes,
                                            DistributedVectorPlanDecodeLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_PLAN_HPP_
