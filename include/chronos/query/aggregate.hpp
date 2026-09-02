#ifndef CHRONOS_QUERY_AGGREGATE_HPP_
#define CHRONOS_QUERY_AGGREGATE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::query {

namespace detail {
class MergeableVectorAggregateStateCodecAccess;
} // namespace detail

inline constexpr std::size_t kMaximumUngroupedAggregateWidth = kDefaultVectorChunkColumnLimit;
inline constexpr std::size_t kDefaultUngroupedAggregateConfigurationByteLimit =
    std::size_t{2U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumGroupedAggregateKeys = kDefaultVectorChunkColumnLimit;
inline constexpr std::size_t kMaximumGroupedAggregateWidth = kDefaultVectorChunkColumnLimit;
inline constexpr std::size_t kMaximumGroupedAggregateGroups = 4096U;
inline constexpr std::size_t kDefaultGroupedAggregateConfigurationByteLimit =
    std::size_t{2U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultGroupedAggregateKeyByteLimit = std::size_t{1U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultAggregateExtremumByteLimit = std::size_t{1U} * 1024U * 1024U;

enum class VectorAggregateOperation : std::uint8_t {
  kCountStar,
  kCount,
  kSum,
  kAverage,
  kMinimum,
  kMaximum,
  kVariancePopulation,
  kVarianceSample,
};

struct VectorAggregateInput {
  std::size_t column_ordinal;
  schema::LogicalType type;
  bool nullable;

  friend bool operator==(const VectorAggregateInput&, const VectorAggregateInput&) = default;
};

struct VectorAggregateDefinition {
  VectorAggregateOperation operation;
  std::optional<VectorAggregateInput> input{std::nullopt};

  friend bool operator==(const VectorAggregateDefinition&,
                         const VectorAggregateDefinition&) = default;
};

struct VectorAggregateOutputShape {
  schema::LogicalType type;
  bool nullable;

  friend constexpr bool operator==(const VectorAggregateOutputShape&,
                                   const VectorAggregateOutputShape&) = default;
};

struct UngroupedAggregateLimits {
  std::size_t maximum_aggregates{kMaximumUngroupedAggregateWidth};
  std::size_t maximum_variable_extremum_bytes{kDefaultAggregateExtremumByteLimit};
  std::size_t maximum_retained_configuration_bytes{
      kDefaultUngroupedAggregateConfigurationByteLimit};
  VectorChunkLimits output_limits{};
};

struct VectorGroupKeyDefinition {
  std::size_t column_ordinal;
  schema::LogicalType type;
  bool nullable;
};

// Stable FNV-1a v1 hash over the exact typed canonical key tuple. NULL, signed zero, and NaN use
// the same equivalence classes as MergeableVectorGroupedAggregateTable. Distributed partitioning
// must use this function rather than a process- or library-dependent hash.
[[nodiscard]] common::Result<std::uint64_t>
canonical_vector_group_key_hash_v1(std::span<const VectorGroupKeyDefinition> definitions,
                                   std::span<const ScalarValue> keys);

struct GroupedAggregateLimits {
  std::size_t maximum_groups{kMaximumGroupedAggregateGroups};
  std::size_t maximum_group_keys{kMaximumGroupedAggregateKeys};
  std::size_t maximum_aggregates{kMaximumGroupedAggregateWidth};
  std::size_t maximum_key_bytes_per_group{kDefaultGroupedAggregateKeyByteLimit};
  std::size_t maximum_variable_extremum_bytes{kDefaultAggregateExtremumByteLimit};
  std::size_t maximum_retained_configuration_bytes{kDefaultGroupedAggregateConfigurationByteLimit};
  VectorChunkLimits output_limits{};
};

// Validates one aggregate definition and returns its exact result type/nullability. The current
// version admits COUNT over every physical type, numeric SUM/AVG/variance, and all-type MIN/MAX.
[[nodiscard]] common::Result<VectorAggregateOutputShape>
vector_aggregate_output_shape(const VectorAggregateDefinition& definition);

// One thread-affine, move-only all-type partial state. The same kernel backs local ungrouped and
// grouped execution and can merge independently accumulated partitions without first rounding AVG,
// variance, or exact numeric SUM to final output cells. Variable-width extrema remain independently
// bounded and query-accounted in both accumulation and merge paths. Finalized and moved-from states
// are terminal and reject further accumulation, merge, or finalization attempts.
class MergeableVectorAggregateState {
public:
  MergeableVectorAggregateState() = delete;
  MergeableVectorAggregateState(const MergeableVectorAggregateState&) = delete;
  MergeableVectorAggregateState& operator=(const MergeableVectorAggregateState&) = delete;
  MergeableVectorAggregateState(MergeableVectorAggregateState&& other) noexcept;
  MergeableVectorAggregateState& operator=(MergeableVectorAggregateState&& other) noexcept;
  ~MergeableVectorAggregateState() = default;

  [[nodiscard]] static common::Result<MergeableVectorAggregateState>
  create(VectorAggregateDefinition definition,
         std::size_t maximum_variable_extremum_bytes = kDefaultAggregateExtremumByteLimit);

  [[nodiscard]] const VectorAggregateDefinition& definition() const noexcept;
  [[nodiscard]] common::Result<void> accumulate_count_star();
  [[nodiscard]] common::Result<void> accumulate_cell(const columnar::ColumnCellView& cell,
                                                     const QueryResourceContext& resources);
  [[nodiscard]] common::Result<void> merge(const MergeableVectorAggregateState& other,
                                           const QueryResourceContext& resources);
  [[nodiscard]] common::Result<ScalarValue> take_result() &&;

private:
  MergeableVectorAggregateState(VectorAggregateDefinition definition,
                                std::size_t maximum_variable_extremum_bytes) noexcept;

  [[nodiscard]] common::Result<void> accumulate_value(const ScalarValue& value);
  [[nodiscard]] common::Result<void>
  accumulate_variable_extremum(const columnar::ColumnCellView& cell,
                               const QueryResourceContext& resources);
  [[nodiscard]] common::Result<void> copy_variable_extremum(const ScalarValue& value,
                                                            const QueryResourceContext& resources);

  VectorAggregateDefinition definition_;
  std::uint64_t count_{};
  std::uint64_t moment_count_{};
  std::array<std::uint32_t, 8U> exact_sum_magnitude_{};
  bool exact_sum_negative_{};
  float float_sum_{};
  double double_sum_{};
  double mean_{};
  double squared_distance_{};
  std::optional<ScalarValue> extremum_;
  QueryMemoryReservation extremum_reservation_;
  std::size_t maximum_variable_extremum_bytes_{};
  bool has_value_{};
  bool finalized_{};

  friend class detail::MergeableVectorAggregateStateCodecAccess;
};

// Consumes its complete input stream without retaining chunks, accumulates one global group, and
// emits exactly one canonical one-row chunk. Empty input therefore produces COUNT zero and NULL for
// every other aggregate. The operator and every instantiated pipeline are uniquely owned and
// thread-affine; cancellation and failures unwind input/output memory through RAII.
class UngroupedAggregateOperator final : public PhysicalOperator {
public:
  ~UngroupedAggregateOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input,
         const std::vector<VectorAggregateDefinition>& definitions,
         UngroupedAggregateLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  class Impl;

  UngroupedAggregateOperator(std::unique_ptr<PhysicalOperator> input, std::unique_ptr<Impl> impl,
                             VectorChunkLimits output_limits) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::unique_ptr<Impl> impl_;
  VectorChunkLimits output_limits_;
  bool emitted_{};
};

// The single query-accounted multi-key/all-type group-state owner used by local GROUP BY,
// distributed sufficient-state workers, and coordinators. It preserves the canonical hash/equality,
// first-seen order, fixed-capacity admission, cancellation, and aggregate kernels. Returned spans
// borrow this thread-affine table and remain valid until its next mutating call, move, destruction,
// or that group's materialization.
class MergeableVectorGroupedAggregateTable {
public:
  MergeableVectorGroupedAggregateTable() = delete;
  ~MergeableVectorGroupedAggregateTable();
  MergeableVectorGroupedAggregateTable(const MergeableVectorGroupedAggregateTable&) = delete;
  MergeableVectorGroupedAggregateTable&
  operator=(const MergeableVectorGroupedAggregateTable&) = delete;
  MergeableVectorGroupedAggregateTable(MergeableVectorGroupedAggregateTable&&) noexcept;
  MergeableVectorGroupedAggregateTable& operator=(MergeableVectorGroupedAggregateTable&&) noexcept;

  [[nodiscard]] static common::Result<MergeableVectorGroupedAggregateTable>
  create(const std::vector<VectorGroupKeyDefinition>& keys,
         const std::vector<VectorAggregateDefinition>& definitions,
         GroupedAggregateLimits limits = {});

  [[nodiscard]] common::Result<void> accumulate(const AccountedVectorChunk& chunk,
                                                const QueryResourceContext& resources);
  // Coordinator-side merge. Keys and states are borrowed only for this synchronous call. An error
  // makes the table terminal and releases all retained state so no partially merged group can be
  // observed or retried.
  [[nodiscard]] common::Result<void>
  merge_group(std::span<const ScalarValue> keys,
              std::span<const MergeableVectorAggregateState> states,
              const QueryResourceContext& resources);
  [[nodiscard]] std::size_t group_count() const noexcept;
  [[nodiscard]] common::Result<std::span<const ScalarValue>>
  group_keys(std::size_t group_index) const;
  [[nodiscard]] common::Result<std::span<const MergeableVectorAggregateState>>
  group_states(std::size_t group_index) const;
  // Starts the terminal output phase and must be called in first-seen group order. No accumulation
  // or merge is accepted after the first materialization.
  [[nodiscard]] common::Result<PhysicalOperatorStep>
  materialize_group(std::size_t group_index, const QueryResourceContext& resources,
                    VectorChunkLimits output_limits = {});

private:
  class Impl;
  explicit MergeableVectorGroupedAggregateTable(std::unique_ptr<Impl> impl) noexcept;

  std::unique_ptr<Impl> impl_;
  std::size_t next_output_group_{};
  bool output_started_{};

  friend class GroupedAggregateOperator;
};

// Consumes its complete input stream into a finite query-accounted hash table and then emits one
// canonical accounted row per pull in first-seen group order. Key order in each row is caller
// order, followed by aggregate order. Empty input emits no groups. NULL key cells compare equal for
// grouping. Every retained variable-width extremum is independently bounded and query-accounted.
class GroupedAggregateOperator final : public PhysicalOperator {
public:
  ~GroupedAggregateOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, const std::vector<VectorGroupKeyDefinition>& keys,
         const std::vector<VectorAggregateDefinition>& definitions,
         GroupedAggregateLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  GroupedAggregateOperator(std::unique_ptr<PhysicalOperator> input,
                           MergeableVectorGroupedAggregateTable table,
                           VectorChunkLimits output_limits) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::optional<MergeableVectorGroupedAggregateTable> table_;
  VectorChunkLimits output_limits_;
  std::size_t output_group_{};
  bool input_consumed_{};
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_AGGREGATE_HPP_
