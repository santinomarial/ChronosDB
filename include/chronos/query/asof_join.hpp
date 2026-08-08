#ifndef CHRONOS_QUERY_ASOF_JOIN_HPP_
#define CHRONOS_QUERY_ASOF_JOIN_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultAsofJoinKeyLimit = 256U;
inline constexpr std::size_t kDefaultAsofJoinStateByteLimit = std::size_t{64U} * 1024U * 1024U;

struct VectorAsofColumnShape {
  schema::LogicalType type;
  bool nullable;

  friend constexpr bool operator==(const VectorAsofColumnShape&,
                                   const VectorAsofColumnShape&) = default;
};

struct VectorAsofEqualityKey {
  std::size_t left_column_ordinal;
  std::size_t right_column_ordinal;

  friend constexpr bool operator==(const VectorAsofEqualityKey&,
                                   const VectorAsofEqualityKey&) = default;
};

struct VectorAsofJoinDefinition {
  std::vector<VectorAsofColumnShape> left_input_columns;
  std::vector<VectorAsofColumnShape> right_input_columns;
  std::vector<VectorAsofEqualityKey> equality_keys;
  std::size_t left_timestamp_column_ordinal;
  std::size_t right_timestamp_column_ordinal;
  std::vector<std::size_t> right_physical_ordering_key_ordinals;
  std::size_t right_row_version_first_column_ordinal;
  std::vector<std::size_t> left_output_column_ordinals;
  std::vector<std::size_t> right_output_column_ordinals;
  bool left_outer{};
};

struct AsofJoinLimits {
  std::uint32_t maximum_left_rows{kDefaultVectorChunkRowLimit};
  std::uint32_t maximum_right_rows{kDefaultVectorChunkRowLimit};
  std::size_t maximum_equality_keys{kDefaultAsofJoinKeyLimit};
  std::size_t maximum_physical_ordering_keys{kDefaultAsofJoinKeyLimit};
  std::size_t maximum_state_bytes{kDefaultAsofJoinStateByteLimit};
  VectorChunkLimits output_limits{};
};

// Returns the conservative state credit acquired before either input is retained.
[[nodiscard]] common::Result<std::size_t> asof_join_state_reservation_bytes(AsofJoinLimits limits);

// Validates the complete definition and returns its exact physical output shape, including the
// final match-presence column. This is the checked planning boundary used by relational plans.
[[nodiscard]] common::Result<std::vector<VectorAsofColumnShape>>
vector_asof_join_output_shape(const VectorAsofJoinDefinition& definition,
                              AsofJoinLimits limits = {});

// Blocking bounded ASOF join over two finite inputs. For each left row, equality keys use SQL
// equality and the right timestamp must be non-NULL and no greater than the non-NULL left
// timestamp. The greatest eligible timestamp wins; exact ties use the right physical-ordering key
// followed by WAL ID, record sequence, and row ordinal. Output is one canonical chunk containing
// configured left columns, configured right columns, and one final non-null BOOL match-presence
// column. Right columns are null-extended for an ASOF LEFT miss.
class AsofJoinOperator final : public PhysicalOperator {
public:
  ~AsofJoinOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> left, std::unique_ptr<PhysicalOperator> right,
         VectorAsofJoinDefinition definition, AsofJoinLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  class State;

  AsofJoinOperator(std::unique_ptr<PhysicalOperator> left, std::unique_ptr<PhysicalOperator> right,
                   VectorAsofJoinDefinition definition, AsofJoinLimits limits,
                   std::unique_ptr<State> state) noexcept;

  std::unique_ptr<PhysicalOperator> left_;
  std::unique_ptr<PhysicalOperator> right_;
  VectorAsofJoinDefinition definition_;
  AsofJoinLimits limits_;
  std::unique_ptr<State> state_;
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_ASOF_JOIN_HPP_
