#ifndef CHRONOS_QUERY_LATEST_HPP_
#define CHRONOS_QUERY_LATEST_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/sort.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultLatestByKeyLimit = 256U;
inline constexpr std::size_t kDefaultLatestPhysicalKeyLimit = 256U;

struct VectorLatestByDefinition {
  std::vector<std::size_t> key_column_ordinals;
  std::size_t timestamp_column_ordinal;
  std::vector<std::size_t> physical_ordering_key_ordinals;
  std::size_t row_version_first_column_ordinal;
};

struct LatestByLimits {
  std::size_t maximum_group_keys{kDefaultLatestByKeyLimit};
  std::size_t maximum_physical_ordering_keys{kDefaultLatestPhysicalKeyLimit};
  SortLimits sort_limits{};
};

// Bounded exact LATEST BY over one finite source. Rows are sorted by group, descending timestamp,
// descending schema physical identity, and descending row-version identity before one winner per
// group is retained. No result depends on input arrival order or sort stability.
class LatestByOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, VectorLatestByDefinition definition,
         LatestByLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  [[nodiscard]] static common::Result<void> compact_groups(AccountedVectorChunk& chunk,
                                                           std::span<const std::size_t> keys,
                                                           const QueryResourceContext& resources);

  LatestByOperator(std::unique_ptr<PhysicalOperator> sorted,
                   VectorLatestByDefinition definition) noexcept;

  std::unique_ptr<PhysicalOperator> sorted_;
  VectorLatestByDefinition definition_;
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_LATEST_HPP_
