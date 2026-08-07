#ifndef CHRONOS_QUERY_SORT_HPP_
#define CHRONOS_QUERY_SORT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/value.hpp"
#include "chronos/query/vector_chunk.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::query {

inline constexpr std::uint32_t kDefaultSortRowLimit = kDefaultVectorChunkRowLimit;
inline constexpr std::size_t kDefaultSortKeyLimit = 256U;
inline constexpr std::size_t kDefaultSortStateByteLimit = std::size_t{32U} * 1024U * 1024U;

enum class PhysicalSortDirection : std::uint8_t { kAscending, kDescending };

struct VectorSortKey {
  std::size_t column_ordinal;
  PhysicalSortDirection direction{PhysicalSortDirection::kAscending};
  ScalarNullPlacement null_placement{ScalarNullPlacement::kLast};

  friend constexpr bool operator==(const VectorSortKey&, const VectorSortKey&) = default;
};

struct SortLimits {
  std::uint32_t maximum_rows{kDefaultSortRowLimit};
  std::size_t maximum_keys{kDefaultSortKeyLimit};
  std::size_t maximum_state_bytes{kDefaultSortStateByteLimit};
  VectorChunkLimits output_limits{};
};

// Returns the conservative state credit acquired before buffering any input. Limit or arithmetic
// failures are reported without allocation.
[[nodiscard]] common::Result<std::size_t> sort_state_reservation_bytes(SortLimits limits);

// Blocking in-memory sort over one finite input. All selected rows and their input chunks stay
// query-accounted until one independent canonical output chunk has been materialized. Equal keys
// preserve logical input order. This operator does not invent SQL's hidden row-version tie-breaker;
// a SQL plan must include the required identity keys explicitly before using it for presentation.
class SortOperator final : public PhysicalOperator {
public:
  ~SortOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, std::vector<VectorSortKey> keys,
         SortLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  class State;

  SortOperator(std::unique_ptr<PhysicalOperator> input, std::vector<VectorSortKey> keys,
               SortLimits limits, std::unique_ptr<State> state) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::vector<VectorSortKey> keys_;
  SortLimits limits_;
  std::unique_ptr<State> state_;
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_SORT_HPP_
