#ifndef CHRONOS_QUERY_PHYSICAL_OPERATOR_HPP_
#define CHRONOS_QUERY_PHYSICAL_OPERATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/vector_chunk.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::query {

class LatestByOperator;

inline constexpr std::size_t kMaximumColumnSubsetWidth = kDefaultVectorChunkColumnLimit;

// Couples retained chunk ownership to query-wide byte credit. The charge may conservatively exceed
// the chunk's local retained-buffer count, but it may never be smaller.
class AccountedVectorChunk {
public:
  AccountedVectorChunk() = delete;
  AccountedVectorChunk(const AccountedVectorChunk&) = delete;
  AccountedVectorChunk& operator=(const AccountedVectorChunk&) = delete;
  AccountedVectorChunk(AccountedVectorChunk&&) noexcept = default;
  AccountedVectorChunk& operator=(AccountedVectorChunk&&) noexcept = default;

  [[nodiscard]] static common::Result<AccountedVectorChunk>
  create(VectorChunk chunk, QueryMemoryReservation reservation,
         const QueryResourceContext& resources);
  [[nodiscard]] static common::Result<AccountedVectorChunk>
  where_true(AccountedVectorChunk input, std::size_t predicate_column);
  [[nodiscard]] static common::Result<AccountedVectorChunk>
  where_timestamp_in_range(AccountedVectorChunk input, std::size_t timestamp_column,
                           const TimestampRangePredicate& predicate);
  [[nodiscard]] static common::Result<AccountedVectorChunk>
  project_columns(AccountedVectorChunk input, std::span<const std::size_t> column_ordinals);
  [[nodiscard]] static AccountedVectorChunk take_first(AccountedVectorChunk input,
                                                       std::size_t maximum_selected_rows);

  [[nodiscard]] const VectorChunk& chunk() const noexcept;
  [[nodiscard]] std::size_t charged_memory_bytes() const noexcept;
  [[nodiscard]] bool belongs_to(const QueryResourceContext& resources) const noexcept;

private:
  AccountedVectorChunk(VectorChunk chunk, QueryMemoryReservation reservation) noexcept;

  VectorChunk chunk_;
  QueryMemoryReservation reservation_;

  friend class LatestByOperator;
};

enum class PhysicalOperatorStepKind : std::uint8_t {
  kChunk,
  kEnd,
};

// One pull result. End-of-stream is explicit and carries no chunk or memory credit.
class PhysicalOperatorStep {
public:
  PhysicalOperatorStep() = delete;
  PhysicalOperatorStep(const PhysicalOperatorStep&) = delete;
  PhysicalOperatorStep& operator=(const PhysicalOperatorStep&) = delete;
  PhysicalOperatorStep(PhysicalOperatorStep&&) noexcept = default;
  PhysicalOperatorStep& operator=(PhysicalOperatorStep&&) noexcept = default;

  [[nodiscard]] static PhysicalOperatorStep chunk(AccountedVectorChunk chunk) noexcept;
  [[nodiscard]] static PhysicalOperatorStep end() noexcept;

  [[nodiscard]] PhysicalOperatorStepKind kind() const noexcept;
  [[nodiscard]] const AccountedVectorChunk* chunk() const noexcept;
  [[nodiscard]] common::Result<AccountedVectorChunk> take_chunk() &&;

private:
  PhysicalOperatorStep(PhysicalOperatorStepKind kind,
                       std::optional<AccountedVectorChunk> chunk) noexcept;

  PhysicalOperatorStepKind kind_;
  std::optional<AccountedVectorChunk> chunk_;
};

// Pull-based operator boundary. A call produces at most one owning accounted chunk, so downstream
// demand provides backpressure. Implementations poll resources at each pull and return kEnd
// repeatedly after successful completion.
class PhysicalOperator {
public:
  PhysicalOperator() = default;
  PhysicalOperator(const PhysicalOperator&) = delete;
  PhysicalOperator& operator=(const PhysicalOperator&) = delete;
  PhysicalOperator(PhysicalOperator&&) = delete;
  PhysicalOperator& operator=(PhysicalOperator&&) = delete;
  virtual ~PhysicalOperator() = default;

  [[nodiscard]] virtual common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) = 0;
};

// The first concrete vector operator. It applies SQL WHERE truth semantics to one BOOL column and
// reuses the input chunk, selection allocation, and memory credit.
class BooleanFilterOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, std::size_t predicate_column);

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  BooleanFilterOperator(std::unique_ptr<PhysicalOperator> input,
                        std::size_t predicate_column) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::size_t predicate_column_;
  bool ended_{};
};

// Applies exact open/closed TIMESTAMP_NS bounds to one physical column. NULL does not match. The
// input chunk, selection allocation, row order, and memory credit are retained.
class TimestampRangeFilterOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, std::size_t timestamp_column,
         TimestampRangePredicate predicate);

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  TimestampRangeFilterOperator(std::unique_ptr<PhysicalOperator> input,
                               std::size_t timestamp_column,
                               TimestampRangePredicate predicate) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::size_t timestamp_column_;
  TimestampRangePredicate predicate_;
  bool ended_{};
};

// Stable projection pushdown for an ordered unique subset of input columns. Direct owned storage is
// released for discarded columns; a shared backing remains conservatively pinned and charged.
// Computed, duplicated, or reordered SQL outputs require the later typed expression/output builder.
class ColumnSubsetOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, std::vector<std::size_t> column_ordinals);

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  ColumnSubsetOperator(std::unique_ptr<PhysicalOperator> input,
                       std::vector<std::size_t> column_ordinals) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::vector<std::size_t> column_ordinals_;
  bool ended_{};
};

// Applies one global SQL LIMIT across chunk boundaries. Reaching the limit destroys the uniquely
// owned upstream pipeline immediately so buffered chunks release their reservations.
class LimitOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, std::uint64_t maximum_rows);

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  LimitOperator(std::unique_ptr<PhysicalOperator> input, std::uint64_t maximum_rows) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::uint64_t remaining_rows_;
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_PHYSICAL_OPERATOR_HPP_
