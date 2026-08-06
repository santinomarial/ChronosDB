#include "chronos/query/physical_operator.hpp"

#include "chronos/common/status.hpp"

#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

} // namespace

AccountedVectorChunk::AccountedVectorChunk(VectorChunk chunk,
                                           QueryMemoryReservation reservation) noexcept
    : chunk_(std::move(chunk)), reservation_(std::move(reservation)) {}

common::Result<AccountedVectorChunk>
AccountedVectorChunk::create(VectorChunk chunk, QueryMemoryReservation reservation,
                             const QueryResourceContext& resources) {
  if (!reservation.is_valid())
    return common::make_unexpected(invalid("accounted vector chunk requires a valid reservation"));
  if (!resources.owns(reservation)) {
    return common::make_unexpected(
        invalid("vector chunk reservation belongs to a different query resource context"));
  }
  if (reservation.bytes() < chunk.retained_buffer_bytes()) {
    return common::make_unexpected(
        invalid("vector chunk reservation is smaller than retained buffers"));
  }
  return AccountedVectorChunk{std::move(chunk), std::move(reservation)};
}

common::Result<AccountedVectorChunk>
AccountedVectorChunk::where_true(AccountedVectorChunk input, const std::size_t predicate_column) {
  common::Result<VectorChunk> filtered =
      VectorChunk::where_true(std::move(input.chunk_), predicate_column);
  if (!filtered.has_value())
    return common::make_unexpected(filtered.error());
  return AccountedVectorChunk{std::move(*filtered), std::move(input.reservation_)};
}

common::Result<AccountedVectorChunk>
AccountedVectorChunk::project_columns(AccountedVectorChunk input,
                                      const std::span<const std::size_t> column_ordinals) {
  common::Result<VectorChunk> projected =
      VectorChunk::project_columns(std::move(input.chunk_), column_ordinals);
  if (!projected.has_value())
    return common::make_unexpected(projected.error());
  return AccountedVectorChunk{std::move(*projected), std::move(input.reservation_)};
}

AccountedVectorChunk AccountedVectorChunk::take_first(AccountedVectorChunk input,
                                                      const std::size_t maximum_selected_rows) {
  input.chunk_ = VectorChunk::take_first(std::move(input.chunk_), maximum_selected_rows);
  return input;
}

const VectorChunk& AccountedVectorChunk::chunk() const noexcept {
  return chunk_;
}

std::size_t AccountedVectorChunk::charged_memory_bytes() const noexcept {
  return reservation_.bytes();
}

bool AccountedVectorChunk::belongs_to(const QueryResourceContext& resources) const noexcept {
  return resources.owns(reservation_);
}

PhysicalOperatorStep::PhysicalOperatorStep(const PhysicalOperatorStepKind kind,
                                           std::optional<AccountedVectorChunk> chunk) noexcept
    : kind_(kind), chunk_(std::move(chunk)) {}

PhysicalOperatorStep PhysicalOperatorStep::chunk(AccountedVectorChunk chunk) noexcept {
  return PhysicalOperatorStep{PhysicalOperatorStepKind::kChunk, std::move(chunk)};
}

PhysicalOperatorStep PhysicalOperatorStep::end() noexcept {
  return PhysicalOperatorStep{PhysicalOperatorStepKind::kEnd, std::nullopt};
}

PhysicalOperatorStepKind PhysicalOperatorStep::kind() const noexcept {
  return kind_;
}

const AccountedVectorChunk* PhysicalOperatorStep::chunk() const noexcept {
  return chunk_ ? std::addressof(*chunk_) : nullptr;
}

common::Result<AccountedVectorChunk> PhysicalOperatorStep::take_chunk() && {
  if (kind_ != PhysicalOperatorStepKind::kChunk || !chunk_.has_value())
    return common::make_unexpected(invalid("end-of-stream has no vector chunk"));
  AccountedVectorChunk result = std::move(*chunk_);
  chunk_.reset();
  return result;
}

BooleanFilterOperator::BooleanFilterOperator(std::unique_ptr<PhysicalOperator> input,
                                             const std::size_t predicate_column) noexcept
    : input_(std::move(input)), predicate_column_(predicate_column) {}

common::Result<std::unique_ptr<PhysicalOperator>>
BooleanFilterOperator::create(std::unique_ptr<PhysicalOperator> input,
                              const std::size_t predicate_column) {
  if (input == nullptr)
    return common::make_unexpected(invalid("Boolean filter input operator must be non-null"));
  try {
    return std::unique_ptr<PhysicalOperator>{
        new BooleanFilterOperator{std::move(input), predicate_column}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Boolean filter operator allocation failed"});
  }
}

common::Result<PhysicalOperatorStep>
BooleanFilterOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());

  common::Result<PhysicalOperatorStep> input = input_->next(resources);
  if (!input.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(input.error());
  }
  if (input->kind() == PhysicalOperatorStepKind::kEnd) {
    ended_ = true;
    return PhysicalOperatorStep::end();
  }
  common::Result<AccountedVectorChunk> chunk = std::move(*input).take_chunk();
  if (!chunk.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(chunk.error());
  }
  if (!chunk->belongs_to(resources)) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(
        invalid("physical operator received a chunk charged to another query"));
  }
  common::Result<AccountedVectorChunk> filtered =
      AccountedVectorChunk::where_true(std::move(*chunk), predicate_column_);
  if (!filtered.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(filtered.error());
  }
  return PhysicalOperatorStep::chunk(std::move(*filtered));
}

ColumnSubsetOperator::ColumnSubsetOperator(std::unique_ptr<PhysicalOperator> input,
                                           std::vector<std::size_t> column_ordinals) noexcept
    : input_(std::move(input)), column_ordinals_(std::move(column_ordinals)) {}

common::Result<std::unique_ptr<PhysicalOperator>>
ColumnSubsetOperator::create(std::unique_ptr<PhysicalOperator> input,
                             std::vector<std::size_t> column_ordinals) {
  if (input == nullptr)
    return common::make_unexpected(invalid("column subset input operator must be non-null"));
  if (column_ordinals.size() > kMaximumColumnSubsetWidth) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "column subset width exceeds the supported limit"});
  }
  for (std::size_t index = 1U; index < column_ordinals.size(); ++index) {
    if (column_ordinals[index - 1U] >= column_ordinals[index]) {
      return common::make_unexpected(
          invalid("column subset ordinals must be unique and strictly increasing"));
    }
  }
  try {
    return std::unique_ptr<PhysicalOperator>{
        new ColumnSubsetOperator{std::move(input), std::move(column_ordinals)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "column subset operator allocation failed"});
  }
}

common::Result<PhysicalOperatorStep>
ColumnSubsetOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());
  common::Result<PhysicalOperatorStep> input = input_->next(resources);
  if (!input.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(input.error());
  }
  if (input->kind() == PhysicalOperatorStepKind::kEnd) {
    ended_ = true;
    return PhysicalOperatorStep::end();
  }
  common::Result<AccountedVectorChunk> chunk = std::move(*input).take_chunk();
  if (!chunk.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(chunk.error());
  }
  if (!chunk->belongs_to(resources)) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(
        invalid("physical operator received a chunk charged to another query"));
  }
  common::Result<AccountedVectorChunk> projected =
      AccountedVectorChunk::project_columns(std::move(*chunk), column_ordinals_);
  if (!projected.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(projected.error());
  }
  return PhysicalOperatorStep::chunk(std::move(*projected));
}

LimitOperator::LimitOperator(std::unique_ptr<PhysicalOperator> input,
                             const std::uint64_t maximum_rows) noexcept
    : input_(std::move(input)), remaining_rows_(maximum_rows), ended_(maximum_rows == 0U) {
  if (ended_)
    input_.reset();
}

common::Result<std::unique_ptr<PhysicalOperator>>
LimitOperator::create(std::unique_ptr<PhysicalOperator> input, const std::uint64_t maximum_rows) {
  if (input == nullptr)
    return common::make_unexpected(invalid("limit input operator must be non-null"));
  try {
    return std::unique_ptr<PhysicalOperator>{new LimitOperator{std::move(input), maximum_rows}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted, "limit operator allocation failed"});
  }
}

common::Result<PhysicalOperatorStep> LimitOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());
  common::Result<PhysicalOperatorStep> input = input_->next(resources);
  if (!input.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(input.error());
  }
  if (input->kind() == PhysicalOperatorStepKind::kEnd) {
    ended_ = true;
    input_.reset();
    return PhysicalOperatorStep::end();
  }
  common::Result<AccountedVectorChunk> chunk = std::move(*input).take_chunk();
  if (!chunk.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(chunk.error());
  }
  if (!chunk->belongs_to(resources)) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(
        invalid("physical operator received a chunk charged to another query"));
  }

  const std::uint64_t selected_rows =
      static_cast<std::uint64_t>(chunk->chunk().selected_row_count());
  if (selected_rows > remaining_rows_) {
    AccountedVectorChunk limited = AccountedVectorChunk::take_first(
        std::move(*chunk), static_cast<std::size_t>(remaining_rows_));
    remaining_rows_ = 0U;
    ended_ = true;
    input_.reset();
    return PhysicalOperatorStep::chunk(std::move(limited));
  }
  remaining_rows_ -= selected_rows;
  if (remaining_rows_ == 0U) {
    ended_ = true;
    input_.reset();
  }
  return PhysicalOperatorStep::chunk(std::move(*chunk));
}

} // namespace chronos::query
