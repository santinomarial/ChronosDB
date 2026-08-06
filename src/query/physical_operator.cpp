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

} // namespace chronos::query
