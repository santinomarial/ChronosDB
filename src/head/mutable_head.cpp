#include "chronos/head/mutable_head.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <atomic>
#include <exception>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::head {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) noexcept {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

[[nodiscard]] bool bit_at(const common::ByteView bytes, const std::uint32_t row) noexcept {
  const std::size_t byte_index = static_cast<std::size_t>(row) / 8U;
  const auto mask = static_cast<std::uint8_t>(1U << (row % 8U));
  return (std::to_integer<std::uint8_t>(bytes[byte_index]) & mask) != 0U;
}

[[nodiscard]] std::uint32_t read_u32_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

struct StorageExtent {
  std::size_t count;
  std::size_t width;
};

[[nodiscard]] common::Result<std::size_t>
add_storage_bytes(const std::size_t current, const StorageExtent extent) {
  const auto bytes = common::checked_multiply(extent.count, extent.width);
  if (!bytes.has_value()) {
    return common::make_unexpected(
        exhausted("mutable-head storage accounting overflowed this platform"));
  }
  const auto total = common::checked_add(current, *bytes);
  if (!total.has_value()) {
    return common::make_unexpected(
        exhausted("mutable-head storage accounting overflowed this platform"));
  }
  return *total;
}

struct ColumnStorage {
  std::vector<std::uint8_t> validity;
  std::vector<std::uint8_t> boolean_values;
  std::vector<std::byte> fixed_values;
  std::vector<std::uint32_t> variable_offsets;
  std::vector<std::byte> variable_values;
  std::size_t fixed_width{};
};

enum class PreparedPhase : std::uint8_t {
  kPreWal,
  kWalStarted,
};

} // namespace

namespace detail {

class HeadPublication {
public:
  HeadPublication(const std::uint32_t row_count, std::optional<HeadCommitPosition> applied_position,
                  std::vector<std::size_t> variable_frontiers) noexcept
      : row_count_(row_count), applied_position_(applied_position),
        variable_frontiers_(std::move(variable_frontiers)) {}

  std::uint32_t row_count_;
  std::optional<HeadCommitPosition> applied_position_;
  std::vector<std::size_t> variable_frontiers_;
};

struct MutableHeadStateConfig {
  std::shared_ptr<const schema::TableSchema> schema;
  schema::TabletId tablet_id;
  std::uint64_t generation;
  std::uint32_t row_capacity;
  std::vector<ColumnStorage> columns;
  std::vector<HeadRowMetadata> row_metadata;
  std::shared_ptr<const HeadPublication> initial_publication;
  std::size_t variable_byte_capacity;
  std::size_t retained_storage_bytes;
};

struct PublicationRange {
  const HeadPublication& base;
  const HeadPublication& next;
};

class MutableHeadState : public std::enable_shared_from_this<MutableHeadState> {
public:
  explicit MutableHeadState(MutableHeadStateConfig config) noexcept
      : schema_(std::move(config.schema)), tablet_id_(config.tablet_id),
        generation_(config.generation), row_capacity_(config.row_capacity),
        columns_(std::move(config.columns)), row_metadata_(std::move(config.row_metadata)),
        publication_(std::move(config.initial_publication)),
        variable_byte_capacity_(config.variable_byte_capacity),
        retained_storage_bytes_(config.retained_storage_bytes) {}

  [[nodiscard]] common::Result<PreparedHeadAppend>
  prepare(std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
          const HeadCommitPosition& position);
  [[nodiscard]] bool wal_started(std::uint64_t token) const noexcept;
  [[nodiscard]] common::Status mark_wal_started(std::uint64_t token);
  [[nodiscard]] common::Result<HeadSnapshot>
  publish(std::uint64_t token, const columnar::OwnedColumnarBatch& batch,
          const std::shared_ptr<const HeadPublication>& base,
          const std::shared_ptr<const HeadPublication>& next);
  [[nodiscard]] common::Status cancel_before_wal(std::uint64_t token);
  void abandon(std::uint64_t token) noexcept;

  [[nodiscard]] HeadSnapshot snapshot() {
    return HeadSnapshot{shared_from_this(),
                        std::atomic_load_explicit(&publication_, std::memory_order_acquire)};
  }
  [[nodiscard]] common::Result<HeadSnapshot> seal();
  [[nodiscard]] MutableHeadMetrics metrics() const noexcept;

  [[nodiscard]] const schema::TableId& table_id() const noexcept {
    return schema_->table_id();
  }
  [[nodiscard]] const schema::TabletId& tablet_id() const noexcept {
    return tablet_id_;
  }
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept {
    return schema_;
  }
  [[nodiscard]] std::uint64_t generation() const noexcept {
    return generation_;
  }
  [[nodiscard]] std::size_t column_count() const noexcept {
    return columns_.size();
  }

  [[nodiscard]] common::Result<HeadColumnView> column_view(const HeadPublication& publication,
                                                           std::size_t ordinal) const;
  [[nodiscard]] common::Result<HeadRowMetadata> row_metadata(const HeadPublication& publication,
                                                             std::uint32_t row) const;
  [[nodiscard]] common::Result<RowVersionIdentity>
  row_version_identity(const HeadPublication& publication, std::uint32_t row) const;

private:
  [[nodiscard]] common::Status
  validate_append(const std::shared_ptr<const columnar::OwnedColumnarBatch>& batch,
                  const HeadCommitPosition& position,
                  const std::shared_ptr<const HeadPublication>& current) const;
  [[nodiscard]] common::Status
  validate_unpublished_boundaries(const HeadPublication& base) const;
  void materialize(const columnar::OwnedColumnarBatch& batch, PublicationRange range) noexcept;

  std::shared_ptr<const schema::TableSchema> schema_;
  schema::TabletId tablet_id_;
  std::uint64_t generation_;
  std::uint32_t row_capacity_;
  std::vector<ColumnStorage> columns_;
  std::vector<HeadRowMetadata> row_metadata_;
  std::shared_ptr<const HeadPublication> publication_;
  std::size_t variable_byte_capacity_;
  std::size_t retained_storage_bytes_;

  bool append_active_{false};
  std::uint64_t active_token_{};
  PreparedPhase active_phase_{PreparedPhase::kPreWal};
  std::uint64_t next_token_{1U};
  std::atomic<bool> sealed_{false};
  std::atomic<bool> failed_{false};
};

} // namespace detail

class PreparedHeadAppend::Impl {
public:
  Impl(std::shared_ptr<detail::MutableHeadState> state,
       std::shared_ptr<const columnar::OwnedColumnarBatch> batch, const std::uint64_t token,
       std::shared_ptr<const detail::HeadPublication> base,
       std::shared_ptr<const detail::HeadPublication> next) noexcept
      : state_(std::move(state)), batch_(std::move(batch)), token_(token), base_(std::move(base)),
        next_(std::move(next)) {}

  ~Impl() {
    if (state_ != nullptr) {
      state_->abandon(token_);
    }
  }

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  [[nodiscard]] bool wal_started() const noexcept {
    return state_ != nullptr && state_->wal_started(token_);
  }

  [[nodiscard]] common::Status mark_wal_started() {
    if (state_ == nullptr) {
      return invalid("prepared mutable-head append is invalid");
    }
    return state_->mark_wal_started(token_);
  }

  [[nodiscard]] common::Result<HeadSnapshot> publish() {
    if (state_ == nullptr || batch_ == nullptr || base_ == nullptr || next_ == nullptr) {
      return common::make_unexpected(invalid("prepared mutable-head append is invalid"));
    }
    common::Result<HeadSnapshot> published = state_->publish(token_, *batch_, base_, next_);
    if (published.has_value()) {
      state_.reset();
      batch_.reset();
      base_.reset();
      next_.reset();
    }
    return published;
  }

  [[nodiscard]] common::Status cancel_before_wal() {
    if (state_ == nullptr) {
      return invalid("prepared mutable-head append is invalid");
    }
    const common::Status status = state_->cancel_before_wal(token_);
    if (status.is_ok()) {
      state_.reset();
      batch_.reset();
      base_.reset();
      next_.reset();
    }
    return status;
  }

private:
  std::shared_ptr<detail::MutableHeadState> state_;
  std::shared_ptr<const columnar::OwnedColumnarBatch> batch_;
  std::uint64_t token_;
  std::shared_ptr<const detail::HeadPublication> base_;
  std::shared_ptr<const detail::HeadPublication> next_;
};

common::Status detail::MutableHeadState::validate_append(
    const std::shared_ptr<const columnar::OwnedColumnarBatch>& batch,
    const HeadCommitPosition& position,
    const std::shared_ptr<const HeadPublication>& current) const {
  if (failed_.load(std::memory_order_acquire)) {
    return unavailable("mutable head is failed and requires fresh recovery");
  }
  if (sealed_.load(std::memory_order_acquire)) {
    return unavailable("mutable head is sealed");
  }
  if (append_active_) {
    return unavailable("mutable head already has a prepared append");
  }
  if (batch == nullptr) {
    return invalid("mutable-head append requires an owning batch pointer");
  }
  if (batch->schema() != *schema_) {
    return invalid("mutable-head append batch does not match the bound schema");
  }
  if (!position.wal_id.is_valid() || position.record_sequence == 0U) {
    return invalid("mutable-head append requires a nonzero WAL identity and record sequence");
  }
  if (current->applied_position_.has_value()) {
    const HeadCommitPosition& applied = *current->applied_position_;
    if (position.wal_id != applied.wal_id || position.record_sequence <= applied.record_sequence) {
      return invalid("mutable-head append position must advance within one WAL history");
    }
  }
  const auto end_row = common::checked_add<std::uint32_t>(current->row_count_, batch->row_count());
  if (!end_row.has_value() || *end_row > row_capacity_) {
    return exhausted("mutable-head append does not fit the remaining row capacity");
  }
  return common::Status::ok();
}

common::Result<PreparedHeadAppend>
detail::MutableHeadState::prepare(std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
                                  const HeadCommitPosition& position) {
  const std::shared_ptr<const HeadPublication> current =
      std::atomic_load_explicit(&publication_, std::memory_order_acquire);
  const common::Status valid = validate_append(batch, position, current);
  if (!valid.is_ok()) {
    return common::make_unexpected(valid);
  }
  if (next_token_ == std::numeric_limits<std::uint64_t>::max()) {
    return common::make_unexpected(exhausted("mutable head exhausted its append token space"));
  }

  try {
    std::vector<std::size_t> next_frontiers = current->variable_frontiers_;
    for (std::size_t ordinal = 0U; ordinal < columns_.size(); ++ordinal) {
      const columnar::OwnedColumnVector* const source = batch->column(ordinal);
      if (source == nullptr) {
        return common::make_unexpected(internal("validated batch lost a schema column"));
      }
      if (!source->type().is_variable_width()) {
        continue;
      }
      const auto next =
          common::checked_add(next_frontiers[ordinal], source->view().values().size());
      if (!next.has_value() || *next > columns_[ordinal].variable_values.size()) {
        return common::make_unexpected(
            exhausted("mutable-head append exceeds a variable-column byte capacity"));
      }
      next_frontiers[ordinal] = *next;
    }

    const auto end_row =
        common::checked_add<std::uint32_t>(current->row_count_, batch->row_count());
    if (!end_row.has_value()) {
      return common::make_unexpected(internal("validated mutable-head row range overflowed"));
    }
    auto next =
        std::make_shared<const HeadPublication>(*end_row, position, std::move(next_frontiers));
    const std::uint64_t token = next_token_;
    auto implementation = std::make_unique<PreparedHeadAppend::Impl>(
        shared_from_this(), std::move(batch), token, current, std::move(next));

    ++next_token_;
    append_active_ = true;
    active_token_ = token;
    active_phase_ = PreparedPhase::kPreWal;
    return PreparedHeadAppend{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable-head append preparation could not allocate its publication state"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable-head append preparation exceeds container limits"));
  }
}

bool detail::MutableHeadState::wal_started(const std::uint64_t token) const noexcept {
  return append_active_ && active_token_ == token && active_phase_ == PreparedPhase::kWalStarted;
}

common::Status detail::MutableHeadState::mark_wal_started(const std::uint64_t token) {
  if (!append_active_ || active_token_ != token) {
    return internal("prepared mutable-head append ownership was lost");
  }
  if (active_phase_ != PreparedPhase::kPreWal) {
    return invalid("prepared mutable-head append has already crossed its pre-WAL boundary");
  }
  if (failed_.load(std::memory_order_acquire) || sealed_.load(std::memory_order_acquire)) {
    return unavailable("mutable head cannot start WAL work in its current state");
  }
  active_phase_ = PreparedPhase::kWalStarted;
  return common::Status::ok();
}

common::Status detail::MutableHeadState::validate_unpublished_boundaries(
    const HeadPublication& base) const {
  for (std::size_t ordinal = 0U; ordinal < columns_.size(); ++ordinal) {
    const ColumnStorage& column = columns_[ordinal];
    if (!schema_->columns()[ordinal].type().is_variable_width()) {
      continue;
    }
    if (column.variable_offsets[base.row_count_] != base.variable_frontiers_[ordinal]) {
      return internal("mutable-head variable offset boundary is inconsistent");
    }
  }
  return common::Status::ok();
}

void detail::MutableHeadState::materialize(const columnar::OwnedColumnarBatch& batch,
                                           const PublicationRange range) noexcept {
  const HeadPublication& base = range.base;
  const HeadPublication& next = range.next;
  const std::uint32_t rows = batch.row_count();
  const std::size_t start_row = base.row_count_;
  for (std::size_t ordinal = 0U; ordinal < columns_.size(); ++ordinal) {
    ColumnStorage& destination = columns_[ordinal];
    const columnar::ColumnVectorView source = batch.column(ordinal)->view();

    if (source.nullable()) {
      for (std::uint32_t row = 0U; row < rows; ++row) {
        destination.validity[start_row + row] = bit_at(source.validity(), row) ? 1U : 0U;
      }
    }

    if (source.type().kind() == schema::LogicalTypeKind::kBool) {
      for (std::uint32_t row = 0U; row < rows; ++row) {
        destination.boolean_values[start_row + row] = bit_at(source.values(), row) ? 1U : 0U;
      }
      continue;
    }

    if (source.type().is_variable_width()) {
      const std::size_t base_frontier = base.variable_frontiers_[ordinal];
      std::copy(source.values().begin(), source.values().end(),
                destination.variable_values.begin() + static_cast<std::ptrdiff_t>(base_frontier));
      for (std::uint32_t row = 0U; row < rows; ++row) {
        const std::size_t source_offset =
            (static_cast<std::size_t>(row) + 1U) * sizeof(std::uint32_t);
        const std::size_t absolute =
            base_frontier + static_cast<std::size_t>(read_u32_le(source.offsets(), source_offset));
        destination.variable_offsets[start_row + row + 1U] = static_cast<std::uint32_t>(absolute);
      }
      continue;
    }

    const std::size_t destination_offset = start_row * destination.fixed_width;
    std::copy(source.values().begin(), source.values().end(),
              destination.fixed_values.begin() + static_cast<std::ptrdiff_t>(destination_offset));
  }

  const HeadCommitPosition& position = next.applied_position_.value();
  for (std::uint32_t row = 0U; row < rows; ++row) {
    row_metadata_[start_row + row] = HeadRowMetadata{.commit_position = position,
                                                     .row_ordinal = row,
                                                     .operation = HeadOperationKind::kAppendRows};
  }
}

common::Result<HeadSnapshot>
detail::MutableHeadState::publish(const std::uint64_t token,
                                  const columnar::OwnedColumnarBatch& batch,
                                  const std::shared_ptr<const HeadPublication>& base,
                                  const std::shared_ptr<const HeadPublication>& next) {
  if (!append_active_ || active_token_ != token) {
    return common::make_unexpected(internal("prepared mutable-head append ownership was lost"));
  }
  if (active_phase_ != PreparedPhase::kWalStarted) {
    return common::make_unexpected(
        invalid("mutable-head append cannot publish before WAL submission begins"));
  }
  if (failed_.load(std::memory_order_acquire) || sealed_.load(std::memory_order_acquire)) {
    return common::make_unexpected(unavailable("mutable head cannot publish in its current state"));
  }
  if (std::atomic_load_explicit(&publication_, std::memory_order_acquire) != base) {
    failed_.store(true, std::memory_order_release);
    append_active_ = false;
    return common::make_unexpected(internal("mutable-head publication base changed unexpectedly"));
  }
  const common::Status boundaries = validate_unpublished_boundaries(*base);
  if (!boundaries.is_ok()) {
    failed_.store(true, std::memory_order_release);
    append_active_ = false;
    return common::make_unexpected(boundaries);
  }

  materialize(batch, PublicationRange{.base = *base, .next = *next});
  std::atomic_store_explicit(&publication_, next, std::memory_order_release);
  append_active_ = false;
  return HeadSnapshot{shared_from_this(), next};
}

common::Status detail::MutableHeadState::cancel_before_wal(const std::uint64_t token) {
  if (!append_active_ || active_token_ != token) {
    return internal("prepared mutable-head append ownership was lost");
  }
  if (active_phase_ != PreparedPhase::kPreWal) {
    return invalid("prepared mutable-head append cannot cancel after WAL submission begins");
  }
  append_active_ = false;
  return common::Status::ok();
}

void detail::MutableHeadState::abandon(const std::uint64_t token) noexcept {
  if (!append_active_ || active_token_ != token) {
    return;
  }
  if (active_phase_ == PreparedPhase::kWalStarted) {
    failed_.store(true, std::memory_order_release);
  }
  append_active_ = false;
}

common::Result<HeadSnapshot> detail::MutableHeadState::seal() {
  if (failed_.load(std::memory_order_acquire)) {
    return common::make_unexpected(unavailable("failed mutable head cannot be sealed for handoff"));
  }
  if (append_active_) {
    return common::make_unexpected(
        unavailable("mutable head cannot seal while an append is prepared"));
  }
  sealed_.store(true, std::memory_order_release);
  return snapshot();
}

MutableHeadMetrics detail::MutableHeadState::metrics() const noexcept {
  const std::shared_ptr<const HeadPublication> publication =
      std::atomic_load_explicit(&publication_, std::memory_order_acquire);
  std::size_t published_variable_bytes = 0U;
  for (const std::size_t frontier : publication->variable_frontiers_) {
    published_variable_bytes += frontier;
  }
  return MutableHeadMetrics{.row_capacity = row_capacity_,
                            .published_rows = publication->row_count_,
                            .variable_byte_capacity = variable_byte_capacity_,
                            .published_variable_bytes = published_variable_bytes,
                            .retained_storage_bytes = retained_storage_bytes_,
                            .sealed = sealed_.load(std::memory_order_acquire),
                            .failed = failed_.load(std::memory_order_acquire)};
}

common::Result<HeadColumnView>
detail::MutableHeadState::column_view(const HeadPublication& publication,
                                      const std::size_t ordinal) const {
  if (ordinal >= columns_.size()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kOutOfRange, "mutable-head snapshot column ordinal is out of range"});
  }
  const schema::ColumnDefinition& definition = schema_->columns()[ordinal];
  const ColumnStorage& storage = columns_[ordinal];
  const std::size_t rows = publication.row_count_;
  const std::span<const std::uint8_t> validity =
      definition.nullable() ? std::span<const std::uint8_t>{storage.validity.data(), rows}
                            : std::span<const std::uint8_t>{};
  const std::span<const std::uint8_t> booleans =
      definition.type().kind() == schema::LogicalTypeKind::kBool
          ? std::span<const std::uint8_t>{storage.boolean_values.data(), rows}
          : std::span<const std::uint8_t>{};
  const common::ByteView fixed =
      storage.fixed_width == 0U
          ? common::ByteView{}
          : common::ByteView{storage.fixed_values.data(), rows * storage.fixed_width};
  const std::span<const std::uint32_t> offsets =
      definition.type().is_variable_width()
          ? std::span<const std::uint32_t>{storage.variable_offsets.data(), rows + 1U}
          : std::span<const std::uint32_t>{};
  const common::ByteView variable = definition.type().is_variable_width()
                                        ? common::ByteView{storage.variable_values.data(),
                                                           publication.variable_frontiers_[ordinal]}
                                        : common::ByteView{};
  return HeadColumnView{definition.id(),
                        definition.type(),
                        definition.nullable(),
                        publication.row_count_,
                        validity,
                        booleans,
                        fixed,
                        offsets,
                        variable,
                        storage.fixed_width};
}

common::Result<HeadRowMetadata>
detail::MutableHeadState::row_metadata(const HeadPublication& publication,
                                       const std::uint32_t row) const {
  if (row >= publication.row_count_) {
    return common::make_unexpected(common::Status{common::StatusCode::kOutOfRange,
                                                  "mutable-head snapshot row is out of range"});
  }
  return row_metadata_[row];
}

common::Result<RowVersionIdentity>
detail::MutableHeadState::row_version_identity(const HeadPublication& publication,
                                               const std::uint32_t row) const {
  const common::Result<HeadRowMetadata> metadata = row_metadata(publication, row);
  if (!metadata.has_value()) {
    return common::make_unexpected(metadata.error());
  }
  return RowVersionIdentity{.table_id = schema_->table_id(),
                            .tablet_id = tablet_id_,
                            .wal_id = metadata->commit_position.wal_id,
                            .record_sequence = metadata->commit_position.record_sequence,
                            .row_ordinal = metadata->row_ordinal};
}

HeadColumnView::HeadColumnView(const schema::ColumnId column_id, const schema::LogicalType type,
                               const bool nullable, const std::uint32_t row_count,
                               const std::span<const std::uint8_t> validity,
                               const std::span<const std::uint8_t> boolean_values,
                               const common::ByteView fixed_values,
                               const std::span<const std::uint32_t> variable_offsets,
                               const common::ByteView variable_values,
                               const std::size_t fixed_width) noexcept
    : column_id_(column_id), type_(type), nullable_(nullable), row_count_(row_count),
      validity_(validity), boolean_values_(boolean_values), fixed_values_(fixed_values),
      variable_offsets_(variable_offsets), variable_values_(variable_values),
      fixed_width_(fixed_width) {}

common::Result<bool> HeadColumnView::is_null(const std::uint32_t row) const {
  if (row >= row_count_) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "mutable-head column row is out of range"});
  }
  return nullable_ && validity_[row] == 0U;
}

common::Result<HeadCellView> HeadColumnView::cell(const std::uint32_t row) const {
  const common::Result<bool> null = is_null(row);
  if (!null.has_value()) {
    return common::make_unexpected(null.error());
  }
  if (*null) {
    return HeadCellView::null();
  }
  if (type_.kind() == schema::LogicalTypeKind::kBool) {
    return HeadCellView::boolean(boolean_values_[row] != 0U);
  }
  if (type_.is_variable_width()) {
    const std::size_t begin = variable_offsets_[row];
    const std::size_t end = variable_offsets_[row + 1U];
    return HeadCellView::bytes(variable_values_.subspan(begin, end - begin));
  }
  return HeadCellView::bytes(
      fixed_values_.subspan(static_cast<std::size_t>(row) * fixed_width_, fixed_width_));
}

common::Result<bool> HeadCellView::boolean() const {
  if (kind_ != Kind::kBoolean) {
    return common::make_unexpected(invalid("mutable-head cell does not contain a Boolean value"));
  }
  return boolean_;
}

common::Result<common::ByteView> HeadCellView::bytes() const {
  if (kind_ != Kind::kBytes) {
    return common::make_unexpected(invalid("mutable-head cell does not contain a byte value"));
  }
  return bytes_;
}

HeadSnapshot::HeadSnapshot(std::shared_ptr<detail::MutableHeadState> state,
                           std::shared_ptr<const detail::HeadPublication> publication) noexcept
    : state_(std::move(state)), publication_(std::move(publication)) {}

const schema::TableId& HeadSnapshot::table_id() const noexcept {
  return state_->table_id();
}

const schema::TabletId& HeadSnapshot::tablet_id() const noexcept {
  return state_->tablet_id();
}

const std::shared_ptr<const schema::TableSchema>& HeadSnapshot::schema_ptr() const noexcept {
  return state_->schema_ptr();
}

std::uint64_t HeadSnapshot::generation() const noexcept {
  return state_->generation();
}

std::uint32_t HeadSnapshot::row_count() const noexcept {
  return publication_->row_count_;
}

std::size_t HeadSnapshot::column_count() const noexcept {
  return state_->column_count();
}

const std::optional<HeadCommitPosition>& HeadSnapshot::applied_position() const noexcept {
  return publication_->applied_position_;
}

common::Result<HeadColumnView> HeadSnapshot::column(const std::size_t ordinal) const {
  return state_->column_view(*publication_, ordinal);
}

common::Result<HeadCellView> HeadSnapshot::cell(const std::size_t column_ordinal,
                                                const std::uint32_t row) const {
  const common::Result<HeadColumnView> view = column(column_ordinal);
  if (!view.has_value()) {
    return common::make_unexpected(view.error());
  }
  return view->cell(row);
}

common::Result<HeadRowMetadata> HeadSnapshot::row_metadata(const std::uint32_t row) const {
  return state_->row_metadata(*publication_, row);
}

common::Result<RowVersionIdentity>
HeadSnapshot::row_version_identity(const std::uint32_t row) const {
  return state_->row_version_identity(*publication_, row);
}

PreparedHeadAppend::PreparedHeadAppend() noexcept = default;
PreparedHeadAppend::~PreparedHeadAppend() = default;
PreparedHeadAppend::PreparedHeadAppend(PreparedHeadAppend&&) noexcept = default;
PreparedHeadAppend& PreparedHeadAppend::operator=(PreparedHeadAppend&&) noexcept = default;

PreparedHeadAppend::PreparedHeadAppend(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

bool PreparedHeadAppend::is_valid() const noexcept {
  return implementation_ != nullptr;
}

bool PreparedHeadAppend::wal_started() const {
  return implementation_ != nullptr && implementation_->wal_started();
}

common::Status PreparedHeadAppend::mark_wal_started() {
  if (implementation_ == nullptr) {
    return invalid("prepared mutable-head append is invalid");
  }
  return implementation_->mark_wal_started();
}

common::Result<HeadSnapshot> PreparedHeadAppend::publish() {
  if (implementation_ == nullptr) {
    return common::make_unexpected(invalid("prepared mutable-head append is invalid"));
  }
  common::Result<HeadSnapshot> published = implementation_->publish();
  if (published.has_value()) {
    implementation_.reset();
  }
  return published;
}

common::Status PreparedHeadAppend::cancel_before_wal() {
  if (implementation_ == nullptr) {
    return invalid("prepared mutable-head append is invalid");
  }
  const common::Status status = implementation_->cancel_before_wal();
  if (status.is_ok()) {
    implementation_.reset();
  }
  return status;
}

MutableHead::MutableHead(std::shared_ptr<detail::MutableHeadState> state) noexcept
    : state_(std::move(state)) {}
MutableHead::~MutableHead() = default;
MutableHead::MutableHead(MutableHead&&) noexcept = default;
MutableHead& MutableHead::operator=(MutableHead&&) noexcept = default;

common::Result<MutableHead> MutableHead::create(std::shared_ptr<const schema::TableSchema> schema,
                                                const schema::TabletId tablet_id,
                                                const std::uint64_t generation,
                                                MutableHeadCapacity capacity) {
  if (schema == nullptr) {
    return common::make_unexpected(invalid("mutable head requires an owning schema pointer"));
  }
  if (generation == 0U || capacity.row_capacity == 0U) {
    return common::make_unexpected(
        invalid("mutable-head generation and row capacity must be nonzero"));
  }
  if (capacity.variable_value_bytes.size() != schema->columns().size()) {
    return common::make_unexpected(
        invalid("mutable-head variable capacities must match schema ordinal count"));
  }

  std::size_t retained_storage_bytes = 0U;
  std::size_t variable_byte_capacity = 0U;
  const std::size_t rows = capacity.row_capacity;
  for (std::size_t ordinal = 0U; ordinal < schema->columns().size(); ++ordinal) {
    const schema::ColumnDefinition& definition = schema->columns()[ordinal];
    const std::size_t variable_capacity = capacity.variable_value_bytes[ordinal];
    if (!definition.type().is_variable_width() && variable_capacity != 0U) {
      return common::make_unexpected(
          invalid("fixed-width mutable-head columns must have zero variable-byte capacity"));
    }
    if (definition.type().is_variable_width() &&
        variable_capacity > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
      return common::make_unexpected(
          invalid("mutable-head variable capacity exceeds the uint32 offset domain"));
    }
    if (definition.nullable()) {
      const auto total = add_storage_bytes(retained_storage_bytes, rows, sizeof(std::uint8_t));
      if (!total.has_value()) {
        return common::make_unexpected(total.error());
      }
      retained_storage_bytes = *total;
    }
    if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
      const auto total = add_storage_bytes(retained_storage_bytes, rows, sizeof(std::uint8_t));
      if (!total.has_value()) {
        return common::make_unexpected(total.error());
      }
      retained_storage_bytes = *total;
    } else if (definition.type().is_variable_width()) {
      const auto offset_count = common::checked_add(rows, std::size_t{1U});
      if (!offset_count.has_value()) {
        return common::make_unexpected(
            exhausted("mutable-head variable offset count overflowed this platform"));
      }
      auto total = add_storage_bytes(retained_storage_bytes, *offset_count, sizeof(std::uint32_t));
      if (!total.has_value()) {
        return common::make_unexpected(total.error());
      }
      total = add_storage_bytes(*total, variable_capacity, sizeof(std::byte));
      if (!total.has_value()) {
        return common::make_unexpected(total.error());
      }
      retained_storage_bytes = *total;
      const auto next_variable = common::checked_add(variable_byte_capacity, variable_capacity);
      if (!next_variable.has_value()) {
        return common::make_unexpected(
            exhausted("mutable-head variable capacity accounting overflowed this platform"));
      }
      variable_byte_capacity = *next_variable;
    } else {
      const auto total =
          add_storage_bytes(retained_storage_bytes, rows, fixed_width(definition.type().kind()));
      if (!total.has_value()) {
        return common::make_unexpected(total.error());
      }
      retained_storage_bytes = *total;
    }
  }
  const auto with_metadata =
      add_storage_bytes(retained_storage_bytes, rows, sizeof(HeadRowMetadata));
  if (!with_metadata.has_value()) {
    return common::make_unexpected(with_metadata.error());
  }
  retained_storage_bytes = *with_metadata;

  try {
    std::vector<ColumnStorage> columns;
    columns.reserve(schema->columns().size());
    for (std::size_t ordinal = 0U; ordinal < schema->columns().size(); ++ordinal) {
      const schema::ColumnDefinition& definition = schema->columns()[ordinal];
      ColumnStorage storage;
      if (definition.nullable()) {
        storage.validity.resize(rows);
      }
      if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
        storage.boolean_values.resize(rows);
      } else if (definition.type().is_variable_width()) {
        storage.variable_offsets.resize(rows + 1U);
        storage.variable_values.resize(capacity.variable_value_bytes[ordinal]);
      } else {
        storage.fixed_width = fixed_width(definition.type().kind());
        storage.fixed_values.resize(rows * storage.fixed_width);
      }
      columns.push_back(std::move(storage));
    }
    std::vector<HeadRowMetadata> metadata(rows);
    auto initial = std::make_shared<const detail::HeadPublication>(
        0U, std::nullopt, std::vector<std::size_t>(schema->columns().size(), 0U));
    auto state = std::make_shared<detail::MutableHeadState>(
        std::move(schema), tablet_id, generation, capacity.row_capacity, std::move(columns),
        std::move(metadata), std::move(initial), variable_byte_capacity, retained_storage_bytes);
    return MutableHead{std::move(state)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable-head storage allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable-head capacity exceeds container limits"));
  }
}

common::Result<PreparedHeadAppend>
MutableHead::prepare_append(std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
                            const HeadCommitPosition position) {
  if (state_ == nullptr) {
    return common::make_unexpected(invalid("mutable head is invalid"));
  }
  return state_->prepare(std::move(batch), position);
}

common::Result<HeadSnapshot> MutableHead::snapshot() const {
  if (state_ == nullptr) {
    return common::make_unexpected(invalid("mutable head is invalid"));
  }
  return state_->snapshot();
}

common::Result<HeadSnapshot> MutableHead::seal() {
  if (state_ == nullptr) {
    return common::make_unexpected(invalid("mutable head is invalid"));
  }
  return state_->seal();
}

MutableHeadMetrics MutableHead::metrics() const {
  if (state_ == nullptr) {
    return {};
  }
  return state_->metrics();
}

} // namespace chronos::head
