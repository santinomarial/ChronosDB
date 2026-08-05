#ifndef CHRONOS_HEAD_MUTABLE_HEAD_HPP_
#define CHRONOS_HEAD_MUTABLE_HEAD_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/types.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::head {

struct HeadCommitPosition {
  wal::WalId wal_id;
  std::uint64_t record_sequence{};

  friend bool operator==(const HeadCommitPosition&, const HeadCommitPosition&) = default;
};

enum class HeadOperationKind : std::uint8_t {
  kAppendRows = 1,
};

struct HeadRowMetadata {
  HeadCommitPosition commit_position;
  std::uint32_t row_ordinal{};
  HeadOperationKind operation{HeadOperationKind::kAppendRows};

  friend bool operator==(const HeadRowMetadata&, const HeadRowMetadata&) = default;
};

struct RowVersionIdentity {
  schema::TableId table_id;
  schema::TabletId tablet_id;
  wal::WalId wal_id;
  std::uint64_t record_sequence{};
  std::uint32_t row_ordinal{};

  friend bool operator==(const RowVersionIdentity&, const RowVersionIdentity&) = default;
};

// One entry per schema ordinal is required. Fixed-width and BOOL columns must use zero; each
// variable-width entry is that column's exact owned byte capacity and may itself be zero.
struct MutableHeadCapacity {
  std::uint32_t row_capacity{};
  std::vector<std::size_t> variable_value_bytes;
};

struct MutableHeadMetrics {
  std::uint32_t row_capacity{};
  std::uint32_t published_rows{};
  std::size_t variable_byte_capacity{};
  std::size_t published_variable_bytes{};
  std::size_t retained_storage_bytes{};
  bool sealed{false};
  bool failed{false};
};

namespace detail {
class HeadPublication;
class MutableHeadState;
} // namespace detail

// A cell returned from a head snapshot. Byte values borrow the snapshot's pinned generation;
// callers must retain that snapshot while using the bytes.
class HeadCellView {
public:
  enum class Kind : std::uint8_t { kNull, kBoolean, kBytes };

  [[nodiscard]] constexpr Kind kind() const noexcept {
    return kind_;
  }
  [[nodiscard]] constexpr bool is_null() const noexcept {
    return kind_ == Kind::kNull;
  }
  [[nodiscard]] common::Result<bool> boolean() const;
  [[nodiscard]] common::Result<common::ByteView> bytes() const;

private:
  explicit constexpr HeadCellView(Kind kind, bool boolean, common::ByteView bytes) noexcept
      : kind_(kind), boolean_(boolean), bytes_(bytes) {}

  [[nodiscard]] static constexpr HeadCellView null() noexcept {
    return HeadCellView{Kind::kNull, false, {}};
  }
  [[nodiscard]] static constexpr HeadCellView boolean(bool value) noexcept {
    return HeadCellView{Kind::kBoolean, value, {}};
  }
  [[nodiscard]] static constexpr HeadCellView bytes(common::ByteView value) noexcept {
    return HeadCellView{Kind::kBytes, false, value};
  }

  Kind kind_;
  bool boolean_;
  common::ByteView bytes_;

  friend class HeadColumnView;
};

// A borrowed, immutable view of one column at one published row/byte boundary. Validity and BOOL
// values use one uint8_t per row. Variable offsets are native in-memory uint32_t values, not a
// durable encoding. The owning HeadSnapshot must outlive this view and every returned cell.
class HeadColumnView {
public:
  HeadColumnView() = delete;

  [[nodiscard]] constexpr const schema::ColumnId& column_id() const noexcept {
    return column_id_;
  }
  [[nodiscard]] constexpr const schema::LogicalType& type() const noexcept {
    return type_;
  }
  [[nodiscard]] constexpr bool nullable() const noexcept {
    return nullable_;
  }
  [[nodiscard]] constexpr std::uint32_t row_count() const noexcept {
    return row_count_;
  }
  [[nodiscard]] constexpr std::span<const std::uint8_t> validity() const noexcept {
    return validity_;
  }
  [[nodiscard]] constexpr std::span<const std::uint8_t> boolean_values() const noexcept {
    return boolean_values_;
  }
  [[nodiscard]] constexpr common::ByteView fixed_values() const noexcept {
    return fixed_values_;
  }
  [[nodiscard]] constexpr std::span<const std::uint32_t> variable_offsets() const noexcept {
    return variable_offsets_;
  }
  [[nodiscard]] constexpr common::ByteView variable_values() const noexcept {
    return variable_values_;
  }

  [[nodiscard]] common::Result<bool> is_null(std::uint32_t row) const;
  [[nodiscard]] common::Result<HeadCellView> cell(std::uint32_t row) const;

private:
  HeadColumnView(schema::ColumnId column_id, schema::LogicalType type, bool nullable,
                 std::uint32_t row_count, std::span<const std::uint8_t> validity,
                 std::span<const std::uint8_t> boolean_values, common::ByteView fixed_values,
                 std::span<const std::uint32_t> variable_offsets, common::ByteView variable_values,
                 std::size_t fixed_width) noexcept;

  schema::ColumnId column_id_;
  schema::LogicalType type_;
  bool nullable_;
  std::uint32_t row_count_;
  std::span<const std::uint8_t> validity_;
  std::span<const std::uint8_t> boolean_values_;
  common::ByteView fixed_values_;
  std::span<const std::uint32_t> variable_offsets_;
  common::ByteView variable_values_;
  std::size_t fixed_width_;

  friend class detail::MutableHeadState;
};

// Copyable owning pin for one acquire-observed publication. All identity, position, column, and
// row boundaries in this object come from the same publication epoch.
class HeadSnapshot {
public:
  HeadSnapshot() = delete;
  HeadSnapshot(const HeadSnapshot&) noexcept = default;
  HeadSnapshot& operator=(const HeadSnapshot&) noexcept = default;
  HeadSnapshot(HeadSnapshot&&) noexcept = default;
  HeadSnapshot& operator=(HeadSnapshot&&) noexcept = default;
  ~HeadSnapshot() = default;

  [[nodiscard]] const schema::TableId& table_id() const noexcept;
  [[nodiscard]] const schema::TabletId& tablet_id() const noexcept;
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept;
  [[nodiscard]] std::uint64_t generation() const noexcept;
  [[nodiscard]] std::uint32_t row_count() const noexcept;
  [[nodiscard]] std::size_t column_count() const noexcept;
  [[nodiscard]] const std::optional<HeadCommitPosition>& applied_position() const noexcept;

  [[nodiscard]] common::Result<HeadColumnView> column(std::size_t ordinal) const;
  [[nodiscard]] common::Result<HeadCellView> cell(std::size_t column_ordinal,
                                                  std::uint32_t row) const;
  [[nodiscard]] common::Result<HeadRowMetadata> row_metadata(std::uint32_t row) const;
  [[nodiscard]] common::Result<RowVersionIdentity> row_version_identity(std::uint32_t row) const;

private:
  HeadSnapshot(std::shared_ptr<detail::MutableHeadState> state,
               std::shared_ptr<const detail::HeadPublication> publication) noexcept;

  std::shared_ptr<detail::MutableHeadState> state_;
  std::shared_ptr<const detail::HeadPublication> publication_;

  friend class detail::MutableHeadState;
};

// Move-only ownership of one prepared unpublished range. Methods and destruction are shard-writer
// operations and must not run concurrently. A pre-WAL drop cancels harmlessly; a post-WAL drop
// fails the head closed while preserving its last published snapshot.
class PreparedHeadAppend {
public:
  PreparedHeadAppend() noexcept;
  ~PreparedHeadAppend();

  PreparedHeadAppend(const PreparedHeadAppend&) = delete;
  PreparedHeadAppend& operator=(const PreparedHeadAppend&) = delete;
  PreparedHeadAppend(PreparedHeadAppend&&) noexcept;
  PreparedHeadAppend& operator=(PreparedHeadAppend&&) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] bool wal_started() const;
  [[nodiscard]] common::Status mark_wal_started();
  [[nodiscard]] common::Result<HeadSnapshot> publish();
  [[nodiscard]] common::Status cancel_before_wal();

private:
  class Impl;
  explicit PreparedHeadAppend(std::unique_ptr<Impl> implementation) noexcept;

  std::unique_ptr<Impl> implementation_;

  friend class detail::MutableHeadState;
};

// One fixed-capacity, schema-bound generation. Exactly one shard thread owns prepare, mark,
// publish, cancel, and seal operations. Any number of threads may concurrently acquire and scan
// snapshots. Moving or destroying the MutableHead concurrently with either activity is invalid.
class MutableHead {
public:
  MutableHead() = delete;
  ~MutableHead();

  MutableHead(const MutableHead&) = delete;
  MutableHead& operator=(const MutableHead&) = delete;
  MutableHead(MutableHead&&) noexcept;
  MutableHead& operator=(MutableHead&&) noexcept;

  [[nodiscard]] static common::Result<MutableHead>
  create(std::shared_ptr<const schema::TableSchema> schema, schema::TabletId tablet_id,
         std::uint64_t generation, MutableHeadCapacity capacity);

  // Performs every expected allocation and capacity check before returning ownership. It copies no
  // user data and changes no published boundary. Only one prepared append may exist at a time.
  [[nodiscard]] common::Result<PreparedHeadAppend>
  prepare_append(std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
                 HeadCommitPosition position);

  [[nodiscard]] common::Result<HeadSnapshot> snapshot() const;

  // Idempotently freezes this generation at its exact current boundary. A prepared append must be
  // resolved first. Tablet-level generation switching and flush handoff are outside this class.
  [[nodiscard]] common::Result<HeadSnapshot> seal();

  [[nodiscard]] MutableHeadMetrics metrics() const;

private:
  explicit MutableHead(std::shared_ptr<detail::MutableHeadState> state) noexcept;

  std::shared_ptr<detail::MutableHeadState> state_;
};

} // namespace chronos::head

#endif // CHRONOS_HEAD_MUTABLE_HEAD_HPP_
