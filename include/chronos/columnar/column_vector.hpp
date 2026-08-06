#ifndef CHRONOS_COLUMNAR_COLUMN_VECTOR_HPP_
#define CHRONOS_COLUMNAR_COLUMN_VECTOR_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace chronos::columnar {

// Returns the exact number of bytes required by an LSB-first row bitmap.
[[nodiscard]] constexpr std::size_t bitmap_size(const std::uint32_t row_count) noexcept {
  const std::size_t widened = row_count;
  return widened / 8U + (widened % 8U == 0U ? 0U : 1U);
}

struct ColumnVectorMetadata {
  schema::ColumnId column_id;
  schema::LogicalType type;
  bool nullable;
  std::uint32_t row_count;
  std::uint32_t null_count;
};

struct ColumnVectorBufferView {
  common::ByteView validity;
  common::ByteView offsets;
  common::ByteView values;
};

struct PhysicalColumnMetadata {
  schema::LogicalType type;
  bool nullable;
  std::uint32_t row_count;
  std::uint32_t null_count;
};

// A row inspection result. Byte values borrow the source vector and remain valid only while that
// immutable vector storage remains alive. Fixed-width bytes retain the canonical little-endian (or
// UUID network-order) representation; variable-width bytes are the exact row slice.
class ColumnCellView {
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
  explicit constexpr ColumnCellView(const Kind kind, const bool boolean,
                                    const common::ByteView bytes) noexcept
      : kind_(kind), boolean_(boolean), bytes_(bytes) {}

  [[nodiscard]] static constexpr ColumnCellView null() noexcept {
    return ColumnCellView{Kind::kNull, false, {}};
  }
  [[nodiscard]] static constexpr ColumnCellView boolean(const bool value) noexcept {
    return ColumnCellView{Kind::kBoolean, value, {}};
  }
  [[nodiscard]] static constexpr ColumnCellView bytes(const common::ByteView value) noexcept {
    return ColumnCellView{Kind::kBytes, false, value};
  }

  Kind kind_;
  bool boolean_;
  common::ByteView bytes_;

  friend class PhysicalColumnView;
};

// Identity-free immutable physical data. This is the shared canonical validation boundary for
// Columnar Batch vectors and standalone CSEG pages. All buffers are borrowed and must remain alive
// and immutable for the view lifetime.
class PhysicalColumnView {
public:
  PhysicalColumnView() = delete;

  [[nodiscard]] static common::Result<PhysicalColumnView> create(PhysicalColumnMetadata metadata,
                                                                 ColumnVectorBufferView buffers);

  [[nodiscard]] constexpr const schema::LogicalType& type() const noexcept {
    return type_;
  }
  [[nodiscard]] constexpr bool nullable() const noexcept {
    return nullable_;
  }
  [[nodiscard]] constexpr std::uint32_t row_count() const noexcept {
    return row_count_;
  }
  [[nodiscard]] constexpr std::uint32_t null_count() const noexcept {
    return null_count_;
  }
  [[nodiscard]] constexpr common::ByteView validity() const noexcept {
    return validity_;
  }
  [[nodiscard]] constexpr common::ByteView offsets() const noexcept {
    return offsets_;
  }
  [[nodiscard]] constexpr common::ByteView values() const noexcept {
    return values_;
  }
  [[nodiscard]] std::size_t buffer_bytes() const noexcept;
  [[nodiscard]] common::Result<bool> is_null(std::uint32_t row) const;
  [[nodiscard]] common::Result<ColumnCellView> cell(std::uint32_t row) const;

private:
  constexpr PhysicalColumnView(const PhysicalColumnMetadata metadata,
                               const ColumnVectorBufferView buffers) noexcept
      : type_(metadata.type), nullable_(metadata.nullable), row_count_(metadata.row_count),
        null_count_(metadata.null_count), validity_(buffers.validity), offsets_(buffers.offsets),
        values_(buffers.values) {}

  schema::LogicalType type_;
  bool nullable_;
  std::uint32_t row_count_;
  std::uint32_t null_count_;
  common::ByteView validity_;
  common::ByteView offsets_;
  common::ByteView values_;

  friend class ColumnVectorView;
  friend class OwnedPhysicalColumn;
};

// A non-owning immutable view over one complete canonical column vector. Construction validates all
// buffer sizes, bitmap rules, offsets, null slots, UTF-8, and decimal domains. The caller keeps
// every referenced buffer alive and immutable for the view's lifetime.
class ColumnVectorView {
public:
  ColumnVectorView() = delete;

  [[nodiscard]] static common::Result<ColumnVectorView> create(ColumnVectorMetadata metadata,
                                                               ColumnVectorBufferView buffers);

  [[nodiscard]] constexpr const schema::ColumnId& column_id() const noexcept {
    return column_id_;
  }
  [[nodiscard]] constexpr const schema::LogicalType& type() const noexcept {
    return physical_.type();
  }
  [[nodiscard]] constexpr bool nullable() const noexcept {
    return physical_.nullable();
  }
  [[nodiscard]] constexpr std::uint32_t row_count() const noexcept {
    return physical_.row_count();
  }
  [[nodiscard]] constexpr std::uint32_t null_count() const noexcept {
    return physical_.null_count();
  }
  [[nodiscard]] constexpr common::ByteView validity() const noexcept {
    return physical_.validity();
  }
  [[nodiscard]] constexpr common::ByteView offsets() const noexcept {
    return physical_.offsets();
  }
  [[nodiscard]] constexpr common::ByteView values() const noexcept {
    return physical_.values();
  }
  [[nodiscard]] constexpr const PhysicalColumnView& physical() const noexcept {
    return physical_;
  }
  [[nodiscard]] std::size_t buffer_bytes() const noexcept;

  [[nodiscard]] common::Result<bool> is_null(std::uint32_t row) const;
  [[nodiscard]] common::Result<ColumnCellView> cell(std::uint32_t row) const;

private:
  constexpr ColumnVectorView(const ColumnVectorMetadata metadata,
                             const ColumnVectorBufferView buffers) noexcept
      : column_id_(metadata.column_id),
        physical_(PhysicalColumnMetadata{.type = metadata.type,
                                         .nullable = metadata.nullable,
                                         .row_count = metadata.row_count,
                                         .null_count = metadata.null_count},
                  buffers) {}

  schema::ColumnId column_id_;
  PhysicalColumnView physical_;

  friend class OwnedColumnVector;
};

struct ColumnVectorBuffers {
  std::vector<std::byte> validity;
  std::vector<std::byte> offsets;
  std::vector<std::byte> values;
};

// Owns one immutable identity-free physical vector. Query intermediates and decoded storage pages
// need the canonical physical representation without fabricating a durable ColumnId. Accessors
// expose only immutable spans; view() borrows this object's storage.
class OwnedPhysicalColumn {
public:
  OwnedPhysicalColumn() = delete;
  OwnedPhysicalColumn(const OwnedPhysicalColumn&) = delete;
  OwnedPhysicalColumn& operator=(const OwnedPhysicalColumn&) = delete;
  OwnedPhysicalColumn(OwnedPhysicalColumn&&) noexcept = default;
  OwnedPhysicalColumn& operator=(OwnedPhysicalColumn&&) noexcept = default;

  [[nodiscard]] static common::Result<OwnedPhysicalColumn> create(PhysicalColumnMetadata metadata,
                                                                  ColumnVectorBuffers buffers);

  [[nodiscard]] PhysicalColumnView view() const noexcept;
  [[nodiscard]] const schema::LogicalType& type() const noexcept;
  [[nodiscard]] bool nullable() const noexcept;
  [[nodiscard]] std::uint32_t row_count() const noexcept;
  [[nodiscard]] std::uint32_t null_count() const noexcept;
  [[nodiscard]] std::size_t buffer_bytes() const noexcept;
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;
  [[nodiscard]] common::Result<bool> is_null(std::uint32_t row) const;
  [[nodiscard]] common::Result<ColumnCellView> cell(std::uint32_t row) const;

private:
  OwnedPhysicalColumn(PhysicalColumnMetadata metadata, ColumnVectorBuffers buffers) noexcept;

  schema::LogicalType type_;
  bool nullable_;
  std::uint32_t row_count_;
  std::uint32_t null_count_;
  ColumnVectorBuffers buffers_;
};

// Owns immutable canonical buffers. Accessors expose only const spans; view() borrows this object.
class OwnedColumnVector {
public:
  OwnedColumnVector() = delete;
  OwnedColumnVector(const OwnedColumnVector&) = delete;
  OwnedColumnVector& operator=(const OwnedColumnVector&) = delete;
  OwnedColumnVector(OwnedColumnVector&&) noexcept = default;
  OwnedColumnVector& operator=(OwnedColumnVector&&) noexcept = default;

  [[nodiscard]] static common::Result<OwnedColumnVector> create(ColumnVectorMetadata metadata,
                                                                ColumnVectorBuffers buffers);

  [[nodiscard]] ColumnVectorView view() const noexcept;
  [[nodiscard]] const schema::ColumnId& column_id() const noexcept;
  [[nodiscard]] const schema::LogicalType& type() const noexcept;
  [[nodiscard]] bool nullable() const noexcept;
  [[nodiscard]] std::uint32_t row_count() const noexcept;
  [[nodiscard]] std::uint32_t null_count() const noexcept;
  [[nodiscard]] std::size_t buffer_bytes() const noexcept;
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;
  [[nodiscard]] common::Result<bool> is_null(std::uint32_t row) const;
  [[nodiscard]] common::Result<ColumnCellView> cell(std::uint32_t row) const;

private:
  OwnedColumnVector(schema::ColumnId column_id, OwnedPhysicalColumn physical) noexcept;

  schema::ColumnId column_id_;
  OwnedPhysicalColumn physical_;
};

} // namespace chronos::columnar

#endif // CHRONOS_COLUMNAR_COLUMN_VECTOR_HPP_
