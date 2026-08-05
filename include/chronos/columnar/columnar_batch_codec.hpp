#ifndef CHRONOS_COLUMNAR_COLUMNAR_BATCH_CODEC_HPP_
#define CHRONOS_COLUMNAR_COLUMNAR_BATCH_CODEC_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/columnar/columnar_batch_format.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <span>
#include <vector>

namespace chronos::columnar {

struct BufferLayout {
  std::size_t offset;
  std::size_t length;

  friend bool operator==(const BufferLayout&, const BufferLayout&) = default;
};

struct ColumnLayout {
  BufferLayout validity;
  BufferLayout offsets;
  BufferLayout values;

  friend bool operator==(const ColumnLayout&, const ColumnLayout&) = default;
};

class ColumnarBatchLayout {
public:
  ColumnarBatchLayout() = delete;

  [[nodiscard]] std::size_t total_length() const noexcept;
  [[nodiscard]] std::span<const ColumnLayout> columns() const noexcept;

private:
  ColumnarBatchLayout(std::size_t total_length, std::vector<ColumnLayout> columns) noexcept;

  std::size_t total_length_;
  std::vector<ColumnLayout> columns_;

  friend common::Result<ColumnarBatchLayout>
  plan_columnar_batch_v1_layout(const OwnedColumnarBatch& batch);
};

// Computes the one canonical v1 placement with checked arithmetic before any output allocation.
[[nodiscard]] common::Result<ColumnarBatchLayout>
plan_columnar_batch_v1_layout(const OwnedColumnarBatch& batch);

// Owns exactly one complete encoded batch. bytes() contains no enclosing framing or trailing data.
class EncodedColumnarBatch {
public:
  EncodedColumnarBatch() = delete;
  EncodedColumnarBatch(const EncodedColumnarBatch&) = delete;
  EncodedColumnarBatch& operator=(const EncodedColumnarBatch&) = delete;
  EncodedColumnarBatch(EncodedColumnarBatch&&) noexcept = default;
  EncodedColumnarBatch& operator=(EncodedColumnarBatch&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;

private:
  explicit EncodedColumnarBatch(std::vector<std::byte> bytes) noexcept;

  std::vector<std::byte> bytes_;

  friend common::Result<EncodedColumnarBatch>
  encode_columnar_batch_v1(const OwnedColumnarBatch& batch);
};

[[nodiscard]] common::Result<EncodedColumnarBatch>
encode_columnar_batch_v1(const OwnedColumnarBatch& batch);

struct ColumnarBatchDecodeLimits {
  std::size_t max_batch_length{format::kMaximumEmbeddedBatchLength};
  std::uint32_t max_rows{std::numeric_limits<std::uint32_t>::max()};
  std::uint32_t max_columns{format::kMaximumColumnCount};
};

enum class ColumnarBatchDecodeErrorKind : std::uint8_t {
  kIncomplete,
  kInvalid,
  kUnsupported,
  kResourceLimit,
};

class ColumnarBatchDecodeError {
public:
  ColumnarBatchDecodeError(ColumnarBatchDecodeErrorKind kind, common::Status status,
                           std::size_t required_size = 0U) noexcept;

  [[nodiscard]] constexpr ColumnarBatchDecodeErrorKind kind() const noexcept {
    return kind_;
  }
  [[nodiscard]] constexpr std::size_t required_size() const noexcept {
    return required_size_;
  }
  [[nodiscard]] const common::Status& status() const noexcept {
    return status_;
  }

private:
  ColumnarBatchDecodeErrorKind kind_;
  common::Status status_;
  std::size_t required_size_;

  friend std::expected<class DecodedColumnarBatchView, ColumnarBatchDecodeError>
  decode_columnar_batch_v1_prefix(common::ByteView bytes, ColumnarBatchDecodeLimits limits);
  friend std::expected<class DecodedColumnarBatchView, ColumnarBatchDecodeError>
  decode_columnar_batch_v1_exact(common::ByteView bytes, ColumnarBatchDecodeLimits limits);
};

// Borrows one complete immutable encoded batch. Its storage must outlive this object and every
// column/cell view obtained from it. The descriptor-view vector is owned by this object.
class DecodedColumnarBatchView {
public:
  DecodedColumnarBatchView() = delete;

  [[nodiscard]] constexpr const schema::TableId& table_id() const noexcept {
    return table_id_;
  }
  [[nodiscard]] constexpr const schema::SchemaId& schema_id() const noexcept {
    return schema_id_;
  }
  [[nodiscard]] constexpr schema::SchemaVersion schema_version() const noexcept {
    return schema_version_;
  }
  [[nodiscard]] constexpr std::uint32_t row_count() const noexcept {
    return row_count_;
  }
  [[nodiscard]] std::span<const ColumnVectorView> columns() const noexcept;
  [[nodiscard]] const ColumnVectorView* column(std::size_t ordinal) const noexcept;
  [[nodiscard]] common::ByteView encoded_bytes() const noexcept;

private:
  DecodedColumnarBatchView(schema::TableId table_id, schema::SchemaId schema_id,
                           schema::SchemaVersion schema_version, std::uint32_t row_count,
                           std::vector<ColumnVectorView> columns,
                           common::ByteView encoded_bytes) noexcept;

  schema::TableId table_id_;
  schema::SchemaId schema_id_;
  schema::SchemaVersion schema_version_;
  std::uint32_t row_count_;
  std::vector<ColumnVectorView> columns_;
  common::ByteView encoded_bytes_;

  friend std::expected<DecodedColumnarBatchView, ColumnarBatchDecodeError>
  decode_columnar_batch_v1_prefix(common::ByteView bytes, ColumnarBatchDecodeLimits limits);
};

using ColumnarBatchDecodeResult = std::expected<DecodedColumnarBatchView, ColumnarBatchDecodeError>;

// Decodes the first canonical batch and ignores later bytes. An incomplete result states the
// minimum currently known required size. Header truncation requires 96; a validated header can
// report its exact total length.
[[nodiscard]] ColumnarBatchDecodeResult
decode_columnar_batch_v1_prefix(common::ByteView bytes, ColumnarBatchDecodeLimits limits = {});

// Requires the input to contain exactly one canonical batch; trailing bytes are invalid.
[[nodiscard]] ColumnarBatchDecodeResult
decode_columnar_batch_v1_exact(common::ByteView bytes, ColumnarBatchDecodeLimits limits = {});

// Performs the catalog-dependent second stage: exact batch/schema identity, version, ordinal,
// type-parameter, and nullability binding. It does not perform routing or role validation.
[[nodiscard]] common::Status validate_columnar_batch_schema(const DecodedColumnarBatchView& batch,
                                                            const schema::TableSchema& schema);

} // namespace chronos::columnar

#endif // CHRONOS_COLUMNAR_COLUMNAR_BATCH_CODEC_HPP_
