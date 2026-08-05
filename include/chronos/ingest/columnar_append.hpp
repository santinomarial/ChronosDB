#ifndef CHRONOS_INGEST_COLUMNAR_APPEND_HPP_
#define CHRONOS_INGEST_COLUMNAR_APPEND_HPP_

#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/ingest/columnar_append_format.hpp"
#include "chronos/ingest/identity.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/application.hpp"
#include "chronos/wal/types.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>

namespace chronos::ingest {

struct ColumnarAppendDigestInput {
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  common::ByteView encoded_batch;
};

[[nodiscard]] common::Result<Sha256Digest>
compute_columnar_append_v1_request_digest(const ColumnarAppendDigestInput& input);

struct ColumnarAppendEncodeInput {
  ClientId client_id;
  ClientBatchId client_batch_id;
  schema::TabletId tablet_id;
};

// Encodes exactly one immutable WAL application payload around one already encoded canonical
// Columnar Batch v1 object. WAL record framing is deliberately not included.
[[nodiscard]] common::Result<wal::EncodedApplicationPayload>
encode_columnar_append_v1(const ColumnarAppendEncodeInput& input,
                          const columnar::EncodedColumnarBatch& batch);

struct ColumnarAppendDecodeLimits {
  std::size_t max_application_payload_length{columnar_append_v1::kMaximumApplicationPayloadLength};
  columnar::ColumnarBatchDecodeLimits batch;
};

enum class ColumnarAppendDecodeErrorKind : std::uint8_t {
  kIncomplete,
  kCorruption,
  kUnsupported,
  kResourceLimit,
  kInternal,
};

class ColumnarAppendDecodeError {
public:
  ColumnarAppendDecodeError(ColumnarAppendDecodeErrorKind kind, common::Status status,
                            std::size_t required_size = 0U) noexcept;

  [[nodiscard]] constexpr ColumnarAppendDecodeErrorKind kind() const noexcept {
    return kind_;
  }
  [[nodiscard]] constexpr std::size_t required_size() const noexcept {
    return required_size_;
  }
  [[nodiscard]] const common::Status& status() const noexcept {
    return status_;
  }

private:
  ColumnarAppendDecodeErrorKind kind_;
  common::Status status_;
  std::size_t required_size_;
};

// Borrows one complete immutable application payload. The payload storage must outlive this view
// and all nested batch column/cell views. Small decoded descriptor metadata is owned by this value.
class DecodedColumnarAppendView {
public:
  DecodedColumnarAppendView() = delete;

  [[nodiscard]] constexpr const ClientId& client_id() const noexcept {
    return client_id_;
  }
  [[nodiscard]] constexpr const ClientBatchId& client_batch_id() const noexcept {
    return client_batch_id_;
  }
  [[nodiscard]] constexpr const schema::TableId& table_id() const noexcept {
    return table_id_;
  }
  [[nodiscard]] constexpr const schema::TabletId& tablet_id() const noexcept {
    return tablet_id_;
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
  [[nodiscard]] constexpr const Sha256Digest& request_digest() const noexcept {
    return request_digest_;
  }
  [[nodiscard]] const columnar::DecodedColumnarBatchView& batch() const noexcept;
  [[nodiscard]] common::ByteView encoded_payload() const noexcept;

private:
  DecodedColumnarAppendView(ClientId client_id, ClientBatchId client_batch_id,
                            schema::TableId table_id, schema::TabletId tablet_id,
                            schema::SchemaId schema_id, schema::SchemaVersion schema_version,
                            std::uint32_t row_count, Sha256Digest request_digest,
                            columnar::DecodedColumnarBatchView batch,
                            common::ByteView encoded_payload) noexcept;

  ClientId client_id_;
  ClientBatchId client_batch_id_;
  schema::TableId table_id_;
  schema::TabletId tablet_id_;
  schema::SchemaId schema_id_;
  schema::SchemaVersion schema_version_;
  std::uint32_t row_count_;
  Sha256Digest request_digest_;
  columnar::DecodedColumnarBatchView batch_;
  common::ByteView encoded_payload_;

  friend std::expected<DecodedColumnarAppendView, ColumnarAppendDecodeError>
  decode_columnar_append_v1_prefix(common::ByteView bytes, ColumnarAppendDecodeLimits limits);
};

using ColumnarAppendDecodeResult =
    std::expected<DecodedColumnarAppendView, ColumnarAppendDecodeError>;

[[nodiscard]] ColumnarAppendDecodeResult
decode_columnar_append_v1_prefix(common::ByteView bytes, ColumnarAppendDecodeLimits limits = {});
[[nodiscard]] ColumnarAppendDecodeResult
decode_columnar_append_v1_exact(common::ByteView bytes, ColumnarAppendDecodeLimits limits = {});

// The caller supplies a DecodedRecord produced by the WAL record codec, so physical CRC validation
// is not repeated. Record format/type and the exact application payload are still checked.
[[nodiscard]] ColumnarAppendDecodeResult
decode_columnar_append_v1_record(const wal::DecodedRecord& record,
                                 ColumnarAppendDecodeLimits limits = {});

// Catalog-dependent validation only. Routing, active-schema admission, retry state, and row-level
// deduplication remain outside this pure command layer.
[[nodiscard]] common::Status
validate_columnar_append_schema(const DecodedColumnarAppendView& command,
                                const schema::TableSchema& schema);

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_COLUMNAR_APPEND_HPP_
