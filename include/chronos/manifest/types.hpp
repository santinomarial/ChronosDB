#ifndef CHRONOS_MANIFEST_TYPES_HPP_
#define CHRONOS_MANIFEST_TYPES_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/ingest/identity.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/wal/types.hpp"

#include <compare>
#include <cstdint>

namespace chronos::manifest {

class DatabaseId {
public:
  DatabaseId() = delete;

  [[nodiscard]] static common::Result<DatabaseId> from_uuid(common::Uuid value) {
    if (value.is_nil()) {
      return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                    "database identity must be nonzero"});
    }
    return DatabaseId{value};
  }

  [[nodiscard]] static common::Result<DatabaseId> from_bytes(common::Uuid::Bytes bytes) {
    return from_uuid(common::Uuid{bytes});
  }

  [[nodiscard]] constexpr const common::Uuid& uuid() const noexcept {
    return value_;
  }
  [[nodiscard]] constexpr const common::Uuid::Bytes& bytes() const noexcept {
    return value_.bytes();
  }

  friend constexpr auto operator<=>(const DatabaseId&, const DatabaseId&) = default;

private:
  explicit constexpr DatabaseId(common::Uuid value) noexcept : value_(value) {}

  common::Uuid value_;
};

struct WalCheckpoint {
  std::uint64_t record_sequence{};
  std::uint64_t segment_number{};
  std::uint64_t byte_offset{};

  friend bool operator==(const WalCheckpoint&, const WalCheckpoint&) = default;
};

struct TabletDescriptor {
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId recovery_schema_id;
  schema::SchemaVersion recovery_schema_version;
  std::uint64_t durable_record_sequence{};
  std::uint64_t first_part_index{};
  std::uint64_t part_count{};
  std::uint64_t durable_row_count{};

  friend bool operator==(const TabletDescriptor&, const TabletDescriptor&) = default;
};

struct PartDescriptor {
  cseg::PartId part_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  std::uint64_t file_length{};
  std::uint64_t row_count{};
  std::uint64_t minimum_record_sequence{};
  std::uint64_t maximum_record_sequence{};
  std::int64_t minimum_event_time{};
  std::int64_t maximum_event_time{};

  friend bool operator==(const PartDescriptor&, const PartDescriptor&) = default;
};

struct RetryDescriptor {
  ingest::ClientId client_id;
  ingest::ClientBatchId client_batch_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  ingest::Sha256Digest request_digest;
  wal::WalId wal_id;
  std::uint64_t record_sequence{};
  std::uint32_t applied_row_count{};

  friend bool operator==(const RetryDescriptor&, const RetryDescriptor&) = default;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_TYPES_HPP_
