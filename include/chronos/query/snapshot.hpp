#ifndef CHRONOS_QUERY_SNAPSHOT_HPP_
#define CHRONOS_QUERY_SNAPSHOT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::query {

// One already-visible immutable row. The provider resolves corrections/tombstones before exposing
// a snapshot. generated_logical_identity is required only when the schema has no deduplication key.
struct ScalarInputRow {
  std::vector<ScalarValue> columns;
  std::vector<std::byte> generated_logical_identity;
  common::Uuid wal_id;
  std::uint64_t record_sequence{};
  std::uint64_t system_commit_position{};
  std::uint32_t row_ordinal{};
};

// Owns a stable, exact-schema view for the full lifetime of one scalar execution. Construction
// validates all column types/nullability and hidden identity metadata before the executor can read
// any row.
class ScalarTableSnapshot {
public:
  ScalarTableSnapshot() = delete;

  [[nodiscard]] static common::Result<ScalarTableSnapshot>
  create(std::shared_ptr<const schema::TableSchema> schema, std::uint64_t committed_position,
         std::vector<ScalarInputRow> rows);

  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept;
  [[nodiscard]] std::uint64_t committed_position() const noexcept;
  [[nodiscard]] std::span<const ScalarInputRow> rows() const noexcept;

private:
  ScalarTableSnapshot(std::shared_ptr<const schema::TableSchema> schema,
                      std::uint64_t committed_position, std::vector<ScalarInputRow> rows) noexcept;

  std::shared_ptr<const schema::TableSchema> schema_;
  std::uint64_t committed_position_{};
  std::vector<ScalarInputRow> rows_;
};

// Implementations return the exact bound schema and the greatest committed snapshot whose system
// timestamp is not later than as_of_system_time_ns. std::nullopt requests the current snapshot.
// The executor retains every returned shared_ptr until execution finishes.
class ScalarSnapshotProvider {
public:
  ScalarSnapshotProvider() = default;
  ScalarSnapshotProvider(const ScalarSnapshotProvider&) = delete;
  ScalarSnapshotProvider& operator=(const ScalarSnapshotProvider&) = delete;
  ScalarSnapshotProvider(ScalarSnapshotProvider&&) = delete;
  ScalarSnapshotProvider& operator=(ScalarSnapshotProvider&&) = delete;
  virtual ~ScalarSnapshotProvider() = default;

  [[nodiscard]] virtual common::Result<std::shared_ptr<const ScalarTableSnapshot>>
  resolve(const std::shared_ptr<const schema::TableSchema>& bound_schema,
          std::optional<std::int64_t> as_of_system_time_ns) const = 0;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_SNAPSHOT_HPP_
