#ifndef CHRONOS_QUERY_TEMPORAL_SNAPSHOT_HPP_
#define CHRONOS_QUERY_TEMPORAL_SNAPSHOT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/snapshot.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::query {

enum class TemporalMutationKind : std::uint8_t {
  kOriginal = 1,
  kCorrection = 2,
  kReplacement = 3,
  kTombstone = 4,
};

struct TemporalMutation {
  std::vector<std::byte> logical_identity;
  std::vector<ScalarValue> columns;
  std::int64_t event_time_ns{};
  std::int64_t receive_time_ns{};
  common::Uuid wal_id;
  std::uint64_t record_sequence{};
  std::uint32_t row_ordinal{};
  TemporalMutationKind kind{TemporalMutationKind::kOriginal};
};

struct TemporalStoreLimits {
  std::size_t maximum_logical_rows{1U << 20U};
  std::size_t maximum_versions{1U << 22U};
  std::size_t maximum_identity_bytes{1024U};
};

struct RetainedTemporalVersion {
  std::uint64_t system_commit_position{};
  std::int64_t system_commit_time_ns{};
  TemporalMutation mutation;
};

// Thread-safe committed temporal state for one exact table schema. A commit batch is validated in
// full and installed atomically at one monotonically increasing system commit position/time. The
// provider returns copied immutable snapshots, so compaction cannot invalidate active readers.
class TemporalSnapshotProvider final : public ScalarSnapshotProvider {
public:
  TemporalSnapshotProvider() = delete;
  ~TemporalSnapshotProvider() override;
  TemporalSnapshotProvider(const TemporalSnapshotProvider&) = delete;
  TemporalSnapshotProvider& operator=(const TemporalSnapshotProvider&) = delete;
  TemporalSnapshotProvider(TemporalSnapshotProvider&&) = delete;
  TemporalSnapshotProvider& operator=(TemporalSnapshotProvider&&) = delete;

  [[nodiscard]] static common::Result<std::unique_ptr<TemporalSnapshotProvider>>
  create(std::shared_ptr<const schema::TableSchema> schema, TemporalStoreLimits limits = {});

  [[nodiscard]] common::Status apply_committed(std::uint64_t system_commit_position,
                                               std::int64_t system_commit_time_ns,
                                               std::vector<TemporalMutation> mutations);

  // Atomically seeds one fresh provider from immutable retained history. Input is canonical by
  // increasing commit position and row ordinal; every row at one position has the same system
  // time and source lineage. The first retained row for an identity may be any mutation kind
  // because compaction can preserve a correction/tombstone predecessor after older history expires.
  // retained_system_time_ns is the caller-proven table-wide boundary, not an extrema inferred from
  // whichever physical row happens to be first.
  [[nodiscard]] common::Status
  restore_retained_history(std::int64_t retained_system_time_ns,
                           std::vector<RetainedTemporalVersion> versions);

  // Single-writer pre-WAL validation for the next commit. Source WAL fields are deliberately
  // ignored because admission has not assigned them yet. The caller must serialize this check,
  // WAL submission, and apply_committed() with every other writer to this provider.
  [[nodiscard]] common::Status
  validate_next_commit(std::int64_t system_commit_time_ns,
                       std::span<const TemporalMutation> mutations) const;

  // A post-WAL application uncertainty makes stale state unsafe to query or extend. Recovery into
  // a fresh provider is required after this idempotent transition.
  [[nodiscard]] common::Status fail_closed();
  [[nodiscard]] bool is_failed() const noexcept;
  [[nodiscard]] const schema::TableSchema& schema() const noexcept;

  [[nodiscard]] common::Result<std::shared_ptr<const ScalarTableSnapshot>>
  resolve(const std::shared_ptr<const schema::TableSchema>& bound_schema,
          std::optional<std::int64_t> as_of_system_time_ns) const override;

  // Discards only versions older than both boundaries while retaining the newest predecessor per
  // logical identity. Requests older than retained_system_time_ns subsequently fail precisely.
  [[nodiscard]] common::Status compact_history(std::uint64_t oldest_observable_commit_position,
                                               std::int64_t retained_system_time_ns);

  [[nodiscard]] std::uint64_t latest_commit_position() const noexcept;
  [[nodiscard]] std::size_t logical_row_count() const noexcept;
  [[nodiscard]] std::size_t version_count() const noexcept;

private:
  class Impl;
  explicit TemporalSnapshotProvider(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_TEMPORAL_SNAPSHOT_HPP_
