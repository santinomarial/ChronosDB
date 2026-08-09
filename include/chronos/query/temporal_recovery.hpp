#ifndef CHRONOS_QUERY_TEMPORAL_RECOVERY_HPP_
#define CHRONOS_QUERY_TEMPORAL_RECOVERY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/temporal_command.hpp"
#include "chronos/query/temporal_snapshot.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "chronos/wal/wal_writer_config.hpp"

#include <cstddef>
#include <memory>
#include <vector>

namespace chronos::query {

struct TemporalRecoveryTableConfig {
  std::shared_ptr<const schema::TableSchema> schema;
  TemporalStoreLimits store_limits;
};

struct TemporalRecoveryConfig {
  std::vector<TemporalRecoveryTableConfig> tables;
  TemporalCommandLimits decode_limits;
};

// Owns fresh temporal providers reconstructed after whole-WAL structural/schema preflight and the
// locked reopened WAL. Recovery supports exact retained schemas supplied by the catalog owner.
// release_writer() transfers the one writer to the live commit coordinator; it may be called once.
// Any preflight or replay failure destroys the complete fresh state, so no partial temporal history
// becomes query-visible.
class RecoveredTemporalState {
public:
  RecoveredTemporalState() = delete;
  ~RecoveredTemporalState();
  RecoveredTemporalState(const RecoveredTemporalState&) = delete;
  RecoveredTemporalState& operator=(const RecoveredTemporalState&) = delete;
  RecoveredTemporalState(RecoveredTemporalState&&) noexcept;
  RecoveredTemporalState& operator=(RecoveredTemporalState&&) noexcept;

  [[nodiscard]] TemporalSnapshotProvider* provider(schema::TableId table_id) noexcept;
  [[nodiscard]] const TemporalSnapshotProvider* provider(schema::TableId table_id) const noexcept;
  [[nodiscard]] std::size_t table_count() const noexcept;
  [[nodiscard]] common::Result<wal::WalWriter> release_writer();

private:
  class Impl;
  explicit RecoveredTemporalState(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;

  friend common::Result<RecoveredTemporalState> recover_temporal_wal(const wal::WalWriterConfig&,
                                                                     const wal::WalRecoveryOptions&,
                                                                     TemporalRecoveryConfig);
};

// Opens and verifies one existing WAL containing Temporal Mutation Command v1 records, preflights
// every command and retained-schema binding, then replays in physical record order into fresh
// providers. Cross-record mutation transitions are validated during replay into disposable state.
[[nodiscard]] common::Result<RecoveredTemporalState>
recover_temporal_wal(const wal::WalWriterConfig& writer_config,
                     const wal::WalRecoveryOptions& recovery_options,
                     TemporalRecoveryConfig recovery_config);

} // namespace chronos::query

#endif // CHRONOS_QUERY_TEMPORAL_RECOVERY_HPP_
