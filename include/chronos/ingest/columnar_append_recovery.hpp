#ifndef CHRONOS_INGEST_COLUMNAR_APPEND_RECOVERY_HPP_
#define CHRONOS_INGEST_COLUMNAR_APPEND_RECOVERY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "chronos/wal/wal_writer_config.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::ingest {

struct ColumnarRecoverySuccessorSchemaConfig {
  std::shared_ptr<const schema::TableSchema> schema;
  head::MutableHeadCapacity head_capacity;
};

// One exact retry outcome already protected by the selected durable prefix. Recovery installs one
// shared immutable outcome into the global directory and its owning tablet before suffix replay.
struct ColumnarRecoveryRetrySeed {
  RetryIdentity identity;
  ColumnarAppendRetryOutcome outcome;
};

// One tablet boundary represented by durable parts and retry descriptors. The recovery schema must
// identify one schema in the configured retained lineage. Commands at or below this boundary may
// replay only as exact matching retry no-ops; later first-time commands populate mutable heads.
struct ColumnarRecoveryTabletSeed {
  schema::SchemaId recovery_schema_id;
  schema::SchemaVersion recovery_schema_version;
  std::uint64_t durable_record_sequence{};
  std::vector<ColumnarRecoveryRetrySeed> retries;
};

// One retained linear schema lineage and its fresh in-memory tablet-state limits. schema is the
// earliest version that retained WAL may append; successors are direct v1 transitions in ascending
// order and carry the capacity for a new generation of that shape. A recovery configuration
// contains exactly one entry per tablet referenced by the WAL history being opened. Durable catalog
// reconstruction remains outside this boundary, so the caller must supply the exact lineage.
struct ColumnarRecoveryTabletConfig {
  std::shared_ptr<const schema::TableSchema> schema;
  schema::TabletId tablet_id;
  TabletStateConfig state;
  std::vector<ColumnarRecoverySuccessorSchemaConfig> successors;
  std::optional<ColumnarRecoveryTabletSeed> durable_seed;
};

struct ColumnarAppendRecoveryConfig {
  RetryDirectoryConfig retry_directory;
  std::vector<ColumnarRecoveryTabletConfig> tablets;
  ColumnarAppendDecodeLimits decode_limits;
  // When present, physical recovery verifies and replays only the suffix after this externally
  // durable global boundary. Tablet durable boundaries may be later and are verified as matching
  // no-ops while the suffix catches up.
  std::optional<wal::WalReplayCheckpoint> checkpoint;
};

// Owns one completely recovered, unpublished-before-success in-memory state and the exclusively
// locked WAL writer positioned after its verified history. Tablet pointers and references remain
// valid until this owner is destroyed. The writer may be released once for live coordination;
// releasing it does not invalidate the recovered retry or tablet state.
class RecoveredColumnarAppendState {
public:
  RecoveredColumnarAppendState() = delete;
  ~RecoveredColumnarAppendState();

  RecoveredColumnarAppendState(const RecoveredColumnarAppendState&) = delete;
  RecoveredColumnarAppendState& operator=(const RecoveredColumnarAppendState&) = delete;
  RecoveredColumnarAppendState(RecoveredColumnarAppendState&&) noexcept;
  RecoveredColumnarAppendState& operator=(RecoveredColumnarAppendState&&) noexcept;

  [[nodiscard]] RetryDirectory& retry_directory() noexcept;
  [[nodiscard]] const RetryDirectory& retry_directory() const noexcept;
  [[nodiscard]] TabletState* tablet(const schema::TabletId& tablet_id) noexcept;
  [[nodiscard]] const TabletState* tablet(const schema::TabletId& tablet_id) const noexcept;
  [[nodiscard]] std::size_t tablet_count() const noexcept;

  // Revalidates and removes only closed segments covered by an externally durable checkpoint.
  // The recovered writer must still be owned by this state. This does not change tablet/retry
  // publication and is intended for a higher-level durable Manifest recovery owner.
  [[nodiscard]] common::Result<wal::WalSegmentReclamationReport>
  reclaim_checkpointed_segments(const wal::WalReplayCheckpoint& checkpoint);
  [[nodiscard]] common::Result<wal::WalWriter> release_writer();

private:
  class Impl;
  explicit RecoveredColumnarAppendState(std::unique_ptr<Impl> implementation) noexcept;

  std::unique_ptr<Impl> implementation_;

  friend common::Result<RecoveredColumnarAppendState>
  recover_columnar_append_wal(const wal::WalWriterConfig&, const wal::WalRecoveryOptions&,
                              ColumnarAppendRecoveryConfig);
};

// Opens an existing WAL and reconstructs fresh in-memory COLUMNAR_APPEND state. With no checkpoint
// it verifies and replays the complete history. With a checkpoint it first restores the configured
// durable tablet/retry prefix, then verifies and replays only the required suffix. Any failure
// discards every partial tablet/retry publication and returns no state or writer.
[[nodiscard]] common::Result<RecoveredColumnarAppendState>
recover_columnar_append_wal(const wal::WalWriterConfig& writer_config,
                            const wal::WalRecoveryOptions& recovery_options,
                            ColumnarAppendRecoveryConfig recovery_config);

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_COLUMNAR_APPEND_RECOVERY_HPP_
