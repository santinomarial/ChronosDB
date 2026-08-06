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
#include <vector>

namespace chronos::ingest {

struct ColumnarRecoverySuccessorSchemaConfig {
  std::shared_ptr<const schema::TableSchema> schema;
  head::MutableHeadCapacity head_capacity;
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
};

struct ColumnarAppendRecoveryConfig {
  RetryDirectoryConfig retry_directory;
  std::vector<ColumnarRecoveryTabletConfig> tablets;
  ColumnarAppendDecodeLimits decode_limits;
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

  [[nodiscard]] common::Result<wal::WalWriter> release_writer();

private:
  class Impl;
  explicit RecoveredColumnarAppendState(std::unique_ptr<Impl> implementation) noexcept;

  std::unique_ptr<Impl> implementation_;

  friend common::Result<RecoveredColumnarAppendState>
  recover_columnar_append_wal(const wal::WalWriterConfig&, const wal::WalRecoveryOptions&,
                              ColumnarAppendRecoveryConfig);
};

// Opens an existing WAL and reconstructs fresh in-memory COLUMNAR_APPEND state. The implementation
// first verifies and preflights the complete history, then replays it serially. Any failure
// discards every partial tablet/retry publication and returns no state or writer.
[[nodiscard]] common::Result<RecoveredColumnarAppendState>
recover_columnar_append_wal(const wal::WalWriterConfig& writer_config,
                            const wal::WalRecoveryOptions& recovery_options,
                            ColumnarAppendRecoveryConfig recovery_config);

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_COLUMNAR_APPEND_RECOVERY_HPP_
