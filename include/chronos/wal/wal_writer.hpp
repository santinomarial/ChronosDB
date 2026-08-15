#ifndef CHRONOS_WAL_WAL_WRITER_HPP_
#define CHRONOS_WAL_WAL_WRITER_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/wal/types.hpp"
#include "chronos/wal/wal_append_result.hpp"
#include "chronos/wal/wal_log_id_generator.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_replay_sink.hpp"
#include "chronos/wal/wal_segment.hpp"
#include "chronos/wal/wal_writer_config.hpp"

#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::io::detail {
class PosixSyscalls;
}

namespace chronos::wal {

struct WalSegmentReclamationReport {
  WalReplayCheckpoint checkpoint;
  std::uint64_t removed_segment_count{};
  std::uint64_t removed_physical_bytes{};
  std::uint64_t directory_sync_count{};

  friend bool operator==(const WalSegmentReclamationReport&,
                         const WalSegmentReclamationReport&) = default;
};

struct WalSegmentReclamationMetrics {
  std::uint64_t attempts{};
  std::uint64_t failures{};
  std::uint64_t removed_segment_count{};
  std::uint64_t removed_physical_bytes{};
  std::uint64_t directory_sync_count{};

  friend bool operator==(const WalSegmentReclamationMetrics&,
                         const WalSegmentReclamationMetrics&) = default;
};

namespace detail {
class WalWriterTestAccess;
struct RecoveredWalState;
} // namespace detail

// WalWriter owns the process advisory lock, WAL directory descriptor, and active segment. It is not
// internally synchronized: one caller must serialize append, synchronize, observation, and close.
// Payload bytes need remain alive only for the duration of append_application_entry().
class WalWriter {
public:
  WalWriter() noexcept;
  ~WalWriter();

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;
  WalWriter(WalWriter&&) noexcept;
  WalWriter& operator=(WalWriter&&) noexcept;

  [[nodiscard]] static common::Result<WalWriter> create_new(const WalWriterConfig& config);
  [[nodiscard]] static common::Result<WalWriter> create_new(const WalWriterConfig& config,
                                                            WalLogIdGenerator& id_generator);
  [[nodiscard]] static common::Result<WalWriter> open_existing(const WalWriterConfig& config,
                                                               const WalRecoveryOptions& options,
                                                               WalReplaySink& replay_sink);
  [[nodiscard]] static common::Result<WalWriter>
  open_existing_from_checkpoint(const WalWriterConfig& config, const WalRecoveryOptions& options,
                                const WalReplayCheckpoint& checkpoint, WalReplaySink& replay_sink);

  // Appends one structurally valid physical APPLICATION_ENTRY. This layer does not assign or
  // interpret application format/kind values. Success is the ASYNC write-path boundary only.
  [[nodiscard]] common::Result<WalAppendResult>
  append_application_entry(common::ByteView application_payload);

  // Synchronizes the active file and returns the complete record boundary now covered by that
  // operation. A failure permanently poisons the writer.
  [[nodiscard]] common::Result<PhysicalWalPosition> synchronize();

  // Removes only fully checkpoint-covered closed segments after validating the complete current
  // namespace and required suffix. The caller must supply a checkpoint from an already durable
  // selected manifest. The active highest segment is never removed. Any namespace mutation
  // failure poisons the writer; successful removals are followed by one directory sync.
  [[nodiscard]] common::Result<WalSegmentReclamationReport>
  reclaim_checkpointed_segments(const WalReplayCheckpoint& checkpoint);

  // Resolves a durable logical record sequence to its verified physical record-end coordinate.
  // The complete current namespace is checked against the live writer before success. A null
  // checkpoint means the requested prefix is older than the first retained segment and therefore
  // has no remaining physical file to reclaim. This method is read-only and thread-affine with all
  // other writer operations.
  [[nodiscard]] common::Result<std::optional<WalReplayCheckpoint>>
  resolve_replay_checkpoint(std::uint64_t record_sequence);

  [[nodiscard]] bool is_open() const noexcept;
  [[nodiscard]] bool is_failed() const noexcept;
  [[nodiscard]] common::Status failure_status() const;
  [[nodiscard]] WalId wal_id() const noexcept;
  [[nodiscard]] WalSegment active_segment() const;
  [[nodiscard]] PhysicalWalPosition written_position() const noexcept;
  [[nodiscard]] PhysicalWalPosition durable_position() const noexcept;
  [[nodiscard]] std::uint64_t written_record_sequence() const noexcept;
  [[nodiscard]] std::uint64_t durable_record_sequence() const noexcept;
  [[nodiscard]] WalSegmentReclamationMetrics reclamation_metrics() const noexcept;
  [[nodiscard]] common::Result<std::uint64_t> next_record_sequence() const;

  // close() does not add an implicit synchronization boundary. It invalidates every owned handle
  // and returns the first material close error. Destruction closes best-effort.
  [[nodiscard]] common::Status close();

private:
  class Impl;

  explicit WalWriter(std::unique_ptr<Impl> implementation) noexcept;
  [[nodiscard]] static common::Result<WalWriter>
  create_new_with(const WalWriterConfig& config, WalLogIdGenerator& id_generator,
                  io::detail::PosixSyscalls& syscalls);
  [[nodiscard]] static common::Result<WalWriter>
  open_existing_with(const WalWriterConfig& config, const WalRecoveryOptions& options,
                     WalReplaySink& replay_sink, io::detail::PosixSyscalls& syscalls,
                     std::optional<WalReplayCheckpoint> checkpoint = std::nullopt);
  [[nodiscard]] static common::Result<WalWriter>
  from_recovered_state(const WalWriterConfig& config, detail::RecoveredWalState state);

  std::unique_ptr<Impl> implementation_;

  friend class detail::WalWriterTestAccess;
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_WRITER_HPP_
