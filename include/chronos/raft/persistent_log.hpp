#ifndef CHRONOS_RAFT_PERSISTENT_LOG_HPP_
#define CHRONOS_RAFT_PERSISTENT_LOG_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/multi_raft.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace chronos::io::detail {
class PosixSyscalls;
}

namespace chronos::raft {

namespace detail {
class RaftPersistentLogTestAccess;
}

inline constexpr std::size_t kRaftSegmentHeaderSize = 64U;
inline constexpr std::uint64_t kDefaultRaftSegmentTargetSize = std::uint64_t{64U} * 1024U * 1024U;
inline constexpr std::uint64_t kMaximumRaftSegmentSize = 1024ULL * 1024ULL * 1024ULL;

struct RaftPersistentLogConfig {
  // Existing dedicated directory. Its parent-directory durability is the caller's responsibility.
  std::string directory_path;
  std::uint16_t file_permissions{0600U};
  std::uint64_t target_segment_size{kDefaultRaftSegmentTargetSize};
  std::size_t maximum_segments{4096U};
  std::size_t maximum_records{1U << 20U};
  std::size_t maximum_groups{4096U};
};

struct RaftPersistentLogOpenOptions {
  // Repairs only a structurally incomplete suffix after the last fully verified record in the
  // highest segment. Checksum-invalid complete headers/records always fail as corruption.
  bool repair_incomplete_final_tail{};
};

struct RaftPhysicalPosition {
  std::uint64_t segment_number{};
  std::uint64_t end_offset{};
  std::uint64_t physical_sequence{};

  friend bool operator==(const RaftPhysicalPosition&, const RaftPhysicalPosition&) = default;
};

struct RaftPersistentLogRecovery {
  std::vector<GroupPersistentState> latest_group_states;
  RaftPhysicalPosition written_position;
  std::uint64_t durable_physical_sequence{};
  std::uint64_t segment_count{};
  std::uint64_t record_count{};
  std::uint64_t repaired_bytes{};
  std::uint64_t base_segment_number{1U};
};

struct RaftPersistentLogReclamation {
  std::uint64_t base_segment_number{};
  std::uint64_t checkpoint_first_physical_sequence{};
  std::uint64_t checkpoint_last_physical_sequence{};
  std::uint64_t reclaimed_segments{};
  std::uint64_t reclaimed_records{};
};

// Single-thread-affine owner of one node-level multiplexed Raft log. append() establishes only the
// complete write boundary. synchronize() establishes the LOCAL durable boundary used by a future
// majority durability coordinator. Any append/sync/rotation error permanently fails the owner.
class RaftPersistentLog {
public:
  RaftPersistentLog() noexcept;
  ~RaftPersistentLog();
  RaftPersistentLog(const RaftPersistentLog&) = delete;
  RaftPersistentLog& operator=(const RaftPersistentLog&) = delete;
  RaftPersistentLog(RaftPersistentLog&&) noexcept;
  RaftPersistentLog& operator=(RaftPersistentLog&&) noexcept;

  [[nodiscard]] static common::Result<RaftPersistentLog>
  create_new(const RaftPersistentLogConfig& config);
  [[nodiscard]] static common::Result<RaftPersistentLog>
  open_existing(const RaftPersistentLogConfig& config,
                const RaftPersistentLogOpenOptions& options = {});

  [[nodiscard]] common::Result<RaftPhysicalPosition> append(const GroupPersistentState& persistent);
  [[nodiscard]] common::Result<RaftPhysicalPosition> synchronize();
  // Installs a complete node-wide full-state checkpoint in a fresh segment before publishing a
  // recovery anchor and reclaiming older whole segments. The checkpoint must contain every known
  // group exactly once with consecutive next physical sequences.
  [[nodiscard]] common::Result<RaftPersistentLogReclamation>
  checkpoint_and_reclaim(const std::vector<GroupPersistentState>& checkpoint);

  [[nodiscard]] const RaftPersistentLogRecovery& recovery() const noexcept;
  [[nodiscard]] RaftPhysicalPosition written_position() const noexcept;
  [[nodiscard]] std::uint64_t durable_physical_sequence() const noexcept;
  [[nodiscard]] bool is_open() const noexcept;
  [[nodiscard]] bool is_failed() const noexcept;
  [[nodiscard]] common::Status failure_status() const;

  // Does not add an implicit synchronization boundary.
  [[nodiscard]] common::Status close();

private:
  class Impl;
  explicit RaftPersistentLog(std::unique_ptr<Impl> impl) noexcept;
  [[nodiscard]] static common::Result<RaftPersistentLog>
  create_new_with(const RaftPersistentLogConfig& config, io::detail::PosixSyscalls& syscalls);
  std::unique_ptr<Impl> impl_;

  friend class detail::RaftPersistentLogTestAccess;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_PERSISTENT_LOG_HPP_
