#ifndef CHRONOS_RAFT_TYPES_HPP_
#define CHRONOS_RAFT_TYPES_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace chronos::raft {

using NodeId = std::uint64_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;

enum class Role : std::uint8_t { kFollower = 1, kCandidate = 2, kLeader = 3 };

struct LogEntry {
  LogIndex index{};
  Term term{};
  std::uint8_t type{1U};
  std::vector<std::byte> payload;

  friend bool operator==(const LogEntry&, const LogEntry&) = default;
};

struct SnapshotMetadata {
  LogIndex last_included_index{};
  Term last_included_term{};
  std::uint64_t manifest_generation{};
  std::array<std::byte, 32U> part_set_checksum{};
  LogIndex configuration_index{};
  std::vector<NodeId> voters;

  friend bool operator==(const SnapshotMetadata&, const SnapshotMetadata&) = default;
};

struct PersistentState {
  Term current_term{};
  std::optional<NodeId> voted_for;
  std::vector<LogEntry> log;
  LogIndex commit_index{};
  LogIndex applied_index{};
  SnapshotMetadata snapshot;

  friend bool operator==(const PersistentState&, const PersistentState&) = default;
};

struct RequestVoteRequest {
  Term term{};
  NodeId candidate_id{};
  LogIndex last_log_index{};
  Term last_log_term{};
};

struct RequestVoteResponse {
  Term term{};
  bool granted{};
};

struct AppendEntriesRequest {
  Term term{};
  NodeId leader_id{};
  LogIndex previous_log_index{};
  Term previous_log_term{};
  std::vector<LogEntry> entries;
  LogIndex leader_commit{};
};

struct AppendEntriesResponse {
  Term term{};
  bool success{};
  LogIndex match_index{};
  std::optional<Term> conflict_term;
  LogIndex conflict_index{};
};

struct InstallSnapshotRequest {
  Term term{};
  NodeId leader_id{};
  SnapshotMetadata snapshot;
};

struct InstallSnapshotResponse {
  Term term{};
  bool success{};
  LogIndex last_included_index{};
};

// A read barrier is an in-memory leadership proof, not durable state. The opaque context is scoped
// to one leader term and correlates a bounded round of current-term voter acknowledgements.
struct ReadBarrierRequest {
  Term term{};
  NodeId leader_id{};
  std::uint64_t context{};

  friend bool operator==(const ReadBarrierRequest&, const ReadBarrierRequest&) = default;
};

struct ReadBarrierResponse {
  Term term{};
  std::uint64_t context{};
  bool accepted{};

  friend bool operator==(const ReadBarrierResponse&, const ReadBarrierResponse&) = default;
};

using Message = std::variant<RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest,
                             AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
                             ReadBarrierRequest, ReadBarrierResponse>;

struct OutboundMessage {
  NodeId destination{};
  Message message;
};

struct PendingSnapshotInstall {
  NodeId source{};
  SnapshotMetadata snapshot;
};

struct ReadBarrier {
  Term term{};
  std::uint64_t context{};
  LogIndex read_index{};

  friend bool operator==(const ReadBarrier&, const ReadBarrier&) = default;
};

// When persistent_state is present, the runtime must durably install it before sending any
// outbound message in the same transition. Committed entries are still invisible until the
// application state machine advances applied_index through mark_applied(). A snapshot_install is a
// request to external application/storage code, not evidence that those bytes are installed.
struct Transition {
  std::optional<PersistentState> persistent_state;
  std::vector<OutboundMessage> outbound;
  std::optional<LogIndex> advanced_commit_index;
  std::optional<PendingSnapshotInstall> snapshot_install;
  // A completed barrier authorizes a linearizable read only after applied_index reaches
  // read_index. It does not make committed-but-unapplied state visible.
  std::optional<ReadBarrier> read_barrier_ready;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TYPES_HPP_
