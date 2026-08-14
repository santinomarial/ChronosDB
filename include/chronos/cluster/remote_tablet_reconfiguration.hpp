#ifndef CHRONOS_CLUSTER_REMOTE_TABLET_RECONFIGURATION_HPP_
#define CHRONOS_CLUSTER_REMOTE_TABLET_RECONFIGURATION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/raft/durable_tablet_reconfiguration.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kRemoteTabletReconfigurationHeaderSize = 80U;
inline constexpr std::size_t kRemoteTabletReconfigurationTrailerSize = 4U;
inline constexpr std::size_t kMaximumRemoteTabletReconfigurationRequestSize =
    kRemoteTabletReconfigurationHeaderSize + raft::kMaximumTabletReconfigurationActionSize +
    kRemoteTabletReconfigurationTrailerSize;

struct RemoteTabletReconfigurationCodecLimits {
  std::size_t maximum_request_bytes{kMaximumRemoteTabletReconfigurationRequestSize};
  raft::TabletReconfigurationActionCodecLimits action;
};

struct RemoteTabletReconfigurationRequest {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::Term required_leader_term{};
  raft::TabletReconfigurationAction action;
};

inline constexpr std::size_t kRemoteTabletReconfigurationResponseHeaderSize = 112U;
inline constexpr std::size_t kRemoteTabletReconfigurationResponseTrailerSize = 4U;
inline constexpr std::size_t kRemoteTabletReconfigurationResponseSize =
    kRemoteTabletReconfigurationResponseHeaderSize +
    kRemoteTabletReconfigurationResponseTrailerSize;

struct RemoteTabletReconfigurationLeaderHint {
  raft::NodeId node_id{};
  raft::Term term{};

  friend bool operator==(const RemoteTabletReconfigurationLeaderHint&,
                         const RemoteTabletReconfigurationLeaderHint&) = default;
};

struct RemoteTabletReconfigurationResponse {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::Term required_leader_term{};
  raft::TabletReconfigurationActionId action_id;
  common::StatusCode status_code{common::StatusCode::kInternal};
  bool already_prepared{};
  std::optional<RemoteTabletReconfigurationLeaderHint> leader_hint;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_remote_tablet_reconfiguration_request_v1(const RemoteTabletReconfigurationRequest& request,
                                                RemoteTabletReconfigurationCodecLimits limits = {});
[[nodiscard]] common::Result<RemoteTabletReconfigurationRequest>
decode_remote_tablet_reconfiguration_request_v1(common::ByteView bytes,
                                                RemoteTabletReconfigurationCodecLimits limits = {});
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_remote_tablet_reconfiguration_response_v1(
    const RemoteTabletReconfigurationResponse& response);
[[nodiscard]] common::Result<RemoteTabletReconfigurationResponse>
decode_remote_tablet_reconfiguration_response_v1(common::ByteView bytes);

// Embedding-owned authorization policy connecting the network authenticator's stable principal to
// an exact cluster node identity. It must outlive the receiver and provide its own synchronization.
class ClusterNodePrincipalAuthorizer {
public:
  virtual ~ClusterNodePrincipalAuthorizer() = default;
  [[nodiscard]] virtual common::Result<bool> authorize_node(std::uint64_t principal_id,
                                                            raft::NodeId claimed_node_id) const = 0;
};

struct RemoteTabletReconfigurationReceiverConfig {
  raft::NodeId local_node_id{};
  schema::TabletId tablet_id;
  raft::GroupId tablet_group_id;
  raft::GroupId metadata_group_id;
  const ClusterNodePrincipalAuthorizer* authorizer{};
  raft::TabletReconfigurationActionLedger* action_ledger{};
  raft::AsyncDurableMultiRaftRuntime* runtime{};
  RemoteTabletReconfigurationCodecLimits codec_limits;
};

struct RemoteTabletReconfigurationAdmission {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::Term required_leader_term{};
  raft::TabletReconfigurationActionId action_id;
  bool already_prepared{};
  raft::AsyncDurableRaftCompletion completion;
  bool response_finished{};
};

// One tablet's authenticated receiver. All configured dependencies are borrowed and must outlive
// it. receive() authenticates source identity before ledger mutation, verifies the target/tablet/
// group binding, durably prepares the exact action, and nonblockingly admits it behind an atomic
// current-leader-term fence. A successful completion is still local durability, not quorum commit.
class RemoteTabletReconfigurationReceiver {
public:
  RemoteTabletReconfigurationReceiver() = delete;
  ~RemoteTabletReconfigurationReceiver() = default;
  RemoteTabletReconfigurationReceiver(const RemoteTabletReconfigurationReceiver&) = delete;
  RemoteTabletReconfigurationReceiver&
  operator=(const RemoteTabletReconfigurationReceiver&) = delete;
  RemoteTabletReconfigurationReceiver(RemoteTabletReconfigurationReceiver&&) noexcept = default;
  RemoteTabletReconfigurationReceiver&
  operator=(RemoteTabletReconfigurationReceiver&&) noexcept = default;

  [[nodiscard]] static common::Result<RemoteTabletReconfigurationReceiver>
  create(RemoteTabletReconfigurationReceiverConfig config);

  [[nodiscard]] common::Result<RemoteTabletReconfigurationAdmission>
  try_receive(common::ByteView request_bytes,
              const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit RemoteTabletReconfigurationReceiver(
      RemoteTabletReconfigurationReceiverConfig config) noexcept;
  RemoteTabletReconfigurationReceiverConfig config_;
};

// Nonblocking carrier adapter. Before the durable completion is ready it returns an empty optional
// and leaves the admission reusable. Once ready it consumes the completion exactly once and emits
// canonical response bytes with the exact request route/action identity. The optional leader hint
// must come from a separately ordered authoritative observation; it is never fabricated here.
[[nodiscard]] common::Result<std::optional<std::vector<std::byte>>>
try_finish_remote_tablet_reconfiguration_admission(
    RemoteTabletReconfigurationAdmission& admission,
    std::optional<RemoteTabletReconfigurationLeaderHint> leader_hint = std::nullopt);

struct RemoteTabletReconfigurationRetryLimits {
  std::size_t maximum_attempts{5U};
  std::chrono::milliseconds initial_backoff{50};
  std::chrono::milliseconds maximum_backoff{5000};
};

enum class RemoteTabletReconfigurationSenderState : std::uint8_t {
  kReady = 1,
  kWaitingForResponse = 2,
  kBackoff = 3,
  kLocallyAccepted = 4,
  kFailed = 5,
};

struct RemoteTabletReconfigurationAttempt {
  std::size_t attempt_number{};
  raft::NodeId target_node_id{};
  raft::Term required_leader_term{};
  std::vector<std::byte> request_bytes;
};

// Deterministic sender-side retry owner for one already ledger-prepared action. It does no I/O and
// owns no clock: the carrier supplies a fresh leader route and monotonic time for each attempt,
// sends the returned bytes, and reports either the exact response bytes or a transport failure.
// Success means receiver-local durable admission only; authoritative reconciliation still proves
// commit/application. The sealed dispatch stays owned for the sender's full lifetime.
class RemoteTabletReconfigurationSender {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  RemoteTabletReconfigurationSender() = delete;
  ~RemoteTabletReconfigurationSender() = default;
  RemoteTabletReconfigurationSender(const RemoteTabletReconfigurationSender&) = delete;
  RemoteTabletReconfigurationSender& operator=(const RemoteTabletReconfigurationSender&) = delete;
  RemoteTabletReconfigurationSender(RemoteTabletReconfigurationSender&&) noexcept = default;
  RemoteTabletReconfigurationSender&
  operator=(RemoteTabletReconfigurationSender&&) noexcept = default;

  [[nodiscard]] static common::Result<RemoteTabletReconfigurationSender>
  create(raft::NodeId source_node_id, raft::PreparedTabletReconfigurationDispatch dispatch,
         RemoteTabletReconfigurationRetryLimits limits = {});

  [[nodiscard]] common::Result<RemoteTabletReconfigurationAttempt>
  begin_attempt(RemoteTabletReconfigurationLeaderHint route, TimePoint now);
  [[nodiscard]] common::Status accept_response(common::ByteView response_bytes, TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(common::StatusCode code, TimePoint now);

  [[nodiscard]] RemoteTabletReconfigurationSenderState state() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_attempt_not_before() const noexcept;
  [[nodiscard]] std::optional<common::StatusCode> last_status_code() const noexcept;
  [[nodiscard]] std::optional<RemoteTabletReconfigurationLeaderHint>
  suggested_leader() const noexcept;
  [[nodiscard]] const raft::TabletReconfigurationActionId& action_id() const noexcept;

private:
  RemoteTabletReconfigurationSender(raft::NodeId source_node_id,
                                    raft::PreparedTabletReconfigurationDispatch dispatch,
                                    RemoteTabletReconfigurationRetryLimits limits) noexcept;
  [[nodiscard]] common::Status schedule(common::StatusCode code, TimePoint now);

  raft::NodeId source_node_id_{};
  raft::PreparedTabletReconfigurationDispatch dispatch_;
  RemoteTabletReconfigurationRetryLimits limits_;
  RemoteTabletReconfigurationSenderState state_{RemoteTabletReconfigurationSenderState::kReady};
  std::size_t attempts_started_{};
  std::chrono::milliseconds next_backoff_{};
  std::optional<TimePoint> next_attempt_not_before_;
  std::optional<common::StatusCode> last_status_code_;
  std::optional<RemoteTabletReconfigurationLeaderHint> active_route_;
  std::optional<RemoteTabletReconfigurationLeaderHint> suggested_leader_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_REMOTE_TABLET_RECONFIGURATION_HPP_
