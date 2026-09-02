#include "chronos/cluster/remote_tablet_reconfiguration.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

class TemporaryDirectory {
public:
  explicit TemporaryDirectory(const char* name) {
    std::string pattern =
        (std::filesystem::temp_directory_path() / (std::string{name} + "-XXXXXX")).string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

template <typename Identifier> [[nodiscard]] Identifier id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] raft::GroupId group(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return raft::GroupId{bytes};
}

[[nodiscard]] schema::TabletId tablet_id(const std::byte seed = std::byte{1U}) {
  return id<schema::TabletId>(seed);
}

[[nodiscard]] raft::TabletReconfigurationAction
action(const schema::TabletId tablet = tablet_id(),
       const raft::GroupId tablet_group = group(std::byte{2U})) {
  return {{tablet, 7U, raft::TabletReconfigurationActionKind::kBeginJointMembership},
          raft::TabletReconfigurationActionKind::kBeginJointMembership,
          {tablet_group, raft::BeginMembershipChangeOperation{{2U, 3U}}}};
}

class FixedAuthorizer final : public ClusterNodePrincipalAuthorizer {
public:
  [[nodiscard]] common::Result<bool>
  authorize_node(const std::uint64_t principal_id,
                 const raft::NodeId claimed_node_id) const override {
    return principal_id == 700U && claimed_node_id == 1U;
  }
};

[[nodiscard]] common::Result<std::vector<std::byte>> finish_admission(
    RemoteTabletReconfigurationAdmission& admission,
    const std::optional<RemoteTabletReconfigurationLeaderHint> leader_hint = std::nullopt) {
  for (std::size_t attempt = 0U; attempt < 100'000U; ++attempt) {
    auto response = try_finish_remote_tablet_reconfiguration_admission(admission, leader_hint);
    if (!response.has_value())
      return common::make_unexpected(std::move(response).error());
    std::optional<std::vector<std::byte>>& ready_response = response.value();
    if (ready_response.has_value())
      return std::move(ready_response.value());
    std::this_thread::yield();
  }
  return common::make_unexpected(
      common::Status{common::StatusCode::kUnavailable, "test completion did not become ready"});
}

TEST(RemoteTabletReconfigurationCodecTest, RoundTripsCanonicalRequestAndRejectsDamage) {
  const RemoteTabletReconfigurationRequest request{1U, 2U, 9U, action()};
  auto encoded = encode_remote_tablet_reconfiguration_request_v1(request);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();

  auto decoded = decode_remote_tablet_reconfiguration_request_v1(*encoded);

  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_node_id, 1U);
  EXPECT_EQ(decoded->target_node_id, 2U);
  EXPECT_EQ(decoded->required_leader_term, 9U);
  EXPECT_EQ(decoded->action.id, request.action.id);
  EXPECT_EQ(decoded->action.kind, request.action.kind);
  EXPECT_EQ(decoded->action.request.group_id, request.action.request.group_id);
  ASSERT_TRUE(std::holds_alternative<raft::BeginMembershipChangeOperation>(
      decoded->action.request.operation));
  EXPECT_EQ(
      std::get<raft::BeginMembershipChangeOperation>(decoded->action.request.operation).new_voters,
      (std::vector<raft::NodeId>{2U, 3U}));
  auto reencoded = encode_remote_tablet_reconfiguration_request_v1(*decoded);
  ASSERT_TRUE(reencoded.has_value());
  EXPECT_EQ(*reencoded, *encoded);

  encoded->back() ^= std::byte{1U};
  auto damaged = decode_remote_tablet_reconfiguration_request_v1(*encoded);
  ASSERT_FALSE(damaged.has_value());
  EXPECT_EQ(damaged.error().code(), common::StatusCode::kCorruption);
  RemoteTabletReconfigurationRequest local_route{2U, 2U, 1U, action()};
  EXPECT_EQ(encode_remote_tablet_reconfiguration_request_v1(local_route).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(RemoteTabletReconfigurationCodecTest, RoundTripsCorrelatedResponseAndRejectsDamage) {
  const RemoteTabletReconfigurationResponse response{
      2U,
      1U,
      9U,
      action().id,
      common::StatusCode::kUnavailable,
      true,
      RemoteTabletReconfigurationLeaderHint{3U, 10U}};
  auto encoded = encode_remote_tablet_reconfiguration_response_v1(response);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), kRemoteTabletReconfigurationResponseSize);

  auto decoded = decode_remote_tablet_reconfiguration_response_v1(*encoded);

  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_node_id, response.source_node_id);
  EXPECT_EQ(decoded->target_node_id, response.target_node_id);
  EXPECT_EQ(decoded->required_leader_term, response.required_leader_term);
  EXPECT_EQ(decoded->action_id, response.action_id);
  EXPECT_EQ(decoded->status_code, response.status_code);
  EXPECT_TRUE(decoded->already_prepared);
  EXPECT_EQ(decoded->leader_hint, response.leader_hint);
  auto reencoded = encode_remote_tablet_reconfiguration_response_v1(*decoded);
  ASSERT_TRUE(reencoded.has_value());
  EXPECT_EQ(*reencoded, *encoded);

  (*encoded)[100U] ^= std::byte{1U};
  auto damaged = decode_remote_tablet_reconfiguration_response_v1(*encoded);
  ASSERT_FALSE(damaged.has_value());
  EXPECT_EQ(damaged.error().code(), common::StatusCode::kCorruption);
}

TEST(RemoteTabletReconfigurationReceiverTest,
     AuthenticatesPreparesAdmitsAndRejectsStaleTermWithoutRaftMutation) {
  TemporaryDirectory actions{"chronos-remote-reconfiguration-actions"};
  TemporaryDirectory raft_log{"chronos-remote-reconfiguration-raft"};
  const raft::GroupId tablet_group = group(std::byte{2U});
  const raft::GroupId metadata_group = group(std::byte{3U});
  auto ledger = raft::TabletReconfigurationActionLedger::create(
      {.directory_path = actions.path().string(), .tablet_id = tablet_id()});
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = raft_log.path().string()}, {{tablet_group, {2U}}});
  ASSERT_TRUE(ledger.has_value()) << ledger.error().to_string();
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{tablet_group, raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  FixedAuthorizer authorizer;
  auto receiver = RemoteTabletReconfigurationReceiver::create({.local_node_id = 2U,
                                                               .tablet_id = tablet_id(),
                                                               .tablet_group_id = tablet_group,
                                                               .metadata_group_id = metadata_group,
                                                               .authorizer = &authorizer,
                                                               .action_ledger = &*ledger,
                                                               .runtime = &*runtime,
                                                               .codec_limits = {}});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  const RemoteTabletReconfigurationRequest request{1U, 2U, 1U, action(tablet_id(), tablet_group)};
  auto encoded = encode_remote_tablet_reconfiguration_request_v1(request);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();

  auto anonymous = receiver->try_receive(
      *encoded, network::PeerAuthenticationResult{.authorized = true, .principal_id = 0U});
  auto impostor = receiver->try_receive(
      *encoded, network::PeerAuthenticationResult{.authorized = true, .principal_id = 701U});

  ASSERT_FALSE(anonymous.has_value());
  ASSERT_FALSE(impostor.has_value());
  EXPECT_EQ(anonymous.error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(impostor.error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(ledger->load(request.action.id).error().code(), common::StatusCode::kNotFound);

  auto admitted = receiver->try_receive(
      *encoded, network::PeerAuthenticationResult{.authorized = true, .principal_id = 700U});

  ASSERT_TRUE(admitted.has_value()) << admitted.error().to_string();
  EXPECT_EQ(admitted->action_id, request.action.id);
  EXPECT_FALSE(admitted->already_prepared);
  auto admitted_response_bytes = finish_admission(*admitted);
  ASSERT_TRUE(admitted_response_bytes.has_value()) << admitted_response_bytes.error().to_string();
  auto admitted_response =
      decode_remote_tablet_reconfiguration_response_v1(*admitted_response_bytes);
  ASSERT_TRUE(admitted_response.has_value()) << admitted_response.error().to_string();
  EXPECT_EQ(admitted_response->source_node_id, 2U);
  EXPECT_EQ(admitted_response->target_node_id, 1U);
  EXPECT_EQ(admitted_response->required_leader_term, 1U);
  EXPECT_EQ(admitted_response->action_id, request.action.id);
  EXPECT_EQ(admitted_response->status_code, common::StatusCode::kOk);
  EXPECT_FALSE(admitted_response->already_prepared);
  EXPECT_EQ(try_finish_remote_tablet_reconfiguration_admission(*admitted).error().code(),
            common::StatusCode::kInvalidArgument);
  ASSERT_TRUE(ledger->load(request.action.id).has_value());

  auto observation = runtime->try_observe_group(tablet_group);
  ASSERT_TRUE(observation.has_value());
  auto observed = observation->wait();
  ASSERT_TRUE(observed.has_value());
  const auto& group_observation = observed->front().observation;
  if (!group_observation.has_value())
    FAIL() << "Raft observation did not contain group state";
  EXPECT_EQ(group_observation.value().last_log_index, 1U);

  auto duplicate = receiver->try_receive(
      *encoded, network::PeerAuthenticationResult{.authorized = true, .principal_id = 700U});
  ASSERT_TRUE(duplicate.has_value()) << duplicate.error().to_string();
  EXPECT_TRUE(duplicate->already_prepared);
  auto duplicate_response_bytes = finish_admission(*duplicate);
  ASSERT_TRUE(duplicate_response_bytes.has_value());
  auto duplicate_response =
      decode_remote_tablet_reconfiguration_response_v1(*duplicate_response_bytes);
  ASSERT_TRUE(duplicate_response.has_value());
  EXPECT_EQ(duplicate_response->status_code, common::StatusCode::kOk);
  EXPECT_TRUE(duplicate_response->already_prepared);

  RemoteTabletReconfigurationRequest stale_request{1U, 2U, 2U, action(tablet_id(), tablet_group)};
  auto stale_bytes = encode_remote_tablet_reconfiguration_request_v1(stale_request);
  ASSERT_TRUE(stale_bytes.has_value());
  auto stale = receiver->try_receive(
      *stale_bytes, network::PeerAuthenticationResult{.authorized = true, .principal_id = 700U});
  ASSERT_TRUE(stale.has_value()) << stale.error().to_string();
  const RemoteTabletReconfigurationLeaderHint leader_hint{2U, 1U};
  auto stale_response_bytes = finish_admission(*stale, leader_hint);
  ASSERT_TRUE(stale_response_bytes.has_value());
  auto stale_response = decode_remote_tablet_reconfiguration_response_v1(*stale_response_bytes);
  ASSERT_TRUE(stale_response.has_value());
  EXPECT_EQ(stale_response->status_code, common::StatusCode::kUnavailable);
  EXPECT_EQ(stale_response->leader_hint,
            std::optional<RemoteTabletReconfigurationLeaderHint>{leader_hint});

  auto final_observation = runtime->try_observe_group(tablet_group);
  ASSERT_TRUE(final_observation.has_value());
  auto final_observed = final_observation->wait();
  ASSERT_TRUE(final_observed.has_value());
  const auto& final_group_observation = final_observed->front().observation;
  if (!final_group_observation.has_value())
    FAIL() << "final Raft observation did not contain group state";
  EXPECT_EQ(final_group_observation.value().last_log_index, 1U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(RemoteTabletReconfigurationReceiverTest, RejectsWrongTargetTabletAndGroupBeforePreparation) {
  TemporaryDirectory actions{"chronos-remote-route-actions"};
  TemporaryDirectory raft_log{"chronos-remote-route-raft"};
  const raft::GroupId tablet_group = group(std::byte{2U});
  const raft::GroupId metadata_group = group(std::byte{3U});
  auto ledger = raft::TabletReconfigurationActionLedger::create(
      {.directory_path = actions.path().string(), .tablet_id = tablet_id()});
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = raft_log.path().string()}, {{tablet_group, {2U}}});
  ASSERT_TRUE(ledger.has_value());
  ASSERT_TRUE(runtime.has_value());
  FixedAuthorizer authorizer;
  auto receiver = RemoteTabletReconfigurationReceiver::create({.local_node_id = 2U,
                                                               .tablet_id = tablet_id(),
                                                               .tablet_group_id = tablet_group,
                                                               .metadata_group_id = metadata_group,
                                                               .authorizer = &authorizer,
                                                               .action_ledger = &*ledger,
                                                               .runtime = &*runtime,
                                                               .codec_limits = {}});
  ASSERT_TRUE(receiver.has_value());
  const network::PeerAuthenticationResult peer{.authorized = true, .principal_id = 700U};

  RemoteTabletReconfigurationRequest wrong_target{1U, 3U, 1U, action(tablet_id(), tablet_group)};
  RemoteTabletReconfigurationRequest wrong_tablet{1U, 2U, 1U,
                                                  action(tablet_id(std::byte{9U}), tablet_group)};
  RemoteTabletReconfigurationRequest wrong_group{1U, 2U, 1U, action(tablet_id(), metadata_group)};
  auto wrong_target_bytes = encode_remote_tablet_reconfiguration_request_v1(wrong_target);
  auto wrong_tablet_bytes = encode_remote_tablet_reconfiguration_request_v1(wrong_tablet);
  auto wrong_group_bytes = encode_remote_tablet_reconfiguration_request_v1(wrong_group);
  ASSERT_TRUE(wrong_target_bytes.has_value());
  ASSERT_TRUE(wrong_tablet_bytes.has_value());
  ASSERT_TRUE(wrong_group_bytes.has_value());

  EXPECT_EQ(receiver->try_receive(*wrong_target_bytes, peer).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(receiver->try_receive(*wrong_tablet_bytes, peer).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(receiver->try_receive(*wrong_group_bytes, peer).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(ledger->load(wrong_target.action.id).error().code(), common::StatusCode::kNotFound);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(RemoteTabletReconfigurationSenderTest,
     CorrelatesResponsesRequiresFreshRoutesAndBacksOffWithinAttemptBudget) {
  TemporaryDirectory actions{"chronos-remote-sender-actions"};
  auto ledger = raft::TabletReconfigurationActionLedger::create(
      {.directory_path = actions.path().string(), .tablet_id = tablet_id()});
  ASSERT_TRUE(ledger.has_value());
  auto dispatch = raft::prepare_received_tablet_reconfiguration_action(action(), *ledger);
  ASSERT_TRUE(dispatch.has_value()) << dispatch.error().to_string();
  auto sender =
      RemoteTabletReconfigurationSender::create(1U, std::move(*dispatch),
                                                {.maximum_attempts = 3U,
                                                 .initial_backoff = std::chrono::milliseconds{10},
                                                 .maximum_backoff = std::chrono::milliseconds{20}});
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto start = RemoteTabletReconfigurationSender::TimePoint{};

  auto first = sender->begin_attempt({2U, 3U}, start);

  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  EXPECT_EQ(first->attempt_number, 1U);
  auto first_request = decode_remote_tablet_reconfiguration_request_v1(first->request_bytes);
  ASSERT_TRUE(first_request.has_value());
  EXPECT_EQ(first_request->source_node_id, 1U);
  EXPECT_EQ(first_request->target_node_id, 2U);
  EXPECT_EQ(first_request->required_leader_term, 3U);
  EXPECT_EQ(first_request->action.id, sender->action_id());
  EXPECT_EQ(sender->begin_attempt({2U, 3U}, start).error().code(),
            common::StatusCode::kUnavailable);

  RemoteTabletReconfigurationResponse wrong{
      3U, 1U, 3U, sender->action_id(), common::StatusCode::kUnavailable, false, std::nullopt};
  auto wrong_bytes = encode_remote_tablet_reconfiguration_response_v1(wrong);
  ASSERT_TRUE(wrong_bytes.has_value());
  EXPECT_EQ(sender->accept_response(*wrong_bytes, start).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), RemoteTabletReconfigurationSenderState::kWaitingForResponse);

  RemoteTabletReconfigurationResponse retryable{2U,
                                                1U,
                                                3U,
                                                sender->action_id(),
                                                common::StatusCode::kUnavailable,
                                                true,
                                                RemoteTabletReconfigurationLeaderHint{3U, 4U}};
  auto retryable_bytes = encode_remote_tablet_reconfiguration_response_v1(retryable);
  ASSERT_TRUE(retryable_bytes.has_value());
  ASSERT_TRUE(sender->accept_response(*retryable_bytes, start).is_ok());
  EXPECT_EQ(sender->state(), RemoteTabletReconfigurationSenderState::kBackoff);
  EXPECT_EQ(sender->next_attempt_not_before(), start + std::chrono::milliseconds{10});
  EXPECT_EQ(sender->suggested_leader(), retryable.leader_hint);
  EXPECT_EQ(sender->begin_attempt({3U, 4U}, start + std::chrono::milliseconds{9}).error().code(),
            common::StatusCode::kUnavailable);

  auto second = sender->begin_attempt({3U, 4U}, start + std::chrono::milliseconds{10});
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_EQ(second->attempt_number, 2U);
  EXPECT_FALSE(sender->suggested_leader().has_value());
  ASSERT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kIoError,
                                             start + std::chrono::milliseconds{20})
                  .is_ok());
  EXPECT_EQ(sender->next_attempt_not_before(), start + std::chrono::milliseconds{40});

  auto third = sender->begin_attempt({4U, 5U}, start + std::chrono::milliseconds{40});
  ASSERT_TRUE(third.has_value()) << third.error().to_string();
  RemoteTabletReconfigurationResponse accepted{
      4U, 1U, 5U, sender->action_id(), common::StatusCode::kOk, true, std::nullopt};
  auto accepted_bytes = encode_remote_tablet_reconfiguration_response_v1(accepted);
  ASSERT_TRUE(accepted_bytes.has_value());
  ASSERT_TRUE(sender->accept_response(*accepted_bytes, start).is_ok());
  EXPECT_EQ(sender->state(), RemoteTabletReconfigurationSenderState::kLocallyAccepted);
  EXPECT_EQ(sender->attempts_started(), 3U);
  EXPECT_EQ(sender->last_status_code(), common::StatusCode::kOk);
  EXPECT_EQ(sender->begin_attempt({4U, 5U}, start).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(RemoteTabletReconfigurationSenderTest, StopsOnTerminalStatusOrExhaustedRetryBudget) {
  TemporaryDirectory first_actions{"chronos-remote-terminal-sender-actions"};
  TemporaryDirectory second_actions{"chronos-remote-exhausted-sender-actions"};
  auto first_ledger = raft::TabletReconfigurationActionLedger::create(
      {.directory_path = first_actions.path().string(), .tablet_id = tablet_id()});
  auto second_ledger = raft::TabletReconfigurationActionLedger::create(
      {.directory_path = second_actions.path().string(), .tablet_id = tablet_id()});
  ASSERT_TRUE(first_ledger.has_value());
  ASSERT_TRUE(second_ledger.has_value());
  auto first_dispatch =
      raft::prepare_received_tablet_reconfiguration_action(action(), *first_ledger);
  auto second_dispatch =
      raft::prepare_received_tablet_reconfiguration_action(action(), *second_ledger);
  ASSERT_TRUE(first_dispatch.has_value());
  ASSERT_TRUE(second_dispatch.has_value());
  auto terminal = RemoteTabletReconfigurationSender::create(1U, std::move(*first_dispatch));
  auto exhausted =
      RemoteTabletReconfigurationSender::create(1U, std::move(*second_dispatch),
                                                {.maximum_attempts = 1U,
                                                 .initial_backoff = std::chrono::milliseconds{1},
                                                 .maximum_backoff = std::chrono::milliseconds{1}});
  ASSERT_TRUE(terminal.has_value());
  ASSERT_TRUE(exhausted.has_value());
  const auto now = RemoteTabletReconfigurationSender::TimePoint{};

  ASSERT_TRUE(terminal->begin_attempt({2U, 1U}, now).has_value());
  ASSERT_TRUE(
      terminal->record_transport_failure(common::StatusCode::kUnauthenticated, now).is_ok());
  EXPECT_EQ(terminal->state(), RemoteTabletReconfigurationSenderState::kFailed);
  EXPECT_EQ(terminal->last_status_code(), common::StatusCode::kUnauthenticated);
  EXPECT_FALSE(terminal->next_attempt_not_before().has_value());

  ASSERT_TRUE(exhausted->begin_attempt({2U, 1U}, now).has_value());
  ASSERT_TRUE(exhausted->record_transport_failure(common::StatusCode::kUnavailable, now).is_ok());
  EXPECT_EQ(exhausted->state(), RemoteTabletReconfigurationSenderState::kFailed);
  EXPECT_EQ(exhausted->last_status_code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(exhausted->attempts_started(), 1U);
}

} // namespace
} // namespace chronos::cluster
