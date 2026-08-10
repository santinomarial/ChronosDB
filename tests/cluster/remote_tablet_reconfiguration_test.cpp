#include "chronos/cluster/remote_tablet_reconfiguration.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
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
                                                               .runtime = &*runtime});
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
  auto completed = admitted->completion.wait();
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_EQ(completed->size(), 1U);
  EXPECT_TRUE(completed->front().status.is_ok()) << completed->front().status.to_string();
  ASSERT_TRUE(completed->front().transition.has_value());
  EXPECT_TRUE(completed->front().transition->persistence.has_value());
  ASSERT_TRUE(ledger->load(request.action.id).has_value());

  auto observation = runtime->try_observe_group(tablet_group);
  ASSERT_TRUE(observation.has_value());
  auto observed = observation->wait();
  ASSERT_TRUE(observed.has_value());
  ASSERT_TRUE(observed->front().observation.has_value());
  EXPECT_EQ(observed->front().observation->last_log_index, 1U);

  auto duplicate = receiver->try_receive(
      *encoded, network::PeerAuthenticationResult{.authorized = true, .principal_id = 700U});
  ASSERT_TRUE(duplicate.has_value()) << duplicate.error().to_string();
  EXPECT_TRUE(duplicate->already_prepared);
  auto duplicate_completed = duplicate->completion.wait();
  ASSERT_TRUE(duplicate_completed.has_value());
  ASSERT_TRUE(duplicate_completed->front().status.is_ok());
  ASSERT_TRUE(duplicate_completed->front().transition.has_value());
  EXPECT_FALSE(duplicate_completed->front().transition->persistence.has_value());

  RemoteTabletReconfigurationRequest stale_request{1U, 2U, 2U, action(tablet_id(), tablet_group)};
  auto stale_bytes = encode_remote_tablet_reconfiguration_request_v1(stale_request);
  ASSERT_TRUE(stale_bytes.has_value());
  auto stale = receiver->try_receive(
      *stale_bytes, network::PeerAuthenticationResult{.authorized = true, .principal_id = 700U});
  ASSERT_TRUE(stale.has_value()) << stale.error().to_string();
  auto stale_completed = stale->completion.wait();
  ASSERT_TRUE(stale_completed.has_value());
  EXPECT_EQ(stale_completed->front().status.code(), common::StatusCode::kUnavailable);
  EXPECT_FALSE(stale_completed->front().transition.has_value());

  auto final_observation = runtime->try_observe_group(tablet_group);
  ASSERT_TRUE(final_observation.has_value());
  auto final_observed = final_observation->wait();
  ASSERT_TRUE(final_observed.has_value());
  ASSERT_TRUE(final_observed->front().observation.has_value());
  EXPECT_EQ(final_observed->front().observation->last_log_index, 1U);
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
                                                               .runtime = &*runtime});
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

} // namespace
} // namespace chronos::cluster
