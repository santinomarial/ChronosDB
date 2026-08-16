#include "chronos/live/subscription_service.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/subscription_messages.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-subscription-service-XXXXXX").string();
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

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] network::NetworkTask request_task(const std::uint64_t connection,
                                                const std::uint64_t request,
                                                const network::MessageType type,
                                                std::vector<std::byte> payload = {},
                                                const std::uint16_t protocol_minor = 2U) {
  return {.connection_id = connection,
          .principal_id = 77U,
          .protocol = {.protocol_major = network::kProtocolMajor,
                       .protocol_minor = protocol_minor,
                       .feature_bits = network::kProtocolV1SubscriptionFeature},
          .frame = {.header = {.protocol_major = network::kProtocolMajor,
                               .protocol_minor = protocol_minor,
                               .message_type = type,
                               .request_id = request},
                    .payload = std::move(payload)}};
}

TEST(SubscriptionServiceTest, OwnsSnapshotResumeBackpressureCancellationAndShutdownLifecycle) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  query::test::SnapshotTabletScanFixture snapshot{2U, 3U};
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = snapshot.schema_ptr()}};
  auto catalog = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
  constexpr std::string_view kSql = "SUBSCRIBE SELECT count(*) AS total FROM metrics";
  auto plan = prepare_subscription_plan(kSql, catalog);
  ASSERT_TRUE(plan.has_value()) << plan.error().status().to_string();

  ResumeTokenMacKey key{};
  key.fill(std::byte{13});
  const schema::TabletId tablet_a = query::test::SnapshotTabletScanFixture::tablet_id();
  const schema::TabletId tablet_b = query::test::SnapshotTabletScanFixture::second_tablet_id();
  const wal::WalId wal = snapshot.snapshot().wal_id();
  DurableMultiTabletSubscriptionConfig owner_config{
      .storage = {.directory_path = directory.path().string(),
                  .identity = {snapshot.snapshot().database_id().uuid(),
                               snapshot.schema_ptr()->table_id(),
                               plan->fingerprint(),
                               snapshot.schema_ptr()->schema_id(),
                               snapshot.schema_ptr()->version(),
                               {{tablet_a, wal}, {tablet_b, wal}}}},
      .source = {.database_id = snapshot.snapshot().database_id().uuid(),
                 .table_id = snapshot.schema_ptr()->table_id(),
                 .plan_fingerprint = plan->fingerprint(),
                 .schema_id = snapshot.schema_ptr()->schema_id(),
                 .schema_version = snapshot.schema_ptr()->version(),
                 .members = {{tablet_b, wal, 1U}, {tablet_a, wal, 1U}},
                 .token_key = key}};
  auto owner = DurableMultiTabletSubscription::create_new(std::move(owner_config));
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  auto resources = query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  auto requests = network::SpscNetworkTaskQueue::create(8U).value();
  auto responses = network::SpscNetworkTaskQueue::create(1U).value();
  auto service = SubscriptionService::create({.owner = &*owner,
                                              .plan = &*plan,
                                              .catalog = catalog,
                                              .resources = &resources,
                                              .storage = &snapshot.storage(),
                                              .publisher = &snapshot.publisher(),
                                              .lineage = &snapshot.lineage(),
                                              .requests = &requests,
                                              .responses = &responses,
                                              .maximum_active_subscriptions = 4U,
                                              .maximum_live_poll_records = 2U});
  ASSERT_TRUE(service.has_value()) << service.error().to_string();

  const common::Uuid subscription_id = uuid(std::byte{14});
  auto subscribe_payload = network::encode_subscription_request(
      {.mode = network::SubscriptionStartMode::kNewQuery,
       .subscription_id = subscription_id,
       .body = std::as_bytes(std::span{kSql.data(), kSql.size()})});
  ASSERT_TRUE(subscribe_payload.has_value());
  ASSERT_TRUE(requests.try_push(request_task(1U, 1U, network::MessageType::kSubscribeRequest,
                                             std::move(*subscribe_payload))));
  ASSERT_TRUE(service->poll_once().is_ok());
  EXPECT_EQ(service->metrics().active_subscriptions, 1U);

  ASSERT_TRUE(service->poll_once().is_ok());
  ASSERT_TRUE(service->poll_once().is_ok());
  EXPECT_EQ(service->metrics().response_backpressure, 1U);
  auto first = responses.try_pop();
  ASSERT_TRUE(first.has_value());
  const network::NetworkTask first_response = std::move(first).value_or(network::NetworkTask{});
  EXPECT_EQ(first_response.frame.header.message_type, network::MessageType::kQueryResult);
  ASSERT_TRUE(service->poll_once().is_ok());
  auto second = responses.try_pop();
  ASSERT_TRUE(second.has_value());
  const network::NetworkTask second_response = std::move(second).value_or(network::NetworkTask{});
  EXPECT_EQ(second_response.frame.header.message_type, network::MessageType::kQueryResult);
  EXPECT_NE(second_response.frame.header.flags & network::kFrameFlagEndStream, 0U);
  ASSERT_TRUE(service->poll_once().is_ok());
  auto ready_task = responses.try_pop();
  ASSERT_TRUE(ready_task.has_value());
  const network::NetworkTask ready_response =
      std::move(ready_task).value_or(network::NetworkTask{});
  EXPECT_EQ(ready_response.frame.header.message_type, network::MessageType::kSubscriptionReady);

  CommittedChange change{.position = {tablet_a, wal, 2U},
                         .schema_id = snapshot.schema_ptr()->schema_id(),
                         .schema_version = snapshot.schema_ptr()->version(),
                         .operation = LogicalChangeOperation::kUpsert,
                         .result_key = {std::byte{1}},
                         .payload = {std::byte{2}}};
  ASSERT_TRUE(owner->publish_committed(std::move(change)).is_ok());
  ASSERT_TRUE(service->poll_once().is_ok());
  auto live_task = responses.try_pop();
  ASSERT_TRUE(live_task.has_value());
  const network::NetworkTask live_response = std::move(live_task).value_or(network::NetworkTask{});
  ASSERT_EQ(live_response.frame.header.message_type, network::MessageType::kSubscriptionChange);
  EXPECT_EQ(live_response.protocol.protocol_minor, 2U);
  EXPECT_EQ(live_response.frame.header.protocol_minor, 2U);
  const auto live =
      network::decode_subscription_change(live_response.frame.payload, {.protocol_minor = 2U});
  ASSERT_TRUE(live.has_value());
  EXPECT_EQ(live->delivery_sequence, 1U);
  EXPECT_EQ(live->source_kind, network::SubscriptionSourceKind::kWal);

  auto acknowledgement = network::encode_subscription_acknowledgement({1U});
  ASSERT_TRUE(acknowledgement.has_value());
  ASSERT_TRUE(requests.try_push(request_task(1U, 1U, network::MessageType::kSubscriptionAcknowledge,
                                             std::move(*acknowledgement))));
  ASSERT_TRUE(service->poll_once().is_ok());
  auto checkpoint_task = responses.try_pop();
  ASSERT_TRUE(checkpoint_task.has_value());
  const network::NetworkTask checkpoint_response =
      std::move(checkpoint_task).value_or(network::NetworkTask{});
  ASSERT_EQ(checkpoint_response.frame.header.message_type,
            network::MessageType::kSubscriptionCheckpoint);
  const auto checkpoint =
      network::decode_subscription_checkpoint(checkpoint_response.frame.payload);
  ASSERT_TRUE(checkpoint.has_value());
  std::vector<std::byte> resume_token{checkpoint->resume_token.begin(),
                                      checkpoint->resume_token.end()};

  ASSERT_TRUE(requests.try_push(request_task(1U, 1U, network::MessageType::kCancel)));
  ASSERT_TRUE(service->poll_once().is_ok());
  auto cancelled_task = responses.try_pop();
  ASSERT_TRUE(cancelled_task.has_value());
  const network::NetworkTask cancelled_response =
      std::move(cancelled_task).value_or(network::NetworkTask{});
  EXPECT_EQ(cancelled_response.connection_id, 1U);
  EXPECT_EQ(cancelled_response.principal_id, 77U);
  EXPECT_EQ(cancelled_response.frame.header.request_id, 1U);
  EXPECT_EQ(cancelled_response.frame.header.message_type, network::MessageType::kSubscriptionEnd);
  const auto cancelled = network::decode_subscription_end(cancelled_response.frame.payload);
  ASSERT_TRUE(cancelled.has_value());
  EXPECT_EQ(cancelled->reason, network::SubscriptionEndReason::kCancelled);

  auto mismatched_resume =
      network::encode_subscription_request({.mode = network::SubscriptionStartMode::kResume,
                                            .subscription_id = uuid(std::byte{15}),
                                            .body = resume_token});
  ASSERT_TRUE(mismatched_resume.has_value());
  ASSERT_TRUE(requests.try_push(request_task(2U, 1U, network::MessageType::kSubscribeRequest,
                                             std::move(*mismatched_resume))));
  ASSERT_TRUE(service->poll_once().is_ok());
  auto mismatch_error = responses.try_pop();
  ASSERT_TRUE(mismatch_error.has_value());
  const network::NetworkTask mismatch_response =
      std::move(mismatch_error).value_or(network::NetworkTask{});
  EXPECT_EQ(mismatch_response.frame.header.message_type, network::MessageType::kError);

  auto resume_payload =
      network::encode_subscription_request({.mode = network::SubscriptionStartMode::kResume,
                                            .subscription_id = subscription_id,
                                            .body = resume_token});
  ASSERT_TRUE(resume_payload.has_value());
  ASSERT_TRUE(requests.try_push(
      request_task(2U, 2U, network::MessageType::kSubscribeRequest, std::move(*resume_payload))));
  ASSERT_TRUE(service->poll_once().is_ok());
  ASSERT_TRUE(service->poll_once().is_ok());
  auto resume_end = responses.try_pop();
  ASSERT_TRUE(resume_end.has_value());
  const network::NetworkTask resume_end_response =
      std::move(resume_end).value_or(network::NetworkTask{});
  EXPECT_EQ(resume_end_response.frame.header.message_type, network::MessageType::kQueryResult);
  EXPECT_NE(resume_end_response.frame.header.flags & network::kFrameFlagEndStream, 0U);
  ASSERT_TRUE(service->poll_once().is_ok());
  auto resume_ready = responses.try_pop();
  ASSERT_TRUE(resume_ready.has_value());
  const network::NetworkTask resume_ready_response =
      std::move(resume_ready).value_or(network::NetworkTask{});
  EXPECT_EQ(resume_ready_response.frame.header.message_type,
            network::MessageType::kSubscriptionReady);

  service->begin_shutdown();
  EXPECT_FALSE(service->accepting());
  ASSERT_TRUE(service->poll_once().is_ok());
  auto shutdown_task = responses.try_pop();
  ASSERT_TRUE(shutdown_task.has_value());
  const network::NetworkTask shutdown_response =
      std::move(shutdown_task).value_or(network::NetworkTask{});
  const auto shutdown = network::decode_subscription_end(shutdown_response.frame.payload);
  ASSERT_TRUE(shutdown.has_value());
  EXPECT_EQ(shutdown->reason, network::SubscriptionEndReason::kServerShutdown);
  EXPECT_TRUE(service->drained());
  EXPECT_EQ(service->metrics().terminal_responses, 2U);
}

TEST(SubscriptionServiceTest, RejectsRaftResumeOnOnePointOneAndDeliversItOnOnePointTwo) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  query::test::SnapshotTabletScanFixture snapshot{1U};
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = snapshot.schema_ptr()}};
  auto catalog = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
  constexpr std::string_view kSql = "SUBSCRIBE SELECT count(*) AS total FROM metrics";
  auto plan = prepare_subscription_plan(kSql, catalog);
  ASSERT_TRUE(plan.has_value()) << plan.error().status().to_string();

  ResumeTokenMacKey token_key{};
  token_key.fill(std::byte{13});
  const schema::TabletId tablet = query::test::SnapshotTabletScanFixture::tablet_id();
  const common::Uuid group_id = uuid(std::byte{21});
  auto owner = DurableMultiTabletSubscription::create_new(
      {.storage = {.directory_path = directory.path().string(),
                   .identity = {snapshot.snapshot().database_id().uuid(),
                                snapshot.schema_ptr()->table_id(),
                                plan->fingerprint(),
                                snapshot.schema_ptr()->schema_id(),
                                snapshot.schema_ptr()->version(),
                                {MultiTabletSubscriptionCheckpointSourceIdentity::raft(tablet,
                                                                                       group_id)}}},
       .source = {.database_id = snapshot.snapshot().database_id().uuid(),
                  .table_id = snapshot.schema_ptr()->table_id(),
                  .plan_fingerprint = plan->fingerprint(),
                  .schema_id = snapshot.schema_ptr()->schema_id(),
                  .schema_version = snapshot.schema_ptr()->version(),
                  .members = {MultiTabletSubscriptionMember::raft(tablet, group_id, 1U)},
                  .token_key = token_key}});
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();

  const common::Uuid subscription_id = uuid(std::byte{22});
  auto registration = owner->register_subscription({subscription_id, plan->fingerprint(),
                                                    snapshot.schema_ptr()->schema_id(),
                                                    snapshot.schema_ptr()->version()});
  ASSERT_TRUE(registration.has_value()) << registration.error().to_string();
  ASSERT_TRUE(owner->complete_snapshot(subscription_id).is_ok());
  ASSERT_TRUE(owner
                  ->publish_committed({.position = SourcePosition::raft(tablet, group_id, 2U),
                                       .schema_id = snapshot.schema_ptr()->schema_id(),
                                       .schema_version = snapshot.schema_ptr()->version(),
                                       .operation = LogicalChangeOperation::kUpsert,
                                       .result_key = {std::byte{1}},
                                       .payload = {std::byte{2}}})
                  .is_ok());
  std::vector<std::byte> resume_token = registration->initial_resume_token;
  owner->abandon(subscription_id);

  auto resources = query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  auto requests = network::SpscNetworkTaskQueue::create(8U).value();
  auto responses = network::SpscNetworkTaskQueue::create(8U).value();
  auto service = SubscriptionService::create({.owner = &*owner,
                                              .plan = &*plan,
                                              .catalog = catalog,
                                              .resources = &resources,
                                              .storage = &snapshot.storage(),
                                              .publisher = &snapshot.publisher(),
                                              .lineage = &snapshot.lineage(),
                                              .requests = &requests,
                                              .responses = &responses});
  ASSERT_TRUE(service.has_value()) << service.error().to_string();
  const auto resume_payload =
      network::encode_subscription_request({.mode = network::SubscriptionStartMode::kResume,
                                            .subscription_id = subscription_id,
                                            .body = resume_token});
  ASSERT_TRUE(resume_payload.has_value());
  ASSERT_TRUE(requests.try_push(
      request_task(1U, 1U, network::MessageType::kSubscribeRequest, *resume_payload, 1U)));
  ASSERT_TRUE(service->poll_once().is_ok());
  auto rejected = responses.try_pop();
  ASSERT_TRUE(rejected.has_value());
  const network::NetworkTask rejected_response =
      std::move(rejected).value_or(network::NetworkTask{});
  EXPECT_EQ(rejected_response.frame.header.message_type, network::MessageType::kError);
  EXPECT_FALSE(service->owns(1U, 1U));

  ASSERT_TRUE(requests.try_push(
      request_task(2U, 1U, network::MessageType::kSubscribeRequest, *resume_payload, 2U)));
  ASSERT_TRUE(service->poll_once().is_ok());
  EXPECT_TRUE(service->owns(2U, 1U));
  ASSERT_TRUE(service->poll_once().is_ok());
  auto snapshot_end = responses.try_pop();
  ASSERT_TRUE(snapshot_end.has_value());
  const network::NetworkTask snapshot_end_response =
      std::move(snapshot_end).value_or(network::NetworkTask{});
  EXPECT_EQ(snapshot_end_response.frame.header.message_type, network::MessageType::kQueryResult);
  ASSERT_TRUE(service->poll_once().is_ok());
  auto ready = responses.try_pop();
  ASSERT_TRUE(ready.has_value());
  const network::NetworkTask ready_response = std::move(ready).value_or(network::NetworkTask{});
  EXPECT_EQ(ready_response.frame.header.message_type, network::MessageType::kSubscriptionReady);
  ASSERT_TRUE(service->poll_once().is_ok());
  auto change = responses.try_pop();
  ASSERT_TRUE(change.has_value());
  const network::NetworkTask change_response = std::move(change).value_or(network::NetworkTask{});
  ASSERT_EQ(change_response.frame.header.message_type, network::MessageType::kSubscriptionChange);
  const auto decoded =
      network::decode_subscription_change(change_response.frame.payload, {.protocol_minor = 2U});
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_kind, network::SubscriptionSourceKind::kRaft);
  EXPECT_TRUE(std::ranges::equal(decoded->source_id, group_id.bytes()));
  EXPECT_EQ(decoded->source_sequence, 2U);
}

} // namespace
} // namespace chronos::live
