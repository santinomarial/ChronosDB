#include "chronos/columnar/column_vector.hpp"
#include "chronos/network/subscription_messages.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/service/single_node_subscription_runtime.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-subscription-runtime-XXXXXX").string();
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

[[nodiscard]] network::NetworkTask request_task(const network::MessageType type,
                                                std::vector<std::byte> payload = {}) {
  return {.connection_id = 1U,
          .principal_id = 7U,
          .frame = {.header = {.message_type = type, .request_id = 11U},
                    .payload = std::move(payload)}};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
committed_batch(const std::shared_ptr<const schema::TableSchema>& schema) {
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(
      columnar::OwnedColumnVector::create(
          {.column_id = schema->event_time_column(),
           .type = schema->columns().front().type(),
           .nullable = false,
           .row_count = 1U,
           .null_count = 0U},
          {.validity = {}, .offsets = {}, .values = std::vector<std::byte>(sizeof(std::int64_t))})
          .value());
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
}

TEST(SingleNodeSubscriptionRuntimeTest, ComposesSnapshotLiveAckAndShutdownAroundAppliedAppends) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  query::test::SnapshotTabletScanFixture snapshot{2U};
  const std::array tables{query::QueryCatalogTableInput{
      .name = "metrics", .quoted = false, .schema = snapshot.schema_ptr()}};
  auto catalog = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
  constexpr std::string_view kSql = "SUBSCRIBE SELECT event_time FROM metrics";
  auto plan = live::prepare_subscription_plan(kSql, catalog);
  ASSERT_TRUE(plan.has_value()) << plan.error().status().to_string();
  live::ResumeTokenMacKey key{};
  key.fill(std::byte{12});
  const schema::TabletId tablet = query::test::SnapshotTabletScanFixture::tablet_id();
  const wal::WalId wal = snapshot.snapshot().wal_id();
  auto coordinator = live::DurableMultiTabletSubscription::create_new(
      {.storage = {.directory_path = directory.path().string(),
                   .identity = {snapshot.snapshot().database_id().uuid(),
                                snapshot.schema_ptr()->table_id(),
                                plan->fingerprint(),
                                snapshot.schema_ptr()->schema_id(),
                                snapshot.schema_ptr()->version(),
                                {{tablet, wal}}}},
       .source = {.database_id = snapshot.snapshot().database_id().uuid(),
                  .table_id = snapshot.schema_ptr()->table_id(),
                  .plan_fingerprint = plan->fingerprint(),
                  .schema_id = snapshot.schema_ptr()->schema_id(),
                  .schema_version = snapshot.schema_ptr()->version(),
                  .members = {{tablet, wal, 1U}},
                  .token_key = key}});
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  query::QueryResourceContext resources =
      query::QueryResourceContext::create(32U * 1024U * 1024U).value();
  auto requests = network::SpscNetworkTaskQueue::create(16U).value();
  auto responses = network::SpscNetworkTaskQueue::create(16U).value();
  SingleNodeCommittedAppendRouter router;
  auto runtime = SingleNodeSubscriptionRuntime::create({.observer_router = &router,
                                                        .plan = &*plan,
                                                        .coordinator = &*coordinator,
                                                        .catalog = catalog,
                                                        .resources = &resources,
                                                        .storage = &snapshot.storage(),
                                                        .publisher = &snapshot.publisher(),
                                                        .lineage = &snapshot.lineage(),
                                                        .requests = &requests,
                                                        .responses = &responses,
                                                        .maximum_active_subscriptions = 4U,
                                                        .maximum_live_poll_records = 2U});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  EXPECT_TRUE(router.metrics().bound);

  const common::Uuid subscription_id = uuid(std::byte{13});
  auto subscribe = network::encode_subscription_request(
      {.mode = network::SubscriptionStartMode::kNewQuery,
       .subscription_id = subscription_id,
       .body = std::as_bytes(std::span{kSql.data(), kSql.size()})});
  ASSERT_TRUE(subscribe.has_value());
  ASSERT_TRUE(requests.try_push(
      request_task(network::MessageType::kSubscribeRequest, std::move(*subscribe))));
  ASSERT_TRUE(runtime->poll_once().is_ok());

  bool ready = false;
  for (std::size_t attempt = 0U; attempt < 8U && !ready; ++attempt) {
    ASSERT_TRUE(runtime->poll_once().is_ok());
    if (auto response = responses.try_pop(); response.has_value())
      ready = response->frame.header.message_type == network::MessageType::kSubscriptionReady;
  }
  ASSERT_TRUE(ready);

  const auto input = committed_batch(snapshot.schema_ptr());
  router.on_applied({.tablet_id = tablet,
                     .position = {.wal_id = wal, .record_sequence = 2U},
                     .batch = input,
                     .outcome = {}});
  EXPECT_EQ(coordinator->checkpoint_generation(), 1U);
  ASSERT_TRUE(runtime->poll_once().is_ok());
  auto change_task = responses.try_pop();
  ASSERT_TRUE(change_task.has_value());
  ASSERT_EQ(change_task->frame.header.message_type, network::MessageType::kSubscriptionChange);
  auto change = network::decode_subscription_change(change_task->frame.payload);
  ASSERT_TRUE(change.has_value()) << change.error().to_string();
  EXPECT_EQ(change->record_sequence, 2U);
  EXPECT_EQ(change->delivery_sequence, 1U);

  auto acknowledgement = network::encode_subscription_acknowledgement({1U});
  ASSERT_TRUE(acknowledgement.has_value());
  ASSERT_TRUE(requests.try_push(
      request_task(network::MessageType::kSubscriptionAcknowledge, std::move(*acknowledgement))));
  ASSERT_TRUE(runtime->poll_once().is_ok());
  auto checkpoint = responses.try_pop();
  ASSERT_TRUE(checkpoint.has_value());
  EXPECT_EQ(checkpoint->frame.header.message_type, network::MessageType::kSubscriptionCheckpoint);

  runtime->begin_shutdown();
  ASSERT_TRUE(runtime->poll_once().is_ok());
  auto ended = responses.try_pop();
  ASSERT_TRUE(ended.has_value());
  ASSERT_EQ(ended->frame.header.message_type, network::MessageType::kSubscriptionEnd);
  auto decoded_end = network::decode_subscription_end(ended->frame.payload);
  ASSERT_TRUE(decoded_end.has_value());
  EXPECT_EQ(decoded_end->reason, network::SubscriptionEndReason::kServerShutdown);
  EXPECT_TRUE(runtime->drained());
  EXPECT_EQ(runtime->metrics().fanout.checkpoint_successes, 1U);
}

} // namespace
} // namespace chronos::service
