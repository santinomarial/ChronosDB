#include "chronos/live/durable_multi_tablet_subscription.hpp"
#include "chronos/live/subscription_plan.hpp"
#include "chronos/live/subscription_service.hpp"
#include "chronos/network/subscription_messages.hpp"
#include "chronos/query/catalog.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-service-allocation-XXXXXX").string();
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

[[nodiscard]] network::NetworkTask request_task(std::vector<std::byte> payload) {
  return {.connection_id = 1U,
          .principal_id = 77U,
          .protocol = {.protocol_major = network::kProtocolMajor,
                       .protocol_minor = 2U,
                       .feature_bits = network::kProtocolV1SubscriptionFeature},
          .frame = {.header = {.protocol_major = network::kProtocolMajor,
                               .protocol_minor = 2U,
                               .message_type = network::MessageType::kSubscribeRequest,
                               .request_id = 1U},
                    .payload = std::move(payload)}};
}

TEST(SubscriptionServiceAllocationFailureTest,
     AdmissionClassifiesOrPublishesEveryOwnedAllocationFailure) {
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
  const common::Uuid subscription_id = uuid(std::byte{14});
  auto encoded_request = network::encode_subscription_request(
      {.mode = network::SubscriptionStartMode::kNewQuery,
       .subscription_id = subscription_id,
       .body = std::as_bytes(std::span{kSql.data(), kSql.size()})});
  ASSERT_TRUE(encoded_request.has_value());
  ResumeTokenMacKey key{};
  key.fill(std::byte{13});
  const schema::TabletId tablet_a = query::test::SnapshotTabletScanFixture::tablet_id();
  const schema::TabletId tablet_b = query::test::SnapshotTabletScanFixture::second_tablet_id();
  const wal::WalId wal = snapshot.snapshot().wal_id();
  bool reached_success = false;

  for (std::size_t fail_after = 0U; fail_after < 1024U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    const std::filesystem::path owner_directory = directory.path() / std::to_string(fail_after);
    std::error_code directory_error;
    ASSERT_TRUE(std::filesystem::create_directory(owner_directory, directory_error));
    ASSERT_FALSE(directory_error);
    DurableMultiTabletSubscriptionConfig owner_config{
        .storage = {.directory_path = owner_directory.string(),
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
    query::QueryResourceContext resources =
        query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
    auto requests = network::SpscNetworkTaskQueue::create(2U).value();
    auto responses = network::SpscNetworkTaskQueue::create(2U).value();
    auto created = SubscriptionService::create({.owner = &*owner,
                                                .plan = &*plan,
                                                .catalog = catalog,
                                                .resources = &resources,
                                                .storage = &snapshot.storage(),
                                                .publisher = &snapshot.publisher(),
                                                .lineage = &snapshot.lineage(),
                                                .requests = &requests,
                                                .responses = &responses,
                                                .maximum_active_subscriptions = 2U,
                                                .maximum_live_poll_records = 2U});
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    std::optional<SubscriptionService> service;
    service.emplace(std::move(*created));
    std::vector<std::byte> payload = *encoded_request;
    ASSERT_TRUE(requests.try_push(request_task(std::move(payload))));

    common::Status status;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      status = service->poll_once();
      observed = failure.observed_allocations();
      failure.disable();
    }
    EXPECT_GT(observed, 0U);
    if (service->owns(1U, 1U)) {
      EXPECT_TRUE(status.is_ok()) << status.to_string();
      reached_success = true;
      service.reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }

    EXPECT_EQ(service->metrics().active_subscriptions, 0U);
    const auto owner_status = owner->status(subscription_id);
    if (owner_status.has_value())
      EXPECT_EQ(owner_status->phase, SubscriptionPhase::kCancelled);
    else
      EXPECT_EQ(owner_status.error().code(), common::StatusCode::kNotFound);
    if (status.is_ok()) {
      auto response = responses.try_pop();
      ASSERT_TRUE(response.has_value());
      const network::NetworkTask response_task =
          std::move(response).value_or(network::NetworkTask{});
      EXPECT_EQ(response_task.frame.header.message_type, network::MessageType::kError);
    } else {
      EXPECT_EQ(status.code(), common::StatusCode::kResourceExhausted);
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }

  EXPECT_TRUE(reached_success);
}

struct ServiceOutputTransition {
  std::string_view name;
  std::size_t completed_outputs;
  network::MessageType expected_type;
  std::uint32_t expected_flags;
};

TEST(SubscriptionServiceAllocationFailureTest, SnapshotOutputsFailClosedAtEveryOwnedAllocation) {
  constexpr std::array<ServiceOutputTransition, 3U> transitions{{
      {"first result", 0U, network::MessageType::kQueryResult, 0U},
      {"end stream", 1U, network::MessageType::kQueryResult, network::kFrameFlagEndStream},
      {"ready", 2U, network::MessageType::kSubscriptionReady, 0U},
  }};
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
  const common::Uuid subscription_id = uuid(std::byte{14});
  auto encoded_request = network::encode_subscription_request(
      {.mode = network::SubscriptionStartMode::kNewQuery,
       .subscription_id = subscription_id,
       .body = std::as_bytes(std::span{kSql.data(), kSql.size()})});
  ASSERT_TRUE(encoded_request.has_value());
  ResumeTokenMacKey key{};
  key.fill(std::byte{13});
  const schema::TabletId tablet_a = query::test::SnapshotTabletScanFixture::tablet_id();
  const schema::TabletId tablet_b = query::test::SnapshotTabletScanFixture::second_tablet_id();
  const wal::WalId wal = snapshot.snapshot().wal_id();
  std::size_t owner_ordinal = 0U;

  for (const ServiceOutputTransition& transition : transitions) {
    SCOPED_TRACE(transition.name);
    bool reached_success = false;
    for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
      SCOPED_TRACE(fail_after);
      const std::filesystem::path owner_directory =
          directory.path() / std::to_string(owner_ordinal++);
      std::error_code directory_error;
      ASSERT_TRUE(std::filesystem::create_directory(owner_directory, directory_error));
      ASSERT_FALSE(directory_error);
      DurableMultiTabletSubscriptionConfig owner_config{
          .storage = {.directory_path = owner_directory.string(),
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
      query::QueryResourceContext resources =
          query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
      auto requests = network::SpscNetworkTaskQueue::create(2U).value();
      auto responses = network::SpscNetworkTaskQueue::create(2U).value();
      auto created = SubscriptionService::create({.owner = &*owner,
                                                  .plan = &*plan,
                                                  .catalog = catalog,
                                                  .resources = &resources,
                                                  .storage = &snapshot.storage(),
                                                  .publisher = &snapshot.publisher(),
                                                  .lineage = &snapshot.lineage(),
                                                  .requests = &requests,
                                                  .responses = &responses,
                                                  .maximum_active_subscriptions = 2U,
                                                  .maximum_live_poll_records = 2U});
      ASSERT_TRUE(created.has_value()) << created.error().to_string();
      std::optional<SubscriptionService> service;
      service.emplace(std::move(*created));
      std::vector<std::byte> payload = *encoded_request;
      ASSERT_TRUE(requests.try_push(request_task(std::move(payload))));
      ASSERT_TRUE(service->poll_once().is_ok());
      ASSERT_TRUE(service->owns(1U, 1U));
      for (std::size_t output = 0U; output < transition.completed_outputs; ++output) {
        ASSERT_TRUE(service->poll_once().is_ok());
        auto completed = responses.try_pop();
        ASSERT_TRUE(completed.has_value());
      }

      common::Status status;
      std::size_t observed = 0U;
      {
        ::chronos::test::ScopedAllocationFailure failure{fail_after};
        status = service->poll_once();
        observed = failure.observed_allocations();
        failure.disable();
      }
      EXPECT_GT(observed, 0U);
      auto response = responses.try_pop();
      if (response.has_value()) {
        const network::NetworkTask response_task =
            std::move(response).value_or(network::NetworkTask{});
        if (response_task.frame.header.message_type == transition.expected_type) {
          EXPECT_TRUE(status.is_ok()) << status.to_string();
          EXPECT_EQ(response_task.frame.header.flags, transition.expected_flags);
          EXPECT_TRUE(service->owns(1U, 1U));
          reached_success = true;
          service.reset();
          EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
          break;
        }
        EXPECT_EQ(response_task.frame.header.message_type, network::MessageType::kError);
      } else {
        EXPECT_FALSE(status.is_ok());
        EXPECT_EQ(status.code(), common::StatusCode::kResourceExhausted);
      }
      EXPECT_FALSE(service->owns(1U, 1U));
      EXPECT_EQ(service->metrics().active_subscriptions, 0U);
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    }
    EXPECT_TRUE(reached_success);
  }
}

} // namespace
} // namespace chronos::live
