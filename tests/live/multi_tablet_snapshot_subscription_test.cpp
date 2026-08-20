#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/live/multi_tablet_snapshot_subscription.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/subscription_messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"
#include "manifest/publication_internal.hpp"
#include "query/snapshot_tablet_scan_test_fixture.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <latch>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
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

struct BoundPlan {
  query::PhysicalPipelinePlan plan;
  std::vector<SnapshotSubscriptionColumn> columns;
};

[[nodiscard]] BoundPlan lower(const std::shared_ptr<const schema::TableSchema>& schema,
                              const std::string_view sql) {
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = schema}};
  auto catalog = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
  auto parsed = query::parse_sql_v1_select(sql);
  auto bound = query::bind_sql_v1_select(std::move(*parsed), std::move(catalog));
  std::vector<SnapshotSubscriptionColumn> columns;
  columns.reserve(bound->outputs().size());
  for (const query::BoundOutputColumn& output : bound->outputs())
    columns.push_back({output.name, output.type, output.nullable});
  auto plan = query::lower_bound_sql_select(*bound);
  return {std::move(*plan), std::move(columns)};
}

[[nodiscard]] BoundPlan lower(const query::test::SnapshotTabletScanFixture& fixture,
                              const std::string_view sql) {
  return lower(fixture.schema_ptr(), sql);
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-subscription-XXXXXX").string();
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

[[nodiscard]] raft::GroupId raft_group_id() {
  return uuid(std::byte{0x71U});
}

[[nodiscard]] schema::TabletId raft_tablet_id() {
  return columnar::test::id<schema::TabletId>(81U);
}

[[nodiscard]] ingest::TabletState raft_tablet() {
  return ingest::TabletState::create(
             columnar::test::batch_schema(), raft_tablet_id(),
             {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
              .maximum_schema_versions = 1U,
              .maximum_sealed_generations = 2U,
              .maximum_retry_entries = 8U})
      .value();
}

[[nodiscard]] ingest::RetryDirectory retry_directory() {
  return ingest::RetryDirectory::create({.maximum_entries = 8U}).value();
}

[[nodiscard]] std::vector<std::byte> raft_command() {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto encoded = ingest::encode_columnar_append_v1(
                           {.client_id = ingest::test::request_id<ingest::ClientId>(1U),
                            .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(2U),
                            .tablet_id = raft_tablet_id()},
                           encoded_batch)
                           .value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

[[nodiscard]] std::vector<std::byte>
fixture_raft_command(const std::shared_ptr<const schema::TableSchema>& schema,
                     const schema::TabletId& tablet) {
  std::vector<std::byte> values;
  for (const std::int64_t value : std::array<std::int64_t, 2U>{700, 701}) {
    const std::uint64_t encoded = std::bit_cast<std::uint64_t>(value);
    for (std::size_t index = 0U; index < sizeof(encoded); ++index)
      values.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(columnar::OwnedColumnVector::create(
                        {.column_id = schema->event_time_column(),
                         .type = schema->columns().front().type(),
                         .nullable = false,
                         .row_count = 2U,
                         .null_count = 0U},
                        {.validity = {}, .offsets = {}, .values = std::move(values)})
                        .value());
  auto batch = columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto encoded = ingest::encode_columnar_append_v1(
                           {.client_id = ingest::test::request_id<ingest::ClientId>(3U),
                            .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(4U),
                            .tablet_id = tablet},
                           encoded_batch)
                           .value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

[[nodiscard]] ResumeTokenMacKey token_key() {
  ResumeTokenMacKey key{};
  key.fill(std::byte{9});
  return key;
}

[[nodiscard]] PlanFingerprint fingerprint() {
  PlanFingerprint plan{};
  plan.fill(std::byte{7});
  return plan;
}

[[nodiscard]] MultiTabletSubscriptionSource
source(const query::test::SnapshotTabletScanFixture& fixture, const std::uint64_t sequence_a,
       const std::uint64_t sequence_b) {
  return {.database_id = fixture.snapshot().database_id().uuid(),
          .table_id = fixture.schema_ptr()->table_id(),
          .plan_fingerprint = fingerprint(),
          .schema_id = fixture.schema_ptr()->schema_id(),
          .schema_version = fixture.schema_ptr()->version(),
          .members = {{query::test::SnapshotTabletScanFixture::second_tablet_id(),
                       fixture.snapshot().wal_id(), sequence_b},
                      {query::test::SnapshotTabletScanFixture::tablet_id(),
                       fixture.snapshot().wal_id(), sequence_a}},
          .token_key = token_key()};
}

[[nodiscard]] SubscriptionRequest request(const query::test::SnapshotTabletScanFixture& fixture) {
  return {uuid(std::byte{6}), fingerprint(), fixture.schema_ptr()->schema_id(),
          fixture.schema_ptr()->version()};
}

[[nodiscard]] CommittedChange change(const query::test::SnapshotTabletScanFixture& fixture,
                                     const std::uint64_t sequence) {
  return {.position = {query::test::SnapshotTabletScanFixture::tablet_id(),
                       fixture.snapshot().wal_id(), sequence},
          .schema_id = fixture.schema_ptr()->schema_id(),
          .schema_version = fixture.schema_ptr()->version(),
          .operation = LogicalChangeOperation::kUpsert,
          .result_key = {std::byte{1}},
          .payload = {std::byte{2}}};
}

[[nodiscard]] std::uint64_t decode_u64(const common::ByteView bytes) {
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[index]))
             << (index * 8U);
  return value;
}

[[nodiscard]] std::uint64_t signed_bits(const std::int64_t value) {
  return std::bit_cast<std::uint64_t>(value);
}

struct PublicationPause {
  std::latch entered{1U};
  std::latch release{1U};
};

void pause_before_publication(void* const context) noexcept {
  auto& pause = *static_cast<PublicationPause*>(context);
  pause.entered.count_down();
  pause.release.wait();
}

TEST(MultiTabletSnapshotSubscriptionTest, ExecutesOneGlobalPlanBeforeOpeningLiveSuffix) {
  query::test::SnapshotTabletScanFixture fixture{2U, 3U};
  const auto* first =
      fixture.snapshot().find_tablet(query::test::SnapshotTabletScanFixture::tablet_id());
  const auto* second =
      fixture.snapshot().find_tablet(query::test::SnapshotTabletScanFixture::second_tablet_id());
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_TRUE(first->applied_position().has_value());
  EXPECT_TRUE(second->applied_position().has_value());
  auto manager = MultiTabletSubscriptionManager::create(source(fixture, 1U, 1U));
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  const SubscriptionRequest subscription_request = request(fixture);
  BoundPlan bound = lower(fixture, "SELECT count(*) AS total FROM metrics");
  auto resources = query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();

  auto subscription = MultiTabletSnapshotSubscription::start(
      *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
      fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
  ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  ASSERT_TRUE(manager->publish_committed(change(fixture, 2U)).is_ok());
  EXPECT_EQ(manager->poll(subscription_request.subscription_id, 4U).error().code(),
            common::StatusCode::kUnavailable);

  const auto result = subscription->next();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->message_type, network::MessageType::kQueryResult);
  const auto decoded = network::decode_query_result_batch(result->payload);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_NE(decoded->cell(0U, 0U), nullptr);
  EXPECT_EQ(decode_u64(decoded->cell(0U, 0U)->value), 5U);

  const auto end = subscription->next();
  ASSERT_TRUE(end.has_value());
  EXPECT_EQ(end->flags, network::kFrameFlagEndStream);
  const auto ready = subscription->next();
  ASSERT_TRUE(ready.has_value()) << ready.error().to_string();
  EXPECT_EQ(ready->message_type, network::MessageType::kSubscriptionReady);
  EXPECT_TRUE(network::decode_subscription_ready(ready->payload).has_value());
  EXPECT_TRUE(subscription->ready());
  const auto live = manager->poll(subscription_request.subscription_id, 4U);
  ASSERT_TRUE(live.has_value());
  ASSERT_EQ(live->size(), 1U);
  EXPECT_EQ(live->front().change->position.record_sequence, 2U);
}

TEST(MultiTabletSnapshotSubscriptionTest,
     LinearizesSnapshotAcquisitionAgainstConcurrentAggregatePublication) {
  query::test::SnapshotTabletScanFixture fixture{1U, 1U};
  const auto successor = fixture.append_first_tablet({.value = 700, .record_sequence = 2U});
  ASSERT_TRUE(successor.has_value()) << successor.error().to_string();

  std::vector<manifest::DatabaseStorageTabletInput> inputs;
  inputs.reserve(fixture.tablet_snapshots().size());
  for (const ingest::TabletSnapshot& tablet : fixture.tablet_snapshots())
    inputs.push_back({.snapshot = tablet});
  PublicationPause pause;
  auto created = manifest::detail::DatabaseStoragePublisherTestAccess::create(
      fixture.selected_manifest(), inputs, &pause_before_publication, &pause);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  manifest::DatabaseStoragePublisher publisher = std::move(*created);

  auto manager = MultiTabletSubscriptionManager::create(source(fixture, 1U, 1U));
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  const SubscriptionRequest subscription_request = request(fixture);
  BoundPlan bound = lower(fixture, "SELECT count(*) AS total FROM metrics");
  auto resources = query::QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
  common::Result<manifest::DatabaseStorageSnapshot> published = common::make_unexpected(
      common::Status{common::StatusCode::kInternal, "publication thread did not run"});
  std::thread writer([&] { published = publisher.publish_tablet_snapshot(*successor); });
  pause.entered.wait();

  auto subscription = MultiTabletSnapshotSubscription::start(
      *manager, subscription_request, resources, fixture.storage(), publisher, fixture.lineage(),
      fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
  pause.release.count_down();
  writer.join();

  ASSERT_TRUE(published.has_value()) << published.error().to_string();
  const auto* current = published->find_tablet(query::test::SnapshotTabletScanFixture::tablet_id());
  ASSERT_NE(current, nullptr);
  ASSERT_TRUE(current->applied_position().has_value());
  EXPECT_EQ(current->applied_position().value_or(head::HeadCommitPosition{}).record_sequence, 2U);
  EXPECT_EQ(published->visible_head_row_count(), 3U);

  ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  const auto result = subscription->next();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  const auto decoded = network::decode_query_result_batch(result->payload);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_NE(decoded->cell(0U, 0U), nullptr);
  EXPECT_EQ(decode_u64(decoded->cell(0U, 0U)->value), 2U);
  ASSERT_TRUE(subscription->next().has_value());
  ASSERT_TRUE(subscription->next().has_value());
  EXPECT_TRUE(subscription->ready());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  auto stale_manager = MultiTabletSubscriptionManager::create(source(fixture, 1U, 1U));
  ASSERT_TRUE(stale_manager.has_value()) << stale_manager.error().to_string();
  BoundPlan stale_bound = lower(fixture, "SELECT count(*) AS total FROM metrics");
  auto rejected = MultiTabletSnapshotSubscription::start(
      *stale_manager, request(fixture), resources, fixture.storage(), publisher, fixture.lineage(),
      fixture.schema_ptr()->schema_id(), stale_bound.plan, std::move(stale_bound.columns));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kUnavailable);
  const auto stale_status = stale_manager->status(subscription_request.subscription_id);
  ASSERT_TRUE(stale_status.has_value());
  EXPECT_EQ(stale_status->phase, SubscriptionPhase::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(MultiTabletSnapshotSubscriptionTest, ExecutesEveryGlobalStageOnceAcrossTheWalVector) {
  query::test::SnapshotTabletScanFixture fixture{2U, 3U};
  const SubscriptionRequest subscription_request = request(fixture);
  auto resources = query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  const auto execute = [&](const std::string_view sql) {
    BoundPlan bound = lower(fixture, sql);
    auto manager = MultiTabletSubscriptionManager::create(source(fixture, 1U, 1U));
    EXPECT_TRUE(manager.has_value()) << manager.error().to_string();
    std::vector<std::vector<std::uint64_t>> rows;
    if (!manager.has_value())
      return rows;
    auto subscription = MultiTabletSnapshotSubscription::start(
        *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
        fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
    EXPECT_TRUE(subscription.has_value()) << subscription.error().to_string();
    if (!subscription.has_value())
      return rows;
    for (;;) {
      auto output = subscription->next();
      EXPECT_TRUE(output.has_value()) << output.error().to_string();
      if (!output.has_value())
        break;
      if (output->message_type == network::MessageType::kSubscriptionReady)
        break;
      auto batch = network::decode_query_result_batch(output->payload);
      EXPECT_TRUE(batch.has_value()) << batch.error().to_string();
      if (!batch.has_value())
        break;
      for (std::uint32_t row = 0U; row < batch->row_count(); ++row) {
        std::vector<std::uint64_t> cells;
        cells.reserve(batch->columns().size());
        for (std::size_t column = 0U; column < batch->columns().size(); ++column) {
          const network::QueryResultCell* cell = batch->cell(row, column);
          EXPECT_NE(cell, nullptr);
          if (cell != nullptr)
            cells.push_back(decode_u64(cell->value));
        }
        rows.push_back(std::move(cells));
      }
    }
    EXPECT_TRUE(subscription->ready());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    return rows;
  };

  EXPECT_EQ(execute("SELECT event_time FROM metrics ORDER BY event_time DESC LIMIT 2"),
            (std::vector<std::vector<std::uint64_t>>{{signed_bits(102)}, {signed_bits(101)}}));
  EXPECT_EQ(
      execute("SELECT time_bucket(INTERVAL '1 second', event_time) AS bucket, count(*) AS rows "
              "FROM metrics GROUP BY time_bucket(INTERVAL '1 second', event_time) ORDER BY bucket"),
      (std::vector<std::vector<std::uint64_t>>{{signed_bits(-1'000'000'000), 2U}, {0U, 3U}}));
  EXPECT_EQ(
      execute("SELECT event_time FROM metrics LATEST BY (event_time) ON "
              "time_bucket(INTERVAL '1 second', event_time) ORDER BY event_time DESC LIMIT 2"),
      (std::vector<std::vector<std::uint64_t>>{{signed_bits(102)}, {signed_bits(101)}}));
}

TEST(MultiTabletSnapshotSubscriptionTest, CancelsWhenAnyTabletBoundaryDisagrees) {
  query::test::SnapshotTabletScanFixture fixture{1U, 1U};
  auto manager = MultiTabletSubscriptionManager::create(source(fixture, 1U, 0U));
  ASSERT_TRUE(manager.has_value());
  const SubscriptionRequest subscription_request = request(fixture);
  BoundPlan bound = lower(fixture, "SELECT event_time FROM metrics");
  auto resources = query::QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  auto rejected = MultiTabletSnapshotSubscription::start(
      *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
      fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kUnavailable);
  const auto status = manager->status(subscription_request.subscription_id);
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(status->phase, SubscriptionPhase::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(MultiTabletSnapshotSubscriptionTest,
     ReadsExactRaftAppliedSnapshotAndRejectsAStaleRegistrationBoundary) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  std::vector<ingest::AsyncRaftTabletApplicationConfig> application_config;
  application_config.push_back({.group_id = raft_group_id(),
                                .snapshot_storage = std::nullopt,
                                .retry_directory = retry_directory(),
                                .tablet = raft_tablet(),
                                .retained_schemas = {columnar::test::batch_schema()},
                                .decode_limits = {}});
  auto application = ingest::AsyncRaftTabletApplication::create(std::move(application_config));
  ASSERT_TRUE(application.has_value()) << application.error().to_string();
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{raft_group_id(), {1U}}}, {},
      *application);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{raft_group_id(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  auto proposal = runtime->try_submit(
      {{raft_group_id(),
        raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, raft_command()}}});
  ASSERT_TRUE(proposal.has_value()) << proposal.error().to_string();
  auto proposed = proposal->wait();
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  ASSERT_EQ(proposed->size(), 1U);
  ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();

  const auto schema = columnar::test::batch_schema();
  auto lineage = schema::SchemaLineage::create(*schema);
  ASSERT_TRUE(lineage.has_value());
  BoundPlan bound = lower(schema, "SELECT count(*) AS total FROM metrics");
  auto resources = query::QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
  const SubscriptionRequest subscription_request{uuid(std::byte{0x72U}), fingerprint(),
                                                 schema->schema_id(), schema->version()};
  MultiTabletSubscriptionSource current{
      .database_id = uuid(std::byte{0x73U}),
      .table_id = schema->table_id(),
      .plan_fingerprint = fingerprint(),
      .schema_id = schema->schema_id(),
      .schema_version = schema->version(),
      .members = {MultiTabletSubscriptionMember::raft(raft_tablet_id(), raft_group_id(), 1U)},
      .token_key = token_key()};
  auto manager = MultiTabletSubscriptionManager::create(current);
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  auto subscription = MultiTabletSnapshotSubscription::start_raft(
      *manager, subscription_request, resources, **application, *lineage, schema->schema_id(),
      bound.plan, std::move(bound.columns));
  ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  auto result = subscription->next();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  const auto decoded = network::decode_query_result_batch(result->payload);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_NE(decoded->cell(0U, 0U), nullptr);
  EXPECT_EQ(decode_u64(decoded->cell(0U, 0U)->value), 2U);
  ASSERT_TRUE(subscription->next().has_value());
  auto ready = subscription->next();
  ASSERT_TRUE(ready.has_value()) << ready.error().to_string();
  EXPECT_EQ(ready->message_type, network::MessageType::kSubscriptionReady);
  EXPECT_TRUE(subscription->ready());

  current.members.front() =
      MultiTabletSubscriptionMember::raft(raft_tablet_id(), raft_group_id(), 0U);
  auto stale_manager = MultiTabletSubscriptionManager::create(std::move(current));
  ASSERT_TRUE(stale_manager.has_value()) << stale_manager.error().to_string();
  BoundPlan stale_bound = lower(schema, "SELECT count(*) AS total FROM metrics");
  const SubscriptionRequest stale_request{uuid(std::byte{0x74U}), fingerprint(),
                                          schema->schema_id(), schema->version()};
  auto stale = MultiTabletSnapshotSubscription::start_raft(
      *stale_manager, stale_request, resources, **application, *lineage, schema->schema_id(),
      stale_bound.plan, std::move(stale_bound.columns));
  ASSERT_FALSE(stale.has_value());
  EXPECT_EQ(stale.error().code(), common::StatusCode::kUnavailable);
  const auto stale_status = stale_manager->status(stale_request.subscription_id);
  ASSERT_TRUE(stale_status.has_value());
  EXPECT_EQ(stale_status->phase, SubscriptionPhase::kCancelled);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(MultiTabletSnapshotSubscriptionTest, ExecutesEveryGlobalStageAcrossExactWalAndRaftBoundaries) {
  query::test::SnapshotTabletScanFixture fixture{3U};
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const raft::GroupId group = raft_group_id();
  const schema::TabletId raft_tablet = raft_tablet_id();
  auto tablet = ingest::TabletState::create(
      fixture.schema_ptr(), raft_tablet,
      {.head_capacity = {.row_capacity = 4U, .variable_value_bytes = {0U}},
       .maximum_schema_versions = 1U,
       .maximum_sealed_generations = 1U,
       .maximum_retry_entries = 4U});
  ASSERT_TRUE(tablet.has_value()) << tablet.error().to_string();
  auto retries = ingest::RetryDirectory::create({.maximum_entries = 4U});
  ASSERT_TRUE(retries.has_value()) << retries.error().to_string();
  std::vector<ingest::AsyncRaftTabletApplicationConfig> application_config;
  application_config.push_back({.group_id = group,
                                .snapshot_storage = std::nullopt,
                                .retry_directory = std::move(*retries),
                                .tablet = std::move(*tablet),
                                .retained_schemas = {fixture.schema_ptr()},
                                .decode_limits = {}});
  auto application = ingest::AsyncRaftTabletApplication::create(std::move(application_config));
  ASSERT_TRUE(application.has_value()) << application.error().to_string();
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, {}, *application);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{group, raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  auto proposal = runtime->try_submit(
      {{group, raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType,
                                      fixture_raft_command(fixture.schema_ptr(), raft_tablet)}}});
  ASSERT_TRUE(proposal.has_value()) << proposal.error().to_string();
  auto proposed = proposal->wait();
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();

  MultiTabletSubscriptionSource mixed{
      .database_id = fixture.snapshot().database_id().uuid(),
      .table_id = fixture.schema_ptr()->table_id(),
      .plan_fingerprint = fingerprint(),
      .schema_id = fixture.schema_ptr()->schema_id(),
      .schema_version = fixture.schema_ptr()->version(),
      .members = {{query::test::SnapshotTabletScanFixture::tablet_id(), fixture.snapshot().wal_id(),
                   1U},
                  MultiTabletSubscriptionMember::raft(raft_tablet, group, 1U)},
      .token_key = token_key()};
  auto manager = MultiTabletSubscriptionManager::create(std::move(mixed));
  ASSERT_TRUE(manager.has_value()) << manager.error().to_string();
  const SubscriptionRequest subscription_request = request(fixture);
  BoundPlan bound = lower(fixture, "SELECT count(*) AS total FROM metrics");
  auto resources = query::QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  auto subscription = MultiTabletSnapshotSubscription::start_mixed(
      *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
      **application, fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan,
      std::move(bound.columns));
  ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  auto result = subscription->next();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  const auto decoded = network::decode_query_result_batch(result->payload);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  ASSERT_NE(decoded->cell(0U, 0U), nullptr);
  EXPECT_EQ(decode_u64(decoded->cell(0U, 0U)->value), 5U);
  ASSERT_TRUE(subscription->next().has_value());
  auto ready = subscription->next();
  ASSERT_TRUE(ready.has_value()) << ready.error().to_string();
  EXPECT_TRUE(subscription->ready());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  const auto execute = [&](const std::string_view sql) {
    MultiTabletSubscriptionSource current{
        .database_id = fixture.snapshot().database_id().uuid(),
        .table_id = fixture.schema_ptr()->table_id(),
        .plan_fingerprint = fingerprint(),
        .schema_id = fixture.schema_ptr()->schema_id(),
        .schema_version = fixture.schema_ptr()->version(),
        .members = {{query::test::SnapshotTabletScanFixture::tablet_id(),
                     fixture.snapshot().wal_id(), 1U},
                    MultiTabletSubscriptionMember::raft(raft_tablet, group, 1U)},
        .token_key = token_key()};
    auto current_manager = MultiTabletSubscriptionManager::create(std::move(current));
    EXPECT_TRUE(current_manager.has_value()) << current_manager.error().to_string();
    std::vector<std::vector<std::uint64_t>> rows;
    if (!current_manager.has_value())
      return rows;
    BoundPlan current_bound = lower(fixture, sql);
    auto current_subscription = MultiTabletSnapshotSubscription::start_mixed(
        *current_manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
        **application, fixture.lineage(), fixture.schema_ptr()->schema_id(), current_bound.plan,
        std::move(current_bound.columns));
    EXPECT_TRUE(current_subscription.has_value()) << current_subscription.error().to_string();
    if (!current_subscription.has_value())
      return rows;
    for (;;) {
      auto output = current_subscription->next();
      EXPECT_TRUE(output.has_value()) << output.error().to_string();
      if (!output.has_value())
        break;
      if (output->message_type == network::MessageType::kSubscriptionReady)
        break;
      auto batch = network::decode_query_result_batch(output->payload);
      EXPECT_TRUE(batch.has_value()) << batch.error().to_string();
      if (!batch.has_value())
        break;
      for (std::uint32_t row = 0U; row < batch->row_count(); ++row) {
        std::vector<std::uint64_t> cells;
        cells.reserve(batch->columns().size());
        for (std::size_t column = 0U; column < batch->columns().size(); ++column) {
          const network::QueryResultCell* cell = batch->cell(row, column);
          EXPECT_NE(cell, nullptr);
          if (cell != nullptr)
            cells.push_back(decode_u64(cell->value));
        }
        rows.push_back(std::move(cells));
      }
    }
    EXPECT_TRUE(current_subscription->ready());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    return rows;
  };

  EXPECT_EQ(execute("SELECT event_time FROM metrics ORDER BY event_time DESC LIMIT 2"),
            (std::vector<std::vector<std::uint64_t>>{{signed_bits(701)}, {signed_bits(700)}}));
  EXPECT_EQ(
      execute("SELECT time_bucket(INTERVAL '1 second', event_time) AS bucket, count(*) AS rows "
              "FROM metrics GROUP BY time_bucket(INTERVAL '1 second', event_time) ORDER BY bucket"),
      (std::vector<std::vector<std::uint64_t>>{{signed_bits(-1'000'000'000), 3U}, {0U, 2U}}));
  EXPECT_EQ(
      execute("SELECT event_time FROM metrics LATEST BY (event_time) ON "
              "time_bucket(INTERVAL '1 second', event_time) ORDER BY event_time DESC LIMIT 2"),
      (std::vector<std::vector<std::uint64_t>>{{signed_bits(701)}, {signed_bits(700)}}));

  MultiTabletSubscriptionSource stale_source{
      .database_id = fixture.snapshot().database_id().uuid(),
      .table_id = fixture.schema_ptr()->table_id(),
      .plan_fingerprint = fingerprint(),
      .schema_id = fixture.schema_ptr()->schema_id(),
      .schema_version = fixture.schema_ptr()->version(),
      .members = {{query::test::SnapshotTabletScanFixture::tablet_id(), fixture.snapshot().wal_id(),
                   1U},
                  MultiTabletSubscriptionMember::raft(raft_tablet, group, 0U)},
      .token_key = token_key()};
  auto stale_manager = MultiTabletSubscriptionManager::create(std::move(stale_source));
  ASSERT_TRUE(stale_manager.has_value()) << stale_manager.error().to_string();
  const SubscriptionRequest stale_request{uuid(std::byte{0x75U}), fingerprint(),
                                          fixture.schema_ptr()->schema_id(),
                                          fixture.schema_ptr()->version()};
  BoundPlan stale_bound = lower(fixture, "SELECT count(*) AS total FROM metrics");
  auto stale = MultiTabletSnapshotSubscription::start_mixed(
      *stale_manager, stale_request, resources, fixture.storage(), fixture.publisher(),
      **application, fixture.lineage(), fixture.schema_ptr()->schema_id(), stale_bound.plan,
      std::move(stale_bound.columns));
  ASSERT_FALSE(stale.has_value());
  EXPECT_EQ(stale.error().code(), common::StatusCode::kUnavailable);
  const auto stale_status = stale_manager->status(stale_request.subscription_id);
  ASSERT_TRUE(stale_status.has_value());
  EXPECT_EQ(stale_status->phase, SubscriptionPhase::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(MultiTabletSnapshotSubscriptionTest, AbandonsWithoutTokenEncodingWhenDriverIsDestroyed) {
  query::test::SnapshotTabletScanFixture fixture{1U, 1U};
  auto manager = MultiTabletSubscriptionManager::create(source(fixture, 1U, 1U));
  ASSERT_TRUE(manager.has_value());
  const SubscriptionRequest subscription_request = request(fixture);
  BoundPlan bound = lower(fixture, "SELECT event_time FROM metrics");
  auto resources = query::QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  {
    auto subscription = MultiTabletSnapshotSubscription::start(
        *manager, subscription_request, resources, fixture.storage(), fixture.publisher(),
        fixture.lineage(), fixture.schema_ptr()->schema_id(), bound.plan, std::move(bound.columns));
    ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  }
  const auto status = manager->status(subscription_request.subscription_id);
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(status->phase, SubscriptionPhase::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::live
