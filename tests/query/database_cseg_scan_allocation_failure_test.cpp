#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/query/database_cseg_scan.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/row_version.hpp"
#include "chronos/query/snapshot_pipeline.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "snapshot_tablet_scan_test_fixture.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class TemporaryAllocationDirectory {
public:
  TemporaryAllocationDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-cseg-scan-allocation-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryAllocationDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  TemporaryAllocationDirectory(const TemporaryAllocationDirectory&) = delete;
  TemporaryAllocationDirectory& operator=(const TemporaryAllocationDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

void write_allocation_fixture_bytes(const std::filesystem::path& path,
                                    const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  // std::ofstream has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(output.good());
}

[[nodiscard]] schema::SchemaLineage allocation_lineage() {
  const schema::ColumnId event = cseg::test::identifier<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(
      schema::ColumnDefinition::create(
          event, "event_time", cseg::test::type(schema::LogicalTypeKind::kTimestampNs), false)
          .value());
  schema::SchemaLineage result =
      schema::SchemaLineage::create(
          schema::TableSchema::create(cseg::test::identifier<schema::TableId>(2U),
                                      cseg::test::identifier<schema::SchemaId>(4U),
                                      schema::SchemaVersion::initial(), std::nullopt, columns,
                                      {.event_time_column = event,
                                       .physical_ordering_key = {event},
                                       .partition_columns = {event},
                                       .shard_key = {event},
                                       .deduplication_key = {}})
              .value())
          .value();
  columns.push_back(
      schema::ColumnDefinition::create(cseg::test::identifier<schema::ColumnId>(7U), "added",
                                       cseg::test::type(schema::LogicalTypeKind::kString), true)
          .value());
  EXPECT_TRUE(result
                  .append(schema::TableSchema::create(cseg::test::identifier<schema::TableId>(2U),
                                                      cseg::test::identifier<schema::SchemaId>(6U),
                                                      schema::SchemaVersion::from_value(2U).value(),
                                                      cseg::test::identifier<schema::SchemaId>(4U),
                                                      std::move(columns),
                                                      {.event_time_column = event,
                                                       .physical_ordering_key = {event},
                                                       .partition_columns = {event},
                                                       .shard_key = {event},
                                                       .deduplication_key = {}})
                              .value())
                  .is_ok());
  return result;
}

struct AggregateAllocationFixture {
  schema::SchemaLineage schemas;
  manifest::DatabaseStorageSnapshot snapshot;
  SnapshotCsegPartScanPlan plan;
  std::vector<std::shared_ptr<const manifest::SnapshotPartImage>> images;
};

[[nodiscard]] AggregateAllocationFixture aggregate_allocation_fixture() {
  TemporaryAllocationDirectory directory;
  EXPECT_FALSE(directory.path().empty());
  EXPECT_TRUE(std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
  EXPECT_TRUE(
      std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
  write_allocation_fixture_bytes(directory.path() / manifest::kManifestDirectoryName /
                                     std::string{manifest::kManifestLockFileName},
                                 {});

  schema::SchemaLineage schemas = allocation_lineage();
  const cseg::EncodedCsegPart encoded = cseg::test::make_valid_part_with_rows(10U, 5U);
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  wal::WalId wal_id{};
  wal_id.bytes = cseg::test::identifier<schema::SchemaId>(0x70U).bytes();
  const manifest::PartDescriptor descriptor{.part_id = cseg::test::identifier<cseg::PartId>(1U),
                                            .table_id = cseg::test::identifier<schema::TableId>(2U),
                                            .tablet_id = tablet,
                                            .schema_id =
                                                cseg::test::identifier<schema::SchemaId>(4U),
                                            .schema_version = schema::SchemaVersion::initial(),
                                            .file_length = encoded.size(),
                                            .row_count = 10U,
                                            .minimum_record_sequence = 7U,
                                            .maximum_record_sequence = 7U,
                                            .minimum_event_time = -100,
                                            .maximum_event_time = -91};
  const std::array tablets{
      manifest::TabletDescriptor{.table_id = descriptor.table_id,
                                 .tablet_id = tablet,
                                 .recovery_schema_id = descriptor.schema_id,
                                 .recovery_schema_version = descriptor.schema_version,
                                 .durable_record_sequence = 7U,
                                 .first_part_index = 0U,
                                 .part_count = 1U,
                                 .durable_row_count = 10U}};
  const std::array parts{descriptor};
  const manifest::DatabaseId database_id = cseg::test::identifier<manifest::DatabaseId>(6U);
  manifest::EncodedManifest encoded_manifest =
      manifest::encode_manifest_v1(
          {.generation = 1U,
           .database_id = database_id,
           .wal_id = wal_id,
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = tablets,
           .parts = parts,
           .retries = {}})
          .value();
  write_allocation_fixture_bytes(directory.path() / manifest::kPartsDirectoryName /
                                     manifest::part_file_name(descriptor.part_id),
                                 encoded.bytes());
  write_allocation_fixture_bytes(directory.path() / manifest::kManifestDirectoryName /
                                     *manifest::manifest_file_name(1U),
                                 encoded_manifest.bytes());

  manifest::ManifestStorage storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()})
          .value();
  const std::array bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet, .lineage = std::cref(schemas)}};
  auto selected = std::make_shared<const manifest::LoadedManifestGeneration>(
      storage
          .load_selected_manifest({.expected_database_id = database_id,
                                   .expected_wal_id = wal_id,
                                   .schema_bindings = bindings,
                                   .decode_limits = {},
                                   .part_validation_limits = {}})
          .value());
  manifest::DatabaseStoragePublisher publisher =
      manifest::DatabaseStoragePublisher::create(selected, {}).value();
  manifest::DatabaseStorageSnapshot snapshot = publisher.snapshot().value();
  const cseg::EventTimePredicate predicate{
      .lower = cseg::EventTimeBound{.value = -97, .inclusive = true},
      .upper = cseg::EventTimeBound{.value = -97, .inclusive = true}};
  SnapshotCsegPartScanPlan plan = plan_snapshot_cseg_part_scan(snapshot, tablet, predicate).value();
  std::vector<std::shared_ptr<const manifest::SnapshotPartImage>> images =
      load_snapshot_cseg_part_scan_images(storage, snapshot, plan, schemas).value();
  return {.schemas = std::move(schemas),
          .snapshot = std::move(snapshot),
          .plan = std::move(plan),
          .images = std::move(images)};
}

template <typename Operation>
[[nodiscard]] auto run_aggregate_with_allocation_failure(const std::size_t fail_after,
                                                         std::size_t& observed,
                                                         Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] PhysicalAsofPlan
allocation_asof_plan(const test::SnapshotTabletScanFixture& fixture) {
  std::vector<PhysicalColumnShape> source_shape{
      {.type = fixture.schema_ptr()->columns().front().type(), .nullable = false}};
  for (const VectorRowVersionColumnKind kind :
       {VectorRowVersionColumnKind::kWalId, VectorRowVersionColumnKind::kRecordSequence,
        VectorRowVersionColumnKind::kRowOrdinal, VectorRowVersionColumnKind::kOperation}) {
    source_shape.push_back(
        {.type = vector_row_version_column_type(kind).value(), .nullable = false});
  }
  std::vector<VectorAsofColumnShape> asof_shape;
  std::vector<std::size_t> outputs;
  for (std::size_t ordinal = 0U; ordinal < source_shape.size(); ++ordinal) {
    asof_shape.push_back(
        {.type = source_shape[ordinal].type, .nullable = source_shape[ordinal].nullable});
    outputs.push_back(ordinal);
  }
  VectorAsofJoinDefinition definition{
      .left_input_columns = asof_shape,
      .right_input_columns = asof_shape,
      .equality_keys = {{.left_column_ordinal = 0U, .right_column_ordinal = 0U}},
      .left_timestamp_column_ordinal = 0U,
      .right_timestamp_column_ordinal = 0U,
      .right_physical_ordering_key_ordinals = {0U},
      .right_row_version_first_column_ordinal = 1U,
      .left_output_column_ordinals = outputs,
      .right_output_column_ordinals = outputs};
  std::vector<PhysicalColumnShape> joined_shape;
  for (const VectorAsofColumnShape& column : vector_asof_join_output_shape(definition).value())
    joined_shape.push_back({.type = column.type, .nullable = column.nullable});
  std::vector<PhysicalAsofPlanJoin> joins;
  joins.push_back({.left_preparation = PhysicalPipelinePlan::create(source_shape, {}).value(),
                   .right_preparation = PhysicalPipelinePlan::create(source_shape, {}).value(),
                   .definition = std::move(definition)});
  return PhysicalAsofPlan::create(
             std::move(joins),
             PhysicalPipelinePlan::create(std::move(joined_shape),
                                          {ColumnSubsetStage{.column_ordinals = {5U}}})
                 .value())
      .value();
}

TEST(DatabaseCsegScanAllocationFailureTest, PlannerClassifiesEveryOwnedAllocationFailure) {
  const AggregateAllocationFixture fixture = aggregate_allocation_fixture();
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto planned = run_aggregate_with_allocation_failure(fail_after, observed, [&] {
      return plan_snapshot_cseg_part_scan(fixture.snapshot,
                                          cseg::test::identifier<schema::TabletId>(3U));
    });
    EXPECT_GT(observed, 0U);
    if (planned.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(planned.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(DatabaseCsegScanAllocationFailureTest, AggregateCreationUnwindsEveryRetainedAllocation) {
  const AggregateAllocationFixture fixture = aggregate_allocation_fixture();
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
    std::vector<std::shared_ptr<const manifest::SnapshotPartImage>> images = fixture.images;
    const std::vector<std::uint32_t> ordinals{1U};
    CsegScanLimits limits;
    limits.row_version_columns = RowVersionScanMode::kAppend;
    std::size_t observed = 0U;
    auto source = run_aggregate_with_allocation_failure(fail_after, observed, [&] {
      return create_snapshot_cseg_part_scan(
          resources, fixture.plan, std::move(images), fixture.schemas,
          cseg::test::identifier<schema::SchemaId>(6U), ordinals, limits);
    });
    EXPECT_GT(observed, 0U);
    if (source.has_value()) {
      reached_success = true;
      source->reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(source.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

TEST(DatabaseCsegScanAllocationFailureTest, CompleteTabletCreationClassifiesEveryNewAllocation) {
  const AggregateAllocationFixture fixture = aggregate_allocation_fixture();
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 96U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
    std::vector<std::shared_ptr<const manifest::SnapshotPartImage>> images = fixture.images;
    const std::vector<std::uint32_t> ordinals{1U};
    SnapshotTabletScanLimits limits;
    limits.cseg.row_version_columns = RowVersionScanMode::kAppend;
    limits.head.row_version_columns = RowVersionScanMode::kAppend;
    std::size_t observed = 0U;
    auto source = run_aggregate_with_allocation_failure(fail_after, observed, [&] {
      return create_snapshot_tablet_scan(
          resources, fixture.snapshot, fixture.plan, std::move(images), fixture.schemas,
          cseg::test::identifier<schema::SchemaId>(6U), ordinals, limits);
    });
    EXPECT_GT(observed, 0U);
    if (source.has_value()) {
      reached_success = true;
      source->reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(source.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

TEST(DatabaseCsegScanAllocationFailureTest,
     SnapshotPipelineInstantiationClassifiesEveryNewAllocation) {
  const test::SnapshotTabletScanFixture fixture{4U};
  PhysicalPipelinePlan pipeline =
      PhysicalPipelinePlan::create(
          {{.type = fixture.schema_ptr()->columns().front().type(), .nullable = false}}, {})
          .value();
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
    std::size_t observed = 0U;
    auto instantiated = run_aggregate_with_allocation_failure(fail_after, observed, [&] {
      return instantiate_snapshot_tablet_pipeline(resources, fixture.storage(), fixture.snapshot(),
                                                  test::SnapshotTabletScanFixture::tablet_id(),
                                                  fixture.lineage(),
                                                  fixture.schema_ptr()->schema_id(), pipeline);
    });
    EXPECT_GT(observed, 0U);
    if (instantiated.has_value()) {
      reached_success = true;
      instantiated->reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(instantiated.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

TEST(DatabaseCsegScanAllocationFailureTest,
     SnapshotAsofInstantiationClassifiesEveryNewAllocationAndReleasesCredit) {
  const test::SnapshotTabletScanFixture fixture{4U};
  const PhysicalAsofPlan plan = allocation_asof_plan(fixture);
  const SnapshotTabletSourceBinding source{
      .target_tablet = test::SnapshotTabletScanFixture::tablet_id(),
      .lineage = std::cref(fixture.lineage()),
      .destination_schema_id = fixture.schema_ptr()->schema_id()};
  const std::array sources{source, source};
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
    std::size_t observed = 0U;
    auto instantiated = run_aggregate_with_allocation_failure(fail_after, observed, [&] {
      return instantiate_snapshot_asof_plan(resources, fixture.storage(), fixture.snapshot(),
                                            sources, plan);
    });
    EXPECT_GT(observed, 0U);
    if (instantiated.has_value()) {
      reached_success = true;
      instantiated->reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(instantiated.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::query
