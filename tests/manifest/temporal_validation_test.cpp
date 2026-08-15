#include "chronos/common/uuid.hpp"
#include "chronos/manifest/temporal_validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"
#include "manifest/manifest_test_support.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] wal::WalId wal_id(const std::uint8_t seed) {
  wal::WalId value{};
  value.bytes.back() = std::byte{seed};
  return value;
}

[[nodiscard]] TemporalWalReclaimCheckpoint wal_checkpoint(const wal::WalId& wal,
                                                          const std::uint64_t record_sequence,
                                                          const std::uint64_t byte_offset) {
  return {wal,
          {.record_sequence = record_sequence, .segment_number = 1U, .byte_offset = byte_offset}};
}

struct Fixture {
  DatabaseId database_id{test::make_id<DatabaseId>(1U)};
  wal::WalId wal{wal_id(2U)};
  schema::TableId table_id{test::make_id<schema::TableId>(3U)};
  schema::TabletId tablet_id{test::make_id<schema::TabletId>(4U)};
  schema::SchemaId schema_id{test::make_id<schema::SchemaId>(5U)};
  std::optional<TemporalWalReclaimCheckpoint> checkpoint{wal_checkpoint(wal, 5U, 128U)};
  std::vector<TemporalTabletDescriptor> tablets{{
      .table_id = table_id,
      .tablet_id = tablet_id,
      .recovery_schema_id = schema_id,
      .recovery_schema_version = schema::SchemaVersion::initial(),
      .source_id = common::Uuid{wal.bytes},
      .durable_position = 5U,
      .reclaim_position = 0U,
      .first_part_index = 0U,
      .part_count = 1U,
      .durable_version_count = 2U,
      .commit_source = ManifestCommitSource::kWal,
  }};
  std::vector<TemporalPartDescriptor> parts{{
      .part_id = test::make_id<cseg::PartId>(6U),
      .table_id = table_id,
      .tablet_id = tablet_id,
      .schema_id = schema_id,
      .schema_version = schema::SchemaVersion::initial(),
      .file_length = 4'096U,
      .row_count = 2U,
      .minimum_commit_position = 4U,
      .maximum_commit_position = 5U,
      .minimum_event_time = 10,
      .maximum_event_time = 20,
      .minimum_system_time = 100,
      .maximum_system_time = 110,
      .source_id = common::Uuid{wal.bytes},
      .content_sha256 = test::make_digest(7U),
      .commit_source = ManifestCommitSource::kWal,
  }};
  std::vector<TemporalRetryDescriptor> retries{{
      .client_id = test::make_id<ingest::ClientId>(8U),
      .client_batch_id = test::make_id<ingest::ClientBatchId>(9U),
      .table_id = table_id,
      .tablet_id = tablet_id,
      .request_digest = test::make_digest(10U),
      .source_id = common::Uuid{wal.bytes},
      .commit_position = 5U,
      .applied_row_count = 2U,
      .commit_source = ManifestCommitSource::kWal,
  }};

  [[nodiscard]] TemporalManifestEncodeInput input(const std::uint64_t generation) const {
    return {.generation = generation,
            .database_id = database_id,
            .wal_reclaim_checkpoint = checkpoint,
            .tablets = tablets,
            .parts = parts,
            .retries = retries};
  }

  void append_successor_state() {
    tablets[0].durable_position = 6U;
    tablets[0].part_count = 2U;
    tablets[0].durable_version_count = 3U;
    checkpoint = wal_checkpoint(wal, 6U, 192U);
    parts.push_back({
        .part_id = test::make_id<cseg::PartId>(7U),
        .table_id = table_id,
        .tablet_id = tablet_id,
        .schema_id = schema_id,
        .schema_version = schema::SchemaVersion::initial(),
        .file_length = 4'096U,
        .row_count = 1U,
        .minimum_commit_position = 6U,
        .maximum_commit_position = 6U,
        .minimum_event_time = 30,
        .maximum_event_time = 30,
        .minimum_system_time = 120,
        .maximum_system_time = 120,
        .source_id = common::Uuid{wal.bytes},
        .content_sha256 = test::make_digest(11U),
        .commit_source = ManifestCommitSource::kWal,
    });
    retries.push_back({
        .client_id = test::make_id<ingest::ClientId>(11U),
        .client_batch_id = test::make_id<ingest::ClientBatchId>(12U),
        .table_id = table_id,
        .tablet_id = tablet_id,
        .request_digest = test::make_digest(13U),
        .source_id = common::Uuid{wal.bytes},
        .commit_position = 6U,
        .applied_row_count = 1U,
        .commit_source = ManifestCommitSource::kWal,
    });
  }

  void use_raft_source() {
    common::Uuid::Bytes bytes{};
    bytes.back() = std::byte{14U};
    const common::Uuid group{bytes};
    checkpoint = std::nullopt;
    tablets[0].source_id = group;
    tablets[0].reclaim_position = 3U;
    tablets[0].commit_source = ManifestCommitSource::kRaft;
    parts[0].source_id = group;
    parts[0].commit_source = ManifestCommitSource::kRaft;
    retries[0].source_id = group;
    retries[0].commit_source = ManifestCommitSource::kRaft;
  }
};

struct LineageFixture {
  explicit LineageFixture(const Fixture& fixture)
      : table_id(fixture.table_id), v1_id(fixture.schema_id) {}

  [[nodiscard]] schema::TableSchema
  schema_value(const schema::SchemaId schema_id, const schema::SchemaVersion version,
               const std::optional<schema::SchemaId> parent) const {
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(table_id, schema_id, version, parent, std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }

  [[nodiscard]] schema::SchemaLineage lineage() const {
    schema::SchemaLineage value =
        schema::SchemaLineage::create(
            schema_value(v1_id, schema::SchemaVersion::initial(), std::nullopt))
            .value();
    EXPECT_TRUE(
        value.append(schema_value(v2_id, schema::SchemaVersion::initial().next().value(), v1_id))
            .is_ok());
    return value;
  }

  schema::TableId table_id;
  schema::SchemaId v1_id;
  schema::SchemaId v2_id{test::make_id<schema::SchemaId>(15U)};
  schema::ColumnId event_id{test::make_id<schema::ColumnId>(16U)};
};

struct DecodedPair {
  EncodedTemporalManifest predecessor_bytes;
  EncodedTemporalManifest next_bytes;
  DecodedTemporalManifestView predecessor;
  DecodedTemporalManifestView next;
};

[[nodiscard]] DecodedPair decode_pair(const Fixture& predecessor, const Fixture& next,
                                      const std::uint64_t next_generation = 3U) {
  EncodedTemporalManifest predecessor_bytes =
      encode_manifest_v2_temporal(predecessor.input(2U)).value();
  EncodedTemporalManifest next_bytes =
      encode_manifest_v2_temporal(next.input(next_generation)).value();
  DecodedTemporalManifestView predecessor_view =
      decode_manifest_v2_temporal_exact(predecessor_bytes.bytes()).value();
  DecodedTemporalManifestView next_view =
      decode_manifest_v2_temporal_exact(next_bytes.bytes()).value();
  return {std::move(predecessor_bytes), std::move(next_bytes), std::move(predecessor_view),
          std::move(next_view)};
}

TEST(TemporalManifestSchemaBindingTest, RequiresExactRetainedLineageCoverage) {
  const Fixture fixture;
  const schema::SchemaLineage lineage = LineageFixture{fixture}.lineage();
  EncodedTemporalManifest bytes = encode_manifest_v2_temporal(fixture.input(2U)).value();
  const DecodedTemporalManifestView manifest =
      decode_manifest_v2_temporal_exact(bytes.bytes()).value();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = fixture.tablet_id, .lineage = std::cref(lineage)}};
  EXPECT_TRUE(validate_manifest_v2_temporal_schema_binding(manifest, bindings).is_ok());
  EXPECT_EQ(validate_manifest_v2_temporal_schema_binding(manifest, {}).code(),
            common::StatusCode::kInvalidArgument);

  std::array sources{TemporalTabletSourceBinding{.tablet_id = fixture.tablet_id,
                                                 .commit_source = ManifestCommitSource::kWal,
                                                 .source_id = common::Uuid{fixture.wal.bytes}}};
  EXPECT_TRUE(validate_manifest_v2_temporal_source_binding(manifest, sources).is_ok());
  EXPECT_EQ(validate_manifest_v2_temporal_source_binding(manifest, {}).code(),
            common::StatusCode::kInvalidArgument);
  sources[0].commit_source = ManifestCommitSource::kRaft;
  EXPECT_EQ(validate_manifest_v2_temporal_source_binding(manifest, sources).code(),
            common::StatusCode::kInvalidArgument);
}

TEST(TemporalManifestTransitionTest, AcceptsAddOnlyMonotonicTemporalSuccessor) {
  const Fixture predecessor;
  Fixture next = predecessor;
  next.append_successor_state();
  const schema::SchemaLineage lineage = LineageFixture{predecessor}.lineage();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = predecessor.tablet_id, .lineage = std::cref(lineage)}};
  const DecodedPair pair = decode_pair(predecessor, next);
  EXPECT_TRUE(
      validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).is_ok());
}

TEST(TemporalManifestTransitionTest, RejectsGenerationDatabaseAndWalCheckpointChanges) {
  const Fixture predecessor;
  const schema::SchemaLineage lineage = LineageFixture{predecessor}.lineage();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = predecessor.tablet_id, .lineage = std::cref(lineage)}};

  DecodedPair pair = decode_pair(predecessor, predecessor, 4U);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  Fixture changed = predecessor;
  changed.database_id = test::make_id<DatabaseId>(99U);
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  changed = predecessor;
  changed.checkpoint = wal_checkpoint(changed.wal, 5U, 192U);
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  changed = predecessor;
  changed.checkpoint = std::nullopt;
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);
}

TEST(TemporalManifestTransitionTest, RejectsSourceReclaimAndSchemaRegression) {
  Fixture predecessor;
  predecessor.use_raft_source();
  Fixture changed = predecessor;
  changed.tablets[0].reclaim_position = 2U;
  const LineageFixture schemas{predecessor};
  const schema::SchemaLineage lineage = schemas.lineage();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = predecessor.tablet_id, .lineage = std::cref(lineage)}};
  DecodedPair pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  predecessor = Fixture{};
  changed = predecessor;
  const wal::WalId other_wal = wal_id(22U);
  changed.wal = other_wal;
  changed.checkpoint = wal_checkpoint(other_wal, 5U, 128U);
  changed.tablets[0].source_id = common::Uuid{other_wal.bytes};
  changed.parts[0].source_id = common::Uuid{other_wal.bytes};
  changed.retries[0].source_id = common::Uuid{other_wal.bytes};
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  predecessor.tablets[0].recovery_schema_id = schemas.v2_id;
  predecessor.tablets[0].recovery_schema_version = schema::SchemaVersion::initial().next().value();
  changed = predecessor;
  changed.tablets[0].recovery_schema_id = schemas.v1_id;
  changed.tablets[0].recovery_schema_version = schema::SchemaVersion::initial();
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);
}

TEST(TemporalManifestTransitionTest, RejectsHistoryAndRetryMutationOrRemoval) {
  const Fixture predecessor;
  const schema::SchemaLineage lineage = LineageFixture{predecessor}.lineage();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = predecessor.tablet_id, .lineage = std::cref(lineage)}};
  Fixture changed = predecessor;
  changed.parts[0].content_sha256 = test::make_digest(99U);
  DecodedPair pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  changed = predecessor;
  changed.parts.clear();
  changed.tablets[0].part_count = 0U;
  changed.tablets[0].durable_version_count = 0U;
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  changed = predecessor;
  changed.retries[0].request_digest = test::make_digest(98U);
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  changed = predecessor;
  changed.retries.clear();
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  changed = predecessor;
  changed.append_successor_state();
  changed.parts.back().minimum_commit_position = predecessor.tablets[0].durable_position;
  changed.parts.back().maximum_commit_position = predecessor.tablets[0].durable_position;
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  changed = predecessor;
  changed.append_successor_state();
  changed.retries.back().commit_position = predecessor.tablets[0].durable_position;
  pair = decode_pair(predecessor, changed);
  EXPECT_EQ(validate_manifest_v2_temporal_transition(pair.predecessor, pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::manifest
