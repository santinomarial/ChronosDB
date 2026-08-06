#include "chronos/common/status.hpp"
#include "chronos/manifest/validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"
#include "manifest/manifest_test_support.hpp"

#include <array>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

struct LineageFixture {
  explicit LineageFixture(const test::ManifestFixture& manifest)
      : table_id(manifest.tablets.front().table_id),
        v1_id(manifest.tablets.front().recovery_schema_id) {}

  [[nodiscard]] schema::TableSchema v1() const {
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(table_id, v1_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(columns), roles())
        .value();
  }

  [[nodiscard]] schema::TableSchema v2() const {
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "renamed_event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(table_id, v2_id,
                                       schema::SchemaVersion::initial().next().value(), v1_id,
                                       std::move(columns), roles())
        .value();
  }

  [[nodiscard]] schema::SchemaLineage lineage() const {
    schema::SchemaLineage result = schema::SchemaLineage::create(v1()).value();
    EXPECT_TRUE(result.append(v2()).is_ok());
    return result;
  }

  [[nodiscard]] schema::TableSchemaRoles roles() const {
    return {.event_time_column = event_id,
            .physical_ordering_key = {event_id},
            .partition_columns = {event_id},
            .shard_key = {event_id},
            .deduplication_key = {}};
  }

  schema::TableId table_id;
  schema::SchemaId v1_id;
  schema::SchemaId v2_id{test::make_id<schema::SchemaId>(0xd1U)};
  schema::ColumnId event_id{test::make_id<schema::ColumnId>(0xd2U)};
};

struct DecodedPair {
  EncodedManifest predecessor_bytes;
  EncodedManifest next_bytes;
  DecodedManifestView predecessor;
  DecodedManifestView next;
};

[[nodiscard]] DecodedPair decode_pair(const ManifestEncodeInput& predecessor,
                                      const ManifestEncodeInput& next) {
  EncodedManifest predecessor_bytes = encode_manifest_v1(predecessor).value();
  EncodedManifest next_bytes = encode_manifest_v1(next).value();
  DecodedManifestView predecessor_view =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  DecodedManifestView next_view = decode_manifest_v1_exact(next_bytes.bytes()).value();
  return {std::move(predecessor_bytes), std::move(next_bytes), std::move(predecessor_view),
          std::move(next_view)};
}

TEST(ManifestSchemaBindingTest, BindsEveryTabletRecoveryAndPartSchemaExactly) {
  const test::ManifestFixture fixture;
  const LineageFixture schemas{fixture};
  const schema::SchemaLineage lineage = schemas.lineage();
  const EncodedManifest encoded = test::encode_fixture(fixture);
  const DecodedManifestView decoded = decode_manifest_v1_exact(encoded.bytes()).value();
  const std::array bindings{TabletSchemaBinding{.tablet_id = fixture.tablets.front().tablet_id,
                                                .lineage = std::cref(lineage)}};
  EXPECT_TRUE(validate_manifest_v1_schema_binding(decoded, bindings).is_ok());
}

TEST(ManifestSchemaBindingTest, RejectsMissingWrongAndUnsortedCatalogBindings) {
  const test::ManifestFixture fixture;
  const LineageFixture schemas{fixture};
  const schema::SchemaLineage lineage = schemas.lineage();
  const EncodedManifest encoded = test::encode_fixture(fixture);
  const DecodedManifestView decoded = decode_manifest_v1_exact(encoded.bytes()).value();
  EXPECT_EQ(validate_manifest_v1_schema_binding(decoded, {}).code(),
            common::StatusCode::kInvalidArgument);

  const test::ManifestFixture other{2U};
  const schema::SchemaLineage wrong_lineage = LineageFixture{other}.lineage();
  const std::array wrong{TabletSchemaBinding{.tablet_id = fixture.tablets.front().tablet_id,
                                             .lineage = std::cref(wrong_lineage)}};
  EXPECT_EQ(validate_manifest_v1_schema_binding(decoded, wrong).code(),
            common::StatusCode::kInvalidArgument);

  const std::array extra{
      TabletSchemaBinding{.tablet_id = fixture.tablets.front().tablet_id,
                          .lineage = std::cref(lineage)},
      TabletSchemaBinding{.tablet_id = test::make_id<schema::TabletId>(0xffU),
                          .lineage = std::cref(lineage)},
  };
  EXPECT_EQ(validate_manifest_v1_schema_binding(decoded, extra).code(),
            common::StatusCode::kInvalidArgument);
}

TEST(ManifestTransitionTest, AcceptsMonotonicSchemaBoundaryPartAndRetrySupersets) {
  test::ManifestFixture old_fixture;
  test::ManifestFixture new_fixture = old_fixture;
  const LineageFixture schemas{old_fixture};
  const schema::SchemaLineage lineage = schemas.lineage();

  new_fixture.tablets.front().recovery_schema_id = schemas.v2_id;
  new_fixture.tablets.front().recovery_schema_version =
      schema::SchemaVersion::initial().next().value();
  ++new_fixture.tablets.front().durable_record_sequence;
  new_fixture.parts.push_back(PartDescriptor{
      .part_id = test::make_id<cseg::PartId>(0xfeU),
      .table_id = new_fixture.tablets.front().table_id,
      .tablet_id = new_fixture.tablets.front().tablet_id,
      .schema_id = schemas.v2_id,
      .schema_version = schema::SchemaVersion::initial().next().value(),
      .file_length = 1'208U,
      .row_count = 1U,
      .minimum_record_sequence = new_fixture.tablets.front().durable_record_sequence,
      .maximum_record_sequence = new_fixture.tablets.front().durable_record_sequence,
      .minimum_event_time = 3'000,
      .maximum_event_time = 3'000,
  });
  new_fixture.tablets.front().part_count = 2U;
  ++new_fixture.tablets.front().durable_row_count;
  ManifestEncodeInput old_input = old_fixture.input(2U);
  ManifestEncodeInput new_input = new_fixture.input(3U);
  new_input.reclaim_checkpoint = {.record_sequence = 6U, .segment_number = 1U, .byte_offset = 136U};
  DecodedPair pair = decode_pair(old_input, new_input);
  const std::array bindings{TabletSchemaBinding{.tablet_id = old_fixture.tablets.front().tablet_id,
                                                .lineage = std::cref(lineage)}};
  EXPECT_TRUE(validate_manifest_v1_transition(pair.predecessor, pair.next, bindings).is_ok());
}

TEST(ManifestTransitionTest, RejectsPartOrRetryRemoval) {
  const test::ManifestFixture old_fixture;
  const LineageFixture schemas{old_fixture};
  const schema::SchemaLineage lineage = schemas.lineage();
  const std::array bindings{TabletSchemaBinding{.tablet_id = old_fixture.tablets.front().tablet_id,
                                                .lineage = std::cref(lineage)}};

  test::ManifestFixture without_part = old_fixture;
  without_part.parts.clear();
  without_part.tablets.front().part_count = 0U;
  without_part.tablets.front().durable_row_count = 0U;
  DecodedPair part_pair = decode_pair(old_fixture.input(2U), without_part.input(3U));
  EXPECT_EQ(validate_manifest_v1_transition(part_pair.predecessor, part_pair.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  test::ManifestFixture without_retry = old_fixture;
  without_retry.retries.clear();
  DecodedPair retry_pair = decode_pair(old_fixture.input(2U), without_retry.input(3U));
  EXPECT_EQ(
      validate_manifest_v1_transition(retry_pair.predecessor, retry_pair.next, bindings).code(),
      common::StatusCode::kInvalidArgument);
}

TEST(ManifestTransitionTest, RejectsBackwardTabletAndSidewaysCheckpointMovement) {
  test::ManifestFixture predecessor_fixture;
  predecessor_fixture.tablets.front().durable_record_sequence += 2U;
  const test::ManifestFixture next_fixture;
  const LineageFixture schemas{predecessor_fixture};
  const schema::SchemaLineage lineage = schemas.lineage();
  const std::array bindings{TabletSchemaBinding{
      .tablet_id = predecessor_fixture.tablets.front().tablet_id, .lineage = std::cref(lineage)}};
  DecodedPair backward = decode_pair(predecessor_fixture.input(2U), next_fixture.input(3U));
  EXPECT_EQ(validate_manifest_v1_transition(backward.predecessor, backward.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  ManifestEncodeInput old_input = next_fixture.input(2U);
  ManifestEncodeInput new_input = next_fixture.input(3U);
  new_input.reclaim_checkpoint.byte_offset = 136U;
  DecodedPair sideways = decode_pair(old_input, new_input);
  EXPECT_EQ(validate_manifest_v1_transition(sideways.predecessor, sideways.next, bindings).code(),
            common::StatusCode::kInvalidArgument);

  new_input = next_fixture.input(3U);
  new_input.reclaim_checkpoint.record_sequence = 6U;
  DecodedPair stationary = decode_pair(old_input, new_input);
  EXPECT_EQ(
      validate_manifest_v1_transition(stationary.predecessor, stationary.next, bindings).code(),
      common::StatusCode::kInvalidArgument);
}

TEST(ManifestTransitionTest, RejectsSchemaRegressionAndStorageIdentityChange) {
  test::ManifestFixture old_fixture;
  test::ManifestFixture new_fixture = old_fixture;
  const LineageFixture schemas{old_fixture};
  const schema::SchemaLineage lineage = schemas.lineage();
  old_fixture.tablets.front().recovery_schema_id = schemas.v2_id;
  old_fixture.tablets.front().recovery_schema_version =
      schema::SchemaVersion::initial().next().value();
  const std::array bindings{TabletSchemaBinding{.tablet_id = old_fixture.tablets.front().tablet_id,
                                                .lineage = std::cref(lineage)}};
  DecodedPair regression = decode_pair(old_fixture.input(2U), new_fixture.input(3U));
  EXPECT_EQ(
      validate_manifest_v1_transition(regression.predecessor, regression.next, bindings).code(),
      common::StatusCode::kInvalidArgument);

  old_fixture = new_fixture;
  new_fixture.database_id = test::make_id<DatabaseId>(0xffU);
  DecodedPair identity = decode_pair(old_fixture.input(2U), new_fixture.input(3U));
  EXPECT_EQ(validate_manifest_v1_transition(identity.predecessor, identity.next, bindings).code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::manifest
