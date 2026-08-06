#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/manifest/compaction.hpp"
#include "chronos/manifest/validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

using cseg::test::identifier;

struct Fixture {
  schema::TableId table_id{identifier<schema::TableId>(2U)};
  schema::TabletId tablet_id{identifier<schema::TabletId>(3U)};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(4U)};
  schema::ColumnId event_id{identifier<schema::ColumnId>(5U)};
  wal::WalId wal_id{};
  schema::TableSchema schema;

  Fixture() : schema(make_schema()) {
    wal_id.bytes = identifier<schema::SchemaId>(0x70U).bytes();
  }

  [[nodiscard]] schema::TableSchema make_schema() const {
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }
};

[[nodiscard]] cseg::EncodedCsegPart make_part(const Fixture& fixture, const std::uint8_t part_seed,
                                              const std::array<std::int64_t, 2U> events,
                                              const std::array<std::uint64_t, 2U> sequences) {
  constexpr std::uint32_t kRows = 2U;
  const auto timestamp = schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value();
  const auto uuid = schema::LogicalType::create(schema::LogicalTypeKind::kUuid).value();
  const auto uint64 = schema::LogicalType::create(schema::LogicalTypeKind::kUInt64).value();
  const auto uint32 = schema::LogicalType::create(schema::LogicalTypeKind::kUInt32).value();
  const auto uint8 = schema::LogicalType::create(schema::LogicalTypeKind::kUInt8).value();
  std::vector<std::byte> event_bytes;
  std::vector<std::byte> wal_bytes;
  std::vector<std::byte> sequence_bytes;
  std::vector<std::byte> ordinal_bytes;
  for (std::size_t row = 0U; row < events.size(); ++row) {
    cseg::test::append_little_endian(event_bytes, events[row]);
    wal_bytes.insert(wal_bytes.end(), fixture.wal_id.bytes.begin(), fixture.wal_id.bytes.end());
    cseg::test::append_little_endian(sequence_bytes, sequences[row]);
    cseg::test::append_little_endian(ordinal_bytes, std::uint32_t{0U});
  }
  const std::vector<std::byte> operations(kRows, std::byte{cseg::format::kAppendRowsOperation});
  std::vector<cseg::EncodedCsegPage> pages;
  pages.push_back(
      cseg::test::encode_fixed_page(timestamp, kRows, event_bytes, cseg::PageCompression::kNone));
  pages.push_back(
      cseg::test::encode_fixed_page(uuid, kRows, wal_bytes, cseg::PageCompression::kNone));
  pages.push_back(
      cseg::test::encode_fixed_page(uint64, kRows, sequence_bytes, cseg::PageCompression::kNone));
  pages.push_back(
      cseg::test::encode_fixed_page(uint32, kRows, ordinal_bytes, cseg::PageCompression::kNone));
  pages.push_back(
      cseg::test::encode_fixed_page(uint8, kRows, operations, cseg::PageCompression::kNone));
  const std::vector<cseg::CsegColumnDescriptor> columns{
      {.column_id = fixture.event_id,
       .storage_kind = cseg::StorageKind::kUser,
       .logical_type = timestamp,
       .nullable = false,
       .event_time = true,
       .schema_ordinal = 0U,
       .ordering_ordinal = 0U},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kWalId,
       .logical_type = uuid,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kRecordSequence,
       .logical_type = uint64,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kRowOrdinal,
       .logical_type = uint32,
       .nullable = false},
      {.column_id = std::nullopt,
       .storage_kind = cseg::StorageKind::kOperation,
       .logical_type = uint8,
       .nullable = false}};
  const std::vector<cseg::CsegGranuleDescriptor> granules{{
      .first_row = 0U,
      .row_count = kRows,
      .first_page_index = 0U,
      .minimum_event_time = events.front(),
      .maximum_event_time = events.back(),
  }};
  return cseg::encode_cseg_v1_part({.part_id = identifier<cseg::PartId>(part_seed),
                                    .table_id = fixture.table_id,
                                    .tablet_id = fixture.tablet_id,
                                    .schema_id = fixture.schema_id,
                                    .schema_version = schema::SchemaVersion::initial(),
                                    .row_count = kRows,
                                    .event_time_column_ordinal = 0U,
                                    .ordering_column_count = 1U,
                                    .minimum_event_time = events.front(),
                                    .maximum_event_time = events.back(),
                                    .columns = columns,
                                    .granules = granules,
                                    .pages = pages})
      .value();
}

struct Inputs {
  cseg::EncodedCsegPart first;
  cseg::EncodedCsegPart second;
  std::array<CompactionPartImage, 2U> images;

  explicit Inputs(const Fixture& fixture)
      : first(make_part(fixture, 0x10U, {-10, 10}, {7U, 9U})),
        second(make_part(fixture, 0x20U, {0, 20}, {8U, 10U})),
        images{{{.part_id = identifier<cseg::PartId>(0x10U), .bytes = first.bytes()},
                {.part_id = identifier<cseg::PartId>(0x20U), .bytes = second.bytes()}}} {}
};

[[nodiscard]] PartDescriptor
descriptor(const Fixture& fixture, const cseg::PartId& part_id, const cseg::EncodedCsegPart& part,
           const std::uint64_t minimum_sequence, const std::uint64_t maximum_sequence,
           const std::int64_t minimum_event, const std::int64_t maximum_event) {
  return {.part_id = part_id,
          .table_id = fixture.table_id,
          .tablet_id = fixture.tablet_id,
          .schema_id = fixture.schema_id,
          .schema_version = schema::SchemaVersion::initial(),
          .file_length = part.size(),
          .row_count = 2U,
          .minimum_record_sequence = minimum_sequence,
          .maximum_record_sequence = maximum_sequence,
          .minimum_event_time = minimum_event,
          .maximum_event_time = maximum_event};
}

TEST(AppendOnlyCompactionTest, DeterministicallyMergesInterleavedPartsAndProvesOutput) {
  const Fixture fixture;
  const Inputs inputs{fixture};
  const cseg::PartId output_id = identifier<cseg::PartId>(0x80U);
  const AppendOnlyCompactionRequest request{
      .inputs = inputs.images,
      .schema = std::cref(fixture.schema),
      .tablet_id = fixture.tablet_id,
      .wal_id = fixture.wal_id,
      .output_part_id = output_id,
      .compression = cseg::PageCompression::kZstd,
  };
  const common::Result<EncodedCompactionPart> first = merge_append_only_cseg_v1(request);
  const common::Result<EncodedCompactionPart> second = merge_append_only_cseg_v1(request);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(first->descriptor.part_id, output_id);
  EXPECT_EQ(first->descriptor.row_count, 4U);
  EXPECT_EQ(first->descriptor.minimum_record_sequence, 7U);
  EXPECT_EQ(first->descriptor.maximum_record_sequence, 10U);
  EXPECT_EQ(first->descriptor.minimum_event_time, -10);
  EXPECT_EQ(first->descriptor.maximum_event_time, 20);
  EXPECT_TRUE(std::ranges::equal(first->encoded_part.bytes(), second->encoded_part.bytes()));

  const cseg::CsegPartDecodeResult decoded =
      cseg::decode_cseg_v1_part_exact(first->encoded_part.bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(cseg::validate_cseg_v1_part(*decoded, fixture.schema, fixture.tablet_id).is_ok());
  const std::array output{
      CompactionPartImage{.part_id = output_id, .bytes = first->encoded_part.bytes()}};
  EXPECT_TRUE(validate_append_only_cseg_v1_equivalence(inputs.images, output, fixture.schema,
                                                       fixture.tablet_id, fixture.wal_id)
                  .is_ok());
}

TEST(AppendOnlyCompactionTest, RejectsDuplicateTupleFreshnessCorruptionAndLimits) {
  const Fixture fixture;
  const Inputs inputs{fixture};
  AppendOnlyCompactionRequest request{
      .inputs = inputs.images,
      .schema = std::cref(fixture.schema),
      .tablet_id = fixture.tablet_id,
      .wal_id = fixture.wal_id,
      .output_part_id = identifier<cseg::PartId>(0x80U),
  };
  request.output_part_id = inputs.images.front().part_id;
  EXPECT_EQ(merge_append_only_cseg_v1(request).error().code(),
            common::StatusCode::kInvalidArgument);

  request.output_part_id = identifier<cseg::PartId>(0x80U);
  request.limits.max_rows = 3U;
  EXPECT_EQ(merge_append_only_cseg_v1(request).error().code(),
            common::StatusCode::kResourceExhausted);

  request.limits.max_rows = 4U;
  request.limits.max_materialized_page_bytes = 1U;
  EXPECT_EQ(merge_append_only_cseg_v1(request).error().code(),
            common::StatusCode::kResourceExhausted);

  const auto duplicate_second = make_part(fixture, 0x20U, {-10, 10}, {7U, 9U});
  const std::array duplicate_images{
      inputs.images.front(),
      CompactionPartImage{.part_id = identifier<cseg::PartId>(0x20U),
                          .bytes = duplicate_second.bytes()},
  };
  request.inputs = duplicate_images;
  request.limits.max_materialized_page_bytes = 512ULL * 1'024ULL * 1'024ULL;
  EXPECT_EQ(merge_append_only_cseg_v1(request).error().code(), common::StatusCode::kCorruption);

  std::vector<std::byte> corrupt(inputs.first.bytes().begin(), inputs.first.bytes().end());
  corrupt.back() ^= std::byte{1U};
  const std::array corrupt_images{
      CompactionPartImage{.part_id = inputs.images.front().part_id, .bytes = corrupt},
      inputs.images.back(),
  };
  request.inputs = corrupt_images;
  EXPECT_EQ(merge_append_only_cseg_v1(request).error().code(), common::StatusCode::kCorruption);
}

TEST(AppendOnlyCompactionTest, BuildsExactReplacementManifestOnlyAfterFullEquivalence) {
  const Fixture fixture;
  const Inputs inputs{fixture};
  const std::array predecessor_parts{
      descriptor(fixture, inputs.images[0].part_id, inputs.first, 7U, 9U, -10, 10),
      descriptor(fixture, inputs.images[1].part_id, inputs.second, 8U, 10U, 0, 20),
  };
  const std::array tablets{
      TabletDescriptor{.table_id = fixture.table_id,
                       .tablet_id = fixture.tablet_id,
                       .recovery_schema_id = fixture.schema_id,
                       .recovery_schema_version = schema::SchemaVersion::initial(),
                       .durable_record_sequence = 10U,
                       .first_part_index = 0U,
                       .part_count = predecessor_parts.size(),
                       .durable_row_count = 4U}};
  const EncodedManifest predecessor_bytes =
      encode_manifest_v1(
          {.generation = 1U,
           .database_id = identifier<DatabaseId>(0x90U),
           .wal_id = fixture.wal_id,
           .reclaim_checkpoint = {.record_sequence = 6U, .segment_number = 1U, .byte_offset = 128U},
           .tablets = tablets,
           .parts = predecessor_parts,
           .retries = {}})
          .value();
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(fixture.schema).value();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = fixture.tablet_id, .lineage = std::cref(lineage)}};
  const AppendOnlyCompactionRequest merge_request{
      .inputs = inputs.images,
      .schema = std::cref(fixture.schema),
      .tablet_id = fixture.tablet_id,
      .wal_id = fixture.wal_id,
      .output_part_id = identifier<cseg::PartId>(0x80U),
  };
  const common::Result<EncodedCompactionPart> merged = merge_append_only_cseg_v1(merge_request);
  ASSERT_TRUE(merged.has_value());
  const common::Result<EncodedManifest> successor =
      build_manifest_v1_for_append_only_compaction({.predecessor = predecessor,
                                                    .inputs = inputs.images,
                                                    .output = std::cref(*merged),
                                                    .schema = std::cref(fixture.schema),
                                                    .schema_bindings = bindings,
                                                    .equivalence_limits = {},
                                                    .part_validation_limits = {}});
  ASSERT_TRUE(successor.has_value()) << successor.error().to_string();
  const ManifestDecodeResult decoded = decode_manifest_v1_exact(successor->bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->generation(), 2U);
  EXPECT_EQ(decoded->reclaim_checkpoint(), predecessor.reclaim_checkpoint());
  ASSERT_EQ(decoded->parts().size(), 1U);
  EXPECT_EQ(decoded->parts().front(), merged->descriptor);
  ASSERT_EQ(decoded->tablets().size(), 1U);
  EXPECT_EQ(decoded->tablets().front().durable_record_sequence, 10U);
  EXPECT_EQ(decoded->tablets().front().durable_row_count, 4U);
  const std::array input_ids{inputs.images[0].part_id, inputs.images[1].part_id};
  const std::array output_ids{merged->descriptor.part_id};
  EXPECT_TRUE(validate_manifest_v1_compaction_transition(predecessor, *decoded, bindings,
                                                         {.tablet_id = fixture.tablet_id,
                                                          .input_part_ids = input_ids,
                                                          .output_part_ids = output_ids})
                  .is_ok());

  EXPECT_EQ(build_manifest_v1_for_append_only_compaction({.predecessor = predecessor,
                                                          .inputs = inputs.images,
                                                          .output = std::cref(*merged),
                                                          .schema = std::cref(fixture.schema),
                                                          .schema_bindings = {},
                                                          .equivalence_limits = {},
                                                          .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::manifest
