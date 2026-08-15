#include "chronos/columnar/column_vector.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/projected_reader.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/query/temporal_cseg_snapshot.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

[[nodiscard]] cseg::CsegColumnDescriptor user_column(const schema::ColumnId column_id) {
  return {.column_id = column_id,
          .storage_kind = cseg::StorageKind::kUser,
          .logical_type = type(schema::LogicalTypeKind::kTimestampNs),
          .nullable = false,
          .event_time = true,
          .schema_ordinal = 0U,
          .ordering_ordinal = 0U};
}

[[nodiscard]] cseg::CsegColumnDescriptor system_column(const cseg::StorageKind kind,
                                                       const schema::LogicalTypeKind logical_type) {
  return {.column_id = std::nullopt,
          .storage_kind = kind,
          .logical_type = type(logical_type),
          .nullable = false,
          .event_time = false,
          .schema_ordinal = std::nullopt,
          .ordering_ordinal = std::nullopt};
}

[[nodiscard]] cseg::EncodedCsegPage page(const schema::LogicalType logical_type,
                                         const common::ByteView offsets,
                                         const common::ByteView values) {
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = 4U, .null_count = 0U},
      {.validity = {}, .offsets = offsets, .values = values});
  return cseg::encode_cseg_v1_page(*physical, cseg::PageCompression::kNone).value();
}

struct Fixture {
  schema::TableId table_id{id<schema::TableId>(1U)};
  schema::TabletId tablet_id{id<schema::TabletId>(2U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(3U)};
  schema::ColumnId event_id{id<schema::ColumnId>(4U)};
  common::Uuid source_id{[] {
    common::Uuid::Bytes bytes{};
    bytes.front() = std::byte{9U};
    return bytes;
  }()};
  std::shared_ptr<const schema::TableSchema> schema;
  std::vector<cseg::CsegColumnDescriptor> columns;
  std::vector<cseg::CsegGranuleDescriptor> granules;
  std::vector<cseg::EncodedCsegPage> pages;
  cseg::EncodedCsegPart encoded;

  Fixture()
      : schema(make_schema()), columns(make_columns()), granules{{.first_row = 0U,
                                                                  .row_count = 4U,
                                                                  .first_page_index = 0U,
                                                                  .minimum_event_time = 10,
                                                                  .maximum_event_time = 40}},
        pages(make_pages()), encoded(cseg::encode_cseg_v2_temporal_part(
                                         {.part_id = id<cseg::PartId>(5U),
                                          .table_id = table_id,
                                          .tablet_id = tablet_id,
                                          .schema_id = schema_id,
                                          .schema_version = schema::SchemaVersion::initial(),
                                          .row_count = 4U,
                                          .event_time_column_ordinal = 0U,
                                          .ordering_column_count = 1U,
                                          .minimum_event_time = 10,
                                          .maximum_event_time = 40,
                                          .columns = columns,
                                          .granules = granules,
                                          .pages = pages})
                                         .value()) {}

  [[nodiscard]] std::shared_ptr<const schema::TableSchema> make_schema() const {
    std::vector<schema::ColumnDefinition> definitions;
    definitions.push_back(
        schema::ColumnDefinition::create(event_id, "event_time",
                                         type(schema::LogicalTypeKind::kTimestampNs), false)
            .value());
    return std::make_shared<const schema::TableSchema>(
        schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                    std::nullopt, std::move(definitions),
                                    {.event_time_column = event_id,
                                     .physical_ordering_key = {event_id},
                                     .partition_columns = {event_id},
                                     .shard_key = {event_id},
                                     .deduplication_key = {event_id}})
            .value());
  }

  [[nodiscard]] std::vector<cseg::CsegColumnDescriptor> make_columns() const {
    return {
        user_column(event_id),
        system_column(cseg::StorageKind::kCommitSource, schema::LogicalTypeKind::kUInt8),
        system_column(cseg::StorageKind::kSourceId, schema::LogicalTypeKind::kUuid),
        system_column(cseg::StorageKind::kCommitPosition, schema::LogicalTypeKind::kUInt64),
        system_column(cseg::StorageKind::kTemporalRowOrdinal, schema::LogicalTypeKind::kUInt32),
        system_column(cseg::StorageKind::kTemporalOperation, schema::LogicalTypeKind::kUInt8),
        system_column(cseg::StorageKind::kLogicalIdentity, schema::LogicalTypeKind::kBinary),
        system_column(cseg::StorageKind::kReceiveTime, schema::LogicalTypeKind::kTimestampNs),
        system_column(cseg::StorageKind::kSystemCommitTime, schema::LogicalTypeKind::kTimestampNs)};
  }

  [[nodiscard]] std::vector<cseg::EncodedCsegPage> make_pages() const {
    std::vector<std::byte> event_times;
    std::vector<std::byte> source_ids;
    std::vector<std::byte> positions;
    std::vector<std::byte> ordinals;
    std::vector<std::byte> receive_times;
    std::vector<std::byte> commit_times;
    std::vector<std::byte> identity_offsets;
    for (const std::int64_t value : {10, 20, 30, 40}) {
      append_le(event_times, value);
    }
    for (std::uint32_t row = 0U; row < 4U; ++row) {
      const auto commit_position = static_cast<std::uint64_t>(row) + 1U;
      const auto time_offset = static_cast<std::int64_t>(row) * 10;
      source_ids.insert(source_ids.end(), source_id.bytes().begin(), source_id.bytes().end());
      append_le(positions, commit_position);
      append_le(ordinals, std::uint32_t{0U});
      append_le(receive_times, std::int64_t{90} + time_offset);
      append_le(commit_times, std::int64_t{100} + time_offset);
      append_le(identity_offsets, row);
    }
    append_le(identity_offsets, std::uint32_t{4U});
    const std::vector<std::byte> sources(4U, std::byte{1U});
    const std::vector<std::byte> operations{std::byte{1U}, std::byte{1U}, std::byte{2U},
                                            std::byte{4U}};
    const std::vector<std::byte> identities{std::byte{'a'}, std::byte{'b'}, std::byte{'a'},
                                            std::byte{'b'}};
    std::vector<cseg::EncodedCsegPage> output;
    output.reserve(columns.size());
    output.push_back(page(columns[0].logical_type, {}, event_times));
    output.push_back(page(columns[1].logical_type, {}, sources));
    output.push_back(page(columns[2].logical_type, {}, source_ids));
    output.push_back(page(columns[3].logical_type, {}, positions));
    output.push_back(page(columns[4].logical_type, {}, ordinals));
    output.push_back(page(columns[5].logical_type, {}, operations));
    output.push_back(page(columns[6].logical_type, identity_offsets, identities));
    output.push_back(page(columns[7].logical_type, {}, receive_times));
    output.push_back(page(columns[8].logical_type, {}, commit_times));
    return output;
  }

  [[nodiscard]] cseg::ProjectedCsegGranule projected() const {
    const schema::SchemaLineage lineage = schema::SchemaLineage::create(*schema).value();
    const auto reader = cseg::open_cseg_v2_temporal_projected_reader_exact(encoded.bytes(), lineage,
                                                                           schema_id, tablet_id);
    const std::array<std::uint32_t, 1U> projection{0U};
    return reader->read_granule(0U, projection).value();
  }
};

[[nodiscard]] std::int64_t event_time(const ScalarInputRow& row) {
  return std::get<std::int64_t>(row.columns[0].storage());
}

TEST(TemporalCsegSnapshotTest, ResolvesCurrentAndAsOfWinnersAndRemovesTombstones) {
  Fixture fixture;
  cseg::ProjectedCsegGranule granule = fixture.projected();
  const std::array<const cseg::ProjectedCsegGranule*, 1U> granules{&granule};
  const TemporalCsegSourceLineage lineage{cseg::temporal_format::CommitSource::kWal,
                                          fixture.source_id};
  const auto current =
      resolve_cseg_v2_temporal_snapshot(fixture.schema, granules, lineage, std::nullopt);
  ASSERT_TRUE(current.has_value()) << current.error().to_string();
  ASSERT_EQ((*current)->rows().size(), 1U);
  EXPECT_EQ(event_time((*current)->rows()[0]), 30);
  EXPECT_EQ((*current)->committed_position(), 4U);

  const auto historical = resolve_cseg_v2_temporal_snapshot(fixture.schema, granules, lineage, 115);
  ASSERT_TRUE(historical.has_value());
  ASSERT_EQ((*historical)->rows().size(), 2U);
  EXPECT_EQ(event_time((*historical)->rows()[0]), 10);
  EXPECT_EQ(event_time((*historical)->rows()[1]), 20);
  EXPECT_EQ((*historical)->committed_position(), 2U);
}

TEST(TemporalCsegSnapshotTest, RejectsForeignLineageAndResourceExcess) {
  Fixture fixture;
  cseg::ProjectedCsegGranule granule = fixture.projected();
  const std::array<const cseg::ProjectedCsegGranule*, 1U> granules{&granule};
  common::Uuid::Bytes foreign_bytes{};
  foreign_bytes.front() = std::byte{8U};
  const auto foreign = resolve_cseg_v2_temporal_snapshot(
      fixture.schema, granules,
      {cseg::temporal_format::CommitSource::kWal, common::Uuid{foreign_bytes}}, std::nullopt);
  ASSERT_FALSE(foreign.has_value());
  EXPECT_EQ(foreign.error().code(), common::StatusCode::kInvalidArgument);

  const auto limited = resolve_cseg_v2_temporal_snapshot(
      fixture.schema, granules, {cseg::temporal_format::CommitSource::kWal, fixture.source_id},
      std::nullopt,
      {.maximum_versions = 3U, .maximum_output_rows = 2U, .maximum_identity_bytes = 1U});
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(TemporalManifestCsegSnapshotTest, ResolvesGenerationPartViewsWithPruningAndBounds) {
  Fixture fixture;
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(*fixture.schema).value();
  const auto descriptor = manifest::describe_manifest_v2_temporal_part_image(
      fixture.encoded.bytes(), *fixture.schema, fixture.tablet_id,
      cseg::temporal_format::CommitSource::kWal, fixture.source_id);
  ASSERT_TRUE(descriptor.has_value()) << descriptor.error().to_string();
  const std::array parts{
      TemporalManifestCsegPartView{.descriptor = &*descriptor, .bytes = fixture.encoded.bytes()}};
  const TemporalCsegSourceLineage source{cseg::temporal_format::CommitSource::kWal,
                                         fixture.source_id};
  const manifest::TemporalTabletDescriptor tablet{
      .table_id = fixture.table_id,
      .tablet_id = fixture.tablet_id,
      .recovery_schema_id = fixture.schema_id,
      .recovery_schema_version = schema::SchemaVersion::initial(),
      .source_id = fixture.source_id,
      .durable_position = descriptor->maximum_commit_position,
      .reclaim_position = 0U,
      .first_part_index = 0U,
      .part_count = 1U,
      .durable_version_count = descriptor->row_count,
      .commit_source = cseg::temporal_format::CommitSource::kWal};

  const auto current = resolve_manifest_v2_temporal_tablet_snapshot(fixture.schema, lineage, tablet,
                                                                    parts, source, std::nullopt);
  ASSERT_TRUE(current.has_value()) << current.error().to_string();
  ASSERT_EQ((*current)->rows().size(), 1U);
  EXPECT_EQ(event_time((*current)->rows()[0]), 30);

  const auto historical = resolve_manifest_v2_temporal_tablet_snapshot(fixture.schema, lineage,
                                                                       tablet, parts, source, 115);
  ASSERT_TRUE(historical.has_value()) << historical.error().to_string();
  ASSERT_EQ((*historical)->rows().size(), 2U);
  EXPECT_EQ(event_time((*historical)->rows()[0]), 10);
  EXPECT_EQ(event_time((*historical)->rows()[1]), 20);

  auto restored = restore_manifest_v2_temporal_tablet_history(
      fixture.schema, lineage, tablet, parts, source, descriptor->minimum_system_time);
  ASSERT_TRUE(restored.has_value()) << restored.error().to_string();
  EXPECT_EQ((*restored)->version_count(), 4U);
  EXPECT_EQ((*restored)->latest_commit_position(), 4U);
  const auto restored_current = (*restored)->resolve(fixture.schema, std::nullopt);
  ASSERT_TRUE(restored_current.has_value());
  ASSERT_EQ((*restored_current)->rows().size(), 1U);
  EXPECT_EQ(event_time((*restored_current)->rows()[0]), 30);
  const auto restored_historical = (*restored)->resolve(fixture.schema, 115);
  ASSERT_TRUE(restored_historical.has_value());
  ASSERT_EQ((*restored_historical)->rows().size(), 2U);
  EXPECT_EQ(event_time((*restored_historical)->rows()[0]), 10);
  EXPECT_EQ(event_time((*restored_historical)->rows()[1]), 20);
  const auto restored_expired =
      (*restored)->resolve(fixture.schema, descriptor->minimum_system_time - 1);
  ASSERT_FALSE(restored_expired.has_value());
  EXPECT_EQ(restored_expired.error().code(), common::StatusCode::kNotFound);

  const auto pruned = resolve_manifest_v2_temporal_tablet_snapshot(
      fixture.schema, lineage, tablet, parts, source, descriptor->minimum_system_time - 1);
  ASSERT_FALSE(pruned.has_value());
  EXPECT_EQ(pruned.error().code(), common::StatusCode::kNotFound);

  TemporalManifestCsegResolutionLimits limits;
  limits.maximum_decoded_buffer_bytes = 1U;
  EXPECT_EQ(resolve_manifest_v2_temporal_tablet_snapshot(fixture.schema, lineage, tablet, parts,
                                                         source, std::nullopt, limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  TemporalManifestCsegResolutionLimits restore_limits;
  restore_limits.resolution.maximum_versions = 3U;
  EXPECT_EQ(restore_manifest_v2_temporal_tablet_history(fixture.schema, lineage, tablet, parts,
                                                        source, descriptor->minimum_system_time, {},
                                                        restore_limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(restore_manifest_v2_temporal_tablet_history(fixture.schema, lineage, tablet, parts,
                                                        source, descriptor->maximum_system_time + 1)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  manifest::TemporalTabletDescriptor incomplete_tablet = tablet;
  ++incomplete_tablet.durable_version_count;
  EXPECT_EQ(resolve_manifest_v2_temporal_tablet_snapshot(fixture.schema, lineage, incomplete_tablet,
                                                         parts, source, std::nullopt)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(restore_manifest_v2_temporal_tablet_history(fixture.schema, lineage, incomplete_tablet,
                                                        parts, source,
                                                        descriptor->minimum_system_time)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  manifest::TemporalPartDescriptor foreign = *descriptor;
  foreign.source_id = common::Uuid{id<schema::SchemaId>(99U).bytes()};
  const std::array foreign_parts{
      TemporalManifestCsegPartView{.descriptor = &foreign, .bytes = fixture.encoded.bytes()}};
  EXPECT_EQ(resolve_manifest_v2_temporal_tablet_snapshot(fixture.schema, lineage, tablet,
                                                         foreign_parts, source, std::nullopt)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
