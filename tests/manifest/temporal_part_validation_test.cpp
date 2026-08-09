#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/page_codec.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] common::Uuid source_id(const std::uint8_t seed = 8U) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
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

[[nodiscard]] cseg::CsegColumnDescriptor user_column() {
  return {.column_id = id<schema::ColumnId>(5U),
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

[[nodiscard]] cseg::EncodedCsegPage encode_page(const schema::LogicalType logical_type,
                                                const common::ByteView offsets,
                                                const common::ByteView values) {
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = 2U, .null_count = 0U},
      {.validity = {}, .offsets = offsets, .values = values});
  return cseg::encode_cseg_v1_page(*physical, cseg::PageCompression::kNone).value();
}

struct TemporalPartFixture {
  cseg::PartId part_id{id<cseg::PartId>(1U)};
  schema::TableId table_id{id<schema::TableId>(2U)};
  schema::TabletId tablet_id{id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(4U)};
  common::Uuid source{source_id()};
  std::vector<cseg::CsegColumnDescriptor> columns{
      user_column(),
      system_column(cseg::StorageKind::kCommitSource, schema::LogicalTypeKind::kUInt8),
      system_column(cseg::StorageKind::kSourceId, schema::LogicalTypeKind::kUuid),
      system_column(cseg::StorageKind::kCommitPosition, schema::LogicalTypeKind::kUInt64),
      system_column(cseg::StorageKind::kTemporalRowOrdinal, schema::LogicalTypeKind::kUInt32),
      system_column(cseg::StorageKind::kTemporalOperation, schema::LogicalTypeKind::kUInt8),
      system_column(cseg::StorageKind::kLogicalIdentity, schema::LogicalTypeKind::kBinary),
      system_column(cseg::StorageKind::kReceiveTime, schema::LogicalTypeKind::kTimestampNs),
      system_column(cseg::StorageKind::kSystemCommitTime, schema::LogicalTypeKind::kTimestampNs),
  };
  std::vector<cseg::CsegGranuleDescriptor> granules{{.first_row = 0U,
                                                     .row_count = 2U,
                                                     .first_page_index = 0U,
                                                     .minimum_event_time = 10,
                                                     .maximum_event_time = 20}};
  std::vector<cseg::EncodedCsegPage> pages;

  explicit TemporalPartFixture(const bool mixed_source_identity = false) {
    std::vector<std::byte> event_times;
    append_le(event_times, std::int64_t{10});
    append_le(event_times, std::int64_t{20});
    const std::vector<std::byte> sources{
        std::byte{static_cast<std::uint8_t>(ManifestCommitSource::kWal)},
        std::byte{static_cast<std::uint8_t>(ManifestCommitSource::kWal)}};
    std::vector<std::byte> source_ids;
    source_ids.insert(source_ids.end(), source.bytes().begin(), source.bytes().end());
    const common::Uuid second_source = mixed_source_identity ? source_id(9U) : source;
    source_ids.insert(source_ids.end(), second_source.bytes().begin(), second_source.bytes().end());
    std::vector<std::byte> positions;
    append_le(positions, std::uint64_t{7U});
    append_le(positions, std::uint64_t{9U});
    std::vector<std::byte> ordinals;
    append_le(ordinals, std::uint32_t{0U});
    append_le(ordinals, std::uint32_t{1U});
    const std::vector<std::byte> operations{
        std::byte{static_cast<std::uint8_t>(cseg::temporal_format::Operation::kOriginal)},
        std::byte{static_cast<std::uint8_t>(cseg::temporal_format::Operation::kOriginal)}};
    std::vector<std::byte> identity_offsets;
    append_le(identity_offsets, std::uint32_t{0U});
    append_le(identity_offsets, std::uint32_t{1U});
    append_le(identity_offsets, std::uint32_t{2U});
    const std::vector<std::byte> identities{std::byte{'a'}, std::byte{'b'}};
    std::vector<std::byte> receive_times;
    append_le(receive_times, std::int64_t{100});
    append_le(receive_times, std::int64_t{101});
    std::vector<std::byte> system_times;
    append_le(system_times, std::int64_t{200});
    append_le(system_times, std::int64_t{201});

    pages.reserve(columns.size());
    pages.push_back(encode_page(columns[0].logical_type, {}, event_times));
    pages.push_back(encode_page(columns[1].logical_type, {}, sources));
    pages.push_back(encode_page(columns[2].logical_type, {}, source_ids));
    pages.push_back(encode_page(columns[3].logical_type, {}, positions));
    pages.push_back(encode_page(columns[4].logical_type, {}, ordinals));
    pages.push_back(encode_page(columns[5].logical_type, {}, operations));
    pages.push_back(encode_page(columns[6].logical_type, identity_offsets, identities));
    pages.push_back(encode_page(columns[7].logical_type, {}, receive_times));
    pages.push_back(encode_page(columns[8].logical_type, {}, system_times));
  }

  [[nodiscard]] cseg::CsegPartEncodeInput input() const {
    return {.part_id = part_id,
            .table_id = table_id,
            .tablet_id = tablet_id,
            .schema_id = schema_id,
            .schema_version = schema::SchemaVersion::initial(),
            .row_count = 2U,
            .event_time_column_ordinal = 0U,
            .ordering_column_count = 1U,
            .minimum_event_time = 10,
            .maximum_event_time = 20,
            .columns = columns,
            .granules = granules,
            .pages = pages};
  }

  [[nodiscard]] schema::TableSchema schema_value() const {
    std::vector<schema::ColumnDefinition> definitions;
    definitions.push_back(
        schema::ColumnDefinition::create(id<schema::ColumnId>(5U), "event_time",
                                         type(schema::LogicalTypeKind::kTimestampNs), false)
            .value());
    return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(definitions),
                                       {.event_time_column = id<schema::ColumnId>(5U),
                                        .physical_ordering_key = {id<schema::ColumnId>(5U)},
                                        .partition_columns = {id<schema::ColumnId>(5U)},
                                        .shard_key = {id<schema::ColumnId>(5U)},
                                        .deduplication_key = {id<schema::ColumnId>(5U)}})
        .value();
  }

  [[nodiscard]] TemporalTabletDescriptor owner() const {
    return {.table_id = table_id,
            .tablet_id = tablet_id,
            .recovery_schema_id = schema_id,
            .recovery_schema_version = schema::SchemaVersion::initial(),
            .source_id = source,
            .durable_position = 9U,
            .reclaim_position = 0U,
            .first_part_index = 0U,
            .part_count = 1U,
            .durable_version_count = 2U,
            .commit_source = ManifestCommitSource::kWal};
  }
};

TEST(TemporalPartValidationTest, DerivesAndValidatesExactDescriptorFromCsegImage) {
  TemporalPartFixture fixture;
  const auto encoded = cseg::encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const schema::TableSchema schema_value = fixture.schema_value();
  const auto descriptor =
      describe_manifest_v2_temporal_part_image(encoded->bytes(), schema_value, fixture.tablet_id,
                                               ManifestCommitSource::kWal, fixture.source);
  ASSERT_TRUE(descriptor.has_value()) << descriptor.error().to_string();

  EXPECT_EQ(descriptor->file_length, encoded->size());
  EXPECT_EQ(descriptor->row_count, 2U);
  EXPECT_EQ(descriptor->minimum_commit_position, 7U);
  EXPECT_EQ(descriptor->maximum_commit_position, 9U);
  EXPECT_EQ(descriptor->minimum_event_time, 10);
  EXPECT_EQ(descriptor->maximum_event_time, 20);
  EXPECT_EQ(descriptor->minimum_system_time, 200);
  EXPECT_EQ(descriptor->maximum_system_time, 201);
  EXPECT_EQ(descriptor->cseg_format_major, cseg::temporal_format::kFormatMajor);
  EXPECT_EQ(descriptor->cseg_format_minor, cseg::temporal_format::kFormatMinor);
  EXPECT_TRUE(validate_manifest_v2_temporal_part_image(*descriptor, fixture.owner(),
                                                       encoded->bytes(), schema_value)
                  .is_ok());
}

TEST(TemporalPartValidationTest, RejectsDescriptorDigestExtremaAndOwnerDisagreements) {
  TemporalPartFixture fixture;
  const auto encoded = cseg::encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  const schema::TableSchema schema_value = fixture.schema_value();
  const auto actual =
      describe_manifest_v2_temporal_part_image(encoded->bytes(), schema_value, fixture.tablet_id,
                                               ManifestCommitSource::kWal, fixture.source);
  ASSERT_TRUE(actual.has_value());
  const auto check_with_owner = [&](const TemporalPartDescriptor& descriptor,
                                    const TemporalTabletDescriptor& owner) {
    return validate_manifest_v2_temporal_part_image(descriptor, owner, encoded->bytes(),
                                                    schema_value)
        .code();
  };
  const auto check = [&](const TemporalPartDescriptor& descriptor) {
    return check_with_owner(descriptor, fixture.owner());
  };

  TemporalPartDescriptor changed = *actual;
  ingest::Sha256Digest::Bytes digest = changed.content_sha256.bytes();
  digest.front() ^= std::byte{1U};
  changed.content_sha256 = ingest::Sha256Digest{digest};
  EXPECT_EQ(check(changed), common::StatusCode::kCorruption);
  changed = *actual;
  --changed.minimum_commit_position;
  EXPECT_EQ(check(changed), common::StatusCode::kCorruption);
  changed = *actual;
  --changed.minimum_system_time;
  EXPECT_EQ(check(changed), common::StatusCode::kCorruption);
  changed = *actual;
  --changed.minimum_event_time;
  EXPECT_EQ(check(changed), common::StatusCode::kCorruption);

  TemporalTabletDescriptor owner = fixture.owner();
  owner.durable_position = 8U;
  EXPECT_EQ(check_with_owner(*actual, owner), common::StatusCode::kCorruption);
  owner = fixture.owner();
  owner.source_id = source_id(9U);
  EXPECT_EQ(check_with_owner(*actual, owner), common::StatusCode::kCorruption);
}

TEST(TemporalPartValidationTest, RejectsMixedLineageCorruptionAndResourceExcess) {
  TemporalPartFixture mixed{true};
  const auto mixed_encoded = cseg::encode_cseg_v2_temporal_part(mixed.input());
  ASSERT_TRUE(mixed_encoded.has_value());
  const schema::TableSchema mixed_schema = mixed.schema_value();
  EXPECT_EQ(describe_manifest_v2_temporal_part_image(mixed_encoded->bytes(), mixed_schema,
                                                     mixed.tablet_id, ManifestCommitSource::kWal,
                                                     mixed.source)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  TemporalPartFixture fixture;
  const auto encoded = cseg::encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  const schema::TableSchema schema_value = fixture.schema_value();
  std::vector<std::byte> damaged(encoded->bytes().begin(), encoded->bytes().end());
  damaged.back() ^= std::byte{1U};
  EXPECT_EQ(describe_manifest_v2_temporal_part_image(damaged, schema_value, fixture.tablet_id,
                                                     ManifestCommitSource::kWal, fixture.source)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  TemporalPartValidationLimits limits;
  limits.decode.max_file_length = encoded->size() - 8U;
  EXPECT_EQ(describe_manifest_v2_temporal_part_image(encoded->bytes(), schema_value,
                                                     fixture.tablet_id, ManifestCommitSource::kWal,
                                                     fixture.source, limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(describe_manifest_v2_temporal_part_image(encoded->bytes(), schema_value,
                                                     id<schema::TabletId>(99U),
                                                     ManifestCommitSource::kWal, fixture.source)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(TemporalReferencedPartValidationTest, RequiresExactCanonicalGenerationCoverage) {
  TemporalPartFixture fixture;
  const auto encoded_part = cseg::encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded_part.has_value());
  const schema::TableSchema schema_value = fixture.schema_value();
  const auto descriptor = describe_manifest_v2_temporal_part_image(
      encoded_part->bytes(), schema_value, fixture.tablet_id, ManifestCommitSource::kWal,
      fixture.source);
  ASSERT_TRUE(descriptor.has_value());
  const std::array tablets{fixture.owner()};
  const std::array parts{*descriptor};
  const auto encoded_manifest = encode_manifest_v2_temporal({.generation = 1U,
                                                             .database_id = id<DatabaseId>(6U),
                                                             .wal_reclaim_checkpoint = std::nullopt,
                                                             .tablets = tablets,
                                                             .parts = parts,
                                                             .retries = {}});
  ASSERT_TRUE(encoded_manifest.has_value());
  const auto manifest = decode_manifest_v2_temporal_exact(encoded_manifest->bytes());
  ASSERT_TRUE(manifest.has_value());
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = fixture.tablet_id, .lineage = std::cref(lineage)}};
  const std::string name = part_file_name(fixture.part_id);
  const std::array images{ReferencedPartImage{.file_name = name, .bytes = encoded_part->bytes()}};

  EXPECT_TRUE(validate_manifest_v2_temporal_referenced_parts(*manifest, bindings, images).is_ok());
  EXPECT_EQ(validate_manifest_v2_temporal_referenced_parts(*manifest, bindings, {}).code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(validate_manifest_v2_temporal_referenced_parts(*manifest, {}, images).code(),
            common::StatusCode::kInvalidArgument);

  const std::array wrong_name{ReferencedPartImage{
      .file_name = "part-00000000000000000000000000000002.cseg", .bytes = encoded_part->bytes()}};
  EXPECT_EQ(validate_manifest_v2_temporal_referenced_parts(*manifest, bindings, wrong_name).code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> damaged(encoded_part->bytes().begin(), encoded_part->bytes().end());
  damaged.back() ^= std::byte{1U};
  const std::array corrupt{
      ReferencedPartImage{.file_name = name, .bytes = common::ByteView{damaged}}};
  EXPECT_EQ(validate_manifest_v2_temporal_referenced_parts(*manifest, bindings, corrupt).code(),
            common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::manifest
