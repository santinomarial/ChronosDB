#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/part_validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "manifest/manifest_test_support.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier cseg_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] wal::WalId cseg_wal_id(const std::uint8_t seed = 0x70U) {
  wal::WalId result{};
  result.bytes.front() = std::byte{seed};
  return result;
}

struct OwnedManifest {
  EncodedManifest bytes;
  DecodedManifestView view;
};

struct ReferencedPartFixture {
  explicit ReferencedPartFixture(
      const cseg::PageCompression compression = cseg::PageCompression::kNone)
      : part(cseg::test::make_valid_part(compression)), table_id(cseg_id<schema::TableId>(2U)),
        tablet_id(cseg_id<schema::TabletId>(3U)), schema_id(cseg_id<schema::SchemaId>(4U)),
        part_id(cseg_id<cseg::PartId>(1U)),
        lineage(schema::SchemaLineage::create(schema_value()).value()),
        descriptor{.part_id = part_id,
                   .table_id = table_id,
                   .tablet_id = tablet_id,
                   .schema_id = schema_id,
                   .schema_version = schema::SchemaVersion::initial(),
                   .file_length = part.size(),
                   .row_count = 2U,
                   .minimum_record_sequence = 7U,
                   .maximum_record_sequence = 7U,
                   .minimum_event_time = -5,
                   .maximum_event_time = 10},
        tablet{.table_id = table_id,
               .tablet_id = tablet_id,
               .recovery_schema_id = schema_id,
               .recovery_schema_version = schema::SchemaVersion::initial(),
               .durable_record_sequence = 7U,
               .first_part_index = 0U,
               .part_count = 1U,
               .durable_row_count = 2U} {}

  [[nodiscard]] schema::TableSchema schema_value() const {
    const schema::ColumnId event_id = cseg_id<schema::ColumnId>(5U);
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

  [[nodiscard]] static OwnedManifest manifest(PartDescriptor part_descriptor,
                                              TabletDescriptor tablet_descriptor,
                                              const wal::WalId& wal_id = cseg_wal_id()) {
    const std::array tablets{tablet_descriptor};
    const std::array parts{part_descriptor};
    EncodedManifest encoded = encode_manifest_v1({
                                                     .generation = 1U,
                                                     .database_id = test::make_id<DatabaseId>(9U),
                                                     .wal_id = wal_id,
                                                     .reclaim_checkpoint = {.record_sequence = 0U,
                                                                            .segment_number = 1U,
                                                                            .byte_offset = 64U},
                                                     .tablets = tablets,
                                                     .parts = parts,
                                                     .retries = {},
                                                 })
                                  .value();
    DecodedManifestView decoded = decode_manifest_v1_exact(encoded.bytes()).value();
    return {.bytes = std::move(encoded), .view = std::move(decoded)};
  }

  [[nodiscard]] std::array<TabletSchemaBinding, 1> bindings() const {
    return {TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  }

  [[nodiscard]] std::array<ReferencedPartImage, 1> images() const {
    return {ReferencedPartImage{.file_name = file_name, .bytes = part.bytes()}};
  }

  cseg::EncodedCsegPart part;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId schema_id;
  cseg::PartId part_id;
  schema::SchemaLineage lineage;
  PartDescriptor descriptor;
  TabletDescriptor tablet;
  std::string file_name{part_file_name(part_id)};
};

TEST(ManifestReferencedPartValidationTest, AcceptsExactPlainAndCompressedPartImages) {
  for (const cseg::PageCompression compression :
       {cseg::PageCompression::kNone, cseg::PageCompression::kZstd}) {
    const ReferencedPartFixture fixture{compression};
    const OwnedManifest manifest =
        ReferencedPartFixture::manifest(fixture.descriptor, fixture.tablet);
    EXPECT_TRUE(
        validate_manifest_v1_referenced_parts(manifest.view, fixture.bindings(), fixture.images())
            .is_ok());
  }
}

TEST(ManifestReferencedPartValidationTest, RequiresExactImageCoverageAndCanonicalFilenameIdentity) {
  const ReferencedPartFixture fixture;
  const OwnedManifest manifest =
      ReferencedPartFixture::manifest(fixture.descriptor, fixture.tablet);
  EXPECT_EQ(validate_manifest_v1_referenced_parts(manifest.view, fixture.bindings(), {}).code(),
            common::StatusCode::kCorruption);

  const std::array wrong_name{ReferencedPartImage{
      .file_name = "part-00000000000000000000000000000002.cseg", .bytes = fixture.part.bytes()}};
  EXPECT_EQ(
      validate_manifest_v1_referenced_parts(manifest.view, fixture.bindings(), wrong_name).code(),
      common::StatusCode::kCorruption);
}

TEST(ManifestReferencedPartValidationTest, RejectsEveryDuplicatedDescriptorDisagreement) {
  const ReferencedPartFixture fixture;
  const auto check = [&fixture](PartDescriptor descriptor, TabletDescriptor tablet) {
    const OwnedManifest manifest = ReferencedPartFixture::manifest(descriptor, tablet);
    return validate_manifest_v1_referenced_parts(manifest.view, fixture.bindings(),
                                                 fixture.images())
        .code();
  };

  PartDescriptor changed = fixture.descriptor;
  changed.file_length += 8U;
  EXPECT_EQ(check(changed, fixture.tablet), common::StatusCode::kCorruption);

  changed = fixture.descriptor;
  changed.row_count = 3U;
  TabletDescriptor tablet = fixture.tablet;
  tablet.durable_row_count = 3U;
  EXPECT_EQ(check(changed, tablet), common::StatusCode::kCorruption);

  changed = fixture.descriptor;
  changed.minimum_record_sequence = 6U;
  EXPECT_EQ(check(changed, fixture.tablet), common::StatusCode::kCorruption);

  changed = fixture.descriptor;
  changed.minimum_event_time = -6;
  EXPECT_EQ(check(changed, fixture.tablet), common::StatusCode::kCorruption);

  changed = fixture.descriptor;
  changed.part_id = cseg_id<cseg::PartId>(2U);
  const OwnedManifest identity = ReferencedPartFixture::manifest(changed, fixture.tablet);
  const std::string changed_name = part_file_name(changed.part_id);
  const std::array image{
      ReferencedPartImage{.file_name = changed_name, .bytes = fixture.part.bytes()}};
  EXPECT_EQ(validate_manifest_v1_referenced_parts(identity.view, fixture.bindings(), image).code(),
            common::StatusCode::kCorruption);
}

TEST(ManifestReferencedPartValidationTest, RejectsCorruptBytesAndWrongWalIdentity) {
  const ReferencedPartFixture fixture;
  const OwnedManifest manifest =
      ReferencedPartFixture::manifest(fixture.descriptor, fixture.tablet);
  std::vector<std::byte> damaged(fixture.part.bytes().begin(), fixture.part.bytes().end());
  damaged.back() ^= std::byte{0x01U};
  const std::array corrupt_image{
      ReferencedPartImage{.file_name = fixture.file_name, .bytes = damaged}};
  EXPECT_EQ(validate_manifest_v1_referenced_parts(manifest.view, fixture.bindings(), corrupt_image)
                .code(),
            common::StatusCode::kCorruption);

  const OwnedManifest wrong_wal =
      ReferencedPartFixture::manifest(fixture.descriptor, fixture.tablet, cseg_wal_id(0x71U));
  EXPECT_EQ(
      validate_manifest_v1_referenced_parts(wrong_wal.view, fixture.bindings(), fixture.images())
          .code(),
      common::StatusCode::kCorruption);
}

TEST(ManifestReferencedPartValidationTest, PreservesCatalogAndResourceFailureClassifications) {
  const ReferencedPartFixture fixture;
  const OwnedManifest manifest =
      ReferencedPartFixture::manifest(fixture.descriptor, fixture.tablet);
  EXPECT_EQ(validate_manifest_v1_referenced_parts(manifest.view, {}, fixture.images()).code(),
            common::StatusCode::kInvalidArgument);

  ReferencedPartValidationLimits limits;
  limits.decode.max_file_length = fixture.part.size() - 8U;
  EXPECT_EQ(validate_manifest_v1_referenced_parts(manifest.view, fixture.bindings(),
                                                  fixture.images(), limits)
                .code(),
            common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::manifest
