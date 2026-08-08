#include "chronos/common/crc32c.hpp"
#include "chronos/manifest/generation_builder.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
#include <limits>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId value{};
  value.bytes.front() = std::byte{0x70U};
  return value;
}

[[nodiscard]] ingest::Sha256Digest digest(const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return ingest::Sha256Digest{bytes};
}

struct Fixture {
  explicit Fixture(const cseg::PageCompression compression = cseg::PageCompression::kNone,
                   const std::uint8_t retry_seed = 0x10U)
      : part(cseg::test::make_valid_part(compression)), schema_value(make_schema()),
        lineage(schema::SchemaLineage::create(schema_value).value()),
        predecessor_bytes(encode_manifest_v1({
                                                 .generation = 1U,
                                                 .database_id = database_id,
                                                 .wal_id = wal_id(),
                                                 .reclaim_checkpoint = {.record_sequence = 0U,
                                                                        .segment_number = 1U,
                                                                        .byte_offset = 64U},
                                                 .tablets = {},
                                                 .parts = {},
                                                 .retries = {},
                                             })
                              .value()),
        predecessor(decode_manifest_v1_exact(predecessor_bytes.bytes()).value()),
        descriptor{.part_id = id<cseg::PartId>(1U),
                   .table_id = id<schema::TableId>(2U),
                   .tablet_id = id<schema::TabletId>(3U),
                   .schema_id = id<schema::SchemaId>(4U),
                   .schema_version = schema::SchemaVersion::initial(),
                   .file_length = part.size(),
                   .row_count = 2U,
                   .minimum_record_sequence = 7U,
                   .maximum_record_sequence = 7U,
                   .minimum_event_time = -5,
                   .maximum_event_time = 10},
        sealed(descriptor, wal_id(), std::move(part)), retries{make_retry(7U, 2U, retry_seed)} {}

  [[nodiscard]] static schema::TableSchema make_schema() {
    const schema::ColumnId event_id = id<schema::ColumnId>(5U);
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                       schema::SchemaVersion::initial(), std::nullopt,
                                       std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }

  [[nodiscard]] static RetryDescriptor
  make_retry(const std::uint64_t sequence, const std::uint32_t rows, const std::uint8_t seed) {
    return {.client_id = id<ingest::ClientId>(seed),
            .client_batch_id = id<ingest::ClientBatchId>(static_cast<std::uint8_t>(seed + 1U)),
            .table_id = id<schema::TableId>(2U),
            .tablet_id = id<schema::TabletId>(3U),
            .request_digest = digest(static_cast<std::uint8_t>(seed + 2U)),
            .wal_id = wal_id(),
            .record_sequence = sequence,
            .applied_row_count = rows};
  }

  [[nodiscard]] std::array<TabletSchemaBinding, 1> bindings() const {
    return {
        TabletSchemaBinding{.tablet_id = id<schema::TabletId>(3U), .lineage = std::cref(lineage)}};
  }

  [[nodiscard]] SealedHeadManifestBuildInput
  input(const std::span<const RetryDescriptor> input_retries) const {
    return {.predecessor = predecessor,
            .sealed_part = sealed,
            .new_retries = input_retries,
            .schema_bindings = bindings_storage,
            .part_validation_limits = {}};
  }

  DatabaseId database_id{id<DatabaseId>(6U)};
  cseg::EncodedCsegPart part;
  schema::TableSchema schema_value;
  schema::SchemaLineage lineage;
  EncodedManifest predecessor_bytes;
  DecodedManifestView predecessor;
  PartDescriptor descriptor;
  EncodedSealedHeadPart sealed;
  std::vector<RetryDescriptor> retries;
  std::array<TabletSchemaBinding, 1> bindings_storage{
      TabletSchemaBinding{.tablet_id = id<schema::TabletId>(3U), .lineage = std::cref(lineage)}};
};

[[nodiscard]] std::uint32_t independent_crc32c(const common::ByteView bytes) {
  std::uint32_t crc = 0xffffffffU;
  for (const std::byte byte : bytes) {
    crc ^= std::to_integer<std::uint8_t>(byte);
    for (std::size_t bit = 0U; bit < 8U; ++bit) {
      const std::uint32_t mask = 0U - (crc & 1U);
      crc = (crc >> 1U) ^ (0x82f63b78U & mask);
    }
  }
  return ~crc;
}

TEST(ManifestGenerationBuilderTest, BuildsCanonicalNextGenerationAndPreservesCheckpoint) {
  const Fixture fixture;
  const common::Result<EncodedManifest> built =
      build_manifest_v1_for_sealed_head(fixture.input(fixture.retries));
  ASSERT_TRUE(built.has_value()) << built.error().to_string();
  const ManifestDecodeResult decoded = decode_manifest_v1_exact(built->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  EXPECT_EQ(decoded->generation(), 2U);
  EXPECT_EQ(decoded->previous_generation(), 1U);
  EXPECT_EQ(decoded->database_id(), fixture.database_id);
  EXPECT_EQ(decoded->wal_id(), wal_id());
  EXPECT_EQ(decoded->reclaim_checkpoint(), fixture.predecessor.reclaim_checkpoint());
  ASSERT_EQ(decoded->tablets().size(), 1U);
  EXPECT_EQ(decoded->tablets().front().durable_record_sequence, 7U);
  EXPECT_EQ(decoded->tablets().front().durable_row_count, 2U);
  ASSERT_EQ(decoded->parts().size(), 1U);
  EXPECT_EQ(decoded->parts().front(), fixture.descriptor);
  ASSERT_EQ(decoded->retries().size(), 1U);
  EXPECT_TRUE(std::ranges::is_sorted(decoded->retries(),
                                     [](const RetryDescriptor& left, const RetryDescriptor& right) {
                                       return std::pair{left.client_id, left.client_batch_id} <
                                              std::pair{right.client_id, right.client_batch_id};
                                     }));
  EXPECT_TRUE(
      validate_manifest_v1_transition(fixture.predecessor, *decoded, fixture.bindings()).is_ok());
  EXPECT_EQ(independent_crc32c(built->bytes()), common::crc32c(built->bytes()));
}

TEST(ManifestGenerationBuilderTest, RepeatedBuildsProduceGoldenBytes) {
  const Fixture fixture;
  const EncodedManifest first =
      build_manifest_v1_for_sealed_head(fixture.input(fixture.retries)).value();
  const EncodedManifest second =
      build_manifest_v1_for_sealed_head(fixture.input(fixture.retries)).value();
  EXPECT_TRUE(std::ranges::equal(first.bytes(), second.bytes()));
  // This complete-generation fingerprint uses the independent tableless implementation above.
  EXPECT_EQ(independent_crc32c(first.bytes()), 0x48674bc7U);
}

TEST(ManifestGenerationBuilderTest, RejectsMissingExtraAndDisagreeingRetryCoverage) {
  const Fixture fixture;
  EXPECT_EQ(build_manifest_v1_for_sealed_head(
                fixture.input(std::span<const RetryDescriptor>{fixture.retries}.first(0U)))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  std::vector<RetryDescriptor> changed = fixture.retries;
  changed.front().applied_row_count = 1U;
  EXPECT_EQ(build_manifest_v1_for_sealed_head(fixture.input(changed)).error().code(),
            common::StatusCode::kInvalidArgument);
  changed = fixture.retries;
  changed.front().record_sequence = 10U;
  EXPECT_EQ(build_manifest_v1_for_sealed_head(fixture.input(changed)).error().code(),
            common::StatusCode::kInvalidArgument);
  changed = fixture.retries;
  changed.push_back(Fixture::make_retry(11U, 1U, 0x30U));
  EXPECT_EQ(build_manifest_v1_for_sealed_head(fixture.input(changed)).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(ManifestGenerationBuilderTest, RetainsAndCanonicallyMergesExistingDurableState) {
  const Fixture fixture;
  PartDescriptor old_part = fixture.descriptor;
  old_part.part_id = id<cseg::PartId>(0x40U);
  old_part.file_length = 1'208U;
  old_part.row_count = 1U;
  old_part.minimum_record_sequence = 5U;
  old_part.maximum_record_sequence = 5U;
  old_part.minimum_event_time = 0;
  old_part.maximum_event_time = 0;
  const std::array parts{old_part};
  const std::array tablets{TabletDescriptor{
      .table_id = fixture.descriptor.table_id,
      .tablet_id = fixture.descriptor.tablet_id,
      .recovery_schema_id = fixture.descriptor.schema_id,
      .recovery_schema_version = fixture.descriptor.schema_version,
      .durable_record_sequence = 5U,
      .first_part_index = 0U,
      .part_count = 1U,
      .durable_row_count = 1U,
  }};
  const std::array old_retries{Fixture::make_retry(5U, 1U, 0x50U)};
  const EncodedManifest predecessor_bytes =
      encode_manifest_v1({
                             .generation = 6U,
                             .database_id = fixture.database_id,
                             .wal_id = wal_id(),
                             .reclaim_checkpoint = {.record_sequence = 0U,
                                                    .segment_number = 1U,
                                                    .byte_offset = 64U},
                             .tablets = tablets,
                             .parts = parts,
                             .retries = old_retries,
                         })
          .value();
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  const common::Result<EncodedManifest> built = build_manifest_v1_for_sealed_head({
      .predecessor = predecessor,
      .sealed_part = fixture.sealed,
      .new_retries = fixture.retries,
      .schema_bindings = fixture.bindings_storage,
      .part_validation_limits = {},
  });
  ASSERT_TRUE(built.has_value()) << built.error().to_string();
  const DecodedManifestView decoded = decode_manifest_v1_exact(built->bytes()).value();
  EXPECT_EQ(decoded.generation(), 7U);
  ASSERT_EQ(decoded.tablets().size(), 1U);
  EXPECT_EQ(decoded.tablets().front().durable_record_sequence, 7U);
  EXPECT_EQ(decoded.tablets().front().durable_row_count, 3U);
  ASSERT_EQ(decoded.parts().size(), 2U);
  EXPECT_EQ(decoded.parts().front(), fixture.descriptor);
  EXPECT_EQ(decoded.parts().back(), old_part);
  ASSERT_EQ(decoded.retries().size(), 2U);
  EXPECT_EQ(decoded.retries().front(), fixture.retries.front());
  EXPECT_EQ(decoded.retries().back(), old_retries.front());
  EXPECT_TRUE(validate_manifest_v1_transition(predecessor, decoded, fixture.bindings()).is_ok());

  RetryDescriptor colliding_retry = fixture.retries.front();
  colliding_retry.record_sequence = 5U;
  colliding_retry.applied_row_count = 1U;
  const std::array colliding_retries{colliding_retry};
  const EncodedManifest collision_bytes =
      encode_manifest_v1(
          {.generation = 6U,
           .database_id = fixture.database_id,
           .wal_id = wal_id(),
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = tablets,
           .parts = parts,
           .retries = colliding_retries})
          .value();
  const DecodedManifestView collision = decode_manifest_v1_exact(collision_bytes.bytes()).value();
  EXPECT_EQ(build_manifest_v1_for_sealed_head({.predecessor = collision,
                                               .sealed_part = fixture.sealed,
                                               .new_retries = fixture.retries,
                                               .schema_bindings = fixture.bindings_storage,
                                               .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(ManifestGenerationBuilderTest, RejectsWalSchemaPartAndBoundaryDisagreement) {
  const Fixture fixture;
  EncodedSealedHeadPart wrong_wal{fixture.descriptor, {}, cseg::test::make_valid_part()};
  const SealedHeadManifestBuildInput wrong_wal_input{.predecessor = fixture.predecessor,
                                                     .sealed_part = wrong_wal,
                                                     .new_retries = fixture.retries,
                                                     .schema_bindings = fixture.bindings_storage,
                                                     .part_validation_limits = {}};
  EXPECT_EQ(build_manifest_v1_for_sealed_head(wrong_wal_input).error().code(),
            common::StatusCode::kInvalidArgument);

  PartDescriptor wrong_descriptor = fixture.descriptor;
  wrong_descriptor.row_count = 3U;
  EncodedSealedHeadPart wrong_part{wrong_descriptor, wal_id(), cseg::test::make_valid_part()};
  EXPECT_EQ(build_manifest_v1_for_sealed_head({.predecessor = fixture.predecessor,
                                               .sealed_part = wrong_part,
                                               .new_retries = fixture.retries,
                                               .schema_bindings = fixture.bindings_storage,
                                               .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kCorruption);

  const std::array<TabletSchemaBinding, 0> no_bindings{};
  EXPECT_EQ(build_manifest_v1_for_sealed_head({.predecessor = fixture.predecessor,
                                               .sealed_part = fixture.sealed,
                                               .new_retries = fixture.retries,
                                               .schema_bindings = no_bindings,
                                               .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  const std::array tablets{
      TabletDescriptor{.table_id = fixture.descriptor.table_id,
                       .tablet_id = fixture.descriptor.tablet_id,
                       .recovery_schema_id = fixture.descriptor.schema_id,
                       .recovery_schema_version = fixture.descriptor.schema_version,
                       .durable_record_sequence = 7U,
                       .first_part_index = 0U,
                       .part_count = 1U,
                       .durable_row_count = 1U}};
  PartDescriptor old_part = fixture.descriptor;
  old_part.part_id = id<cseg::PartId>(0x40U);
  old_part.file_length = 1'208U;
  old_part.row_count = 1U;
  old_part.minimum_record_sequence = 7U;
  old_part.maximum_record_sequence = 7U;
  old_part.minimum_event_time = 0;
  old_part.maximum_event_time = 0;
  const std::array parts{old_part};
  const EncodedManifest predecessor_bytes =
      encode_manifest_v1({
                             .generation = 1U,
                             .database_id = fixture.database_id,
                             .wal_id = wal_id(),
                             .reclaim_checkpoint = {.record_sequence = 0U,
                                                    .segment_number = 1U,
                                                    .byte_offset = 64U},
                             .tablets = tablets,
                             .parts = parts,
                             .retries = {},
                         })
          .value();
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  EXPECT_EQ(build_manifest_v1_for_sealed_head({.predecessor = predecessor,
                                               .sealed_part = fixture.sealed,
                                               .new_retries = fixture.retries,
                                               .schema_bindings = fixture.bindings_storage,
                                               .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(ManifestGenerationBuilderTest, RejectsGenerationOverflowWithoutProducingBytes) {
  const Fixture fixture;
  const EncodedManifest predecessor_bytes =
      encode_manifest_v1(
          {.generation = std::numeric_limits<std::uint64_t>::max(),
           .database_id = fixture.database_id,
           .wal_id = wal_id(),
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = {},
           .parts = {},
           .retries = {}})
          .value();
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(predecessor_bytes.bytes()).value();
  EXPECT_EQ(build_manifest_v1_for_sealed_head({.predecessor = predecessor,
                                               .sealed_part = fixture.sealed,
                                               .new_retries = fixture.retries,
                                               .schema_bindings = fixture.bindings_storage,
                                               .part_validation_limits = {}})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

TEST(ManifestGenerationBuilderPropertyTest, GeneratedIdentitiesRemainDeterministicAndCanonical) {
  constexpr std::uint64_t multiplier = 6'364'136'223'846'793'005ULL;
  constexpr std::uint64_t increment = 1'442'695'040'888'963'407ULL;
  std::uint64_t state = 0x9e3779b97f4a7c15ULL;
  for (std::size_t trial = 0U; trial < 128U; ++trial) {
    state = state * multiplier + increment;
    const auto seed = static_cast<std::uint8_t>((state % 0xfcU) + 1U);
    const Fixture fixture{
        trial % 2U == 0U ? cseg::PageCompression::kNone : cseg::PageCompression::kZstd, seed};
    const EncodedManifest baseline =
        build_manifest_v1_for_sealed_head(fixture.input(fixture.retries)).value();
    const EncodedManifest candidate =
        build_manifest_v1_for_sealed_head(fixture.input(fixture.retries)).value();
    EXPECT_TRUE(std::ranges::equal(baseline.bytes(), candidate.bytes()));
    EXPECT_TRUE(decode_manifest_v1_exact(candidate.bytes()).has_value());
  }
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-generation-builder-XXXXXX").string();
    char* const created = ::mkdtemp(pattern.data());
    if (created != nullptr) {
      path_ = created;
    }
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid nonce(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

TEST(ManifestGenerationBuilderTest, InstallsPartThenGeneratedManifestWithoutTranslation) {
  const Fixture fixture{cseg::PageCompression::kZstd};
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / kManifestDirectoryName));
  {
    std::ofstream lock{directory.path() / kManifestDirectoryName /
                       std::string{kManifestLockFileName}};
    ASSERT_TRUE(lock.good());
  }
  {
    std::ofstream initial{directory.path() / kManifestDirectoryName / *manifest_file_name(1U),
                          std::ios::binary};
    // std::ofstream's byte-oriented write API has no std::byte overload.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    initial.write(reinterpret_cast<const char*>(fixture.predecessor_bytes.bytes().data()),
                  static_cast<std::streamsize>(fixture.predecessor_bytes.size()));
    ASSERT_TRUE(initial.good());
  }
  common::Result<ManifestStorage> storage =
      ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  const common::Result<InstalledPart> installed_part = storage->install_part({
      .encoded_part = std::cref(fixture.sealed.encoded_part),
      .descriptor = fixture.descriptor,
      .wal_id = wal_id(),
      .schema = std::cref(fixture.schema_value),
      .nonce = nonce(0xa0U),
      .validation_limits = {},
  });
  ASSERT_TRUE(installed_part.has_value()) << installed_part.error().to_string();
  const common::Result<EncodedManifest> next =
      build_manifest_v1_for_sealed_head(fixture.input(fixture.retries));
  ASSERT_TRUE(next.has_value()) << next.error().to_string();
  const common::Result<InstalledManifest> installed_manifest = storage->install_manifest({
      .encoded_manifest = std::cref(*next),
      .schema_bindings = fixture.bindings_storage,
      .nonce = nonce(0xb0U),
      .decode_limits = {},
      .part_validation_limits = {},
      .compaction_equivalence_limits = {},
  });
  ASSERT_TRUE(installed_manifest.has_value()) << installed_manifest.error().to_string();
  EXPECT_EQ(installed_manifest->generation, 2U);
  const common::Result<LoadedManifestGeneration> selected = storage->load_selected_manifest({
      .expected_database_id = fixture.database_id,
      .expected_wal_id = wal_id(),
      .schema_bindings = fixture.bindings_storage,
      .decode_limits = {},
      .part_validation_limits = {},
  });
  ASSERT_TRUE(selected.has_value()) << selected.error().to_string();
  EXPECT_EQ(selected->generation(), 2U);
  EXPECT_EQ(selected->parts().front(), fixture.descriptor);
  EXPECT_TRUE(std::ranges::equal(selected->retries(), fixture.retries));
}

} // namespace
} // namespace chronos::manifest
