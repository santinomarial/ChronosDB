#include "chronos/cluster/tablet_physical_part_install.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::cluster {
namespace {

class InstallTemporaryDirectory {
public:
  explicit InstallTemporaryDirectory(const std::string& prefix) {
    std::string pattern = (std::filesystem::temp_directory_path() / (prefix + "-XXXXXX")).string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~InstallTemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Identity> [[nodiscard]] Identity id(const std::uint8_t seed) {
  return Identity::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::TableSchema schema_value() {
  const schema::ColumnId event_time = id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_time, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  return schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                     schema::SchemaVersion::initial(), std::nullopt,
                                     std::move(columns),
                                     {.event_time_column = event_time,
                                      .physical_ordering_key = {event_time},
                                      .partition_columns = {event_time},
                                      .shard_key = {event_time},
                                      .deduplication_key = {event_time}})
      .value();
}

void establish_manifest_layout(const std::filesystem::path& root) {
  ASSERT_TRUE(std::filesystem::create_directory(root / manifest::kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(root / manifest::kManifestDirectoryName));
  std::ofstream lock{root / manifest::kManifestDirectoryName / manifest::kManifestLockFileName,
                     std::ios::binary};
  ASSERT_TRUE(lock.good());
}

struct PhysicalPartFixture {
  common::Uuid group_id{uuid(8U)};
  cseg::EncodedCsegPart encoded{cseg::test::make_valid_temporal_part(
      cseg::PageCompression::kNone,
      {.commit_source = cseg::temporal_format::CommitSource::kRaft, .source_id = group_id})};
  schema::TableSchema schema{schema_value()};
  manifest::TemporalPartDescriptor descriptor{manifest::describe_manifest_v2_temporal_part_image(
                                                  encoded.bytes(), schema, id<schema::TabletId>(3U),
                                                  manifest::ManifestCommitSource::kRaft, group_id)
                                                  .value()};
  manifest::TemporalTabletDescriptor owner{.table_id = descriptor.table_id,
                                           .tablet_id = descriptor.tablet_id,
                                           .recovery_schema_id = descriptor.schema_id,
                                           .recovery_schema_version = descriptor.schema_version,
                                           .source_id = group_id,
                                           .durable_position = descriptor.maximum_commit_position,
                                           .reclaim_position = 0U,
                                           .first_part_index = 0U,
                                           .part_count = 1U,
                                           .durable_version_count = descriptor.row_count,
                                           .commit_source = manifest::ManifestCommitSource::kRaft};
  TabletPhysicalPartTransferSession session{.table_id = descriptor.table_id,
                                            .tablet_id = descriptor.tablet_id,
                                            .group_id = group_id,
                                            .placement_epoch = 11U,
                                            .source_node = 12U,
                                            .target_node = 13U,
                                            .manifest_generation = 14U,
                                            .part_id = descriptor.part_id,
                                            .total_bytes = descriptor.file_length,
                                            .content_sha256 = descriptor.content_sha256};

  [[nodiscard]] TabletPhysicalPartChunkStorageConfig
  transfer_config(const std::filesystem::path& directory) const {
    return {.directory_path = directory.string(),
            .session = session,
            .codec_limits = {.maximum_object_bytes = encoded.size(),
                             .maximum_chunk_bytes = 512U,
                             .maximum_encoded_bytes = 1024U}};
  }

  [[nodiscard]] TabletPhysicalPartInstallRequest request(const common::Uuid nonce) const {
    return {.expected_manifest_generation = session.manifest_generation,
            .descriptor = descriptor,
            .owner = owner,
            .schema = std::cref(schema),
            .nonce = nonce,
            .maximum_materialized_bytes = encoded.size(),
            .validation_limits = {}};
  }
};

void receive(PhysicalPartFixture& fixture, TabletPhysicalPartChunkStorage& transfer) {
  const common::ByteView bytes = fixture.encoded.bytes();
  constexpr std::size_t kSplit = 257U;
  std::uint64_t offset = 0U;
  while (offset < bytes.size()) {
    const std::size_t count = std::min(kSplit, bytes.size() - static_cast<std::size_t>(offset));
    std::vector<std::byte> payload(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                                   bytes.begin() + static_cast<std::ptrdiff_t>(offset + count));
    ASSERT_TRUE(
        transfer
            .install({.session = fixture.session, .offset = offset, .bytes = std::move(payload)})
            .has_value());
    offset += count;
  }
}

TEST(TabletPhysicalPartInstallTest, InstallsVerifiedReceivedCsegAndRetriesExactly) {
  InstallTemporaryDirectory transfer_directory{"chronos-part-install-transfer"};
  InstallTemporaryDirectory database{"chronos-part-install-database"};
  ASSERT_FALSE(transfer_directory.path().empty());
  ASSERT_FALSE(database.path().empty());
  establish_manifest_layout(database.path());
  PhysicalPartFixture fixture;
  auto transfer =
      TabletPhysicalPartChunkStorage::create(fixture.transfer_config(transfer_directory.path()));
  ASSERT_TRUE(transfer.has_value()) << transfer.error().to_string();
  receive(fixture, *transfer);
  auto destination =
      manifest::ManifestStorage::open_existing({.database_root = database.path().string()});
  ASSERT_TRUE(destination.has_value()) << destination.error().to_string();

  auto installed =
      install_tablet_physical_part(*transfer, *destination, fixture.request(uuid(20U)));
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->transfer.session, fixture.session);
  EXPECT_EQ(installed->part.descriptor, fixture.descriptor);
  const auto final_path = database.path() / manifest::kPartsDirectoryName /
                          manifest::part_file_name(fixture.descriptor.part_id);
  EXPECT_TRUE(std::filesystem::is_regular_file(final_path));
  EXPECT_EQ(std::filesystem::file_size(final_path), fixture.encoded.size());

  auto retried = install_tablet_physical_part(*transfer, *destination, fixture.request(uuid(21U)));
  ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
  EXPECT_EQ(destination->metrics().installed_parts, 1U);
}

TEST(TabletPhysicalPartInstallTest, RejectsAuthorityMismatchAndMaterializationExhaustion) {
  InstallTemporaryDirectory transfer_directory{"chronos-part-install-transfer"};
  InstallTemporaryDirectory database{"chronos-part-install-database"};
  establish_manifest_layout(database.path());
  PhysicalPartFixture fixture;
  auto transfer =
      TabletPhysicalPartChunkStorage::create(fixture.transfer_config(transfer_directory.path()));
  ASSERT_TRUE(transfer.has_value());
  receive(fixture, *transfer);
  auto destination =
      manifest::ManifestStorage::open_existing({.database_root = database.path().string()});
  ASSERT_TRUE(destination.has_value());

  auto wrong_generation = fixture.request(uuid(22U));
  ++wrong_generation.expected_manifest_generation;
  EXPECT_EQ(install_tablet_physical_part(*transfer, *destination, wrong_generation).error().code(),
            common::StatusCode::kInvalidArgument);
  auto wrong_source = fixture.request(uuid(23U));
  wrong_source.owner.source_id = uuid(99U);
  EXPECT_EQ(install_tablet_physical_part(*transfer, *destination, wrong_source).error().code(),
            common::StatusCode::kCorruption);
  auto exhausted = fixture.request(uuid(24U));
  exhausted.maximum_materialized_bytes = fixture.encoded.size() - 1U;
  EXPECT_EQ(install_tablet_physical_part(*transfer, *destination, exhausted).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(std::filesystem::is_empty(database.path() / manifest::kPartsDirectoryName));
  EXPECT_EQ(destination->metrics().attempts, 0U);
}

} // namespace
} // namespace chronos::cluster
