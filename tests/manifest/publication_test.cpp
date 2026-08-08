#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/compaction.hpp"
#include "chronos/manifest/generation_builder.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/sealed_head_flush.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "manifest/publication_internal.hpp"

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <latch>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <thread>
#include <type_traits>
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
  wal::WalId result{};
  result.bytes.front() = std::byte{0x70U};
  return result;
}

[[nodiscard]] ingest::Sha256Digest digest(const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return ingest::Sha256Digest{bytes};
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-publication-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    const char* const created = ::mkdtemp(writable.data());
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

[[nodiscard]] std::shared_ptr<const schema::TableSchema> make_schema() {
  const schema::ColumnId event = id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = event,
                                   .physical_ordering_key = {event},
                                   .partition_columns = {event},
                                   .shard_key = {event},
                                   .deduplication_key = {}})
          .value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(const std::shared_ptr<const schema::TableSchema>& schema_value,
      const std::span<const std::int64_t> values) {
  std::vector<std::byte> encoded;
  encoded.reserve(values.size() * sizeof(std::int64_t));
  for (const std::int64_t value : values) {
    append_le(encoded, value);
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(columnar::OwnedColumnVector::create(
                        {.column_id = schema_value->event_time_column(),
                         .type = schema_value->columns().front().type(),
                         .nullable = false,
                         .row_count = static_cast<std::uint32_t>(values.size()),
                         .null_count = 0U},
                        {.validity = {}, .offsets = {}, .values = std::move(encoded)})
                        .value());
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema_value, std::move(columns)).value());
}

struct PublishedTabletFixture {
  PublishedTabletFixture()
      : schema_value(make_schema()),
        state(ingest::TabletState::create(
                  schema_value, id<schema::TabletId>(3U),
                  {.head_capacity = {.row_capacity = 2U, .variable_value_bytes = {0U}},
                   .maximum_schema_versions = 1U,
                   .maximum_sealed_generations = 1U,
                   .maximum_retry_entries = 8U,
                   .flush_queue = nullptr})
                  .value()) {
    const std::array first_values{std::int64_t{-5}, std::int64_t{10}};
    first_identity = {.client_id = id<ingest::ClientId>(0x11U),
                      .client_batch_id = id<ingest::ClientBatchId>(0x12U)};
    first_mutation = {.table_id = schema_value->table_id(),
                      .tablet_id = id<schema::TabletId>(3U),
                      .request_digest = digest(0x13U)};
    ingest::PreparedTabletAppend first =
        state.prepare_append(first_identity, first_mutation, batch(schema_value, first_values))
            .value();
    EXPECT_TRUE(first.mark_wal_started().is_ok());
    ingest::TabletAppendResult first_result =
        first.publish({.wal_id = wal_id(), .record_sequence = 7U}).value();
    first_outcome = first_result.outcome;
    first_epoch = first_result.snapshot;

    const std::array second_values{std::int64_t{20}};
    const ingest::RetryIdentity second_identity{.client_id = id<ingest::ClientId>(0x21U),
                                                .client_batch_id =
                                                    id<ingest::ClientBatchId>(0x22U)};
    const ingest::ColumnarAppendMutationIdentity second_mutation{
        .table_id = schema_value->table_id(),
        .tablet_id = id<schema::TabletId>(3U),
        .request_digest = digest(0x23U)};
    ingest::PreparedTabletAppend second =
        state.prepare_append(second_identity, second_mutation, batch(schema_value, second_values))
            .value();
    EXPECT_TRUE(second.mark_wal_started().is_ok());
    latest = second.publish({.wal_id = wal_id(), .record_sequence = 8U}).value().snapshot;
  }

  std::shared_ptr<const schema::TableSchema> schema_value;
  ingest::TabletState state;
  ingest::RetryIdentity first_identity{id<ingest::ClientId>(1U), id<ingest::ClientBatchId>(2U)};
  ingest::ColumnarAppendMutationIdentity first_mutation{id<schema::TableId>(1U),
                                                        id<schema::TabletId>(2U), digest(1U)};
  std::shared_ptr<const ingest::ColumnarAppendRetryOutcome> first_outcome;
  ingest::TabletSnapshot first_epoch{state.snapshot().value()};
  ingest::TabletSnapshot latest{state.snapshot().value()};
};

void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  // std::ofstream has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(output.good());
}

struct DurableFixture {
  DurableFixture()
      : lineage(schema::SchemaLineage::create(*tablet.schema_value).value()),
        database_id(id<DatabaseId>(6U)),
        generation_one_bytes(encode_manifest_v1({.generation = 1U,
                                                 .database_id = database_id,
                                                 .wal_id = wal_id(),
                                                 .reclaim_checkpoint = {.record_sequence = 0U,
                                                                        .segment_number = 1U,
                                                                        .byte_offset = 64U},
                                                 .tablets = {},
                                                 .parts = {},
                                                 .retries = {}})
                                 .value()),
        generation_one_view(decode_manifest_v1_exact(generation_one_bytes.bytes()).value()) {
    EXPECT_FALSE(directory.path().empty());
    EXPECT_TRUE(std::filesystem::create_directory(directory.path() / kPartsDirectoryName));
    EXPECT_TRUE(std::filesystem::create_directory(directory.path() / kManifestDirectoryName));
    write_bytes(directory.path() / kManifestDirectoryName / std::string{kManifestLockFileName}, {});
    write_bytes(directory.path() / kManifestDirectoryName / *manifest_file_name(1U),
                generation_one_bytes.bytes());
    storage = std::make_unique<ManifestStorage>(
        ManifestStorage::open_existing({.database_root = directory.path().string()}).value());
    generation_one = std::make_shared<const LoadedManifestGeneration>(
        storage
            ->load_selected_manifest({.expected_database_id = database_id,
                                      .expected_wal_id = wal_id(),
                                      .schema_bindings = {},
                                      .decode_limits = {},
                                      .part_validation_limits = {}})
            .value());

    flushed = std::make_unique<EncodedSealedHeadPart>(
        encode_sealed_head_v1({.snapshot = tablet.latest.sealed_generations().front(),
                               .part_id = id<cseg::PartId>(9U),
                               .compression = cseg::PageCompression::kNone})
            .value());
    retry = {.client_id = tablet.first_identity.client_id,
             .client_batch_id = tablet.first_identity.client_batch_id,
             .table_id = tablet.first_mutation.table_id,
             .tablet_id = tablet.first_mutation.tablet_id,
             .request_digest = tablet.first_mutation.request_digest,
             .wal_id = wal_id(),
             .record_sequence = 7U,
             .applied_row_count = 2U};
    const std::array retries{retry};
    const std::array bindings{
        TabletSchemaBinding{.tablet_id = tablet.latest.tablet_id(), .lineage = std::cref(lineage)}};
    generation_two_bytes = std::make_unique<EncodedManifest>(
        build_manifest_v1_for_sealed_head({.predecessor = generation_one_view,
                                           .sealed_part = *flushed,
                                           .new_retries = retries,
                                           .schema_bindings = bindings,
                                           .part_validation_limits = {}})
            .value());
    EXPECT_TRUE(storage
                    ->install_part({.encoded_part = std::cref(flushed->encoded_part),
                                    .descriptor = flushed->descriptor,
                                    .wal_id = wal_id(),
                                    .schema = std::cref(*tablet.schema_value),
                                    .nonce = id<DatabaseId>(0xa0U).uuid(),
                                    .validation_limits = {}})
                    .has_value());
    EXPECT_TRUE(storage
                    ->install_manifest({.encoded_manifest = std::cref(*generation_two_bytes),
                                        .schema_bindings = bindings,
                                        .nonce = id<DatabaseId>(0xb0U).uuid(),
                                        .decode_limits = {},
                                        .part_validation_limits = {},
                                        .compaction_equivalence_limits = {}})
                    .has_value());
    generation_two = std::make_shared<const LoadedManifestGeneration>(
        storage
            ->load_selected_manifest({.expected_database_id = database_id,
                                      .expected_wal_id = wal_id(),
                                      .schema_bindings = bindings,
                                      .decode_limits = {},
                                      .part_validation_limits = {}})
            .value());
  }

  [[nodiscard]] DatabaseStoragePublisher publisher() const {
    const std::array inputs{DatabaseStorageTabletInput{.snapshot = std::cref(tablet.latest)}};
    return DatabaseStoragePublisher::create(generation_one, inputs).value();
  }

  [[nodiscard]] DurableManifestPublicationRequest request() const {
    replacement = {.tablet_id = tablet.latest.tablet_id(),
                   .head_generation = tablet.latest.sealed_generations().front().generation(),
                   .replacement_part_id = flushed->descriptor.part_id};
    return {.selected_manifest = generation_two, .replacements = {&replacement, 1U}};
  }

  PublishedTabletFixture tablet;
  schema::SchemaLineage lineage;
  DatabaseId database_id;
  EncodedManifest generation_one_bytes;
  DecodedManifestView generation_one_view;
  TemporaryDirectory directory;
  std::unique_ptr<ManifestStorage> storage;
  std::shared_ptr<const LoadedManifestGeneration> generation_one;
  std::unique_ptr<EncodedSealedHeadPart> flushed;
  RetryDescriptor retry{id<ingest::ClientId>(1U),
                        id<ingest::ClientBatchId>(2U),
                        id<schema::TableId>(3U),
                        id<schema::TabletId>(4U),
                        digest(1U),
                        wal_id(),
                        1U,
                        1U};
  std::unique_ptr<EncodedManifest> generation_two_bytes;
  std::shared_ptr<const LoadedManifestGeneration> generation_two;
  mutable SealedHeadReplacement replacement{id<schema::TabletId>(1U), 1U, id<cseg::PartId>(2U)};
};

TEST(DatabaseStoragePublicationTest, RefreshesOneCompleteTabletEpochUnderTheSameManifest) {
  const DurableFixture fixture;
  const std::array first_input{
      DatabaseStorageTabletInput{.snapshot = std::cref(fixture.tablet.first_epoch)}};
  DatabaseStoragePublisher publisher =
      DatabaseStoragePublisher::create(fixture.generation_one, first_input).value();
  const DatabaseStorageSnapshot before = publisher.snapshot().value();
  ASSERT_NE(before.find_tablet(fixture.tablet.latest.tablet_id()), nullptr);
  EXPECT_TRUE(before.find_tablet(fixture.tablet.latest.tablet_id())->sealed_heads().empty());
  EXPECT_EQ(before.visible_head_row_count(), 2U);

  const DatabaseStorageSnapshot after =
      publisher.publish_tablet_snapshot(fixture.tablet.latest).value();
  ASSERT_EQ(after.find_tablet(fixture.tablet.latest.tablet_id())->sealed_heads().size(), 1U);
  EXPECT_EQ(after.find_tablet(fixture.tablet.latest.tablet_id())->active_head()->row_count(), 1U);
  EXPECT_EQ(after.visible_head_row_count(), 3U);
  EXPECT_TRUE(before.find_tablet(fixture.tablet.latest.tablet_id())->sealed_heads().empty());
  const auto stale = publisher.publish_tablet_snapshot(fixture.tablet.first_epoch);
  ASSERT_FALSE(stale.has_value());
  EXPECT_EQ(stale.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(publisher.snapshot()->visible_head_row_count(), 3U);
}

TEST(DatabaseStoragePublicationTest, AtomicallySubstitutesDurablePartForExactSealedHead) {
  std::optional<DatabaseStorageSnapshot> old;
  std::optional<DatabaseStorageSnapshot> next;
  std::optional<PartDescriptor> replacement_part;
  std::size_t old_manifest_size = 0U;
  {
    const DurableFixture fixture;
    DatabaseStoragePublisher publisher = fixture.publisher();
    old = publisher.snapshot().value();
    next = publisher.publish_manifest(fixture.request()).value();
    replacement_part = fixture.flushed->descriptor;
    old_manifest_size = fixture.generation_one_bytes.size();
  }

  // Both database epochs remain self-contained after the storage owner, TabletState, and live
  // publisher are gone.
  EXPECT_EQ(old->generation(), 1U);
  EXPECT_TRUE(old->parts().empty());
  ASSERT_EQ(old->find_tablet(id<schema::TabletId>(3U))->sealed_heads().size(), 1U);
  EXPECT_EQ(old->visible_head_row_count(), 3U);
  EXPECT_EQ(next->generation(), 2U);
  ASSERT_EQ(next->parts().size(), 1U);
  EXPECT_EQ(next->parts().front(), *replacement_part);
  EXPECT_TRUE(next->find_tablet(id<schema::TabletId>(3U))->sealed_heads().empty());
  ASSERT_NE(next->find_tablet(id<schema::TabletId>(3U))->active_head(), nullptr);
  EXPECT_EQ(next->find_tablet(id<schema::TabletId>(3U))->active_head()->row_count(), 1U);
  EXPECT_EQ(next->visible_head_row_count(), 1U);
  ASSERT_EQ(next->retirement_receipts().size(), 1U);
  EXPECT_EQ(next->retirement_receipts().front().head_generation(), 1U);
  EXPECT_EQ(next->retirement_receipts().front().row_count(), 2U);
  EXPECT_EQ(next->retirement_receipts().front().minimum_record_sequence(), 7U);
  EXPECT_EQ(next->retirement_receipts().front().maximum_record_sequence(), 7U);
  EXPECT_EQ(old->manifest_bytes().size(), old_manifest_size);
  const auto old_cell =
      old->find_tablet(id<schema::TabletId>(3U))->sealed_heads().front().cell({0U, 0U});
  ASSERT_TRUE(old_cell.has_value()) << old_cell.error().to_string();
  EXPECT_TRUE(old_cell->bytes().has_value());
}

struct PauseHook {
  std::latch entered{1};
  std::latch release{1};
};

void pause_before_publication(void* context) noexcept {
  auto* const hook = static_cast<PauseHook*>(context);
  hook->entered.count_down();
  hook->release.wait();
}

TEST(DatabaseStoragePublicationTest, CompactionPublishesOneEpochAndRetainsOldSnapshotInputs) {
  std::optional<DatabaseStorageSnapshot> old;
  std::optional<DatabaseStorageSnapshot> next;
  std::optional<PartDescriptor> input_descriptor;
  std::optional<PartDescriptor> output_descriptor;
  std::shared_ptr<const SnapshotPartImage> query_image;
  {
    DurableFixture fixture;
    DatabaseStoragePublisher publisher = fixture.publisher();
    ASSERT_TRUE(publisher.publish_manifest(fixture.request()).has_value());
    old = publisher.snapshot().value();
    ASSERT_EQ(old->retirement_receipts().size(), 1U);
    const ingest::TabletSnapshot retired_tablet =
        fixture.tablet.state.retire_sealed_generation(old->retirement_receipts().front()).value();
    // Create a newer publication object under the same Manifest before compaction. The older
    // snapshot must still pin its selected input even though it is not the immediate predecessor.
    ASSERT_TRUE(publisher.publish_tablet_snapshot(retired_tablet).has_value());
    const std::array load_bindings{TabletSchemaBinding{
        .tablet_id = fixture.tablet.latest.tablet_id(), .lineage = std::cref(fixture.lineage)}};
    const std::array load_ids{fixture.flushed->descriptor.part_id};
    std::vector<SnapshotPartImage> loaded =
        fixture.storage->load_snapshot_part_images(*old, load_ids, load_bindings, {}).value();
    query_image = std::make_shared<const SnapshotPartImage>(std::move(loaded.front()));

    const std::array input_images{CompactionPartImage{
        .part_id = fixture.flushed->descriptor.part_id,
        .bytes = fixture.flushed->encoded_part.bytes(),
    }};
    common::Result<EncodedCompactionPart> merged =
        merge_append_only_cseg_v1({.inputs = input_images,
                                   .schema = std::cref(*fixture.tablet.schema_value),
                                   .tablet_id = fixture.tablet.latest.tablet_id(),
                                   .wal_id = wal_id(),
                                   .output_part_id = id<cseg::PartId>(10U),
                                   .limits = {}});
    ASSERT_TRUE(merged.has_value()) << merged.error().to_string();
    const DecodedManifestView predecessor =
        decode_manifest_v1_exact(fixture.generation_two_bytes->bytes()).value();
    const std::array bindings{TabletSchemaBinding{.tablet_id = fixture.tablet.latest.tablet_id(),
                                                  .lineage = std::cref(fixture.lineage)}};
    common::Result<EncodedManifest> candidate = build_manifest_v1_for_append_only_compaction(
        {.predecessor = predecessor,
         .inputs = input_images,
         .output = std::cref(*merged),
         .schema = std::cref(*fixture.tablet.schema_value),
         .schema_bindings = bindings,
         .equivalence_limits = {},
         .part_validation_limits = {}});
    ASSERT_TRUE(candidate.has_value()) << candidate.error().to_string();
    ASSERT_TRUE(fixture.storage
                    ->install_part({.encoded_part = std::cref(merged->encoded_part),
                                    .descriptor = merged->descriptor,
                                    .wal_id = merged->wal_id,
                                    .schema = std::cref(*fixture.tablet.schema_value),
                                    .nonce = id<DatabaseId>(0xc0U).uuid(),
                                    .validation_limits = {}})
                    .has_value());
    const std::array input_ids{fixture.flushed->descriptor.part_id};
    const std::array output_ids{merged->descriptor.part_id};
    const ManifestCompactionReplacement replacement{
        .tablet_id = fixture.tablet.latest.tablet_id(),
        .input_part_ids = input_ids,
        .output_part_ids = output_ids,
    };
    ASSERT_TRUE(fixture.storage
                    ->install_manifest({.encoded_manifest = std::cref(*candidate),
                                        .schema_bindings = bindings,
                                        .nonce = id<DatabaseId>(0xd0U).uuid(),
                                        .decode_limits = {},
                                        .part_validation_limits = {},
                                        .compaction_replacement = &replacement,
                                        .compaction_equivalence_limits = {}})
                    .has_value());
    auto generation_three = std::make_shared<const LoadedManifestGeneration>(
        fixture.storage
            ->load_selected_manifest({.expected_database_id = fixture.database_id,
                                      .expected_wal_id = wal_id(),
                                      .schema_bindings = bindings,
                                      .decode_limits = {},
                                      .part_validation_limits = {}})
            .value());
    const DurableCompactionPublicationRequest publication_request{
        .selected_manifest = generation_three,
        .schema_bindings = bindings,
        .replacement = replacement,
    };
    PauseHook hook;
    DatabaseStoragePublisher atomic_publisher =
        detail::DatabaseStoragePublisherTestAccess::create(fixture.generation_two, {},
                                                           &pause_before_publication, &hook)
            .value();
    std::optional<DatabaseStorageSnapshot> atomically_published;
    std::thread writer([&] {
      atomically_published =
          atomic_publisher.publish_compaction_manifest(publication_request).value();
    });
    hook.entered.wait();
    const DatabaseStorageSnapshot during = atomic_publisher.snapshot().value();
    EXPECT_EQ(during.generation(), 2U);
    ASSERT_EQ(during.parts().size(), 1U);
    EXPECT_EQ(during.parts().front(), fixture.flushed->descriptor);
    hook.release.count_down();
    writer.join();
    ASSERT_TRUE(atomically_published.has_value());
    const DatabaseStorageSnapshot published_snapshot = atomically_published.value_or(during);
    EXPECT_EQ(published_snapshot.generation(), 3U);
    ASSERT_EQ(published_snapshot.parts().size(), 1U);
    EXPECT_EQ(published_snapshot.parts().front(), merged->descriptor);
    EXPECT_EQ(during.generation(), 2U);

    next = publisher.publish_compaction_manifest(publication_request).value();
    input_descriptor = fixture.flushed->descriptor;
    output_descriptor = merged->descriptor;

    common::Result<std::vector<RetiredPartSet>> retired = publisher.drain_retired_part_sets();
    ASSERT_TRUE(retired.has_value());
    ASSERT_EQ(retired->size(), 1U);
    RetiredPartSet retirement = std::move(retired->front());
    EXPECT_EQ(retirement.predecessor_generation(), 2U);
    ASSERT_EQ(retirement.parts().size(), 1U);
    EXPECT_EQ(retirement.parts().front().part_id, input_descriptor->part_id);
    EXPECT_EQ(retirement.parts().front().file_length, input_descriptor->file_length);
    EXPECT_TRUE(retirement.is_pinned());
    EXPECT_TRUE(publisher.drain_retired_part_sets()->empty());

    const PartReclamationReport pending =
        fixture.storage
            ->reclaim_retired_parts({.selected_manifest = std::cref(*generation_three),
                                     .retirement = std::cref(retirement),
                                     .decode_limits = {}})
            .value();
    EXPECT_EQ(pending.outcome, PartReclamationOutcome::kPending);
    EXPECT_EQ(pending.removed_parts, 0U);

    ASSERT_TRUE(old.has_value());
    EXPECT_EQ(old->generation(), 2U);
    ASSERT_EQ(old->parts().size(), 1U);
    EXPECT_EQ(old->parts().front(), *input_descriptor);
    EXPECT_EQ(old->visible_head_row_count(), 1U);
    ASSERT_NE(old->find_tablet(id<schema::TabletId>(3U))->active_head(), nullptr);
    EXPECT_EQ(old->find_tablet(id<schema::TabletId>(3U))->active_head()->row_count(), 1U);
    EXPECT_TRUE(decode_manifest_v1_exact(old->manifest_bytes()).has_value());
    std::optional<DatabaseStorageRetentionToken> token{old->retention_token()};
    EXPECT_EQ(token->generation(), 2U);
    old.reset();
    EXPECT_TRUE(retirement.is_pinned());
    token.reset();
    EXPECT_TRUE(retirement.is_pinned());
    query_image.reset();
    EXPECT_FALSE(retirement.is_pinned());

    const PartReclamationReport reclaimed =
        fixture.storage
            ->reclaim_retired_parts({.selected_manifest = std::cref(*generation_three),
                                     .retirement = std::cref(retirement),
                                     .decode_limits = {}})
            .value();
    EXPECT_EQ(reclaimed.outcome, PartReclamationOutcome::kReclaimed);
    EXPECT_EQ(reclaimed.removed_parts, 1U);
    EXPECT_EQ(reclaimed.removed_bytes, input_descriptor->file_length);
    EXPECT_EQ(reclaimed.directory_syncs, 1U);
    EXPECT_FALSE(std::filesystem::exists(fixture.directory.path() / kPartsDirectoryName /
                                         part_file_name(input_descriptor->part_id)));

    const PartReclamationReport repeated =
        fixture.storage
            ->reclaim_retired_parts({.selected_manifest = std::cref(*generation_three),
                                     .retirement = std::cref(retirement),
                                     .decode_limits = {}})
            .value();
    EXPECT_EQ(repeated.outcome, PartReclamationOutcome::kReclaimed);
    EXPECT_EQ(repeated.removed_parts, 0U);
    EXPECT_EQ(repeated.already_absent_parts, 1U);
    EXPECT_EQ(repeated.directory_syncs, 0U);
    EXPECT_EQ(fixture.storage->reclamation_metrics().attempts, 3U);
    EXPECT_EQ(fixture.storage->reclamation_metrics().pending, 1U);
    EXPECT_EQ(fixture.storage->reclamation_metrics().reclaimed_parts, 1U);

    EXPECT_TRUE(std::filesystem::exists(fixture.directory.path() / kPartsDirectoryName /
                                        part_file_name(output_descriptor->part_id)));
  }

  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(next->generation(), 3U);
  ASSERT_EQ(next->parts().size(), 1U);
  EXPECT_EQ(next->parts().front(), *output_descriptor);
  EXPECT_EQ(next->visible_head_row_count(), 1U);
  ASSERT_NE(next->find_tablet(id<schema::TabletId>(3U))->active_head(), nullptr);
  EXPECT_EQ(next->find_tablet(id<schema::TabletId>(3U))->active_head()->row_count(), 1U);
  EXPECT_TRUE(decode_manifest_v1_exact(next->manifest_bytes()).has_value());
}

TEST(DatabaseStoragePublicationTest, ReadersSeeOnlyOldOrNewCompleteEpochAtReleaseStore) {
  const DurableFixture fixture;
  PauseHook hook;
  const std::array inputs{DatabaseStorageTabletInput{.snapshot = std::cref(fixture.tablet.latest)}};
  DatabaseStoragePublisher publisher =
      detail::DatabaseStoragePublisherTestAccess::create(fixture.generation_one, inputs,
                                                         &pause_before_publication, &hook)
          .value();
  std::optional<DatabaseStorageSnapshot> published;
  std::thread writer([&] { published = publisher.publish_manifest(fixture.request()).value(); });
  hook.entered.wait();

  const DatabaseStorageSnapshot during = publisher.snapshot().value();
  EXPECT_EQ(during.generation(), 1U);
  EXPECT_TRUE(during.parts().empty());
  EXPECT_TRUE(during.retirement_receipts().empty());
  EXPECT_EQ(during.find_tablet(fixture.tablet.latest.tablet_id())->sealed_heads().size(), 1U);
  hook.release.count_down();
  writer.join();

  ASSERT_TRUE(published.has_value());
  const DatabaseStorageSnapshot published_snapshot = published.value_or(during);
  EXPECT_EQ(published_snapshot.generation(), 2U);
  EXPECT_EQ(published_snapshot.parts().size(), 1U);
  EXPECT_EQ(published_snapshot.retirement_receipts().size(), 1U);
  EXPECT_TRUE(
      published_snapshot.find_tablet(fixture.tablet.latest.tablet_id())->sealed_heads().empty());
  EXPECT_EQ(during.generation(), 1U);
  EXPECT_EQ(during.find_tablet(fixture.tablet.latest.tablet_id())->sealed_heads().size(), 1U);
}

TEST(DatabaseStoragePublicationTest, ReceiptRetiresTabletHeadIdempotentlyAndReleasesBackpressure) {
  DurableFixture fixture;
  DatabaseStoragePublisher publisher = fixture.publisher();
  const ingest::TabletSnapshot before_retirement = fixture.tablet.state.snapshot().value();

  const std::array third_values{std::int64_t{30}, std::int64_t{40}};
  const ingest::RetryIdentity third_identity{.client_id = id<ingest::ClientId>(0x31U),
                                             .client_batch_id = id<ingest::ClientBatchId>(0x32U)};
  const ingest::ColumnarAppendMutationIdentity third_mutation{
      .table_id = fixture.tablet.schema_value->table_id(),
      .tablet_id = fixture.tablet.latest.tablet_id(),
      .request_digest = digest(0x33U)};
  EXPECT_EQ(fixture.tablet.state
                .prepare_append(third_identity, third_mutation,
                                batch(fixture.tablet.schema_value, third_values))
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  const DatabaseStorageSnapshot database = publisher.publish_manifest(fixture.request()).value();
  ASSERT_EQ(database.retirement_receipts().size(), 1U);
  const ingest::TabletSnapshot retired =
      fixture.tablet.state.retire_sealed_generation(database.retirement_receipts().front()).value();
  EXPECT_TRUE(retired.sealed_generations().empty());
  EXPECT_EQ(retired.visible_row_count(), 1U);
  EXPECT_EQ(fixture.tablet.state.metrics().sealed_generations, 0U);
  EXPECT_EQ(fixture.tablet.state.metrics().visible_rows, 1U);
  ASSERT_EQ(before_retirement.sealed_generations().size(), 1U);
  EXPECT_EQ(before_retirement.visible_row_count(), 3U);

  const ingest::TabletSnapshot repeated =
      fixture.tablet.state.retire_sealed_generation(database.retirement_receipts().front()).value();
  EXPECT_TRUE(repeated.sealed_generations().empty());
  ingest::PreparedTabletAppend unblocked =
      fixture.tablet.state
          .prepare_append(third_identity, third_mutation,
                          batch(fixture.tablet.schema_value, third_values))
          .value();
  EXPECT_TRUE(unblocked.cancel_before_wal().is_ok());
}

TEST(DatabaseStoragePublicationTest, HostileReplacementFailsClosedWithoutPartialPublication) {
  const DurableFixture fixture;
  DatabaseStoragePublisher publisher = fixture.publisher();
  SealedHeadReplacement wrong = {.tablet_id = fixture.tablet.latest.tablet_id(),
                                 .head_generation = 99U,
                                 .replacement_part_id = fixture.flushed->descriptor.part_id};
  const auto failed = publisher.publish_manifest(
      {.selected_manifest = fixture.generation_two, .replacements = {&wrong, 1U}});
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(publisher.is_usable());
  EXPECT_EQ(publisher.snapshot().error().code(), common::StatusCode::kUnavailable);
}

TEST(DatabaseStoragePublicationTest, HostileCompactionSuccessorFailsClosedWithoutPublication) {
  const DurableFixture fixture;
  DatabaseStoragePublisher publisher = fixture.publisher();
  const std::array bindings{TabletSchemaBinding{.tablet_id = fixture.tablet.latest.tablet_id(),
                                                .lineage = std::cref(fixture.lineage)}};
  const std::array input_ids{id<cseg::PartId>(0xeeU)};
  const std::array output_ids{fixture.flushed->descriptor.part_id};
  const auto failed = publisher.publish_compaction_manifest(
      {.selected_manifest = fixture.generation_two,
       .schema_bindings = bindings,
       .replacement = {.tablet_id = fixture.tablet.latest.tablet_id(),
                       .input_part_ids = input_ids,
                       .output_part_ids = output_ids}});
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(publisher.is_usable());
  EXPECT_EQ(publisher.snapshot().error().code(), common::StatusCode::kUnavailable);
}

TEST(DatabaseStoragePublicationTest, RejectsReintroductionOfRowsCoveredByDurableBoundary) {
  const DurableFixture fixture;
  DatabaseStoragePublisher publisher = fixture.publisher();
  ASSERT_TRUE(publisher.publish_manifest(fixture.request()).has_value());
  const auto reintroduced = publisher.publish_tablet_snapshot(fixture.tablet.latest);
  ASSERT_FALSE(reintroduced.has_value());
  EXPECT_EQ(reintroduced.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(publisher.is_usable());
  EXPECT_EQ(publisher.snapshot()->generation(), 2U);
}

} // namespace
} // namespace chronos::manifest
