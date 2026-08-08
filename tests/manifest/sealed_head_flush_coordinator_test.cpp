#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/sealed_head_flush_queue.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/generation_builder.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/sealed_head_flush.hpp"
#include "chronos/manifest/sealed_head_flush_coordinator.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <string>
#include <system_error>
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

[[nodiscard]] common::Uuid nonce(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
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
        (std::filesystem::temp_directory_path() / "chronos-flush-coordinator-XXXXXX").string();
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

void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  // std::ofstream has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(output.good());
}

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

struct FlushFixture {
  FlushFixture()
      : schema_value(make_schema()), lineage(schema::SchemaLineage::create(*schema_value).value()),
        queue(ingest::SealedHeadFlushQueue::create({.capacity = 2U}).value()),
        database_id(id<DatabaseId>(6U)) {
    state = std::make_unique<ingest::TabletState>(
        ingest::TabletState::create(
            schema_value, id<schema::TabletId>(3U),
            {.head_capacity = {.row_capacity = 2U, .variable_value_bytes = {0U}},
             .maximum_schema_versions = 1U,
             .maximum_sealed_generations = 2U,
             .maximum_retry_entries = 8U,
             .flush_queue = queue})
            .value());

    first_identity = {.client_id = id<ingest::ClientId>(0x11U),
                      .client_batch_id = id<ingest::ClientBatchId>(0x12U)};
    first_mutation = {.table_id = schema_value->table_id(),
                      .tablet_id = id<schema::TabletId>(3U),
                      .request_digest = digest(0x13U)};
    const std::array first_values{std::int64_t{-5}, std::int64_t{10}};
    ingest::PreparedTabletAppend first =
        state->prepare_append(first_identity, first_mutation, batch(schema_value, first_values))
            .value();
    EXPECT_TRUE(first.mark_wal_started().is_ok());
    first_outcome = first.publish({.wal_id = wal_id(), .record_sequence = 7U}).value().outcome;

    const ingest::RetryIdentity second_identity{.client_id = id<ingest::ClientId>(0x21U),
                                                .client_batch_id =
                                                    id<ingest::ClientBatchId>(0x22U)};
    const ingest::ColumnarAppendMutationIdentity second_mutation{
        .table_id = schema_value->table_id(),
        .tablet_id = id<schema::TabletId>(3U),
        .request_digest = digest(0x23U)};
    const std::array second_values{std::int64_t{20}};
    ingest::PreparedTabletAppend second =
        state->prepare_append(second_identity, second_mutation, batch(schema_value, second_values))
            .value();
    EXPECT_TRUE(second.mark_wal_started().is_ok());
    latest = std::make_unique<ingest::TabletSnapshot>(
        second.publish({.wal_id = wal_id(), .record_sequence = 8U}).value().snapshot);

    EXPECT_TRUE(std::filesystem::create_directory(directory.path() / kPartsDirectoryName));
    EXPECT_TRUE(std::filesystem::create_directory(directory.path() / kManifestDirectoryName));
    write_bytes(directory.path() / kManifestDirectoryName / std::string{kManifestLockFileName}, {});
    generation_one = std::make_unique<EncodedManifest>(
        encode_manifest_v1({.generation = 1U,
                            .database_id = database_id,
                            .wal_id = wal_id(),
                            .reclaim_checkpoint = {.record_sequence = 0U,
                                                   .segment_number = 1U,
                                                   .byte_offset = 64U},
                            .tablets = {},
                            .parts = {},
                            .retries = {}})
            .value());
    write_bytes(directory.path() / kManifestDirectoryName / *manifest_file_name(1U),
                generation_one->bytes());
    storage = std::make_unique<ManifestStorage>(
        ManifestStorage::open_existing({.database_root = directory.path().string()}).value());
    selected_one = std::make_shared<const LoadedManifestGeneration>(
        storage
            ->load_selected_manifest({.expected_database_id = database_id,
                                      .expected_wal_id = wal_id(),
                                      .schema_bindings = {},
                                      .decode_limits = {},
                                      .part_validation_limits = {}})
            .value());
    const std::array inputs{DatabaseStorageTabletInput{.snapshot = std::cref(*latest)}};
    publisher = std::make_unique<DatabaseStoragePublisher>(
        DatabaseStoragePublisher::create(selected_one, inputs).value());
  }

  [[nodiscard]] RetryDescriptor retry() const {
    return {.client_id = first_identity.client_id,
            .client_batch_id = first_identity.client_batch_id,
            .table_id = first_mutation.table_id,
            .tablet_id = first_mutation.tablet_id,
            .request_digest = first_mutation.request_digest,
            .wal_id = first_outcome->wal_id,
            .record_sequence = first_outcome->record_sequence,
            .applied_row_count = first_outcome->applied_row_count};
  }

  [[nodiscard]] std::array<TabletSchemaBinding, 1> bindings() const {
    return {TabletSchemaBinding{.tablet_id = latest->tablet_id(), .lineage = std::cref(lineage)}};
  }

  [[nodiscard]] static SealedHeadFlushOperation
  operation(const cseg::PartId& part_id, const std::span<const RetryDescriptor> retries,
            const std::span<const TabletSchemaBinding> schema_bindings) {
    return {.part_id = part_id,
            .part_nonce = nonce(0xa0U),
            .manifest_nonce = nonce(0xb0U),
            .compression = cseg::PageCompression::kNone,
            .new_retries = retries,
            .schema_bindings = schema_bindings,
            .manifest_decode_limits = {},
            .part_validation_limits = {}};
  }

  void install_successor(const cseg::PartId& part_id,
                         const std::span<const RetryDescriptor> retries,
                         const std::span<const TabletSchemaBinding> schema_bindings) const {
    EncodedSealedHeadPart encoded =
        encode_sealed_head_v1({.snapshot = latest->sealed_generations().front(),
                               .part_id = part_id,
                               .compression = cseg::PageCompression::kNone})
            .value();
    ASSERT_TRUE(storage
                    ->install_part({.encoded_part = std::cref(encoded.encoded_part),
                                    .descriptor = encoded.descriptor,
                                    .wal_id = encoded.wal_id,
                                    .schema = std::cref(*schema_value),
                                    .nonce = nonce(0xa0U),
                                    .validation_limits = {}})
                    .has_value());
    const DecodedManifestView predecessor =
        decode_manifest_v1_exact(generation_one->bytes()).value();
    EncodedManifest successor =
        build_manifest_v1_for_sealed_head({.predecessor = predecessor,
                                           .sealed_part = encoded,
                                           .new_retries = retries,
                                           .schema_bindings = schema_bindings,
                                           .part_validation_limits = {}})
            .value();
    ASSERT_TRUE(storage
                    ->install_manifest({.encoded_manifest = std::cref(successor),
                                        .schema_bindings = schema_bindings,
                                        .nonce = nonce(0xb0U),
                                        .decode_limits = {},
                                        .part_validation_limits = {},
                                        .compaction_equivalence_limits = {}})
                    .has_value());
  }

  std::shared_ptr<const schema::TableSchema> schema_value;
  schema::SchemaLineage lineage;
  std::shared_ptr<ingest::SealedHeadFlushQueue> queue;
  DatabaseId database_id;
  TemporaryDirectory directory;
  std::unique_ptr<ingest::TabletState> state;
  ingest::RetryIdentity first_identity{id<ingest::ClientId>(1U), id<ingest::ClientBatchId>(2U)};
  ingest::ColumnarAppendMutationIdentity first_mutation{id<schema::TableId>(1U),
                                                        id<schema::TabletId>(2U), digest(1U)};
  std::shared_ptr<const ingest::ColumnarAppendRetryOutcome> first_outcome;
  std::unique_ptr<ingest::TabletSnapshot> latest;
  std::unique_ptr<EncodedManifest> generation_one;
  std::unique_ptr<ManifestStorage> storage;
  std::shared_ptr<const LoadedManifestGeneration> selected_one;
  std::unique_ptr<DatabaseStoragePublisher> publisher;
};

TEST(SealedHeadFlushCoordinatorTest, InstallsPublishesRetiresAndCompletesOneQueuedHead) {
  FlushFixture fixture;
  SealedHeadFlushCoordinator coordinator =
      SealedHeadFlushCoordinator::create(fixture.queue, *fixture.storage, *fixture.publisher)
          .value();
  const RetryDescriptor retry = fixture.retry();
  const std::array retries{retry};
  const auto bindings = fixture.bindings();
  const cseg::PartId part_id = id<cseg::PartId>(9U);

  const auto completed = coordinator.try_flush_one(
      *fixture.state, FlushFixture::operation(part_id, retries, bindings));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_TRUE(completed->has_value());
  const SealedHeadFlushCompletion completion =
      completed.value().value(); // NOLINT(bugprone-unchecked-optional-access)
  EXPECT_EQ(completion.manifest_generation, 2U);
  EXPECT_EQ(completion.row_count, 2U);
  EXPECT_FALSE(completion.resumed_durable_manifest);
  EXPECT_TRUE(fixture.state->snapshot()->sealed_generations().empty());
  EXPECT_EQ(fixture.queue->metrics().occupied, 0U);
  EXPECT_EQ(fixture.publisher->snapshot()->generation(), 2U);
  EXPECT_EQ(fixture.publisher->snapshot()->parts().size(), 1U);
  EXPECT_EQ(coordinator.metrics().completed, 1U);
  EXPECT_EQ(coordinator.metrics().encoded_rows, 2U);
  EXPECT_FALSE(
      coordinator
          .try_flush_one(*fixture.state, FlushFixture::operation(part_id, retries, bindings))
          ->has_value());
  EXPECT_EQ(coordinator.metrics().empty_polls, 1U);
}

TEST(SealedHeadFlushCoordinatorTest, KeepsWorkRetryableAfterPreManifestFailure) {
  FlushFixture fixture;
  SealedHeadFlushCoordinator coordinator =
      SealedHeadFlushCoordinator::create(fixture.queue, *fixture.storage, *fixture.publisher)
          .value();
  const auto bindings = fixture.bindings();
  const std::array<RetryDescriptor, 0> missing_retries{};
  const auto failed = coordinator.try_flush_one(
      *fixture.state, FlushFixture::operation(id<cseg::PartId>(8U), missing_retries, bindings));
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(coordinator.is_usable());
  EXPECT_EQ(fixture.queue->metrics().ready, 1U);

  const std::array retries{fixture.retry()};
  const auto completed = coordinator.try_flush_one(
      *fixture.state, FlushFixture::operation(id<cseg::PartId>(9U), retries, bindings));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_TRUE(completed->has_value());
  EXPECT_EQ(fixture.storage->scan_namespace()->final_parts.size(), 2U);
  EXPECT_EQ(coordinator.metrics().failures, 1U);
}

TEST(SealedHeadFlushCoordinatorTest, ResumesAnInstalledButUnpublishedSuccessor) {
  FlushFixture fixture;
  const std::array retries{fixture.retry()};
  const auto bindings = fixture.bindings();
  const cseg::PartId part_id = id<cseg::PartId>(9U);
  fixture.install_successor(part_id, retries, bindings);

  SealedHeadFlushCoordinator coordinator =
      SealedHeadFlushCoordinator::create(fixture.queue, *fixture.storage, *fixture.publisher)
          .value();
  const auto completed = coordinator.try_flush_one(
      *fixture.state, FlushFixture::operation(part_id, retries, bindings));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_TRUE(completed->has_value());
  const SealedHeadFlushCompletion completion =
      completed.value().value(); // NOLINT(bugprone-unchecked-optional-access)
  EXPECT_TRUE(completion.resumed_durable_manifest);
  EXPECT_EQ(fixture.publisher->snapshot()->generation(), 2U);
  EXPECT_TRUE(fixture.state->snapshot()->sealed_generations().empty());
  EXPECT_EQ(coordinator.metrics().resumed_durable_manifests, 1U);
}

TEST(SealedHeadFlushCoordinatorTest, FailsTabletClosedForAnUnrelatedDurableSuccessor) {
  FlushFixture fixture;
  const std::array retries{fixture.retry()};
  const auto bindings = fixture.bindings();
  fixture.install_successor(id<cseg::PartId>(9U), retries, bindings);
  SealedHeadFlushCoordinator coordinator =
      SealedHeadFlushCoordinator::create(fixture.queue, *fixture.storage, *fixture.publisher)
          .value();

  const auto failed = coordinator.try_flush_one(
      *fixture.state, FlushFixture::operation(id<cseg::PartId>(10U), retries, bindings));
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kCorruption);
  EXPECT_FALSE(coordinator.is_usable());
  EXPECT_TRUE(coordinator.metrics().failed);
  EXPECT_TRUE(fixture.state->metrics().failed);
  EXPECT_EQ(fixture.queue->metrics().ready, 1U);
}

TEST(SealedHeadFlushCoordinatorTest, RejectsMissingDependenciesAndInvalidNonces) {
  FlushFixture fixture;
  EXPECT_EQ(SealedHeadFlushCoordinator::create(nullptr, *fixture.storage, *fixture.publisher)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  SealedHeadFlushCoordinator coordinator =
      SealedHeadFlushCoordinator::create(fixture.queue, *fixture.storage, *fixture.publisher)
          .value();
  const std::array retries{fixture.retry()};
  const auto bindings = fixture.bindings();
  SealedHeadFlushOperation operation =
      FlushFixture::operation(id<cseg::PartId>(9U), retries, bindings);
  operation.part_nonce = {};
  EXPECT_EQ(coordinator.try_flush_one(*fixture.state, operation).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(fixture.queue->metrics().ready, 1U);
}

} // namespace
} // namespace chronos::manifest
