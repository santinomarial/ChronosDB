#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/manifest/checkpoint_builder.hpp"
#include "chronos/manifest/generation_builder.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/sealed_head_flush.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/wal/wal_replay_sink.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "columnar/columnar_test_support.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint16_t value) {
  return columnar::test::id<Identifier>(value);
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

[[nodiscard]] std::vector<std::byte> read_file(const std::filesystem::path& path) {
  std::ifstream stream{path, std::ios::binary | std::ios::ate};
  const std::streamsize size = stream.tellg();
  if (!stream.good() || size < 0) {
    return {};
  }
  std::vector<std::byte> bytes(static_cast<std::size_t>(size));
  stream.seekg(0);
  // std::ifstream's byte-oriented API has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  stream.read(reinterpret_cast<char*>(bytes.data()), size);
  return bytes;
}

void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream stream{path, std::ios::binary | std::ios::trunc};
  // std::ofstream's byte-oriented API has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  stream.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
}

class CountingReplaySink final : public wal::WalReplaySink {
public:
  [[nodiscard]] common::Status preflight(const wal::WalReplayRecord&) override {
    return common::Status::ok();
  }

  [[nodiscard]] common::Status replay(const wal::WalReplayRecord&) override {
    ++replayed;
    return common::Status::ok();
  }

  std::uint64_t replayed{};
};

[[nodiscard]] std::shared_ptr<const schema::TableSchema> make_schema() {
  const schema::ColumnId event = id<schema::ColumnId>(11U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(
      schema::ColumnDefinition::create(
          event, "event_time", columnar::test::type(schema::LogicalTypeKind::kTimestampNs), false)
          .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(12U), id<schema::SchemaId>(13U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = event,
                                   .physical_ordering_key = {event},
                                   .partition_columns = {event},
                                   .shard_key = {event},
                                   .deduplication_key = {}})
          .value());
}

[[nodiscard]] std::uint64_t independent_fnv1a64(const common::ByteView bytes) {
  std::uint64_t hash = 14'695'981'039'346'656'037ULL;
  for (const std::byte byte : bytes) {
    hash ^= std::to_integer<std::uint8_t>(byte);
    hash *= 1'099'511'628'211ULL;
  }
  return hash;
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
make_batch(const std::shared_ptr<const schema::TableSchema>& schema,
           const std::span<const std::int64_t> values) {
  std::vector<std::byte> bytes;
  bytes.reserve(values.size() * sizeof(std::int64_t));
  for (const std::int64_t value : values) {
    append_le(bytes, value);
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(columnar::test::fixed_vector(
      11U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs), false,
      static_cast<std::uint32_t>(values.size()), {}, 0U, std::move(bytes)));
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
}

enum class WalShape : std::uint8_t {
  kSingle,
  kPrependUnclaimed,
  kDuplicate,
  kTrailingUnsupported,
};

struct FixtureInput {
  std::span<const std::int64_t> command_values;
  std::span<const std::int64_t> stored_values;
  WalShape shape{WalShape::kSingle};
  bool rich_batch{};
};

[[nodiscard]] std::shared_ptr<const schema::TableSchema> fixture_schema(const FixtureInput& input) {
  return input.rich_batch ? columnar::test::batch_schema() : make_schema();
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
fixture_batch(const std::shared_ptr<const schema::TableSchema>& schema, const FixtureInput& input) {
  if (!input.rich_batch) {
    return make_batch(schema, input.command_values);
  }
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema, columnar::test::batch_columns()).value());
}

// Every optional below is populated in construction order before it is exposed. Keeping the
// encoded owners optional lets the fixture build the borrowed views without requiring fake
// default objects.
// NOLINTBEGIN(bugprone-unchecked-optional-access)
class Fixture {
public:
  explicit Fixture(const FixtureInput& input)
      : directory_("chronos-manifest-checkpoint"), schema_(fixture_schema(input)),
        wal_directory_(directory_.path().string()),
        lineage_(schema::SchemaLineage::create(*schema_).value()),
        command_batch_(fixture_batch(schema_, input)) {
    EXPECT_TRUE(directory_.valid());
    const columnar::EncodedColumnarBatch batch_bytes =
        columnar::encode_columnar_batch_v1(*command_batch_).value();
    application_.emplace(
        ingest::encode_columnar_append_v1({.client_id = id<ingest::ClientId>(21U),
                                           .client_batch_id = id<ingest::ClientBatchId>(22U),
                                           .tablet_id = tablet_id()},
                                          batch_bytes)
            .value());

    wal::test::FixedWalIdGenerator generator{wal::test::make_wal_id(0x71U)};
    wal::WalWriter writer =
        wal::WalWriter::create_new({.directory_path = directory_.path().string()}, generator)
            .value();
    wal_id_ = writer.wal_id();
    if (input.shape == WalShape::kPrependUnclaimed) {
      const wal::EncodedApplicationPayload unclaimed =
          ingest::encode_columnar_append_v1({.client_id = id<ingest::ClientId>(23U),
                                             .client_batch_id = id<ingest::ClientBatchId>(24U),
                                             .tablet_id = id<schema::TabletId>(15U)},
                                            batch_bytes)
              .value();
      EXPECT_TRUE(writer.append_application_entry(unclaimed.bytes()).has_value());
    }
    append_ = writer.append_application_entry(application_->bytes()).value();
    if (input.shape == WalShape::kDuplicate) {
      final_append_ = writer.append_application_entry(application_->bytes()).value();
    } else if (input.shape == WalShape::kTrailingUnsupported) {
      const wal::EncodedApplicationPayload unsupported =
          wal::encode_application_payload({.application_format = 1U,
                                           .application_kind = 99U,
                                           .application_flags = 0U,
                                           .application_body = {}})
              .value();
      final_append_ = writer.append_application_entry(unsupported.bytes()).value();
    }
    EXPECT_TRUE(writer.synchronize().has_value());
    EXPECT_TRUE(writer.close().is_ok());

    predecessor_bytes_.emplace(
        encode_manifest_v1({.generation = 1U,
                            .database_id = id<DatabaseId>(31U),
                            .wal_id = wal_id_,
                            .reclaim_checkpoint = {.record_sequence = 0U,
                                                   .segment_number = wal::kFirstSegmentNumber,
                                                   .byte_offset = wal::kSegmentHeaderSize},
                            .tablets = {},
                            .parts = {},
                            .retries = {}})
            .value());
    predecessor_.emplace(decode_manifest_v1_exact(predecessor_bytes_->bytes()).value());

    const std::shared_ptr<const columnar::OwnedColumnarBatch> durable_batch =
        input.stored_values.empty() ? command_batch_ : make_batch(schema_, input.stored_values);
    const std::vector<std::size_t> variable_capacity =
        input.rich_batch ? std::vector<std::size_t>{0U, 1U, 0U} : std::vector<std::size_t>{0U};
    head::MutableHead head = head::MutableHead::create(schema_, tablet_id(), 1U,
                                                       {.row_capacity = durable_batch->row_count(),
                                                        .variable_value_bytes = variable_capacity})
                                 .value();
    head::PreparedHeadAppend prepared = head.prepare_append(durable_batch).value();
    EXPECT_TRUE(prepared.mark_wal_started().is_ok());
    EXPECT_TRUE(prepared.publish({.wal_id = wal_id_, .record_sequence = append_.record_sequence})
                    .has_value());
    const head::HeadSnapshot snapshot = head.seal().value();
    sealed_.emplace(encode_sealed_head_v1({.snapshot = snapshot,
                                           .part_id = id<cseg::PartId>(41U),
                                           .compression = cseg::PageCompression::kZstd})
                        .value());

    const ingest::DecodedColumnarAppendView command =
        ingest::decode_columnar_append_v1_exact(application_->bytes()).value();
    retry_ = {.client_id = command.client_id(),
              .client_batch_id = command.client_batch_id(),
              .table_id = command.table_id(),
              .tablet_id = command.tablet_id(),
              .request_digest = command.request_digest(),
              .wal_id = wal_id_,
              .record_sequence = append_.record_sequence,
              .applied_row_count = command.row_count()};
    binding_ = TabletSchemaBinding{.tablet_id = tablet_id(), .lineage = std::cref(lineage_)};
    candidate_bytes_.emplace(
        build_manifest_v1_for_sealed_head({.predecessor = *predecessor_,
                                           .sealed_part = *sealed_,
                                           .new_retries = std::span{&retry_, 1U},
                                           .schema_bindings = std::span{&*binding_, 1U},
                                           .part_validation_limits = {}})
            .value());
    candidate_.emplace(decode_manifest_v1_exact(candidate_bytes_->bytes()).value());
    if (input.shape == WalShape::kDuplicate) {
      std::vector<TabletDescriptor> tablets{candidate_->tablets().begin(),
                                            candidate_->tablets().end()};
      tablets.front().durable_record_sequence = final_append_->record_sequence;
      candidate_bytes_.emplace(
          encode_manifest_v1({.generation = candidate_->generation(),
                              .database_id = candidate_->database_id(),
                              .wal_id = candidate_->wal_id(),
                              .reclaim_checkpoint = candidate_->reclaim_checkpoint(),
                              .tablets = tablets,
                              .parts = candidate_->parts(),
                              .retries = candidate_->retries()})
              .value());
      candidate_.emplace(decode_manifest_v1_exact(candidate_bytes_->bytes()).value());
    }
  }

  [[nodiscard]] static schema::TabletId tablet_id() {
    return id<schema::TabletId>(14U);
  }

  [[nodiscard]] ManifestCheckpointBuildInput input() const {
    image_file_name_ = part_file_name(candidate_->parts().front().part_id);
    image_.emplace(
        ReferencedPartImage{.file_name = image_file_name_, .bytes = sealed_->encoded_part.bytes()});
    return {.wal_directory = wal_directory_,
            .predecessor = *predecessor_,
            .candidate = *candidate_,
            .schema_bindings = std::span{&*binding_, 1U},
            .referenced_parts = std::span{&*image_, 1U},
            .command_decode_limits = {},
            .part_validation_limits = {}};
  }

  [[nodiscard]] const wal::WalAppendResult& append() const noexcept {
    return append_;
  }
  [[nodiscard]] const std::optional<wal::WalAppendResult>& final_append() const noexcept {
    return final_append_;
  }
  [[nodiscard]] const DecodedManifestView& predecessor() const noexcept {
    return *predecessor_;
  }
  [[nodiscard]] const DecodedManifestView& candidate() const noexcept {
    return *candidate_;
  }
  [[nodiscard]] const RetryDescriptor& retry() const noexcept {
    return retry_;
  }
  [[nodiscard]] const TabletSchemaBinding& binding() const noexcept {
    return *binding_;
  }
  [[nodiscard]] const schema::TableSchema& schema_value() const noexcept {
    return *schema_;
  }
  [[nodiscard]] const EncodedSealedHeadPart& sealed() const noexcept {
    return *sealed_;
  }
  [[nodiscard]] const std::filesystem::path& directory() const noexcept {
    return directory_.path();
  }

private:
  wal::test::TemporaryDirectory directory_;
  std::shared_ptr<const schema::TableSchema> schema_;
  std::string wal_directory_;
  schema::SchemaLineage lineage_;
  std::shared_ptr<const columnar::OwnedColumnarBatch> command_batch_;
  std::optional<wal::EncodedApplicationPayload> application_;
  wal::WalId wal_id_{};
  wal::WalAppendResult append_{};
  std::optional<wal::WalAppendResult> final_append_;
  std::optional<EncodedManifest> predecessor_bytes_;
  std::optional<DecodedManifestView> predecessor_;
  std::optional<EncodedSealedHeadPart> sealed_;
  RetryDescriptor retry_{.client_id = id<ingest::ClientId>(1U),
                         .client_batch_id = id<ingest::ClientBatchId>(2U),
                         .table_id = id<schema::TableId>(3U),
                         .tablet_id = id<schema::TabletId>(4U),
                         .request_digest = ingest::Sha256Digest{ingest::Sha256Digest::Bytes{}},
                         .wal_id = {},
                         .record_sequence = 0U,
                         .applied_row_count = 0U};
  std::optional<TabletSchemaBinding> binding_;
  std::optional<EncodedManifest> candidate_bytes_;
  std::optional<DecodedManifestView> candidate_;
  mutable std::string image_file_name_;
  mutable std::optional<ReferencedPartImage> image_;
};
// NOLINTEND(bugprone-unchecked-optional-access)

TEST(ManifestCheckpointBuilderTest, ProvesExactRowsAndAdvancesToThePhysicalRecordEnd) {
  const std::array<std::int64_t, 3> values{30, -4, 20};
  const Fixture fixture{{.command_values = values, .stored_values = {}}};
  const common::Result<CheckpointedManifestGeneration> built =
      build_manifest_v1_checkpointed_generation(fixture.input());
  ASSERT_TRUE(built.has_value()) << built.error().to_string();
  EXPECT_EQ(built->previous_checkpoint, fixture.predecessor().reclaim_checkpoint());
  EXPECT_EQ(built->reclaim_checkpoint.record_sequence, fixture.append().record_sequence);
  EXPECT_EQ(built->reclaim_checkpoint.segment_number, fixture.append().record_end.segment_number);
  EXPECT_EQ(built->reclaim_checkpoint.byte_offset, fixture.append().record_end.byte_offset);
  EXPECT_EQ(built->newly_checkpointed_records, 1U);
  EXPECT_EQ(built->validated_applied_rows, values.size());
  EXPECT_EQ(built->wal_report.classification, wal::WalScanClassification::kClean);
  const ManifestDecodeResult decoded = decode_manifest_v1_exact(built->encoded_manifest.bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->reclaim_checkpoint(), built->reclaim_checkpoint);
  EXPECT_TRUE(validate_manifest_v1_transition(fixture.predecessor(), *decoded,
                                              std::span{&fixture.binding(), 1U})
                  .is_ok());
  // Independent bytewise FNV-1a fingerprint of the complete frozen fixture.
  EXPECT_EQ(independent_fnv1a64(built->encoded_manifest.bytes()), 10'737'659'261'143'427'910ULL);
}

TEST(ManifestCheckpointBuilderTest, RejectsCsegValuesThatDoNotMatchTheWalCommand) {
  const std::array<std::int64_t, 3> command{30, -4, 20};
  const std::array<std::int64_t, 3> stored{30, -5, 20};
  const Fixture fixture{{.command_values = command, .stored_values = stored}};
  const auto built = build_manifest_v1_checkpointed_generation(fixture.input());
  ASSERT_FALSE(built.has_value());
  EXPECT_EQ(built.error().code(), common::StatusCode::kCorruption);
}

TEST(ManifestCheckpointBuilderTest, ProvesNullableVariableAndPackedBooleanCellsExactly) {
  const Fixture fixture{{.command_values = {}, .stored_values = {}, .rich_batch = true}};
  const auto built = build_manifest_v1_checkpointed_generation(fixture.input());
  ASSERT_TRUE(built.has_value()) << built.error().to_string();
  EXPECT_EQ(built->reclaim_checkpoint.record_sequence, 1U);
  EXPECT_EQ(built->validated_applied_rows, 2U);
}

TEST(ManifestCheckpointBuilderTest, RejectsProtectedRetryDigestDisagreement) {
  const std::array<std::int64_t, 2> values{1, 2};
  const Fixture fixture{{.command_values = values, .stored_values = {}}};
  RetryDescriptor changed = fixture.retry();
  ingest::Sha256Digest::Bytes digest = changed.request_digest.bytes();
  digest.front() ^= std::byte{0x80U};
  changed.request_digest = ingest::Sha256Digest{digest};
  const std::array retries{changed};
  const EncodedManifest bytes =
      encode_manifest_v1({.generation = fixture.candidate().generation(),
                          .database_id = fixture.candidate().database_id(),
                          .wal_id = fixture.candidate().wal_id(),
                          .reclaim_checkpoint = fixture.candidate().reclaim_checkpoint(),
                          .tablets = fixture.candidate().tablets(),
                          .parts = fixture.candidate().parts(),
                          .retries = retries})
          .value();
  const DecodedManifestView changed_candidate = decode_manifest_v1_exact(bytes.bytes()).value();
  ManifestCheckpointBuildInput input = fixture.input();
  input.candidate = changed_candidate;
  const auto built = build_manifest_v1_checkpointed_generation(input);
  ASSERT_FALSE(built.has_value());
  EXPECT_EQ(built.error().code(), common::StatusCode::kCorruption);
}

TEST(ManifestCheckpointBuilderTest, RejectsAnIncompleteFinalWalRecordWithoutPublishingBytes) {
  const std::array<std::int64_t, 2> values{1, 2};
  const Fixture fixture{{.command_values = values, .stored_values = {}}};
  const std::filesystem::path segment = fixture.directory() / "wal-00000000000000000001.cwal";
  ASSERT_NO_THROW(
      std::filesystem::resize_file(segment, fixture.append().record_end.byte_offset - 1U));
  const auto built = build_manifest_v1_checkpointed_generation(fixture.input());
  ASSERT_FALSE(built.has_value());
  EXPECT_EQ(built.error().code(), common::StatusCode::kOutOfRange);
}

TEST(ManifestCheckpointBuilderTest, RejectsPhysicalWalCorruptionBeforeCoverageReplay) {
  const std::array<std::int64_t, 2> values{1, 2};
  const Fixture fixture{{.command_values = values, .stored_values = {}}};
  const std::filesystem::path segment = fixture.directory() / "wal-00000000000000000001.cwal";
  std::vector<std::byte> bytes = read_file(segment);
  ASSERT_GT(bytes.size(), wal::kSegmentHeaderSize + wal::kRecordHeaderSize);
  bytes[wal::kSegmentHeaderSize + wal::kRecordHeaderSize] ^= std::byte{0x01U};
  write_file(segment, bytes);
  const auto built = build_manifest_v1_checkpointed_generation(fixture.input());
  ASSERT_FALSE(built.has_value());
  EXPECT_EQ(built.error().code(), common::StatusCode::kCorruption);
}

TEST(ManifestCheckpointBuilderTest, RejectsTabletBoundaryThatIsAbsentFromTheWalSuffix) {
  const std::array<std::int64_t, 2> values{1, 2};
  const Fixture fixture{{.command_values = values, .stored_values = {}}};
  std::vector<TabletDescriptor> tablets{fixture.candidate().tablets().begin(),
                                        fixture.candidate().tablets().end()};
  tablets.front().durable_record_sequence = 2U;
  const EncodedManifest bytes =
      encode_manifest_v1({.generation = fixture.candidate().generation(),
                          .database_id = fixture.candidate().database_id(),
                          .wal_id = fixture.candidate().wal_id(),
                          .reclaim_checkpoint = fixture.candidate().reclaim_checkpoint(),
                          .tablets = tablets,
                          .parts = fixture.candidate().parts(),
                          .retries = fixture.candidate().retries()})
          .value();
  const DecodedManifestView changed_candidate = decode_manifest_v1_exact(bytes.bytes()).value();
  ManifestCheckpointBuildInput input = fixture.input();
  input.candidate = changed_candidate;
  const auto built = build_manifest_v1_checkpointed_generation(input);
  ASSERT_FALSE(built.has_value());
  EXPECT_EQ(built.error().code(), common::StatusCode::kCorruption);
}

TEST(ManifestCheckpointBuilderPropertyTest, GeneratedExactRowsAdvanceDeterministically) {
  constexpr std::uint64_t multiplier = 6'364'136'223'846'793'005ULL;
  constexpr std::uint64_t increment = 1'442'695'040'888'963'407ULL;
  for (std::uint64_t seed = 1U; seed <= 16U; ++seed) {
    SCOPED_TRACE(seed);
    std::uint64_t state = seed;
    const std::size_t count = 1U + static_cast<std::size_t>(seed % 17U);
    std::vector<std::int64_t> values;
    values.reserve(count);
    for (std::size_t row = 0U; row < count; ++row) {
      state = state * multiplier + increment;
      values.push_back(std::bit_cast<std::int64_t>(state));
    }
    const Fixture fixture{{.command_values = values, .stored_values = {}}};
    const auto first = build_manifest_v1_checkpointed_generation(fixture.input());
    const auto second = build_manifest_v1_checkpointed_generation(fixture.input());
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    ASSERT_TRUE(second.has_value()) << second.error().to_string();
    EXPECT_TRUE(
        std::ranges::equal(first->encoded_manifest.bytes(), second->encoded_manifest.bytes()));
    EXPECT_EQ(first->validated_applied_rows, count);
  }
}

TEST(ManifestCheckpointBuilderTest, MissingEarlierTabletCoverageKeepsTheGlobalPrefixClosed) {
  const std::array<std::int64_t, 2> values{7, 8};
  const Fixture fixture{
      {.command_values = values, .stored_values = {}, .shape = WalShape::kPrependUnclaimed}};
  ASSERT_EQ(fixture.append().record_sequence, 2U);
  const auto built = build_manifest_v1_checkpointed_generation(fixture.input());
  ASSERT_TRUE(built.has_value()) << built.error().to_string();
  EXPECT_EQ(built->reclaim_checkpoint, fixture.predecessor().reclaim_checkpoint());
  EXPECT_EQ(built->newly_checkpointed_records, 0U);
  EXPECT_EQ(built->validated_applied_rows, values.size());
}

TEST(ManifestCheckpointBuilderTest, ExactRetryDuplicateAdvancesWithoutDuplicateCsegRows) {
  const std::array<std::int64_t, 2> values{7, 8};
  const Fixture fixture{
      {.command_values = values, .stored_values = {}, .shape = WalShape::kDuplicate}};
  ASSERT_TRUE(fixture.final_append().has_value());
  const auto built = build_manifest_v1_checkpointed_generation(fixture.input());
  ASSERT_TRUE(built.has_value()) << built.error().to_string();
  EXPECT_EQ(built->reclaim_checkpoint.record_sequence, 2U);
  EXPECT_EQ(built->reclaim_checkpoint.byte_offset,
            fixture.final_append().value_or(wal::WalAppendResult{}).record_end.byte_offset);
  EXPECT_EQ(built->newly_checkpointed_records, 2U);
  EXPECT_EQ(built->validated_applied_rows, values.size());
}

TEST(ManifestCheckpointBuilderTest, UnsupportedSuffixKindPreventsAnyCheckpointProof) {
  const std::array<std::int64_t, 1> values{9};
  const Fixture fixture{
      {.command_values = values, .stored_values = {}, .shape = WalShape::kTrailingUnsupported}};
  const auto built = build_manifest_v1_checkpointed_generation(fixture.input());
  ASSERT_FALSE(built.has_value());
  EXPECT_EQ(built.error().code(), common::StatusCode::kNotSupported);
}

TEST(ManifestCheckpointBuilderTest, RejectsNewRetryAtAnAlreadyCheckpointedTabletBoundary) {
  const std::array<std::int64_t, 2> values{4, 5};
  const Fixture fixture{{.command_values = values, .stored_values = {}}};
  ManifestCheckpointBuildInput fixture_input = fixture.input();
  auto checkpointed_result = build_manifest_v1_checkpointed_generation(fixture_input);
  ASSERT_TRUE(checkpointed_result.has_value()) << checkpointed_result.error().to_string();
  const CheckpointedManifestGeneration checkpointed = std::move(*checkpointed_result);
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(checkpointed.encoded_manifest.bytes()).value();
  std::vector<RetryDescriptor> retries{predecessor.retries().begin(), predecessor.retries().end()};
  RetryDescriptor injected = retries.front();
  injected.client_id = id<ingest::ClientId>(23U);
  injected.client_batch_id = id<ingest::ClientBatchId>(24U);
  retries.push_back(injected);
  std::ranges::sort(retries, [](const RetryDescriptor& left, const RetryDescriptor& right) {
    return std::pair{left.client_id, left.client_batch_id} <
           std::pair{right.client_id, right.client_batch_id};
  });
  const EncodedManifest candidate_bytes =
      encode_manifest_v1({.generation = predecessor.generation() + 1U,
                          .database_id = predecessor.database_id(),
                          .wal_id = predecessor.wal_id(),
                          .reclaim_checkpoint = predecessor.reclaim_checkpoint(),
                          .tablets = predecessor.tablets(),
                          .parts = predecessor.parts(),
                          .retries = retries})
          .value();
  const DecodedManifestView candidate = decode_manifest_v1_exact(candidate_bytes.bytes()).value();
  fixture_input.predecessor = predecessor;
  fixture_input.candidate = candidate;
  const auto built = build_manifest_v1_checkpointed_generation(fixture_input);
  ASSERT_FALSE(built.has_value());
  EXPECT_EQ(built.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(ManifestCheckpointBuilderTest, InstallsAndSelectsTheProvenCoordinateWithoutTranslation) {
  const std::array<std::int64_t, 2> values{4, 5};
  const Fixture fixture{{.command_values = values, .stored_values = {}}};
  const auto checkpointed = build_manifest_v1_checkpointed_generation(fixture.input());
  ASSERT_TRUE(checkpointed.has_value()) << checkpointed.error().to_string();

  wal::test::TemporaryDirectory database{"chronos-checkpoint-install"};
  ASSERT_TRUE(database.valid());
  ASSERT_TRUE(std::filesystem::create_directory(database.path() / kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(database.path() / kManifestDirectoryName));
  {
    std::ofstream lock{database.path() / kManifestDirectoryName /
                       std::string{kManifestLockFileName}};
    ASSERT_TRUE(lock.good());
  }
  {
    std::ofstream initial{database.path() / kManifestDirectoryName / *manifest_file_name(1U),
                          std::ios::binary};
    const common::ByteView bytes = fixture.predecessor().encoded_bytes();
    // std::ofstream's byte-oriented API has no std::byte overload.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    initial.write(reinterpret_cast<const char*>(bytes.data()),
                  static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(initial.good());
  }
  auto storage = ManifestStorage::open_existing({.database_root = database.path().string()});
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  common::Uuid::Bytes nonce_bytes{};
  nonce_bytes.front() = std::byte{0xa1U};
  const auto installed_part = storage->install_part({
      .encoded_part = std::cref(fixture.sealed().encoded_part),
      .descriptor = fixture.sealed().descriptor,
      .wal_id = fixture.candidate().wal_id(),
      .schema = std::cref(fixture.schema_value()),
      .nonce = common::Uuid{nonce_bytes},
      .validation_limits = {},
  });
  ASSERT_TRUE(installed_part.has_value()) << installed_part.error().to_string();
  nonce_bytes.front() = std::byte{0xb1U};
  const auto installed_manifest = storage->install_manifest({
      .encoded_manifest = std::cref(checkpointed->encoded_manifest),
      .schema_bindings = std::span{&fixture.binding(), 1U},
      .nonce = common::Uuid{nonce_bytes},
      .decode_limits = {},
      .part_validation_limits = {},
      .compaction_equivalence_limits = {},
  });
  ASSERT_TRUE(installed_manifest.has_value()) << installed_manifest.error().to_string();
  EXPECT_EQ(installed_manifest->reclaim_checkpoint, checkpointed->reclaim_checkpoint);
  const auto selected = storage->load_selected_manifest({
      .expected_database_id = fixture.predecessor().database_id(),
      .expected_wal_id = fixture.predecessor().wal_id(),
      .schema_bindings = std::span{&fixture.binding(), 1U},
      .decode_limits = {},
      .part_validation_limits = {},
  });
  ASSERT_TRUE(selected.has_value()) << selected.error().to_string();
  EXPECT_EQ(selected->reclaim_checkpoint(), checkpointed->reclaim_checkpoint);

  CountingReplaySink suffix;
  const auto report =
      wal::inspect_wal_suffix(fixture.directory().string(),
                              {.wal_id = selected->wal_id(),
                               .record_sequence = selected->reclaim_checkpoint().record_sequence,
                               .segment_number = selected->reclaim_checkpoint().segment_number,
                               .byte_offset = selected->reclaim_checkpoint().byte_offset},
                              suffix);
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(suffix.replayed, 0U);
}

} // namespace
} // namespace chronos::manifest
