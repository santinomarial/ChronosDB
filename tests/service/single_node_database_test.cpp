#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/uuid_generator.hpp"
#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/multiplexed_log.hpp"
#include "chronos/raft/persistent_log.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/service/native_protocol_service.hpp"
#include "chronos/service/single_node_database.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-single-node-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] std::string read_binary_file(const std::filesystem::path& path) {
  std::ifstream input{path, std::ios::binary};
  return {std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

class DeterministicIdentityGenerator final : public NativeIdentityGenerator {
public:
  explicit DeterministicIdentityGenerator(const std::uint8_t first) noexcept : next_(first) {}

  [[nodiscard]] common::Result<common::Uuid> generate() override {
    common::Uuid::Bytes bytes{};
    bytes.front() = static_cast<std::byte>(next_++);
    return common::Uuid{bytes};
  }

private:
  std::uint8_t next_;
};

class FifthCallFailingEntropySource final : public common::UuidEntropySource {
public:
  [[nodiscard]] common::Result<common::Uuid::Bytes> read() override {
    ++calls;
    if (calls == 5U) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kIoError, "injected system entropy failure"});
    }
    common::Uuid::Bytes bytes{};
    bytes.front() = static_cast<std::byte>(calls);
    return bytes;
  }

  std::size_t calls{};
};

class RecordingCommittedAppendObserver final : public SingleNodeCommittedAppendObserver {
public:
  void on_applied(AppliedSingleNodeColumnarAppend append) noexcept override {
    ++notifications;
    last.emplace(std::move(append));
  }

  std::size_t notifications{};
  std::optional<AppliedSingleNodeColumnarAppend> last;
};

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

constexpr std::string_view kCreateSql =
    "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, price "
    "DECIMAL(20, 8) NOT NULL, note STRING) EVENT TIME ts ORDER KEY (symbol, ts) "
    "PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) DEDUP KEY "
    "(symbol, ts) RETENTION INTERVAL '30 days' SYSTEM HISTORY RETENTION INTERVAL "
    "'7 days' ALLOWED LATENESS INTERVAL '0 seconds'";

[[nodiscard]] runtime::DatabaseBootstrapDescriptor descriptor() {
  return {.database_id = uuid(1U),
          .metadata_group_id = uuid(2U),
          .local_node_id = 1U,
          .mutable_head_rows = 4U,
          .maximum_sealed_generations = 4U,
          .variable_column_bytes = 64U,
          .maximum_retry_entries = 128U,
          .wal_segment_target_bytes = wal::kSegmentSizeLimit,
          .raft_segment_target_bytes = std::uint64_t{1U} * 1024U * 1024U};
}

[[nodiscard]] SingleNodeDatabaseConfig config(const TemporaryDirectory& directory) {
  return {.bootstrap = {.database_root = directory.path().string(), .new_database = descriptor()},
          .wal_recovery = {.repair_incomplete_final_tail = false},
          .raft_recovery = {.repair_incomplete_final_tail = false}};
}

struct AnchoredMetadataLog {
  std::filesystem::path raft_directory;
  std::filesystem::path anchor;
  std::filesystem::path retained_segment;
  std::filesystem::path reclaimed_segment;
  bool ready{};
};

void provision_anchored_metadata_log(const TemporaryDirectory& directory,
                                     AnchoredMetadataLog& output) {
  auto created = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  ASSERT_TRUE(created->shutdown().is_ok());

  const std::filesystem::path raft_directory =
      directory.path() / runtime::kDatabaseRaftDirectoryName;
  auto log = raft::RaftPersistentLog::open_existing(
      {.directory_path = raft_directory.string(),
       .target_segment_size = descriptor().raft_segment_target_bytes});
  ASSERT_TRUE(log.has_value()) << log.error().to_string();
  std::vector<raft::GroupPersistentState> checkpoint = log->recovery().latest_group_states;
  ASSERT_EQ(checkpoint.size(), 1U);
  checkpoint.front().physical_sequence = log->written_position().physical_sequence + 1U;
  auto reclaimed = log->checkpoint_and_reclaim(checkpoint);
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  ASSERT_EQ(reclaimed->base_segment_number, 2U);
  ASSERT_TRUE(log->close().is_ok());

  auto anchored = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(anchored.has_value()) << anchored.error().to_string();
  ASSERT_TRUE(anchored->shutdown().is_ok());

  output.raft_directory = raft_directory;
  output.anchor = raft_directory / "raft-base-00000000000000000002.rbase";
  output.retained_segment = raft_directory / "raft-00000000000000000002.rlog";
  output.reclaimed_segment = raft_directory / "raft-00000000000000000001.rlog";
  ASSERT_TRUE(std::filesystem::is_regular_file(output.anchor));
  ASSERT_TRUE(std::filesystem::is_regular_file(output.retained_segment));
  ASSERT_FALSE(std::filesystem::exists(output.reclaimed_segment));
  output.ready = true;
}

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(70U);
}

[[nodiscard]] raft::ProposeOperation metadata_proposal(raft::MetadataCommand command) {
  return {raft::kRaftMetadataCommandEntryType,
          raft::encode_metadata_command_v1(std::move(command)).value()};
}

void seed_catalog(const TemporaryDirectory& directory) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(config(directory).bootstrap);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  const auto schema = columnar::test::batch_schema();
  const raft::CatalogTableDefinition definition{
      .name = "events", .quoted = false, .schema = schema};
  const raft::GroupId group = descriptor().metadata_group_id;
  const raft::RaftPersistentLogConfig raft_config{
      .directory_path = bootstrap->raft_directory_path(),
      .target_segment_size = descriptor().raft_segment_target_bytes};
  ASSERT_TRUE(bootstrap->close().is_ok());
  auto runtime = raft::DurableMultiRaftRuntime::create_new(1U, raft_config, {{group, {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group, raft::StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch({{group,
                                    raft::ProposeOperation{
                                        raft::kRaftSchemaDefinitionEntryType,
                                        raft::encode_schema_definition_v1(definition).value()}}})
                  .has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch({{group, metadata_proposal(raft::TablePolicyMetadata{
                                               schema->table_id(), 100, 1000, 500, 10, 128U})}})
                  .has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch({{group, metadata_proposal(raft::TabletPlacementMetadata{
                                               schema->table_id(), tablet_id(), 1U, {1U}, 1U})}})
                  .has_value());
  ASSERT_TRUE(runtime->close().is_ok());
}

void seed_schema_prefix(const TemporaryDirectory& directory) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(config(directory).bootstrap);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto parsed = query::parse_sql_v1_create_table(kCreateSql);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto empty = std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, {}).value());
  auto bound = query::bind_sql_v1_create_table(std::move(*parsed), std::move(empty));
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  const std::vector columns{id<schema::ColumnId>(43U), id<schema::ColumnId>(44U),
                            id<schema::ColumnId>(45U), id<schema::ColumnId>(46U)};
  auto table = query::materialize_sql_v1_table_schema(*bound, id<schema::TableId>(41U),
                                                      id<schema::SchemaId>(42U), columns);
  ASSERT_TRUE(table.has_value()) << table.error().status().to_string();
  const raft::CatalogTableDefinition definition{
      .name = "trades",
      .quoted = false,
      .schema = std::make_shared<const schema::TableSchema>(std::move(*table))};
  const raft::GroupId group = descriptor().metadata_group_id;
  const raft::RaftPersistentLogConfig raft_config{
      .directory_path = bootstrap->raft_directory_path(),
      .target_segment_size = descriptor().raft_segment_target_bytes};
  ASSERT_TRUE(bootstrap->close().is_ok());
  auto raft_runtime = raft::DurableMultiRaftRuntime::create_new(1U, raft_config, {{group, {1U}}});
  ASSERT_TRUE(raft_runtime.has_value()) << raft_runtime.error().to_string();
  ASSERT_TRUE(raft_runtime->execute_batch({{group, raft::StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(raft_runtime
                  ->execute_batch({{group,
                                    raft::ProposeOperation{
                                        raft::kRaftSchemaDefinitionEntryType,
                                        raft::encode_schema_definition_v1(definition).value()}}})
                  .has_value());
  ASSERT_TRUE(raft_runtime->close().is_ok());
}

[[nodiscard]] query::SqlResult<query::BoundSqlCreateTable>
bind_create(SingleNodeDatabase& database) {
  auto parsed = query::parse_sql_v1_create_table(kCreateSql);
  if (!parsed.has_value())
    return std::unexpected(parsed.error());
  return query::bind_sql_v1_create_table(std::move(*parsed), database.query_catalog());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(const std::byte timestamp_tail = std::byte{0U}) {
  std::vector<columnar::OwnedColumnVector> columns = columnar::test::batch_columns();
  std::vector<std::byte> timestamps(16U);
  timestamps.back() = timestamp_tail;
  columns[0] =
      columnar::test::fixed_vector(1U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs),
                                   false, 2U, {}, 0U, std::move(timestamps));
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(), std::move(columns))
          .value());
}

[[nodiscard]] network::NetworkTask ingest_task(const std::uint8_t seed,
                                               const network::DurabilityMode durability) {
  const auto input = batch(static_cast<std::byte>(seed));
  const auto encoded_batch = columnar::encode_columnar_batch_v1(*input).value();
  const auto append =
      ingest::encode_columnar_append_v1(
          {.client_id = ingest::test::request_id<ingest::ClientId>(seed),
           .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(seed + 32U),
           .tablet_id = tablet_id()},
          encoded_batch)
          .value();
  auto payload = network::encode_ingest_request(durability, append.bytes()).value();
  return {.connection_id = 9U,
          .principal_id = 7U,
          .frame = {.header = {.message_type = network::MessageType::kIngestRequest,
                               .request_id = seed,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] network::NetworkTask query_task(const std::uint64_t request_id,
                                              const std::string_view sql) {
  auto payload = network::encode_query_request(sql).value();
  return {.connection_id = 9U,
          .principal_id = 7U,
          .frame = {.header = {.message_type = network::MessageType::kQueryRequest,
                               .request_id = request_id,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] std::int64_t native_query_count(NativeProtocolService& service,
                                              const std::uint64_t request_id,
                                              const std::string_view table_name) {
  auto result = service.execute_query(
      query_task(request_id, std::string{"SELECT count(*) AS rows FROM "}.append(table_name)));
  EXPECT_TRUE(result.has_value()) << (result.has_value() ? std::string{}
                                                         : result.error().to_string());
  if (!result.has_value() || result->responses.empty())
    return -1;
  auto batch = network::decode_query_result_batch(result->responses.front().frame.payload);
  EXPECT_TRUE(batch.has_value()) << (batch.has_value() ? std::string{} : batch.error().to_string());
  if (!batch.has_value() || batch->cell(0U, 0U) == nullptr)
    return -1;
  common::ByteReader count{batch->cell(0U, 0U)->value};
  return count.read_i64_le().value_or(-1);
}

[[nodiscard]] std::int64_t query_count(SingleNodeDatabase& database) {
  auto parsed = query::parse_sql_v1_select("SELECT count(*) AS rows FROM events");
  EXPECT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto bound = query::bind_sql_v1_select(std::move(*parsed), database.query_catalog());
  EXPECT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto lowered = query::lower_bound_sql_select(*bound);
  EXPECT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  const auto schema = columnar::test::batch_schema();
  const auto* lineage = database.find_lineage(schema->table_id());
  EXPECT_NE(lineage, nullptr);
  auto* tablet = database.find_tablet(tablet_id());
  EXPECT_NE(tablet, nullptr);
  auto snapshot = tablet->snapshot();
  EXPECT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  query::QueryResourceContext resources =
      query::QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  auto pipeline = query::instantiate_tablet_state_pipeline(resources, *snapshot, *lineage,
                                                           schema->schema_id(), *lowered);
  EXPECT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  auto step = (*pipeline)->next(resources);
  EXPECT_TRUE(step.has_value()) << step.error().to_string();
  EXPECT_EQ(step->kind(), query::PhysicalOperatorStepKind::kChunk);
  const auto cell = step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
  EXPECT_TRUE(cell.has_value());
  common::ByteReader reader{cell->bytes().value()};
  return reader.read_i64_le().value();
}

TEST(SingleNodeDatabaseTest, CreatesAndReopensAnEmptyDatabaseWithoutConfiguredTablets) {
  TemporaryDirectory directory;
  auto created = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  EXPECT_TRUE(created->query_catalog()->tables().empty());
  EXPECT_TRUE(std::filesystem::is_directory(directory.path() / manifest::kPartsDirectoryName));
  EXPECT_TRUE(std::filesystem::is_regular_file(directory.path() / manifest::kManifestDirectoryName /
                                               *manifest::manifest_file_name(1U)));
  EXPECT_EQ(manifest::ManifestStorage::open_existing({.database_root = directory.path().string()})
                .error()
                .code(),
            common::StatusCode::kUnavailable);
  ASSERT_TRUE(created->shutdown().is_ok());
  {
    auto storage =
        manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  }
  auto reopened = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_TRUE(reopened->query_catalog()->tables().empty());
  EXPECT_TRUE(reopened->shutdown().is_ok());
}

TEST(SingleNodeDatabaseTest, RejectsMissingAuthoritativeRaftAnchorWithoutAdoptingRetainedBase) {
  TemporaryDirectory directory;
  AnchoredMetadataLog anchored;
  provision_anchored_metadata_log(directory, anchored);
  ASSERT_TRUE(anchored.ready);
  const std::string pristine_segment = read_binary_file(anchored.retained_segment);
  ASSERT_GT(pristine_segment.size(), raft::kRaftSegmentHeaderSize);

  ASSERT_EQ(::unlink(anchored.anchor.c_str()), 0);
  const int raft_directory_file =
      ::open(anchored.raft_directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  ASSERT_GE(raft_directory_file, 0);
  ASSERT_EQ(::fsync(raft_directory_file), 0);
  ASSERT_EQ(::close(raft_directory_file), 0);
  ASSERT_FALSE(std::filesystem::exists(anchored.anchor));

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    SCOPED_TRACE(attempt);
    auto missing = SingleNodeDatabase::open_or_create(config(directory));
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error().code(), common::StatusCode::kCorruption);
    EXPECT_NE(missing.error().to_string().find("Raft recovery base segment is absent"),
              std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(anchored.anchor));
    EXPECT_EQ(read_binary_file(anchored.retained_segment), pristine_segment);
    EXPECT_FALSE(std::filesystem::exists(anchored.reclaimed_segment));
  }
}

TEST(SingleNodeDatabaseTest, RejectsTruncatedAuthoritativeRaftAnchorWithoutFallbackOrRewrite) {
  TemporaryDirectory directory;
  AnchoredMetadataLog anchored;
  provision_anchored_metadata_log(directory, anchored);
  ASSERT_TRUE(anchored.ready);
  const std::string pristine_anchor = read_binary_file(anchored.anchor);
  ASSERT_EQ(pristine_anchor.size(), 64U);
  const std::string pristine_segment = read_binary_file(anchored.retained_segment);

  const std::string truncated_anchor = pristine_anchor.substr(0U, pristine_anchor.size() - 1U);
  const int anchor_file = ::open(anchored.anchor.c_str(), O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
  ASSERT_GE(anchor_file, 0);
  ASSERT_EQ(::ftruncate(anchor_file, static_cast<off_t>(truncated_anchor.size())), 0);
  ASSERT_EQ(::fsync(anchor_file), 0);
  ASSERT_EQ(::close(anchor_file), 0);
  ASSERT_EQ(read_binary_file(anchored.anchor), truncated_anchor);

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    SCOPED_TRACE(attempt);
    auto rejected = SingleNodeDatabase::open_or_create(config(directory));
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
    EXPECT_NE(rejected.error().to_string().find("Raft recovery anchor has an invalid size"),
              std::string::npos);
    EXPECT_EQ(read_binary_file(anchored.anchor), truncated_anchor);
    EXPECT_EQ(read_binary_file(anchored.retained_segment), pristine_segment);
    EXPECT_FALSE(std::filesystem::exists(anchored.reclaimed_segment));
  }
}

TEST(SingleNodeDatabaseTest, RejectsChecksumValidInvalidRaftAnchorSemanticsWithoutFallback) {
  enum class AnchorMutation : std::uint8_t {
    kMagic,
    kMajorVersion,
    kMinorVersion,
    kHeaderSize,
    kBaseSegment,
    kFirstSequence,
    kLastSequence,
    kGroupCount,
    kRangeCount,
    kReserved
  };
  struct SemanticCase {
    const char* name;
    AnchorMutation mutation;
    common::StatusCode expected_code;
    std::string_view expected_diagnostic;
  };
  constexpr std::array cases{
      SemanticCase{"magic", AnchorMutation::kMagic, common::StatusCode::kCorruption,
                   "Raft recovery anchor is invalid"},
      SemanticCase{"major version", AnchorMutation::kMajorVersion,
                   common::StatusCode::kNotSupported,
                   "Raft recovery-anchor version is unsupported"},
      SemanticCase{"minor version", AnchorMutation::kMinorVersion,
                   common::StatusCode::kNotSupported,
                   "Raft recovery-anchor version is unsupported"},
      SemanticCase{"header size", AnchorMutation::kHeaderSize, common::StatusCode::kCorruption,
                   "Raft recovery-anchor fields are invalid"},
      SemanticCase{"base segment", AnchorMutation::kBaseSegment, common::StatusCode::kCorruption,
                   "Raft recovery-anchor fields are invalid"},
      SemanticCase{"first sequence", AnchorMutation::kFirstSequence,
                   common::StatusCode::kCorruption, "Raft recovery-anchor fields are invalid"},
      SemanticCase{"last sequence", AnchorMutation::kLastSequence, common::StatusCode::kCorruption,
                   "Raft recovery-anchor fields are invalid"},
      SemanticCase{"zero group count", AnchorMutation::kGroupCount, common::StatusCode::kCorruption,
                   "Raft recovery-anchor fields are invalid"},
      SemanticCase{"range count", AnchorMutation::kRangeCount, common::StatusCode::kCorruption,
                   "Raft recovery-anchor fields are invalid"},
      SemanticCase{"reserved", AnchorMutation::kReserved, common::StatusCode::kCorruption,
                   "Raft recovery-anchor fields are invalid"}};

  for (const SemanticCase& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    TemporaryDirectory directory;
    AnchoredMetadataLog anchored;
    provision_anchored_metadata_log(directory, anchored);
    ASSERT_TRUE(anchored.ready);
    const std::string pristine_anchor = read_binary_file(anchored.anchor);
    ASSERT_EQ(pristine_anchor.size(), 64U);
    const std::string pristine_segment = read_binary_file(anchored.retained_segment);

    std::string malformed_anchor = pristine_anchor;
    common::MutableByteView anchor_bytes =
        std::as_writable_bytes(std::span{malformed_anchor.data(), malformed_anchor.size()});
    const auto write_u16 = [&anchor_bytes](const std::size_t offset, const std::uint16_t value) {
      common::ByteWriter writer{anchor_bytes.subspan(offset, sizeof(value))};
      return writer.write_u16_le(value);
    };
    const auto write_u32 = [&anchor_bytes](const std::size_t offset, const std::uint32_t value) {
      common::ByteWriter writer{anchor_bytes.subspan(offset, sizeof(value))};
      return writer.write_u32_le(value);
    };
    const auto write_u64 = [&anchor_bytes](const std::size_t offset, const std::uint64_t value) {
      common::ByteWriter writer{anchor_bytes.subspan(offset, sizeof(value))};
      return writer.write_u64_le(value);
    };
    switch (test_case.mutation) {
    case AnchorMutation::kMagic:
      anchor_bytes[0U] ^= std::byte{1U};
      break;
    case AnchorMutation::kMajorVersion:
      ASSERT_TRUE(write_u16(8U, 2U).is_ok());
      break;
    case AnchorMutation::kMinorVersion:
      ASSERT_TRUE(write_u16(10U, 1U).is_ok());
      break;
    case AnchorMutation::kHeaderSize:
      ASSERT_TRUE(write_u32(12U, 63U).is_ok());
      break;
    case AnchorMutation::kBaseSegment:
      ASSERT_TRUE(write_u64(16U, 3U).is_ok());
      break;
    case AnchorMutation::kFirstSequence:
      ASSERT_TRUE(write_u64(24U, 0U).is_ok());
      break;
    case AnchorMutation::kLastSequence:
      ASSERT_TRUE(write_u64(32U, 0U).is_ok());
      break;
    case AnchorMutation::kGroupCount:
      ASSERT_TRUE(write_u64(40U, 0U).is_ok());
      break;
    case AnchorMutation::kRangeCount:
      ASSERT_TRUE(write_u64(40U, 2U).is_ok());
      break;
    case AnchorMutation::kReserved:
      anchor_bytes[52U] = std::byte{1U};
      break;
    }
    std::fill(anchor_bytes.begin() + 48, anchor_bytes.begin() + 52, std::byte{0U});
    common::ByteWriter checksum_writer{anchor_bytes.subspan(48U, 4U)};
    ASSERT_TRUE(
        checksum_writer.write_u32_le(common::crc32c(common::ByteView{anchor_bytes})).is_ok());
    ASSERT_TRUE(checksum_writer.full());

    const int anchor_file = ::open(anchored.anchor.c_str(), O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
    ASSERT_GE(anchor_file, 0);
    ASSERT_EQ(::pwrite(anchor_file, malformed_anchor.data(), malformed_anchor.size(), 0),
              static_cast<ssize_t>(malformed_anchor.size()));
    ASSERT_EQ(::fsync(anchor_file), 0);
    ASSERT_EQ(::close(anchor_file), 0);
    ASSERT_EQ(read_binary_file(anchored.anchor), malformed_anchor);

    for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
      SCOPED_TRACE(attempt);
      auto rejected = SingleNodeDatabase::open_or_create(config(directory));
      ASSERT_FALSE(rejected.has_value());
      EXPECT_EQ(rejected.error().code(), test_case.expected_code);
      EXPECT_NE(rejected.error().to_string().find(test_case.expected_diagnostic),
                std::string::npos);
      EXPECT_EQ(read_binary_file(anchored.anchor), malformed_anchor);
      EXPECT_EQ(read_binary_file(anchored.retained_segment), pristine_segment);
      EXPECT_FALSE(std::filesystem::exists(anchored.reclaimed_segment));
    }
  }
}

TEST(SingleNodeDatabaseTest, RejectsCorruptAnchoredRaftSegmentWithoutFallbackOrRewrite) {
  TemporaryDirectory directory;
  AnchoredMetadataLog anchored;
  provision_anchored_metadata_log(directory, anchored);
  ASSERT_TRUE(anchored.ready);
  const std::string pristine_anchor = read_binary_file(anchored.anchor);
  ASSERT_EQ(pristine_anchor.size(), 64U);
  const std::string pristine_segment = read_binary_file(anchored.retained_segment);
  ASSERT_GT(pristine_segment.size(), raft::kRaftSegmentHeaderSize);

  constexpr std::size_t kCoveredSegmentNumberByte = 16U;
  std::string corrupted_segment = pristine_segment;
  corrupted_segment.at(kCoveredSegmentNumberByte) = static_cast<char>(
      static_cast<unsigned char>(corrupted_segment.at(kCoveredSegmentNumberByte)) ^ 1U);
  const int segment_file =
      ::open(anchored.retained_segment.c_str(), O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
  ASSERT_GE(segment_file, 0);
  ASSERT_EQ(::pwrite(segment_file, corrupted_segment.data() + kCoveredSegmentNumberByte, 1U,
                     static_cast<off_t>(kCoveredSegmentNumberByte)),
            static_cast<ssize_t>(1));
  ASSERT_EQ(::fsync(segment_file), 0);
  ASSERT_EQ(::close(segment_file), 0);
  ASSERT_EQ(read_binary_file(anchored.retained_segment), corrupted_segment);

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    SCOPED_TRACE(attempt);
    auto rejected = SingleNodeDatabase::open_or_create(config(directory));
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
    EXPECT_NE(rejected.error().to_string().find("Raft segment header checksum mismatch"),
              std::string::npos);
    EXPECT_EQ(read_binary_file(anchored.anchor), pristine_anchor);
    EXPECT_EQ(read_binary_file(anchored.retained_segment), corrupted_segment);
    EXPECT_FALSE(std::filesystem::exists(anchored.reclaimed_segment));
  }
}

TEST(SingleNodeDatabaseTest, RejectsCorruptAnchoredRaftRecordWithoutFallbackOrRewrite) {
  TemporaryDirectory directory;
  AnchoredMetadataLog anchored;
  provision_anchored_metadata_log(directory, anchored);
  ASSERT_TRUE(anchored.ready);
  const std::string pristine_anchor = read_binary_file(anchored.anchor);
  ASSERT_EQ(pristine_anchor.size(), 64U);
  const std::string pristine_segment = read_binary_file(anchored.retained_segment);
  constexpr std::size_t kFirstCheckpointPayloadByte =
      raft::kRaftSegmentHeaderSize + raft::kMultiplexedLogHeaderSize;
  ASSERT_GT(pristine_segment.size(), kFirstCheckpointPayloadByte);

  std::string corrupted_segment = pristine_segment;
  corrupted_segment.at(kFirstCheckpointPayloadByte) = static_cast<char>(
      static_cast<unsigned char>(corrupted_segment.at(kFirstCheckpointPayloadByte)) ^ 1U);
  const int segment_file =
      ::open(anchored.retained_segment.c_str(), O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
  ASSERT_GE(segment_file, 0);
  ASSERT_EQ(::pwrite(segment_file, corrupted_segment.data() + kFirstCheckpointPayloadByte, 1U,
                     static_cast<off_t>(kFirstCheckpointPayloadByte)),
            static_cast<ssize_t>(1));
  ASSERT_EQ(::fsync(segment_file), 0);
  ASSERT_EQ(::close(segment_file), 0);
  ASSERT_EQ(read_binary_file(anchored.retained_segment), corrupted_segment);

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    SCOPED_TRACE(attempt);
    auto rejected = SingleNodeDatabase::open_or_create(config(directory));
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
    EXPECT_NE(rejected.error().to_string().find("multiplexed log payload checksum mismatch"),
              std::string::npos);
    EXPECT_EQ(read_binary_file(anchored.anchor), pristine_anchor);
    EXPECT_EQ(read_binary_file(anchored.retained_segment), corrupted_segment);
    EXPECT_FALSE(std::filesystem::exists(anchored.reclaimed_segment));
  }
}

TEST(SingleNodeDatabaseTest, RejectsTruncatedAnchoredRaftRecordEvenWhenTailRepairIsAuthorized) {
  TemporaryDirectory directory;
  AnchoredMetadataLog anchored;
  provision_anchored_metadata_log(directory, anchored);
  ASSERT_TRUE(anchored.ready);
  const std::string pristine_anchor = read_binary_file(anchored.anchor);
  ASSERT_EQ(pristine_anchor.size(), 64U);
  const std::string pristine_segment = read_binary_file(anchored.retained_segment);
  const auto segment_bytes =
      std::as_bytes(std::span{pristine_segment.data(), pristine_segment.size()});
  ASSERT_GE(segment_bytes.size(), raft::kRaftSegmentHeaderSize + raft::kMultiplexedLogHeaderSize);
  auto checkpoint_header = raft::inspect_multiplexed_log_record_header_v1(
      segment_bytes.subspan(raft::kRaftSegmentHeaderSize, raft::kMultiplexedLogHeaderSize));
  ASSERT_TRUE(checkpoint_header.has_value()) << checkpoint_header.error().to_string();
  const std::size_t checkpoint_record_end =
      raft::kRaftSegmentHeaderSize + checkpoint_header->encoded_size;
  ASSERT_LE(checkpoint_record_end, pristine_segment.size());

  const std::string truncated_segment = pristine_segment.substr(0U, checkpoint_record_end - 1U);
  const int segment_file =
      ::open(anchored.retained_segment.c_str(), O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
  ASSERT_GE(segment_file, 0);
  ASSERT_EQ(::ftruncate(segment_file, static_cast<off_t>(truncated_segment.size())), 0);
  ASSERT_EQ(::fsync(segment_file), 0);
  ASSERT_EQ(::close(segment_file), 0);
  ASSERT_EQ(read_binary_file(anchored.retained_segment), truncated_segment);

  auto repair_authorized = config(directory);
  repair_authorized.raft_recovery.repair_incomplete_final_tail = true;
  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    SCOPED_TRACE(attempt);
    auto rejected = SingleNodeDatabase::open_or_create(repair_authorized);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
    EXPECT_NE(
        rejected.error().to_string().find("Raft recovery checkpoint contains an incomplete record"),
        std::string::npos);
    EXPECT_EQ(read_binary_file(anchored.anchor), pristine_anchor);
    EXPECT_EQ(read_binary_file(anchored.retained_segment), truncated_segment);
    EXPECT_FALSE(std::filesystem::exists(anchored.reclaimed_segment));
  }
}

TEST(SingleNodeDatabaseTest, MoveAssignmentClosesTheReplacedDatabaseBeforeTakingOwnership) {
  TemporaryDirectory replaced_directory;
  TemporaryDirectory incoming_directory;
  auto target = SingleNodeDatabase::open_or_create(config(replaced_directory));
  ASSERT_TRUE(target.has_value()) << target.error().to_string();
  auto source = SingleNodeDatabase::open_or_create(config(incoming_directory));
  ASSERT_TRUE(source.has_value()) << source.error().to_string();

  *target = std::move(*source);

  auto moved_from_snapshot = source->storage_snapshot();
  ASSERT_FALSE(moved_from_snapshot.has_value());
  EXPECT_EQ(moved_from_snapshot.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(source->find_tablet(tablet_id()), nullptr);
  EXPECT_EQ(source->find_lineage(columnar::test::batch_schema()->table_id()), nullptr);
  EXPECT_TRUE(source->shutdown().is_ok());
  auto reopened_replaced = SingleNodeDatabase::open_or_create(config(replaced_directory));
  ASSERT_TRUE(reopened_replaced.has_value()) << reopened_replaced.error().to_string();
  EXPECT_TRUE(reopened_replaced->shutdown().is_ok());

  EXPECT_TRUE(target->shutdown().is_ok());
  auto reopened_incoming = SingleNodeDatabase::open_or_create(config(incoming_directory));
  ASSERT_TRUE(reopened_incoming.has_value()) << reopened_incoming.error().to_string();
  EXPECT_TRUE(reopened_incoming->shutdown().is_ok());
}

TEST(SingleNodeDatabaseTest, RecoversCatalogWalRowsAndVectorQueryVisibility) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  ASSERT_EQ(database->query_catalog()->tables().size(), 1U);
  ASSERT_NE(database->find_tablet(tablet_id()), nullptr);
  const schema::TableId subscription_table = batch()->schema().table_id();
  auto subscription_context = database->subscription_snapshot_context(subscription_table);
  ASSERT_TRUE(subscription_context.has_value()) << subscription_context.error().to_string();
  EXPECT_NE(subscription_context->storage, nullptr);
  EXPECT_NE(subscription_context->publisher, nullptr);
  ASSERT_NE(subscription_context->lineage, nullptr);
  EXPECT_EQ(subscription_context->lineage->table_id(), subscription_table);
  const auto appended = database->execute_append(
      tablet_id(), {.client_id = ingest::test::request_id<ingest::ClientId>(1U),
                    .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(2U),
                    .batch = batch(),
                    .durability = wal::WalDurabilityMode::kLocalSync});
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(query_count(*database), 2);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto recovered = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->find_tablet(tablet_id())->snapshot()->visible_row_count(), 2U);
  EXPECT_EQ(query_count(*recovered), 2);
  EXPECT_TRUE(recovered->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, AppliesLocalSyncIngestAndReturnsPositionlessMatchingRetry) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  RecordingCommittedAppendObserver observer;
  SingleNodeDatabaseConfig database_config = config(directory);
  database_config.committed_append_observer = &observer;
  auto database = SingleNodeDatabase::open_or_create(database_config);
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};

  auto applied = service.execute_ingest(ingest_task(3U, network::DurabilityMode::kLocalSync));
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  EXPECT_EQ(applied->connection_id, 9U);
  EXPECT_EQ(applied->principal_id, 7U);
  EXPECT_EQ(applied->frame.header.message_type, network::MessageType::kIngestAcknowledgement);
  const auto applied_ack = network::decode_ingest_acknowledgement(applied->frame.payload);
  ASSERT_TRUE(applied_ack.has_value()) << applied_ack.error().to_string();
  EXPECT_EQ(applied_ack->requested_durability, network::DurabilityMode::kLocalSync);
  EXPECT_EQ(applied_ack->effective_durability, network::DurabilityMode::kLocalSync);
  EXPECT_EQ(applied_ack->outcome, network::IngestOutcome::kApplied);
  EXPECT_NE(applied_ack->record_sequence, 0U);
  EXPECT_NE(applied_ack->segment_number, 0U);
  ASSERT_EQ(observer.notifications, 1U);
  if (!observer.last.has_value()) {
    ADD_FAILURE() << "applied append notification was not published";
    return;
  }
  const auto& notification = *observer.last;
  EXPECT_EQ(notification.tablet_id, tablet_id());
  EXPECT_EQ(notification.position.record_sequence, applied_ack->record_sequence);
  EXPECT_EQ(notification.batch->row_count(), 2U);
  EXPECT_EQ(notification.outcome->record_sequence, applied_ack->record_sequence);

  auto matching = service.execute_ingest(ingest_task(3U, network::DurabilityMode::kAsync));
  ASSERT_TRUE(matching.has_value()) << matching.error().to_string();
  const auto matching_ack = network::decode_ingest_acknowledgement(matching->frame.payload);
  ASSERT_TRUE(matching_ack.has_value()) << matching_ack.error().to_string();
  EXPECT_EQ(matching_ack->requested_durability, network::DurabilityMode::kAsync);
  EXPECT_EQ(matching_ack->effective_durability, network::DurabilityMode::kAsync);
  EXPECT_EQ(matching_ack->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(matching_ack->record_sequence, 0U);
  EXPECT_EQ(matching_ack->segment_number, 0U);
  EXPECT_EQ(matching_ack->byte_offset, 0U);
  EXPECT_EQ(observer.notifications, 1U);
  EXPECT_EQ(database->find_tablet(tablet_id())->snapshot()->visible_row_count(), 2U);

  auto query = service.execute_query(query_task(40U, "SELECT count(*) AS rows FROM events"));
  ASSERT_TRUE(query.has_value()) << query.error().to_string();
  ASSERT_EQ(query->responses.size(), 2U);
  EXPECT_EQ(query->result_rows, 1U);
  EXPECT_GT(query->payload_bytes, 0U);
  EXPECT_EQ(query->responses[0].frame.header.message_type, network::MessageType::kQueryResult);
  EXPECT_EQ(query->responses[1].frame.header.message_type, network::MessageType::kQueryEnd);
  EXPECT_EQ(query->responses[0].frame.header.request_id, 40U);
  const auto result = network::decode_query_result_batch(query->responses[0].frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->row_count(), 1U);
  ASSERT_EQ(result->columns().size(), 1U);
  EXPECT_EQ(result->columns()[0].name, "rows");
  const network::QueryResultCell* const cell = result->cell(0U, 0U);
  ASSERT_NE(cell, nullptr);
  ASSERT_FALSE(cell->is_null);
  common::ByteReader count{cell->value};
  EXPECT_EQ(count.read_i64_le().value(), 2);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, DefaultsToFiniteSixtyFourMebibyteQueryBudgets) {
  const NativeProtocolServiceLimits limits;
  constexpr std::size_t expected_bytes = std::size_t{64U} * 1024U * 1024U;
  EXPECT_EQ(limits.maximum_query_memory_bytes, expected_bytes);
  EXPECT_EQ(limits.maximum_response_payload_bytes, expected_bytes);
}

TEST(NativeProtocolServiceTest, FlushesSealedHeadsAndRecoversOnlyTheWalSuffix) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};

  for (std::uint8_t seed = 10U; seed < 13U; ++seed) {
    auto response = service.execute_ingest(ingest_task(seed, network::DurabilityMode::kLocalSync));
    ASSERT_TRUE(response.has_value()) << response.error().to_string();
    EXPECT_EQ(response->frame.header.message_type, network::MessageType::kIngestAcknowledgement);
  }
  auto storage = database->storage_snapshot();
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  EXPECT_EQ(storage->generation(), 2U);
  ASSERT_EQ(storage->parts().size(), 1U);
  EXPECT_EQ(storage->parts().front().row_count, 4U);
  ASSERT_EQ(storage->durable_tablets().size(), 1U);
  EXPECT_EQ(storage->durable_tablets().front().durable_row_count, 4U);
  EXPECT_EQ(storage->retries().size(), 2U);
  EXPECT_EQ(storage->visible_head_row_count(), 2U);
  EXPECT_EQ(native_query_count(service, 46U, "events"), 6);
  EXPECT_TRUE(database->shutdown().is_ok());

  auto recovered = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  auto recovered_storage = recovered->storage_snapshot();
  ASSERT_TRUE(recovered_storage.has_value()) << recovered_storage.error().to_string();
  EXPECT_EQ(recovered_storage->generation(), 3U);
  EXPECT_EQ(recovered_storage->reclaim_checkpoint().record_sequence, 2U);
  ASSERT_EQ(recovered_storage->parts().size(), 1U);
  EXPECT_EQ(recovered_storage->parts().front().row_count, 4U);
  EXPECT_EQ(recovered_storage->visible_head_row_count(), 2U);
  EXPECT_EQ(recovered->find_tablet(tablet_id())->snapshot()->visible_row_count(), 2U);
  NativeProtocolService recovered_service{*recovered};
  EXPECT_EQ(native_query_count(recovered_service, 47U, "events"), 6);
  EXPECT_TRUE(recovered->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, ExecutesAsofAcrossTheCompleteManifestSnapshotAfterRestart) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};

  for (std::uint8_t seed = 20U; seed < 23U; ++seed) {
    auto response = service.execute_ingest(ingest_task(seed, network::DurabilityMode::kLocalSync));
    ASSERT_TRUE(response.has_value()) << response.error().to_string();
  }
  constexpr std::string_view sql = "SELECT count(*) AS rows FROM events AS l ASOF JOIN events AS r "
                                   "ON l.ts = r.ts AND r.ts <= l.ts";
  auto joined = service.execute_query(query_task(48U, sql));
  ASSERT_TRUE(joined.has_value()) << joined.error().to_string();
  ASSERT_EQ(joined->responses.size(), 2U);
  auto result = network::decode_query_result_batch(joined->responses.front().frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  common::ByteReader count{result->cell(0U, 0U)->value};
  EXPECT_EQ(count.read_i64_le().value(), 6);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto reopened = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  NativeProtocolService reopened_service{*reopened};
  auto recovered = reopened_service.execute_query(query_task(49U, sql));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  ASSERT_EQ(recovered->responses.size(), 2U);
  result = network::decode_query_result_batch(recovered->responses.front().frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  common::ByteReader recovered_count{result->cell(0U, 0U)->value};
  EXPECT_EQ(recovered_count.read_i64_le().value(), 6);
  EXPECT_TRUE(reopened->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, RejectsHistoricalSqlInsteadOfReadingTheCurrentSnapshot) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};
  auto response = service.execute_ingest(ingest_task(30U, network::DurabilityMode::kLocalSync));
  ASSERT_TRUE(response.has_value()) << response.error().to_string();

  auto historical = service.execute_query(
      query_task(50U, "SELECT count(*) AS rows FROM events FOR SYSTEM_TIME AS OF "
                      "TIMESTAMP '1970-01-01 00:00:00Z'"));
  ASSERT_TRUE(historical.has_value()) << historical.error().to_string();
  ASSERT_EQ(historical->responses.size(), 1U);
  EXPECT_EQ(historical->result_rows, 0U);
  EXPECT_EQ(historical->responses.front().frame.header.message_type, network::MessageType::kError);
  auto error = network::decode_error_message(historical->responses.front().frame.payload);
  ASSERT_TRUE(error.has_value()) << error.error().to_string();
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kExecutionFailure);
  constexpr std::string_view kMessage = "native FOR SYSTEM_TIME storage is not configured";
  EXPECT_TRUE(std::ranges::equal(error->message,
                                 std::as_bytes(std::span{kMessage.data(), kMessage.size()})));
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, RejectsMalformedIngestWithProtocolError) {
  TemporaryDirectory directory;
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};
  auto task = ingest_task(4U, network::DurabilityMode::kAsync);
  task.frame.payload.resize(3U);

  auto response = service.execute_ingest(std::move(task));
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  EXPECT_EQ(response->frame.header.message_type, network::MessageType::kError);
  const auto error = network::decode_error_message(response->frame.payload);
  ASSERT_TRUE(error.has_value()) << error.error().to_string();
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kInvalidRequest);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, ConvertsBoundedQueryOverflowToOneTerminalError) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  ASSERT_TRUE(
      database
          ->execute_append(tablet_id(),
                           {.client_id = ingest::test::request_id<ingest::ClientId>(1U),
                            .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(2U),
                            .batch = batch()})
          .has_value());
  ASSERT_TRUE(database->flush_ready_heads().has_value());
  NativeProtocolServiceLimits limits;
  limits.maximum_result_rows = 1U;
  NativeProtocolService service{*database, limits};

  auto query = service.execute_query(query_task(41U, "SELECT * FROM events"));
  ASSERT_TRUE(query.has_value()) << query.error().to_string();
  ASSERT_EQ(query->responses.size(), 1U);
  EXPECT_EQ(query->result_rows, 0U);
  EXPECT_EQ(query->responses[0].frame.header.message_type, network::MessageType::kError);
  const auto error = network::decode_error_message(query->responses[0].frame.payload);
  ASSERT_TRUE(error.has_value()) << error.error().to_string();
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kOverloaded);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, EmitsDescribedZeroRowResultBeforeQueryEnd) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};

  auto query = service.execute_query(query_task(42U, "SELECT * FROM events WHERE false"));
  ASSERT_TRUE(query.has_value()) << query.error().to_string();
  ASSERT_EQ(query->responses.size(), 2U);
  EXPECT_EQ(query->result_rows, 0U);
  EXPECT_EQ(query->responses[0].frame.header.message_type, network::MessageType::kQueryResult);
  EXPECT_EQ(query->responses[1].frame.header.message_type, network::MessageType::kQueryEnd);
  const auto result = network::decode_query_result_batch(query->responses[0].frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->row_count(), 0U);
  EXPECT_EQ(result->columns().size(), 3U);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, CreatesTableWithInjectedIdentitiesAndReturnsDurableResult) {
  TemporaryDirectory directory;
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  DeterministicIdentityGenerator identities{50U};
  NativeProtocolService service{*database, identities};

  auto ddl = service.execute_query(query_task(43U, kCreateSql));
  ASSERT_TRUE(ddl.has_value()) << ddl.error().to_string();
  ASSERT_EQ(ddl->responses.size(), 2U);
  EXPECT_EQ(ddl->result_rows, 1U);
  EXPECT_EQ(ddl->responses[0].frame.header.message_type, network::MessageType::kQueryResult);
  EXPECT_EQ(ddl->responses[1].frame.header.message_type, network::MessageType::kQueryEnd);
  const auto result = network::decode_query_result_batch(ddl->responses[0].frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->row_count(), 1U);
  ASSERT_EQ(result->columns().size(), 5U);
  EXPECT_EQ(result->columns()[0].name, "table_id");
  EXPECT_EQ(result->columns()[3].name, "metadata_index");
  EXPECT_EQ(result->columns()[4].name, "resumed_incomplete_creation");
  const auto* table = result->cell(0U, 0U);
  const auto* schema = result->cell(0U, 1U);
  const auto* tablet = result->cell(0U, 2U);
  const auto* metadata = result->cell(0U, 3U);
  const auto* resumed = result->cell(0U, 4U);
  ASSERT_NE(table, nullptr);
  ASSERT_NE(schema, nullptr);
  ASSERT_NE(tablet, nullptr);
  ASSERT_NE(metadata, nullptr);
  ASSERT_NE(resumed, nullptr);
  EXPECT_EQ(table->value.front(), std::byte{50U});
  EXPECT_EQ(schema->value.front(), std::byte{51U});
  EXPECT_EQ(tablet->value.front(), std::byte{52U});
  common::ByteReader metadata_index{metadata->value};
  EXPECT_NE(metadata_index.read_u64_le().value(), 0U);
  ASSERT_EQ(resumed->value.size(), 1U);
  EXPECT_EQ(resumed->value.front(), std::byte{0U});

  auto count = service.execute_query(query_task(44U, "SELECT count(*) AS rows FROM trades"));
  ASSERT_TRUE(count.has_value()) << count.error().to_string();
  ASSERT_EQ(count->responses.size(), 2U);
  const auto count_result = network::decode_query_result_batch(count->responses[0].frame.payload);
  ASSERT_TRUE(count_result.has_value()) << count_result.error().to_string();
  common::ByteReader count_value{count_result->cell(0U, 0U)->value};
  EXPECT_EQ(count_value.read_i64_le().value(), 0);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, CreatesTableWithDefaultSystemIdentities) {
  TemporaryDirectory directory;
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  NativeProtocolService service{*database};

  auto ddl = service.execute_query(query_task(45U, kCreateSql));
  ASSERT_TRUE(ddl.has_value()) << ddl.error().to_string();
  ASSERT_EQ(ddl->responses.size(), 2U);
  ASSERT_EQ(ddl->responses[0].frame.header.message_type, network::MessageType::kQueryResult);
  const auto result = network::decode_query_result_batch(ddl->responses[0].frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->row_count(), 1U);
  for (std::size_t column = 0U; column < 3U; ++column) {
    const auto* const identity = result->cell(0U, column);
    ASSERT_NE(identity, nullptr);
    EXPECT_TRUE(std::ranges::any_of(identity->value,
                                    [](const std::byte value) { return value != std::byte{0U}; }));
  }
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, RejectsSystemEntropyFailureBeforeTableCreation) {
  TemporaryDirectory directory;
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  FifthCallFailingEntropySource entropy;
  common::SystemUuidGenerator identities{entropy};
  NativeProtocolService service{*database, identities};

  auto failed = service.execute_query(query_task(46U, kCreateSql));
  ASSERT_TRUE(failed.has_value()) << failed.error().to_string();
  ASSERT_EQ(failed->responses.size(), 1U);
  ASSERT_EQ(failed->responses[0].frame.header.message_type, network::MessageType::kError);
  const auto failure = network::decode_error_message(failed->responses[0].frame.payload);
  ASSERT_TRUE(failure.has_value()) << failure.error().to_string();
  EXPECT_EQ(failure->code, network::ProtocolErrorCode::kExecutionFailure);
  constexpr std::string_view kFailure = "injected system entropy failure";
  EXPECT_TRUE(std::ranges::equal(failure->message,
                                 std::as_bytes(std::span{kFailure.data(), kFailure.size()})));

  auto missing = service.execute_query(query_task(47U, "SELECT count(*) FROM trades"));
  ASSERT_TRUE(missing.has_value()) << missing.error().to_string();
  ASSERT_EQ(missing->responses.size(), 1U);
  ASSERT_EQ(missing->responses[0].frame.header.message_type, network::MessageType::kError);
  const auto missing_error = network::decode_error_message(missing->responses[0].frame.payload);
  ASSERT_TRUE(missing_error.has_value()) << missing_error.error().to_string();
  EXPECT_EQ(missing_error->code, network::ProtocolErrorCode::kInvalidRequest);

  auto retried = service.execute_query(query_task(48U, kCreateSql));
  ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
  ASSERT_EQ(retried->responses.size(), 2U);
  ASSERT_EQ(retried->responses[0].frame.header.message_type, network::MessageType::kQueryResult);
  const auto result = network::decode_query_result_batch(retried->responses[0].frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_NE(result->cell(0U, 4U), nullptr);
  ASSERT_EQ(result->cell(0U, 4U)->value.size(), 1U);
  EXPECT_EQ(result->cell(0U, 4U)->value.front(), std::byte{0U});
  EXPECT_EQ(retried->responses[1].frame.header.message_type, network::MessageType::kQueryEnd);
  EXPECT_EQ(entropy.calls, 12U);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(NativeProtocolServiceTest, AppliesLocalSyncSqlInsertAndRecoversItsRows) {
  TemporaryDirectory directory;
  seed_catalog(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  DeterministicIdentityGenerator identities{80U};
  NativeProtocolService service{*database, identities};

  auto inserted = service.execute_query(
      query_task(45U, "INSERT INTO events VALUES "
                      "(TIMESTAMP '1970-01-01 00:00:00.000000001Z', 'first', true), "
                      "(TIMESTAMP '1970-01-01 00:00:00.000000002Z', NULL, false)"));
  ASSERT_TRUE(inserted.has_value()) << inserted.error().to_string();
  ASSERT_EQ(inserted->responses.size(), 2U);
  EXPECT_EQ(inserted->result_rows, 1U);
  EXPECT_EQ(inserted->responses[0].frame.header.message_type, network::MessageType::kQueryResult);
  EXPECT_EQ(inserted->responses[1].frame.header.message_type, network::MessageType::kQueryEnd);
  const auto result = network::decode_query_result_batch(inserted->responses[0].frame.payload);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->row_count(), 1U);
  ASSERT_EQ(result->columns().size(), 5U);
  EXPECT_EQ(result->columns()[0].name, "applied_rows");
  EXPECT_EQ(result->columns()[1].name, "record_sequence");
  EXPECT_EQ(result->columns()[4].name, "matching_retry");
  common::ByteReader rows{result->cell(0U, 0U)->value};
  common::ByteReader sequence{result->cell(0U, 1U)->value};
  common::ByteReader segment{result->cell(0U, 2U)->value};
  EXPECT_EQ(rows.read_u64_le().value(), 2U);
  EXPECT_NE(sequence.read_u64_le().value(), 0U);
  EXPECT_NE(segment.read_u64_le().value(), 0U);
  ASSERT_EQ(result->cell(0U, 4U)->value.size(), 1U);
  EXPECT_EQ(result->cell(0U, 4U)->value.front(), std::byte{0U});

  auto count = service.execute_query(query_task(46U, "SELECT count(*) AS rows FROM events"));
  ASSERT_TRUE(count.has_value()) << count.error().to_string();
  const auto count_result = network::decode_query_result_batch(count->responses[0].frame.payload);
  ASSERT_TRUE(count_result.has_value()) << count_result.error().to_string();
  common::ByteReader count_value{count_result->cell(0U, 0U)->value};
  EXPECT_EQ(count_value.read_i64_le().value(), 2);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto recovered = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  NativeProtocolService recovered_service{*recovered, identities};
  auto recovered_count =
      recovered_service.execute_query(query_task(47U, "SELECT count(*) AS rows FROM events"));
  ASSERT_TRUE(recovered_count.has_value()) << recovered_count.error().to_string();
  const auto recovered_result =
      network::decode_query_result_batch(recovered_count->responses[0].frame.payload);
  ASSERT_TRUE(recovered_result.has_value()) << recovered_result.error().to_string();
  common::ByteReader recovered_value{recovered_result->cell(0U, 0U)->value};
  EXPECT_EQ(recovered_value.read_i64_le().value(), 2);
  EXPECT_TRUE(recovered->shutdown().is_ok());
}

TEST(SingleNodeDatabaseTest, CreatesACompleteTableAndReopensItsRuntimeCatalog) {
  TemporaryDirectory directory;
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto bound = bind_create(*database);
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto created =
      database->create_table(*bound,
                             {.table_id = id<schema::TableId>(11U),
                              .schema_id = id<schema::SchemaId>(12U),
                              .column_ids = {id<schema::ColumnId>(13U), id<schema::ColumnId>(14U),
                                             id<schema::ColumnId>(15U), id<schema::ColumnId>(16U)},
                              .tablet_id = id<schema::TabletId>(17U)},
                             64U);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  EXPECT_FALSE(created->resumed_incomplete_creation);
  EXPECT_EQ(database->query_catalog()->tables().size(), 1U);
  EXPECT_NE(database->find_tablet(created->tablet_id), nullptr);
  EXPECT_NE(database->find_lineage(created->table_id), nullptr);
  EXPECT_EQ(bind_create(*database).error().status().code(), common::StatusCode::kAlreadyExists);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto reopened = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->query_catalog()->tables().size(), 1U);
  EXPECT_NE(reopened->find_tablet(created->tablet_id), nullptr);
  EXPECT_EQ(reopened->metadata_catalog().table_policies.front().retry_retention_positions, 64U);
  EXPECT_TRUE(reopened->shutdown().is_ok());
}

TEST(SingleNodeDatabaseTest, ResumesAnIncompleteSchemaPrefixUsingItsDurableIdentities) {
  TemporaryDirectory directory;
  seed_schema_prefix(directory);
  auto database = SingleNodeDatabase::open_or_create(config(directory));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  EXPECT_TRUE(database->query_catalog()->tables().empty());
  auto bound = bind_create(*database);
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto created =
      database->create_table(*bound,
                             {.table_id = id<schema::TableId>(71U),
                              .schema_id = id<schema::SchemaId>(72U),
                              .column_ids = {id<schema::ColumnId>(73U), id<schema::ColumnId>(74U),
                                             id<schema::ColumnId>(75U), id<schema::ColumnId>(76U)},
                              .tablet_id = id<schema::TabletId>(77U)},
                             32U);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  EXPECT_TRUE(created->resumed_incomplete_creation);
  EXPECT_EQ(created->table_id, id<schema::TableId>(41U));
  EXPECT_EQ(created->schema_id, id<schema::SchemaId>(42U));
  EXPECT_EQ(created->tablet_id, id<schema::TabletId>(77U));
  EXPECT_EQ(database->query_catalog()->tables().size(), 1U);
  EXPECT_TRUE(database->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
