#include "chronos/common/status.hpp"
#include "chronos/query/spill_sort.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-spill-sort-XXXXXX").string();
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

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t byte = 0U; byte < sizeof(value); ++byte)
    bytes[offset + byte] = static_cast<std::byte>((value >> (byte * 8U)) & 0xffU);
}

[[nodiscard]] columnar::OwnedPhysicalColumn
int64_column(const std::span<const std::int64_t> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kInt64),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
string_column(const std::span<const std::optional<std::string>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.offsets.resize((values.size() + 1U) * sizeof(std::uint32_t));
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
    } else {
      set_bit(buffers.validity, static_cast<std::uint32_t>(row));
      // The preceding presence check proves this dereference.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      for (const char value : *values[row])
        buffers.values.push_back(static_cast<std::byte>(value));
    }
    store_u32(buffers.offsets, (row + 1U) * sizeof(std::uint32_t),
              static_cast<std::uint32_t>(buffers.values.size()));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kString),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk chunk(const QueryResourceContext& resources,
                                         std::vector<std::int64_t> key,
                                         std::vector<std::optional<std::string>> secondary,
                                         std::vector<std::int64_t> identity) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column(key));
  columns.push_back(string_column(secondary));
  columns.push_back(int64_column(identity));
  VectorChunk output =
      VectorChunk::create(std::move(columns),
                          VectorSelection::all(static_cast<std::uint32_t>(key.size())).value())
          .value();
  QueryMemoryReservation reservation =
      resources.reserve(output.retained_buffer_bytes() + 512U).value();
  return AccountedVectorChunk::create(std::move(output), std::move(reservation), resources).value();
}

class ChunkSource final : public PhysicalOperator {
public:
  explicit ChunkSource(std::vector<AccountedVectorChunk> chunks) : chunks_(std::move(chunks)) {}

  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (cursor_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[cursor_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t cursor_{};
};

class EmptySource final : public PhysicalOperator {
public:
  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] std::int64_t read_int64(const VectorChunk& chunk_value, const std::size_t column,
                                      const std::size_t row) {
  const common::ByteView bytes = chunk_value.cell({column, row}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte)
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  return std::bit_cast<std::int64_t>(bits);
}

[[nodiscard]] SpillSortLimits tiny_limits() {
  SpillSortLimits limits;
  limits.maximum_rows = 128U;
  limits.maximum_runs = 64U;
  limits.maximum_spill_bytes = 1U << 20U;
  limits.maximum_serialized_record_bytes = 1U << 10U;
  limits.maximum_configuration_bytes = 1U << 20U;
  limits.run_sort_limits.maximum_rows = 2U;
  limits.run_sort_limits.maximum_state_bytes = 1U << 20U;
  limits.run_sort_limits.output_limits.maximum_rows = 2U;
  limits.merge_output_limits.maximum_rows = 2U;
  limits.merge_output_limits.maximum_state_bytes = 1U << 20U;
  limits.merge_output_limits.output_limits.maximum_rows = 2U;
  return limits;
}

[[nodiscard]] std::unique_ptr<PhysicalOperator>
create_spill(std::vector<AccountedVectorChunk> chunks, const std::filesystem::path& path,
             SpillSortLimits limits = tiny_limits()) {
  io::PosixDirectory directory = io::PosixDirectory::open(path.string()).value();
  return SpillSortOperator::create(
             std::make_unique<ChunkSource>(std::move(chunks)),
             std::vector<VectorSortKey>{{.column_ordinal = 0U,
                                         .direction = PhysicalSortDirection::kAscending,
                                         .null_placement = ScalarNullPlacement::kLast},
                                        {.column_ordinal = 1U,
                                         .direction = PhysicalSortDirection::kDescending,
                                         .null_placement = ScalarNullPlacement::kFirst}},
             std::move(directory), "query-17", limits)
      .value();
}

TEST(SpillSortOperatorTest, MergesMultipleRunsWithExactKeysStableTiesAndBoundedChunks) {
  TemporaryDirectory temporary;
  ASSERT_FALSE(temporary.path().empty());
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> input;
  input.push_back(chunk(resources, {2, 1}, {"z", "a"}, {0, 1}));
  input.push_back(chunk(resources, {1, 2}, {"b", std::nullopt}, {2, 3}));
  input.push_back(chunk(resources, {1, 1}, {"b", "b"}, {4, 5}));
  std::unique_ptr<PhysicalOperator> sorted = create_spill(std::move(input), temporary.path());

  std::vector<std::int64_t> identities;
  for (;;) {
    common::Result<PhysicalOperatorStep> step = sorted->next(resources);
    ASSERT_TRUE(step.has_value()) << step.error().message();
    if (step->kind() == PhysicalOperatorStepKind::kEnd)
      break;
    AccountedVectorChunk output = std::move(*step).take_chunk().value();
    EXPECT_LE(output.chunk().selected_row_count(), 2U);
    EXPECT_TRUE(output.chunk().selection().is_identity());
    for (std::size_t row = 0U; row < output.chunk().selected_row_count(); ++row)
      identities.push_back(read_int64(output.chunk(), 2U, row));
  }
  EXPECT_EQ(identities, (std::vector<std::int64_t>{2, 4, 5, 1, 3, 0}));
  EXPECT_TRUE(io::PosixDirectory::open(temporary.path().string())->list_entries()->empty());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ(sorted->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(SpillSortOperatorTest, EmptyInputAndEarlyDestructionRemoveEveryOwnedFile) {
  TemporaryDirectory temporary;
  QueryResourceContext empty_resources =
      QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  io::PosixDirectory empty_directory = io::PosixDirectory::open(temporary.path().string()).value();
  auto empty = SpillSortOperator::create(std::make_unique<EmptySource>(),
                                         std::vector<VectorSortKey>{{.column_ordinal = 0U}},
                                         std::move(empty_directory), "empty", tiny_limits());
  ASSERT_TRUE(empty.has_value());
  EXPECT_EQ((*empty)->next(empty_resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(empty_resources.reserved_memory_bytes(), 0U);

  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> input;
  input.push_back(chunk(resources, {3, 1}, {"a", "a"}, {0, 1}));
  input.push_back(chunk(resources, {4, 2}, {"a", "a"}, {2, 3}));
  std::unique_ptr<PhysicalOperator> sorted = create_spill(std::move(input), temporary.path());
  ASSERT_EQ(sorted->next(resources)->kind(), PhysicalOperatorStepKind::kChunk);
  EXPECT_FALSE(io::PosixDirectory::open(temporary.path().string())->list_entries()->empty());
  sorted.reset();
  EXPECT_TRUE(io::PosixDirectory::open(temporary.path().string())->list_entries()->empty());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(SpillSortOperatorTest, RejectsHostileChunkRunCountDiskAndRecordShapes) {
  TemporaryDirectory temporary;
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> oversized;
  oversized.push_back(chunk(resources, {3, 2, 1}, {"a", "a", "a"}, {0, 1, 2}));
  auto too_wide = create_spill(std::move(oversized), temporary.path());
  EXPECT_EQ(too_wide->next(resources).error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  QueryResourceContext run_resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  SpillSortLimits one_run = tiny_limits();
  one_run.maximum_runs = 1U;
  std::vector<AccountedVectorChunk> multiple;
  multiple.push_back(chunk(run_resources, {2, 1}, {"a", "a"}, {0, 1}));
  multiple.push_back(chunk(run_resources, {4, 3}, {"a", "a"}, {2, 3}));
  auto limited_runs = create_spill(std::move(multiple), temporary.path(), one_run);
  EXPECT_EQ(limited_runs->next(run_resources).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(run_resources.reserved_memory_bytes(), 0U);

  QueryResourceContext disk_resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  SpillSortLimits no_disk = tiny_limits();
  no_disk.maximum_spill_bytes = 32U;
  std::vector<AccountedVectorChunk> disk_input;
  disk_input.push_back(chunk(disk_resources, {1}, {"payload"}, {0}));
  auto disk_limited = create_spill(std::move(disk_input), temporary.path(), no_disk);
  EXPECT_EQ(disk_limited->next(disk_resources).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(disk_resources.reserved_memory_bytes(), 0U);
}

TEST(SpillSortOperatorTest, DetectsUnreadRunCorruptionAndRemovesAllFiles) {
  TemporaryDirectory temporary;
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> input;
  input.push_back(chunk(resources, {2, 1}, {"z", "a"}, {0, 1}));
  input.push_back(chunk(resources, {1, 2}, {"b", std::nullopt}, {2, 3}));
  input.push_back(chunk(resources, {1, 1}, {"b", "b"}, {4, 5}));
  std::unique_ptr<PhysicalOperator> sorted = create_spill(std::move(input), temporary.path());
  common::Result<PhysicalOperatorStep> first = sorted->next(resources);
  ASSERT_TRUE(first.has_value()) << first.error().message();
  ASSERT_EQ(first->kind(), PhysicalOperatorStepKind::kChunk);
  first = PhysicalOperatorStep::end();

  io::PosixDirectory directory = io::PosixDirectory::open(temporary.path().string()).value();
  const std::vector<io::DirectoryEntry> entries = directory.list_entries().value();
  ASSERT_EQ(entries.size(), 3U);
  io::PosixFile file =
      directory.open_regular_file(entries.back().name, io::FileOpenMode::kReadWrite).value();
  const std::uint64_t file_size = file.size().value();
  ASSERT_GT(file_size, 0U);
  std::array<std::byte, 1> byte{};
  ASSERT_EQ(file.read_at(file_size - 1U, byte).value(), 1U);
  byte[0] ^= std::byte{0xffU};
  ASSERT_TRUE(file.write_all_at(file_size - 1U, byte).is_ok());
  ASSERT_TRUE(file.close().is_ok());

  common::Result<PhysicalOperatorStep> corrupted = sorted->next(resources);
  ASSERT_FALSE(corrupted.has_value());
  EXPECT_EQ(corrupted.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(resources.is_cancelled());
  EXPECT_TRUE(directory.list_entries()->empty());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(SpillSortOperatorPropertyTest, MatchesIndependentStableModelAcrossForcedRuns) {
  TemporaryDirectory temporary;
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{64U} * 1024U * 1024U).value();
  struct Row {
    std::int64_t key;
    std::string secondary;
    std::int64_t identity;
  };
  std::mt19937_64 random{0x5350494c4c0057ULL};
  std::vector<Row> model;
  std::vector<AccountedVectorChunk> chunks;
  for (std::int64_t identity = 0; identity < 48; identity += 2) {
    std::vector<std::int64_t> keys;
    std::vector<std::optional<std::string>> secondary;
    std::vector<std::int64_t> identities;
    for (std::int64_t offset = 0; offset < 2; ++offset) {
      const std::int64_t key_value = static_cast<std::int64_t>(random() % 7U) - 3;
      const std::string text(1U, static_cast<char>('a' + random() % 4U));
      keys.push_back(key_value);
      secondary.emplace_back(text);
      identities.push_back(identity + offset);
      model.push_back({.key = key_value, .secondary = text, .identity = identity + offset});
    }
    chunks.push_back(
        chunk(resources, std::move(keys), std::move(secondary), std::move(identities)));
  }
  std::stable_sort(model.begin(), model.end(), [](const Row& left, const Row& right) {
    if (left.key != right.key)
      return left.key < right.key;
    return left.secondary > right.secondary;
  });
  std::unique_ptr<PhysicalOperator> sorted = create_spill(std::move(chunks), temporary.path());
  std::size_t row = 0U;
  for (;;) {
    common::Result<PhysicalOperatorStep> step = sorted->next(resources);
    ASSERT_TRUE(step.has_value()) << step.error().message();
    if (step->kind() == PhysicalOperatorStepKind::kEnd)
      break;
    AccountedVectorChunk output = std::move(*step).take_chunk().value();
    for (std::size_t local = 0U; local < output.chunk().selected_row_count(); ++local) {
      ASSERT_LT(row, model.size());
      EXPECT_EQ(read_int64(output.chunk(), 2U, local), model[row].identity);
      ++row;
    }
  }
  EXPECT_EQ(row, model.size());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(SpillSortOperatorTest, CreationValidatesDirectoryPrefixAndFiniteLimits) {
  SpillSortLimits invalid_limits = tiny_limits();
  invalid_limits.maximum_rows = 0U;
  EXPECT_EQ(spill_sort_configuration_reservation_bytes(invalid_limits).error().code(),
            common::StatusCode::kInvalidArgument);
  TemporaryDirectory temporary;
  io::PosixDirectory directory = io::PosixDirectory::open(temporary.path().string()).value();
  auto invalid_prefix = SpillSortOperator::create(
      std::make_unique<EmptySource>(), std::vector<VectorSortKey>{{.column_ordinal = 0U}},
      std::move(directory), "../escape", tiny_limits());
  EXPECT_EQ(invalid_prefix.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
