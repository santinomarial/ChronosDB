#include "chronos/common/rotating_log_sink.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::common {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern = (std::filesystem::temp_directory_path() / "chronos-log-XXXXXX").string();
    char* const created = ::mkdtemp(pattern.data());
    if (created != nullptr)
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

[[nodiscard]] LogRecord record(const std::string_view message) {
  using namespace std::chrono;
  return {.timestamp = sys_days{year{2026} / August / 31},
          .severity = LogSeverity::kInfo,
          .component = "test",
          .event = "rotation",
          .message = message,
          .fields = {}};
}

[[nodiscard]] RotatingJsonLogSinkConfig default_config(std::string path) {
  return {.path = std::move(path),
          .maximum_file_bytes = std::uint64_t{64U} * 1024U * 1024U,
          .retained_file_count = 5U};
}

[[nodiscard]] std::string read_file(const std::filesystem::path& path) {
  std::ifstream input{path, std::ios::binary};
  return {std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

TEST(RotatingJsonLogSinkTest, RetainsNewestBoundedArchivesInOrder) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::filesystem::path path = directory.path() / "chronos.jsonl";
  const auto encoded = encode_json_log(record("first!"));
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto sink = RotatingJsonLogSink::open({.path = path.string(),
                                         .maximum_file_bytes = encoded->size() + 1U,
                                         .retained_file_count = 2U});
  ASSERT_TRUE(sink.has_value()) << sink.error().to_string();

  ASSERT_TRUE((*sink)->write(record("first!")).is_ok());
  ASSERT_TRUE((*sink)->write(record("second")).is_ok());
  ASSERT_TRUE((*sink)->write(record("third!")).is_ok());
  ASSERT_TRUE((*sink)->write(record("fourth")).is_ok());

  EXPECT_NE(read_file(path).find("fourth"), std::string::npos);
  EXPECT_NE(read_file(path.string() + ".1").find("third!"), std::string::npos);
  EXPECT_NE(read_file(path.string() + ".2").find("second"), std::string::npos);
  EXPECT_EQ(read_file(path.string() + ".2").find("first!"), std::string::npos);
}

TEST(RotatingJsonLogSinkTest, ZeroRetentionReplacesTheActiveGenerationWithoutAnArchive) {
  TemporaryDirectory directory;
  const std::filesystem::path path = directory.path() / "chronos.jsonl";
  const auto encoded = encode_json_log(record("first!"));
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto sink = RotatingJsonLogSink::open({.path = path.string(),
                                         .maximum_file_bytes = encoded->size() + 1U,
                                         .retained_file_count = 0U});
  ASSERT_TRUE(sink.has_value()) << sink.error().to_string();
  ASSERT_TRUE((*sink)->write(record("first!")).is_ok());
  ASSERT_TRUE((*sink)->write(record("second")).is_ok());
  EXPECT_NE(read_file(path).find("second"), std::string::npos);
  EXPECT_EQ(read_file(path).find("first!"), std::string::npos);
  EXPECT_FALSE(std::filesystem::exists(path.string() + ".1"));
}

TEST(RotatingJsonLogSinkTest, HoldsExclusivePathLockUntilDestruction) {
  TemporaryDirectory directory;
  const std::filesystem::path path = directory.path() / "chronos.jsonl";
  auto first = RotatingJsonLogSink::open(default_config(path.string()));
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  auto competing = RotatingJsonLogSink::open(default_config(path.string()));
  ASSERT_FALSE(competing.has_value());
  EXPECT_EQ(competing.error().code(), StatusCode::kIoError);

  first->reset();
  auto reopened = RotatingJsonLogSink::open(default_config(path.string()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
}

TEST(RotatingJsonLogSinkTest, SerializesConcurrentWritersThroughOneOwnedStream) {
  TemporaryDirectory directory;
  const std::filesystem::path path = directory.path() / "chronos.jsonl";
  auto sink = RotatingJsonLogSink::open(
      {.path = path.string(), .maximum_file_bytes = 1U << 20U, .retained_file_count = 1U});
  ASSERT_TRUE(sink.has_value()) << sink.error().to_string();
  constexpr std::size_t kThreadCount = 4U;
  constexpr std::size_t kLinesPerThread = 32U;
  std::vector<std::thread> writers;
  writers.reserve(kThreadCount);
  for (std::size_t thread = 0U; thread < kThreadCount; ++thread) {
    writers.emplace_back([&sink] {
      for (std::size_t line = 0U; line < kLinesPerThread; ++line)
        EXPECT_TRUE((*sink)->write(record("concurrent")).is_ok());
    });
  }
  for (std::thread& writer : writers)
    writer.join();
  const std::string bytes = read_file(path);
  EXPECT_EQ(static_cast<std::size_t>(std::count(bytes.begin(), bytes.end(), '\n')),
            kThreadCount * kLinesPerThread);
}

TEST(RotatingJsonLogSinkTest, RotatesAnOversizedExistingFileBeforeAppending) {
  TemporaryDirectory directory;
  const std::filesystem::path path = directory.path() / "chronos.jsonl";
  {
    std::ofstream output{path, std::ios::binary};
    output << std::string(256U, 'e') << '\n';
  }
  auto sink = RotatingJsonLogSink::open(
      {.path = path.string(), .maximum_file_bytes = 32U, .retained_file_count = 1U});
  ASSERT_TRUE(sink.has_value()) << sink.error().to_string();
  const Status written = (*sink)->write(record("x"));
  ASSERT_FALSE(written.is_ok());
  EXPECT_EQ(written.code(), StatusCode::kResourceExhausted);

  auto larger = encode_json_log(record("x"));
  ASSERT_TRUE(larger.has_value());
  sink->reset();
  sink = RotatingJsonLogSink::open({.path = path.string(),
                                    .maximum_file_bytes = larger->size() + 1U,
                                    .retained_file_count = 1U});
  ASSERT_TRUE(sink.has_value()) << sink.error().to_string();
  ASSERT_TRUE((*sink)->write(record("x")).is_ok());
  EXPECT_EQ(read_file(path.string() + ".1").size(), 257U);
  EXPECT_NE(read_file(path).find("\"message\":\"x\""), std::string::npos);
}

TEST(RotatingJsonLogSinkTest, RotationFailureIsTerminalAndDoesNotReplaceAnArchiveDirectory) {
  TemporaryDirectory directory;
  const std::filesystem::path path = directory.path() / "chronos.jsonl";
  const auto encoded = encode_json_log(record("one"));
  ASSERT_TRUE(encoded.has_value());
  ASSERT_TRUE(std::filesystem::create_directory(path.string() + ".1"));
  auto sink = RotatingJsonLogSink::open({.path = path.string(),
                                         .maximum_file_bytes = encoded->size() + 1U,
                                         .retained_file_count = 1U});
  ASSERT_TRUE(sink.has_value()) << sink.error().to_string();
  ASSERT_TRUE((*sink)->write(record("one")).is_ok());
  const Status failed = (*sink)->write(record("two"));
  EXPECT_EQ(failed.code(), StatusCode::kIoError);
  EXPECT_EQ((*sink)->write(record("two")), failed);
  EXPECT_TRUE(std::filesystem::is_directory(path.string() + ".1"));
}

TEST(RotatingJsonLogSinkTest, ValidatesConfigurationAndRejectsOversizedLinesWithoutPoisoning) {
  TemporaryDirectory directory;
  auto sink = RotatingJsonLogSink::open(default_config(""));
  ASSERT_FALSE(sink.has_value());
  EXPECT_EQ(sink.error().code(), StatusCode::kInvalidArgument);
  sink = RotatingJsonLogSink::open({.path = (directory.path() / "zero").string(),
                                    .maximum_file_bytes = 0U,
                                    .retained_file_count = 5U});
  ASSERT_FALSE(sink.has_value());
  EXPECT_EQ(sink.error().code(), StatusCode::kInvalidArgument);
  sink = RotatingJsonLogSink::open({.path = (directory.path() / "many").string(),
                                    .maximum_file_bytes = std::uint64_t{64U} * 1024U * 1024U,
                                    .retained_file_count = kMaximumRetainedLogFiles + 1U});
  ASSERT_FALSE(sink.has_value());
  EXPECT_EQ(sink.error().code(), StatusCode::kInvalidArgument);

  const std::filesystem::path path = directory.path() / "bounded.jsonl";
  sink = RotatingJsonLogSink::open(
      {.path = path.string(), .maximum_file_bytes = 128U, .retained_file_count = 1U});
  ASSERT_TRUE(sink.has_value()) << sink.error().to_string();
  EXPECT_EQ((*sink)->write(record(std::string(256U, 'x'))).code(), StatusCode::kResourceExhausted);
  EXPECT_TRUE((*sink)->write(record("ok")).is_ok());
}

TEST(RotatingJsonLogSinkTest, RefusesToFollowAnActiveFileSymlink) {
  TemporaryDirectory directory;
  const std::filesystem::path target = directory.path() / "target";
  {
    std::ofstream output{target};
    output << "protected";
  }
  const std::filesystem::path link = directory.path() / "chronos.jsonl";
  std::error_code error;
  std::filesystem::create_symlink(target, link, error);
  ASSERT_FALSE(error) << error.message();
  const auto sink = RotatingJsonLogSink::open(default_config(link.string()));
  ASSERT_FALSE(sink.has_value());
  EXPECT_EQ(sink.error().code(), StatusCode::kIoError);
  EXPECT_EQ(read_file(target), "protected");
}

} // namespace
} // namespace chronos::common
