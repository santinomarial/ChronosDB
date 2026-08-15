#include "chronos/common/log.hpp"

#include <array>
#include <chrono>
#include <cstdio>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

namespace chronos::common {
namespace {

[[nodiscard]] LogRecord fixed_record(const std::span<const LogField> fields = {}) {
  using namespace std::chrono;
  return {.timestamp = sys_days{year{2026} / August / 15} + hours{17} + minutes{42} + seconds{3} +
                       milliseconds{19},
          .severity = LogSeverity::kWarning,
          .component = "chronosd",
          .event = "test_event",
          .message = "quoted \"message\"\nline",
          .fields = fields};
}

TEST(StructuredLogTest, EncodesStableRfc3339JsonWithEscapingAndFields) {
  const std::array fields{LogField{"request_id", "7"}, LogField{"detail", "path\\value"}};
  const auto encoded = encode_json_log(fixed_record(fields));
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(
      *encoded,
      R"({"timestamp":"2026-08-15T17:42:03.019Z","severity":"WARNING","component":"chronosd","event":"test_event","message":"quoted \"message\"\nline","request_id":"7","detail":"path\\value"})");
}

TEST(StructuredLogTest, PreservesValidUtf8AndReplacesInvalidBytes) {
  const std::string invalid{"ok\xc3\xa9\xff", 5U};
  LogRecord record = fixed_record();
  record.message = invalid;
  const auto encoded = encode_json_log(record);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_NE(encoded->find("oké\\ufffd"), std::string::npos);
}

TEST(StructuredLogTest, RejectsAmbiguousAndOversizedFields) {
  const std::array reserved{LogField{"severity", "shadow"}};
  auto encoded = encode_json_log(fixed_record(reserved));
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), StatusCode::kInvalidArgument);

  const std::array duplicate{LogField{"tablet", "one"}, LogField{"tablet", "two"}};
  encoded = encode_json_log(fixed_record(duplicate));
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), StatusCode::kInvalidArgument);

  const std::array invalid_name{LogField{"bad name", "value"}};
  encoded = encode_json_log(fixed_record(invalid_name));
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), StatusCode::kInvalidArgument);

  std::string oversized(kMaximumLogFieldValueBytes + 1U, 'x');
  const std::array too_large{LogField{"value", oversized}};
  encoded = encode_json_log(fixed_record(too_large));
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), StatusCode::kInvalidArgument);
}

TEST(StructuredLogTest, WritesAndFlushesExactlyOneLine) {
  std::FILE* const output = std::tmpfile();
  ASSERT_NE(output, nullptr);
  const Status written = write_json_log(output, fixed_record());
  ASSERT_TRUE(written.is_ok()) << written.to_string();
  ASSERT_EQ(std::fseek(output, 0L, SEEK_SET), 0);
  std::array<char, 1024U> bytes{};
  const std::size_t count = std::fread(bytes.data(), 1U, bytes.size(), output);
  ASSERT_GT(count, 0U);
  const std::string line{bytes.data(), count};
  EXPECT_EQ(line.back(), '\n');
  EXPECT_EQ(line.find('\n'), line.size() - 1U);
  EXPECT_EQ(std::fclose(output), 0);
}

TEST(StructuredLogTest, SerializesConcurrentLinesWithoutInterleaving) {
  std::FILE* const output = std::tmpfile();
  ASSERT_NE(output, nullptr);
  constexpr std::size_t kThreadCount = 4U;
  constexpr std::size_t kLinesPerThread = 64U;
  std::vector<std::thread> writers;
  writers.reserve(kThreadCount);
  for (std::size_t thread = 0U; thread < kThreadCount; ++thread) {
    writers.emplace_back([output, thread] {
      const std::string writer = std::to_string(thread);
      const std::array fields{LogField{"writer", writer}};
      for (std::size_t line = 0U; line < kLinesPerThread; ++line) {
        const Status written = write_json_log(output, fixed_record(fields));
        ASSERT_TRUE(written.is_ok()) << written.to_string();
      }
    });
  }
  for (std::thread& writer : writers)
    writer.join();
  ASSERT_EQ(std::fseek(output, 0L, SEEK_SET), 0);
  std::array<char, 1024U> line{};
  std::size_t lines = 0U;
  while (std::fgets(line.data(), static_cast<int>(line.size()), output) != nullptr) {
    const std::string value{line.data()};
    EXPECT_TRUE(value.starts_with("{\"timestamp\":"));
    EXPECT_TRUE(value.ends_with("}\n"));
    ++lines;
  }
  EXPECT_EQ(lines, kThreadCount * kLinesPerThread);
  EXPECT_EQ(std::fclose(output), 0);
}

TEST(StructuredLogTest, RejectsNullOutputAndOutOfRangeTimestamp) {
  EXPECT_EQ(write_json_log(nullptr, fixed_record()).code(), StatusCode::kInvalidArgument);
  LogRecord record = fixed_record();
  record.timestamp = std::chrono::sys_days{std::chrono::year{10'000} / std::chrono::January / 1};
  const auto encoded = encode_json_log(record);
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), StatusCode::kInvalidArgument);
}

TEST(StructuredLogTest, NamesEverySeverity) {
  EXPECT_EQ(log_severity_name(LogSeverity::kDebug), "DEBUG");
  EXPECT_EQ(log_severity_name(LogSeverity::kInfo), "INFO");
  EXPECT_EQ(log_severity_name(LogSeverity::kWarning), "WARNING");
  EXPECT_EQ(log_severity_name(LogSeverity::kError), "ERROR");
  EXPECT_EQ(log_severity_name(LogSeverity::kCritical), "CRITICAL");
}

} // namespace
} // namespace chronos::common
