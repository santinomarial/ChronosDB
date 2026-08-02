#include "wal/wal_recovery_test_support.hpp"

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <string>
#include <sys/wait.h>

#ifndef CHRONOS_WALDUMP_PATH
#error "CHRONOS_WALDUMP_PATH must name the built chronos-waldump executable"
#endif

namespace chronos::wal {
namespace {

[[nodiscard]] std::string read_text(const std::filesystem::path& path) {
  std::ifstream input{path};
  return std::string{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

[[nodiscard]] int run_waldump(const std::filesystem::path& directory,
                              const std::filesystem::path& output,
                              const std::filesystem::path& error) {
  const std::string command = std::string{"\""} + CHRONOS_WALDUMP_PATH + "\" \"" +
                              directory.string() + "\" > \"" + output.string() + "\" 2> \"" +
                              error.string() + "\"";
  const int result = std::system(command.c_str());
  return WIFEXITED(result) ? WEXITSTATUS(result) : -1;
}

TEST(WaldumpTest, PrintsVerifiedMetadataWithoutPayloadBytesOrMutation) {
  test::TemporaryDirectory temporary{"chronos-waldump-clean"};
  test::TemporaryDirectory output_directory{"chronos-waldump-output"};
  ASSERT_TRUE(temporary.valid());
  ASSERT_TRUE(output_directory.valid());
  test::create_wal(temporary.path(), 1U);
  const std::filesystem::path output = output_directory.path() / "dump.out";
  const std::filesystem::path error = output_directory.path() / "dump.err";

  EXPECT_EQ(run_waldump(temporary.path(), output, error), 0);
  const std::string text = read_text(output);
  EXPECT_NE(text.find("record sequence=1 segment=1 offset=64"), std::string::npos);
  EXPECT_NE(text.find("classification=CLEAN"), std::string::npos);
  EXPECT_EQ(text.find("application_body"), std::string::npos);
  EXPECT_TRUE(read_text(error).empty());
}

TEST(WaldumpTest, ReportsIncompleteTailWithoutRepairingOrReplayingPrefix) {
  test::TemporaryDirectory temporary{"chronos-waldump-incomplete"};
  test::TemporaryDirectory output_directory{"chronos-waldump-output"};
  ASSERT_TRUE(temporary.valid());
  ASSERT_TRUE(output_directory.valid());
  test::create_wal(temporary.path(), 1U);
  const std::filesystem::path segment = temporary.path() / "wal-00000000000000000001.cwal";
  const std::array<std::byte, 3> tail{std::byte{0x01}};
  test::append_bytes(segment, tail);
  const auto size_before = std::filesystem::file_size(segment);
  const std::filesystem::path output = output_directory.path() / "dump.out";
  const std::filesystem::path error = output_directory.path() / "dump.err";

  EXPECT_EQ(run_waldump(temporary.path(), output, error), 3);
  const std::string text = read_text(output);
  EXPECT_NE(text.find("classification=INCOMPLETE_FINAL_TAIL"), std::string::npos);
  EXPECT_EQ(text.find("record sequence="), std::string::npos);
  EXPECT_EQ(std::filesystem::file_size(segment), size_before);
}

} // namespace
} // namespace chronos::wal
