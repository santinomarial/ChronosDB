#include "wal/wal_recovery_test_support.hpp"

#include <array>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <iterator>
#include <spawn.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

#ifndef CHRONOS_WALDUMP_PATH
#error "CHRONOS_WALDUMP_PATH must name the built chronos-waldump executable"
#endif

extern char** environ;

namespace chronos::wal {
namespace {

[[nodiscard]] std::string read_text(const std::filesystem::path& path) {
  std::ifstream input{path};
  return std::string{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

struct WaldumpInvocation {
  std::filesystem::path directory;
  std::filesystem::path output;
  std::filesystem::path error;
};

[[nodiscard]] int run_waldump(const WaldumpInvocation& invocation) {
  posix_spawn_file_actions_t actions;
  if (::posix_spawn_file_actions_init(&actions) != 0) {
    return -1;
  }
  const auto destroy_actions = [&actions]() { ::posix_spawn_file_actions_destroy(&actions); };
  if (::posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO,
                                         invocation.output.c_str(), O_WRONLY | O_CREAT | O_TRUNC,
                                         0600) != 0 ||
      ::posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, invocation.error.c_str(),
                                         O_WRONLY | O_CREAT | O_TRUNC, 0600) != 0) {
    destroy_actions();
    return -1;
  }

  std::string executable{CHRONOS_WALDUMP_PATH};
  std::string directory = invocation.directory.string();
  std::array<char*, 3> arguments{executable.data(), directory.data(), nullptr};
  pid_t child = -1;
  const int spawn_status =
      ::posix_spawn(&child, executable.c_str(), &actions, nullptr, arguments.data(), environ);
  destroy_actions();
  if (spawn_status != 0) {
    return -1;
  }

  int child_status = 0;
  pid_t wait_result = -1;
  do {
    wait_result = ::waitpid(child, &child_status, 0);
  } while (wait_result == -1 && errno == EINTR);
  return wait_result == child && WIFEXITED(child_status) ? WEXITSTATUS(child_status) : -1;
}

TEST(WaldumpTest, PrintsVerifiedMetadataWithoutPayloadBytesOrMutation) {
  test::TemporaryDirectory temporary{"chronos-waldump-clean"};
  test::TemporaryDirectory output_directory{"chronos-waldump-output"};
  ASSERT_TRUE(temporary.valid());
  ASSERT_TRUE(output_directory.valid());
  test::create_wal(temporary.path(), 1U);
  const std::filesystem::path output = output_directory.path() / "dump.out";
  const std::filesystem::path error = output_directory.path() / "dump.err";

  EXPECT_EQ(run_waldump({.directory = temporary.path(), .output = output, .error = error}), 0);
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

  EXPECT_EQ(run_waldump({.directory = temporary.path(), .output = output, .error = error}), 3);
  const std::string text = read_text(output);
  EXPECT_NE(text.find("classification=INCOMPLETE_FINAL_TAIL"), std::string::npos);
  EXPECT_EQ(text.find("record sequence="), std::string::npos);
  EXPECT_EQ(std::filesystem::file_size(segment), size_before);
}

} // namespace
} // namespace chronos::wal
