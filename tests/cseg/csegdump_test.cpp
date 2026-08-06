#include "chronos/common/crc32c.hpp"
#include "chronos/cseg/format.hpp"
#include "cseg_test_fixture.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <span>
#include <spawn.h>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <system_error>
#include <unistd.h>
#include <vector>

#ifndef CHRONOS_CSEGDUMP_PATH
#error "CHRONOS_CSEGDUMP_PATH must name the built chronos-csegdump executable"
#endif

#if defined(__APPLE__)
extern char** environ;
#endif

namespace chronos::cseg {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::array<char, 64> path{};
    constexpr std::string_view kPattern{"/tmp/chronos-csegdump-XXXXXX"};
    std::ranges::copy(kPattern, path.begin());
    if (char* created = ::mkdtemp(path.data()); created != nullptr) {
      path_ = created;
    }
  }

  ~TemporaryDirectory() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return !path_.empty();
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  ASSERT_TRUE(output.is_open());
  // Character streams are permitted to access an object's byte representation.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(output.good());
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = std::byte{static_cast<std::uint8_t>(value)};
  bytes[offset + 1U] = std::byte{static_cast<std::uint8_t>(value >> 8U)};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

[[nodiscard]] std::vector<std::byte> unsupported_version_part() {
  const EncodedCsegPart encoded = test::make_valid_part();
  const CsegPartDecodeResult original = decode_cseg_v1_part_exact(encoded.bytes());
  if (!original.has_value()) {
    return {};
  }
  const std::size_t metadata_size = original->metadata().encoded_metadata().size();
  std::vector<std::byte> bytes(encoded.bytes().begin(), encoded.bytes().end());
  store_u16(bytes, format::kFormatMinorOffset, 1U);
  store_u32(bytes, format::kHeaderCrc32cOffset,
            common::crc32c(common::ByteView{bytes}.first(format::kHeaderCrc32cOffset)));
  const std::size_t metadata_crc_offset = metadata_size - format::kMetadataCrc32cLength;
  store_u32(bytes, metadata_crc_offset,
            common::crc32c(common::ByteView{bytes}.first(metadata_crc_offset)));
  return bytes;
}

[[nodiscard]] std::string read_text(const std::filesystem::path& path) {
  std::ifstream input{path};
  return std::string{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

struct Invocation {
  std::span<const std::string_view> arguments;
  std::filesystem::path output;
  std::filesystem::path error;
};

[[nodiscard]] int run_csegdump(const Invocation& invocation) {
  posix_spawn_file_actions_t actions{};
  if (::posix_spawn_file_actions_init(&actions) != 0) {
    return -1;
  }
  const auto destroy_actions = [&actions]() { ::posix_spawn_file_actions_destroy(&actions); };
  if (::posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, invocation.output.c_str(),
                                         O_WRONLY | O_CREAT | O_TRUNC, 0600) != 0 ||
      ::posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, invocation.error.c_str(),
                                         O_WRONLY | O_CREAT | O_TRUNC, 0600) != 0) {
    destroy_actions();
    return -1;
  }

  std::vector<std::string> storage;
  storage.reserve(invocation.arguments.size() + 1U);
  storage.emplace_back(CHRONOS_CSEGDUMP_PATH);
  for (const std::string_view argument : invocation.arguments) {
    storage.emplace_back(argument);
  }
  std::vector<char*> arguments;
  arguments.reserve(storage.size() + 1U);
  for (std::string& argument : storage) {
    arguments.push_back(argument.data());
  }
  arguments.push_back(nullptr);
  pid_t child = -1;
  const int spawn_status =
      ::posix_spawn(&child, storage.front().c_str(), &actions, nullptr, arguments.data(), environ);
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

TEST(CsegdumpTest, PrintsValidatedSummaryAndOptionalDescriptors) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  const EncodedCsegPart encoded = test::make_valid_part();
  const std::filesystem::path input = temporary.path() / "part.cseg";
  const std::filesystem::path output = temporary.path() / "dump.out";
  const std::filesystem::path error = temporary.path() / "dump.err";
  write_bytes(input, encoded.bytes());
  const std::string input_string = input.string();
  const std::array<std::string_view, 2> arguments{"--descriptors", input_string};

  EXPECT_EQ(run_csegdump({.arguments = arguments, .output = output, .error = error}), 0);
  const std::string text = read_text(output);
  EXPECT_NE(text.find("classification=VALID validation=STRUCTURAL_AND_SCHEMA_INDEPENDENT_SEMANTIC"),
            std::string::npos);
  EXPECT_NE(text.find("rows=2 columns=5 granules=1 pages=5"), std::string::npos);
  EXPECT_NE(text.find("column ordinal=0 storage=USER type=TIMESTAMP_NS"), std::string::npos);
  EXPECT_NE(text.find("page ordinal=4 granule=0 column=4 compression=NONE"), std::string::npos);
  EXPECT_EQ(text.find("application_body"), std::string::npos);
  EXPECT_TRUE(read_text(error).empty());
}

TEST(CsegdumpTest, ClassifiesIncompleteInputAndEnforcesInputLimit) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  const EncodedCsegPart encoded = test::make_valid_part();
  const std::filesystem::path input = temporary.path() / "short.cseg";
  const std::filesystem::path output = temporary.path() / "dump.out";
  const std::filesystem::path error = temporary.path() / "dump.err";
  write_bytes(input, encoded.bytes().first(encoded.size() - 1U));
  const std::string input_string = input.string();
  const std::array<std::string_view, 1> incomplete_arguments{input_string};

  EXPECT_EQ(run_csegdump({.arguments = incomplete_arguments, .output = output, .error = error}), 3);
  EXPECT_NE(read_text(error).find("required_size="), std::string::npos);

  const std::array<std::string_view, 3> limited_arguments{"--max-bytes", "16", input_string};
  EXPECT_EQ(run_csegdump({.arguments = limited_arguments, .output = output, .error = error}), 1);
  EXPECT_NE(read_text(error).find("exceeds the configured in-memory inspection limit"),
            std::string::npos);
}

TEST(CsegdumpTest, RejectsInvalidOptionsWithoutReadingAFile) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  const std::filesystem::path output = temporary.path() / "dump.out";
  const std::filesystem::path error = temporary.path() / "dump.err";
  const std::array<std::string_view, 1> arguments{"--max-bytes"};

  EXPECT_EQ(run_csegdump({.arguments = arguments, .output = output, .error = error}), 2);
  EXPECT_NE(read_text(error).find("Usage:"), std::string::npos);
}

TEST(CsegdumpTest, ReturnsDistinctExitForAuthenticatedUnsupportedFormat) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  const std::vector<std::byte> bytes = unsupported_version_part();
  ASSERT_FALSE(bytes.empty());
  const std::filesystem::path input = temporary.path() / "future.cseg";
  const std::filesystem::path output = temporary.path() / "dump.out";
  const std::filesystem::path error = temporary.path() / "dump.err";
  write_bytes(input, bytes);
  const std::string input_string = input.string();
  const std::array<std::string_view, 1> arguments{input_string};

  EXPECT_EQ(run_csegdump({.arguments = arguments, .output = output, .error = error}), 4);
  EXPECT_NE(read_text(error).find("CSEG format version is unsupported"), std::string::npos);
}

} // namespace
} // namespace chronos::cseg
