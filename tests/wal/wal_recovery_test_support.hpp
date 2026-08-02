#ifndef CHRONOS_TESTS_WAL_WAL_RECOVERY_TEST_SUPPORT_HPP_
#define CHRONOS_TESTS_WAL_WAL_RECOVERY_TEST_SUPPORT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/status.hpp"
#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_replay_sink.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

namespace chronos::wal::test {

class CollectingReplaySink final : public WalReplaySink {
public:
  common::Status preflight(const WalReplayRecord& record) override {
    preflight_sequences.push_back(record.header.record_sequence);
    if (!preflight_failure.is_ok()) {
      return preflight_failure;
    }
    return common::Status::ok();
  }

  common::Status replay(const WalReplayRecord& record) override {
    replay_sequences.push_back(record.header.record_sequence);
    payloads.emplace_back(record.payload.begin(), record.payload.end());
    if (!replay_failure.is_ok()) {
      return replay_failure;
    }
    return common::Status::ok();
  }

  common::Status preflight_failure;
  common::Status replay_failure;
  std::vector<std::uint64_t> preflight_sequences;
  std::vector<std::uint64_t> replay_sequences;
  std::vector<std::vector<std::byte>> payloads;
};

struct WalFixtureConfig {
  std::size_t record_count{};
  std::uint64_t target_segment_size{kSegmentSizeLimit};
};

inline void create_wal(const std::filesystem::path& directory, const WalFixtureConfig& fixture) {
  FixedWalIdGenerator generator{make_wal_id()};
  const WalWriterConfig config{.directory_path = directory.string(),
                               .target_segment_size = fixture.target_segment_size,
                               .maximum_application_payload = kApplicationEnvelopeSize};
  common::Result<WalWriter> created = WalWriter::create_new(config, generator);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  for (std::size_t index = 0; index < fixture.record_count; ++index) {
    const std::vector<std::byte> payload = make_application_payload();
    const common::Result<WalAppendResult> appended = created->append_application_entry(payload);
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  }
  ASSERT_TRUE(created->synchronize().has_value());
  ASSERT_TRUE(created->close().is_ok());
}

inline void create_wal(const std::filesystem::path& directory, const std::size_t record_count) {
  create_wal(directory, WalFixtureConfig{.record_count = record_count});
}

[[nodiscard]] inline std::vector<std::byte> read_file(const std::filesystem::path& path) {
  std::ifstream input{path, std::ios::binary};
  const std::vector<char> characters{std::istreambuf_iterator<char>{input},
                                     std::istreambuf_iterator<char>{}};
  std::vector<std::byte> bytes;
  bytes.reserve(characters.size());
  for (const char character : characters) {
    bytes.push_back(static_cast<std::byte>(static_cast<unsigned char>(character)));
  }
  return bytes;
}

inline void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  for (const std::byte byte : bytes) {
    output.put(static_cast<char>(std::to_integer<unsigned char>(byte)));
  }
  ASSERT_TRUE(output.good());
}

inline void append_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary | std::ios::app};
  for (const std::byte byte : bytes) {
    output.put(static_cast<char>(std::to_integer<unsigned char>(byte)));
  }
  ASSERT_TRUE(output.good());
}

[[nodiscard]] inline std::vector<std::byte>
encode_physical_record(const std::uint64_t sequence, const std::uint16_t type,
                       const std::vector<std::byte>& payload = make_application_payload()) {
  const common::Result<RecordHeader> header = make_record_header(
      {.record_type = type, .record_sequence = sequence, .payload_length = payload.size()});
  EXPECT_TRUE(header.has_value());
  if (!header.has_value()) {
    return {};
  }
  std::vector<std::byte> encoded(header->total_length);
  const common::Result<std::size_t> size = encode_record(*header, payload, encoded);
  EXPECT_TRUE(size.has_value());
  return encoded;
}

} // namespace chronos::wal::test

#endif // CHRONOS_TESTS_WAL_WAL_RECOVERY_TEST_SUPPORT_HPP_
