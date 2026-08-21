#include "chronos/raft/transport_codec.hpp"

#include <cstddef>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

struct GoldenCase {
  std::string_view filename;
  RaftTransportEnvelope envelope;
};

[[nodiscard]] GroupId golden_group_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{9U};
  return GroupId{bytes};
}

[[nodiscard]] SnapshotMetadata golden_snapshot() {
  SnapshotMetadata value;
  value.last_included_index = 8U;
  value.last_included_term = 3U;
  value.manifest_generation = 11U;
  value.part_set_checksum.fill(std::byte{0x5aU});
  value.configuration_index = 7U;
  value.voters = {1U, 2U, 3U};
  return value;
}

[[nodiscard]] RaftTransportEnvelope golden_envelope(Message message) {
  return {.group_id = golden_group_id(),
          .source = 1U,
          .destination = 2U,
          .message = std::move(message)};
}

[[nodiscard]] std::vector<std::byte> load_hex_fixture(const std::string_view filename) {
  std::ifstream input{std::string{CHRONOS_RAFT_FIXTURE_DIR} + "/" + std::string{filename}};
  std::string hex;
  if (!(input >> hex))
    return {};
  std::string trailing;
  if (input >> trailing)
    return {};
  if (hex.empty() || hex.size() % 2U != 0U)
    return {};

  const auto digit = [](const char value) -> unsigned int {
    if (value >= '0' && value <= '9')
      return static_cast<unsigned int>(value - '0');
    if (value >= 'a' && value <= 'f')
      return static_cast<unsigned int>(value - 'a') + 10U;
    return 16U;
  };
  std::vector<std::byte> bytes;
  bytes.reserve(hex.size() / 2U);
  for (std::size_t offset = 0U; offset < hex.size(); offset += 2U) {
    const unsigned int high = digit(hex[offset]);
    const unsigned int low = digit(hex[offset + 1U]);
    if (high > 15U || low > 15U)
      return {};
    bytes.push_back(static_cast<std::byte>((high << 4U) | low));
  }
  return bytes;
}

[[nodiscard]] std::vector<GoldenCase> golden_cases() {
  std::vector<GoldenCase> cases;
  cases.push_back(
      {"request-vote-request.hex", golden_envelope(RequestVoteRequest{4U, 1U, 12U, 3U})});
  cases.push_back({"request-vote-response.hex", golden_envelope(RequestVoteResponse{4U, true})});
  cases.push_back({"append-entries-request.hex",
                   golden_envelope(AppendEntriesRequest{
                       4U,
                       1U,
                       12U,
                       3U,
                       {{13U, 4U, 1U, {std::byte{0x11U}, std::byte{0x22U}}}, {14U, 4U, 253U, {}}},
                       11U})});
  cases.push_back({"append-entries-response.hex",
                   golden_envelope(AppendEntriesResponse{4U, false, 12U, 3U, 7U})});
  cases.push_back({"install-snapshot-request.hex",
                   golden_envelope(InstallSnapshotRequest{4U, 1U, golden_snapshot()})});
  cases.push_back(
      {"install-snapshot-response.hex", golden_envelope(InstallSnapshotResponse{4U, true, 8U})});
  cases.push_back({"read-barrier-request.hex", golden_envelope(ReadBarrierRequest{4U, 1U, 19U})});
  cases.push_back(
      {"read-barrier-response.hex", golden_envelope(ReadBarrierResponse{4U, 19U, true})});
  return cases;
}

TEST(RaftTransportCodecGoldenTest, EveryCurrentMessageHasCompilerIndependentCanonicalBytes) {
  for (const GoldenCase& test_case : golden_cases()) {
    SCOPED_TRACE(test_case.filename);
    const std::vector<std::byte> fixture = load_hex_fixture(test_case.filename);
    ASSERT_GE(fixture.size(), kRaftTransportHeaderSize + kRaftTransportTrailerSize);

    auto expected_length =
        raft_transport_frame_length_v1(common::ByteView{fixture}.first(kRaftTransportHeaderSize));
    ASSERT_TRUE(expected_length.has_value()) << expected_length.error().to_string();
    EXPECT_EQ(*expected_length, fixture.size());

    auto encoded = encode_raft_transport_envelope_v1(test_case.envelope);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
    EXPECT_EQ(*encoded, fixture);

    auto decoded = decode_raft_transport_envelope_v1(fixture);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    EXPECT_EQ(*decoded, test_case.envelope);
  }
}

} // namespace
} // namespace chronos::raft
