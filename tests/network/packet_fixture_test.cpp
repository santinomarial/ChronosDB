#include "chronos/network/protocol.hpp"

#include <cstddef>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::network {
namespace {

[[nodiscard]] std::vector<std::byte> load_hex_fixture(const std::string_view name) {
  std::ifstream input{std::string{CHRONOS_NETWORK_FIXTURE_DIR} + "/" + std::string{name}};
  std::string hex;
  input >> hex;
  std::vector<std::byte> bytes;
  bytes.reserve(hex.size() / 2U);
  const auto digit = [](const char value) -> unsigned int {
    if (value >= '0' && value <= '9')
      return static_cast<unsigned int>(value - '0');
    if (value >= 'a' && value <= 'f')
      return static_cast<unsigned int>(value - 'a') + 10U;
    return 16U;
  };
  if (hex.size() % 2U != 0U)
    return {};
  for (std::size_t offset = 0U; offset < hex.size(); offset += 2U) {
    const unsigned int high = digit(hex[offset]);
    const unsigned int low = digit(hex[offset + 1U]);
    if (high > 15U || low > 15U)
      return {};
    bytes.push_back(static_cast<std::byte>((high << 4U) | low));
  }
  return bytes;
}

TEST(NetworkPacketFixtureTest, AcceptedAndRejectedPacketsRemainStable) {
  const std::vector<std::byte> accepted = load_hex_fixture("query-result-end-stream.hex");
  const auto decoded = decode_frame(accepted);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->header.message_type, MessageType::kQueryResult);
  EXPECT_EQ(decoded->header.request_id, 0x0102030405060708ULL);
  EXPECT_FALSE(decode_frame(load_hex_fixture("query-result-truncated-header.hex")).has_value());
  EXPECT_FALSE(decode_frame(load_hex_fixture("query-result-bad-header-crc.hex")).has_value());
}

} // namespace
} // namespace chronos::network
