#include "chronos/common/crc32c.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

namespace chronos::common {
namespace {

constexpr std::array<std::byte, 9> kCheckInput{
    std::byte{0x31}, std::byte{0x32}, std::byte{0x33}, std::byte{0x34}, std::byte{0x35},
    std::byte{0x36}, std::byte{0x37}, std::byte{0x38}, std::byte{0x39}};

TEST(Crc32cTest, HandlesEmptyAndStandardCheckInputs) {
  EXPECT_EQ(crc32c(ByteView{}), 0U);
  EXPECT_EQ(crc32c(kCheckInput), 0xe3069283U);

  const std::array<std::byte, 1> one_byte{std::byte{0x61}};
  EXPECT_EQ(crc32c(one_byte), 0xc1d04330U);
}

TEST(Crc32cTest, ProcessesBinaryZerosAndArbitraryBytesDeterministically) {
  const std::array<std::byte, 32> zeros{};
  const std::array<std::byte, 8> binary{
      std::byte{0x00}, std::byte{0xff}, std::byte{0x80}, std::byte{0x01},
      std::byte{0x7f}, std::byte{0x55}, std::byte{0xaa}, std::byte{0x00}};
  EXPECT_EQ(crc32c(zeros), crc32c(zeros));
  EXPECT_NE(crc32c(zeros), 0U);
  EXPECT_EQ(crc32c(binary), crc32c(binary));
  EXPECT_NE(crc32c(binary), crc32c(zeros));
}

TEST(Crc32cTest, EverySplitPointMatchesOneShot) {
  const std::uint32_t expected = crc32c(kCheckInput);
  for (std::size_t split = 0; split <= kCheckInput.size(); ++split) {
    SCOPED_TRACE(::testing::Message() << "split=" << split);
    Crc32c checksum;
    checksum.extend(ByteView{kCheckInput}.first(split));
    checksum.extend(ByteView{kCheckInput}.subspan(split));
    EXPECT_EQ(checksum.value(), expected);

    const std::uint32_t prefix = crc32c(ByteView{kCheckInput}.first(split));
    EXPECT_EQ(extend_crc32c(prefix, ByteView{kCheckInput}.subspan(split)), expected);
  }
}

TEST(Crc32cTest, RandomChunkBoundariesMatchOneShot) {
  constexpr std::uint64_t kSeed = 0x7a91dce542683fb0ULL;
  std::mt19937_64 random{kSeed};
  std::vector<std::byte> bytes(4096);
  for (std::byte& byte : bytes) {
    byte = static_cast<std::byte>(random() & 0xffU);
  }
  const std::uint32_t expected = crc32c(bytes);

  for (std::size_t trial = 0; trial < 200; ++trial) {
    SCOPED_TRACE(::testing::Message() << "seed=" << kSeed << " trial=" << trial);
    Crc32c checksum;
    std::size_t offset = 0;
    while (offset < bytes.size()) {
      const std::size_t requested = 1U + static_cast<std::size_t>(random() % 97U);
      const std::size_t chunk = std::min(requested, bytes.size() - offset);
      checksum.extend(ByteView{bytes}.subspan(offset, chunk));
      offset += chunk;
    }
    EXPECT_EQ(checksum.value(), expected);
  }
}

TEST(Crc32cTest, InstancesAndResetRemainIndependent) {
  Crc32c first;
  Crc32c second;
  first.extend(kCheckInput);
  EXPECT_EQ(first.value(), 0xe3069283U);
  EXPECT_EQ(second.value(), 0U);

  second.extend(kCheckInput);
  EXPECT_EQ(second.value(), first.value());
  first.reset();
  EXPECT_EQ(first.value(), 0U);
  EXPECT_EQ(second.value(), 0xe3069283U);
}

} // namespace
} // namespace chronos::common
