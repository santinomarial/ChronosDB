#include "chronos/manifest/codec.hpp"
#include "manifest/manifest_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::manifest {
namespace {

TEST(ManifestCodecPropertyTest, DeterministicGeneratedModelsRoundTripExactly) {
  for (std::uint16_t seed = 1U; seed <= 96U; ++seed) {
    const test::ManifestFixture fixture{static_cast<std::uint8_t>(seed)};
    const std::uint64_t generation = static_cast<std::uint64_t>(seed) + 1U;
    const EncodedManifest encoded = test::encode_fixture(fixture, generation);
    const ManifestDecodeResult decoded = decode_manifest_v1_exact(encoded.bytes());
    ASSERT_TRUE(decoded.has_value()) << seed;
    EXPECT_EQ(decoded->generation(), generation);
    EXPECT_EQ(decoded->database_id(), fixture.database_id);
    EXPECT_EQ(decoded->wal_id(), fixture.wal_id);
    EXPECT_TRUE(std::ranges::equal(decoded->tablets(), fixture.tablets));
    EXPECT_TRUE(std::ranges::equal(decoded->parts(), fixture.parts));
    EXPECT_TRUE(std::ranges::equal(decoded->retries(), fixture.retries));
  }
}

TEST(ManifestCodecPropertyTest, EverySingleBitMutationFailsIntegrityValidation) {
  const test::ManifestFixture fixture;
  const EncodedManifest encoded = test::encode_fixture(fixture);
  std::vector<std::byte> damaged(encoded.bytes().begin(), encoded.bytes().end());
  for (std::size_t index = 0U; index < damaged.size(); ++index) {
    for (std::uint8_t bit = 0U; bit < 8U; ++bit) {
      damaged[index] ^= static_cast<std::byte>(1U << bit);
      const ManifestDecodeResult decoded = decode_manifest_v1_exact(damaged);
      ASSERT_FALSE(decoded.has_value()) << index << ':' << static_cast<unsigned>(bit);
      EXPECT_EQ(decoded.error().kind(), ManifestDecodeErrorKind::kCorruption)
          << index << ':' << static_cast<unsigned>(bit);
      damaged[index] ^= static_cast<std::byte>(1U << bit);
    }
  }
}

} // namespace
} // namespace chronos::manifest
