#include "chronos/common/status.hpp"
#include "chronos/manifest/naming.hpp"
#include "manifest/manifest_test_support.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <string>
#include <string_view>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Uuid nonce() {
  common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(index);
  }
  return common::Uuid{bytes};
}

TEST(ManifestNamingTest, FormatsAndParsesInstalledPartExactly) {
  const cseg::PartId part_id = test::make_id<cseg::PartId>(0xabU);
  const std::string name = part_file_name(part_id);
  EXPECT_EQ(name, "part-000000000000000000000000000000ab.cseg");
  ASSERT_TRUE(parse_part_file_name(name).has_value());
  EXPECT_EQ(*parse_part_file_name(name), part_id);
}

TEST(ManifestNamingTest, FormatsAndParsesTemporaryPartExactly) {
  const cseg::PartId part_id = test::make_id<cseg::PartId>(0xabU);
  const std::string name = temporary_part_file_name(part_id, nonce());
  EXPECT_EQ(name,
            ".part-000000000000000000000000000000ab.cseg.tmp-000102030405060708090a0b0c0d0e0f");
  const common::Result<TemporaryPartName> parsed = parse_temporary_part_file_name(name);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->part_id, part_id);
  EXPECT_EQ(parsed->nonce, nonce());
}

TEST(ManifestNamingTest, FormatsAndParsesGenerationBoundariesExactly) {
  EXPECT_FALSE(manifest_file_name(0U).has_value());
  EXPECT_EQ(*manifest_file_name(1U), "manifest-00000000000000000001.cman");
  const std::string maximum = *manifest_file_name(std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(maximum, "manifest-18446744073709551615.cman");
  EXPECT_EQ(*parse_manifest_file_name(maximum), std::numeric_limits<std::uint64_t>::max());

  const std::string temporary = *temporary_manifest_file_name(42U, nonce());
  EXPECT_EQ(temporary, ".manifest-00000000000000000042.cman.tmp-000102030405060708090a0b0c0d0e0f");
  const common::Result<TemporaryManifestName> parsed =
      parse_temporary_manifest_file_name(temporary);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->generation, 42U);
  EXPECT_EQ(parsed->nonce, nonce());
}

TEST(ManifestNamingTest, RejectsNoncanonicalAndHostileNames) {
  constexpr std::array<std::string_view, 12> kInvalidParts{
      "part-00000000000000000000000000000000.cseg",
      "part-0000000000000000000000000000000a.CSEG",
      "part-0000000000000000000000000000000A.cseg",
      "part-0000000000000000000000000000000.cseg",
      "part-0000000000000000000000000000000aa.cseg",
      "part-0000000000000000000000000000000g.cseg",
      "/part-0000000000000000000000000000000a.cseg",
      "../part-0000000000000000000000000000000a.cseg",
      "part-0000000000000000000000000000000a.cseg/",
      ".part-0000000000000000000000000000000a.cseg",
      "part-0000000000000000000000000000000a.cseg.tmp-00000000000000000000000000000000",
      "part-0000000000000000000000000000000a.csegx",
  };
  for (const std::string_view name : kInvalidParts) {
    const common::Result<cseg::PartId> parsed = parse_part_file_name(name);
    ASSERT_FALSE(parsed.has_value()) << name;
    EXPECT_EQ(parsed.error().code(), common::StatusCode::kInvalidArgument) << name;
  }

  constexpr std::array<std::string_view, 8> kInvalidManifests{
      "manifest-00000000000000000000.cman",  "manifest-18446744073709551616.cman",
      "manifest-0000000000000000001.cman",   "manifest-000000000000000000001.cman",
      "manifest-0000000000000000000a.cman",  "manifest-00000000000000000001.CMAN",
      ".manifest-00000000000000000001.cman", "../manifest-00000000000000000001.cman",
  };
  for (const std::string_view name : kInvalidManifests) {
    const common::Result<std::uint64_t> parsed = parse_manifest_file_name(name);
    ASSERT_FALSE(parsed.has_value()) << name;
    EXPECT_EQ(parsed.error().code(), common::StatusCode::kInvalidArgument) << name;
  }
}

TEST(ManifestNamingTest, RejectsMalformedTemporaryNamesAndAcceptsZeroNonce) {
  const cseg::PartId part_id = test::make_id<cseg::PartId>(1U);
  const common::Uuid zero_nonce{};
  EXPECT_TRUE(
      parse_temporary_part_file_name(temporary_part_file_name(part_id, zero_nonce)).has_value());
  EXPECT_TRUE(parse_temporary_manifest_file_name(*temporary_manifest_file_name(1U, zero_nonce))
                  .has_value());

  std::string uppercase = temporary_part_file_name(part_id, nonce());
  uppercase.back() = 'F';
  EXPECT_FALSE(parse_temporary_part_file_name(uppercase).has_value());
  EXPECT_FALSE(parse_temporary_part_file_name(".part-00.cseg.tmp-00").has_value());
  EXPECT_FALSE(parse_temporary_manifest_file_name(
                   ".manifest-00000000000000000001.cman.tmp-000102030405060708090a0b0c0d0e")
                   .has_value());
}

TEST(ManifestNamingPropertyTest, CanonicalGenerationNamesRoundTripDeterministically) {
  std::uint64_t state = 0x7a9d'31b4'c2e8'05f1ULL;
  for (std::size_t iteration = 0U; iteration < 10'000U; ++iteration) {
    state ^= state << 13U;
    state ^= state >> 7U;
    state ^= state << 17U;
    const std::uint64_t generation = state == 0U ? 1U : state;
    const common::Result<std::string> encoded = manifest_file_name(generation);
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(*parse_manifest_file_name(*encoded), generation);
  }
}

} // namespace
} // namespace chronos::manifest
