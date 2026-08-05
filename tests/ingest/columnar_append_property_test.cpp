#include "chronos/ingest/columnar_append.hpp"
#include "ingest/ingest_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <random>

namespace chronos::ingest {
namespace {

TEST(ColumnarAppendPropertyTest, DeterministicNominalIdentitiesRoundTripForFixedSeed) {
  constexpr std::uint32_t kSeed = 0x43415031U;
  std::mt19937 generator{kSeed};
  const columnar::EncodedColumnarBatch batch = test::encoded_batch();
  for (std::uint32_t trial = 0U; trial < 200U; ++trial) {
    const auto first_byte = static_cast<std::uint8_t>(1U + generator() % 220U);
    const auto batch_byte = static_cast<std::uint8_t>(1U + generator() % 220U);
    const auto tablet_value = static_cast<std::uint16_t>(1U + generator() % 65000U);
    const ColumnarAppendEncodeInput input{
        .client_id = test::request_id<ClientId>(first_byte),
        .client_batch_id = test::request_id<ClientBatchId>(batch_byte),
        .tablet_id = columnar::test::id<schema::TabletId>(tablet_value)};
    const auto first = encode_columnar_append_v1(input, batch);
    const auto second = encode_columnar_append_v1(input, batch);
    ASSERT_TRUE(first.has_value()) << "seed=" << kSeed << " trial=" << trial;
    ASSERT_TRUE(second.has_value());
    EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes()));
    const auto decoded = decode_columnar_append_v1_exact(first->bytes());
    ASSERT_TRUE(decoded.has_value())
        << "seed=" << kSeed << " trial=" << trial << ' '
        << decoded.error().status().to_string();
    EXPECT_EQ(decoded->client_id(), input.client_id);
    EXPECT_EQ(decoded->client_batch_id(), input.client_batch_id);
    EXPECT_EQ(decoded->tablet_id(), input.tablet_id);
  }
}

TEST(ColumnarAppendPropertyTest, ClientIdentityIsExcludedButMutationIdentityIsDigestProtected) {
  const columnar::EncodedColumnarBatch batch = test::encoded_batch();
  const auto first = encode_columnar_append_v1(
      {.client_id = test::request_id<ClientId>(1U),
       .client_batch_id = test::request_id<ClientBatchId>(20U),
       .tablet_id = columnar::test::id<schema::TabletId>(52U)},
      batch);
  const auto second = encode_columnar_append_v1(
      {.client_id = test::request_id<ClientId>(2U),
       .client_batch_id = test::request_id<ClientBatchId>(30U),
       .tablet_id = columnar::test::id<schema::TabletId>(52U)},
      batch);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  constexpr std::size_t kDigest = wal::kApplicationEnvelopeSize +
                                  columnar_append_v1::kRequestDigestOffset;
  EXPECT_TRUE(std::ranges::equal(first->bytes().subspan(kDigest, Sha256Digest::kSize),
                                 second->bytes().subspan(kDigest, Sha256Digest::kSize)));

  for (std::size_t offset = 0U; offset < Sha256Digest::kSize; ++offset) {
    std::vector<std::byte> bytes(first->bytes().begin(), first->bytes().end());
    bytes[kDigest + offset] ^= std::byte{1U};
    const auto decoded = decode_columnar_append_v1_exact(bytes);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().kind(), ColumnarAppendDecodeErrorKind::kCorruption);
  }
}

} // namespace
} // namespace chronos::ingest
