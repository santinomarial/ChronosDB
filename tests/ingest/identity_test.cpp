#include "chronos/ingest/identity.hpp"

#include <gtest/gtest.h>
#include <type_traits>

namespace chronos::ingest {
namespace {

TEST(IngestIdentityTest, ClientAndBatchIdentitiesAreDistinctNonzeroNominalTypes) {
  common::Uuid::Bytes zero{};
  EXPECT_EQ(ClientId::from_bytes(zero).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(ClientBatchId::from_bytes(zero).error().code(), common::StatusCode::kInvalidArgument);
  zero.back() = std::byte{1U};
  const ClientId client = ClientId::from_bytes(zero).value();
  const ClientBatchId batch = ClientBatchId::from_bytes(zero).value();
  EXPECT_EQ(client.bytes(), zero);
  EXPECT_EQ(batch.bytes(), zero);
  static_assert(!std::is_same_v<ClientId, ClientBatchId>);
}

} // namespace
} // namespace chronos::ingest
