#include "chronos/cluster/distributed_vector_result_exchange.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

TEST(DistributedVectorResultExchangeAllocationFailureTest, ClassifiesOwnedCodecAllocations) {
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const query::DistributedVectorResultSchema result_schema_value{
      .columns = {{"value", type, false}}};
  const std::array<network::QueryResultColumn, 1U> columns{
      network::QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  const std::array<std::byte, 8U> value{std::byte{1U}};
  const std::array<network::QueryResultCell, 1U> cells{network::QueryResultCell{.value = value}};
  const std::vector<std::byte> batch =
      network::encode_query_result_batch(1U, columns, cells).value();
  const DistributedVectorResultExchangeMessage message{
      .query_id = uuid(1U),
      .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
      .sequence = 1U,
      .terminal = true,
      .encoded_result_batch = batch};

  bool encoded_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_result_exchange_message_v2(message, result_schema_value);
    });
    if (result.has_value()) {
      encoded_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(encoded_success);

  const auto encoded =
      encode_distributed_vector_result_exchange_message_v2(message, result_schema_value);
  ASSERT_TRUE(encoded.has_value());
  bool decoded_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return decode_distributed_vector_result_exchange_message_v2_exact(encoded->bytes(),
                                                                        result_schema_value);
    });
    if (result.has_value()) {
      decoded_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(decoded_success);
}

TEST(DistributedVectorResultCoordinatorV2AllocationFailureTest,
     ClassifiesConstructionRetentionAndFinalPublication) {
  const schema::LogicalType type_value =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const query::DistributedVectorResultSchema stable_schema{
      .columns = {{"value", type_value, false}}};
  const schema::TabletId tablet_id = schema::TabletId::from_uuid(uuid(2U)).value();
  const DistributedVectorResultExchangeMessage message{
      .query_id = uuid(1U), .tablet_id = tablet_id, .sequence = 1U, .terminal = true};

  bool construction_succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    query::DistributedVectorResultSchema schema_value = stable_schema;
    std::vector<schema::TabletId> tablets{tablet_id};
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorResultCoordinatorV2::create(uuid(1U), std::move(tablets),
                                                          std::move(schema_value));
    });
    if (result.has_value()) {
      construction_succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(construction_succeeded);

  bool retention_succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto coordinator =
        DistributedVectorResultCoordinatorV2::create(uuid(1U), {tablet_id}, stable_schema);
    ASSERT_TRUE(coordinator.has_value());
    const common::Status accepted =
        run_failure(fail_after, [&] { return coordinator->accept(message); });
    if (accepted.is_ok()) {
      retention_succeeded = true;
      break;
    }
    EXPECT_EQ(accepted.code(), common::StatusCode::kResourceExhausted);
    ASSERT_TRUE(coordinator->accept(message).is_ok());
    EXPECT_TRUE(std::move(*coordinator).finish().has_value());
  }
  EXPECT_TRUE(retention_succeeded);

  bool finish_succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto coordinator =
        DistributedVectorResultCoordinatorV2::create(uuid(1U), {tablet_id}, stable_schema);
    ASSERT_TRUE(coordinator.has_value());
    ASSERT_TRUE(coordinator->accept(message).is_ok());
    auto result = run_failure(fail_after, [&] { return std::move(*coordinator).finish(); });
    if (result.has_value()) {
      finish_succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(std::move(*coordinator).finish().has_value());
  }
  EXPECT_TRUE(finish_succeeded);
}

} // namespace
} // namespace chronos::cluster
