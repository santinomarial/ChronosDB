#include "chronos/query/asof_join.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace {

class EmptySource final : public chronos::query::PhysicalOperator {
public:
  chronos::common::Result<chronos::query::PhysicalOperatorStep>
  next(const chronos::query::QueryResourceContext&) override {
    return chronos::query::PhysicalOperatorStep::end();
  }
};

[[nodiscard]] chronos::schema::LogicalType type(const chronos::schema::LogicalTypeKind kind) {
  return chronos::schema::LogicalType::create(kind).value();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  if (size < 8U)
    return 0;
  const std::size_t left_width = static_cast<std::size_t>(data[0] & 7U);
  const std::size_t right_width = static_cast<std::size_t>(data[1] & 15U);
  const std::size_t left_timestamp = (data[7] & 8U) != 0U && left_width != 0U
                                         ? static_cast<std::size_t>(data[2]) % left_width
                                         : static_cast<std::size_t>(data[2]);
  const std::size_t right_timestamp = (data[7] & 8U) != 0U && right_width != 0U
                                          ? static_cast<std::size_t>(data[3]) % right_width
                                          : static_cast<std::size_t>(data[3]);
  std::vector<chronos::query::VectorAsofColumnShape> left;
  std::vector<chronos::query::VectorAsofColumnShape> right;
  left.reserve(left_width);
  right.reserve(right_width);
  for (std::size_t ordinal = 0U; ordinal < left_width; ++ordinal) {
    left.push_back(
        {.type = type(ordinal == left_timestamp ? chronos::schema::LogicalTypeKind::kTimestampNs
                                                : chronos::schema::LogicalTypeKind::kInt64),
         .nullable = (data[(ordinal + 3U) % size] & 1U) != 0U});
  }
  for (std::size_t ordinal = 0U; ordinal < right_width; ++ordinal) {
    right.push_back(
        {.type = type(ordinal == right_timestamp ? chronos::schema::LogicalTypeKind::kTimestampNs
                                                 : chronos::schema::LogicalTypeKind::kInt64),
         .nullable = (data[(ordinal + 5U) % size] & 1U) != 0U});
  }
  const std::size_t suffix_first = static_cast<std::size_t>(data[6] & 7U);
  if (suffix_first + 4U <= right.size()) {
    right[suffix_first] = {.type = type(chronos::schema::LogicalTypeKind::kUuid),
                           .nullable = false};
    right[suffix_first + 1U] = {.type = type(chronos::schema::LogicalTypeKind::kUInt64),
                                .nullable = false};
    right[suffix_first + 2U] = {.type = type(chronos::schema::LogicalTypeKind::kUInt32),
                                .nullable = false};
    right[suffix_first + 3U] = {.type = type(chronos::schema::LogicalTypeKind::kUInt8),
                                .nullable = false};
  }
  const std::size_t key_count = static_cast<std::size_t>(data[4] & 3U);
  std::vector<chronos::query::VectorAsofEqualityKey> keys;
  keys.reserve(key_count);
  for (std::size_t index = 0U; index < key_count; ++index) {
    std::size_t left_key = static_cast<std::size_t>(data[(index + 1U) % size]);
    std::size_t right_key = static_cast<std::size_t>(data[(index + 2U) % size]);
    if ((data[7] & 8U) != 0U && left_width != 0U && right_width != 0U) {
      left_key %= left_width;
      right_key %= right_width;
    }
    keys.push_back({.left_column_ordinal = left_key, .right_column_ordinal = right_key});
  }
  const std::size_t physical_ordinal = (data[7] & 8U) != 0U && right_width != 0U
                                           ? static_cast<std::size_t>(data[5]) % right_width
                                           : static_cast<std::size_t>(data[5]);
  std::vector<std::size_t> physical{physical_ordinal};
  std::vector<std::size_t> left_output;
  for (std::size_t ordinal = 0U; ordinal < left_width; ++ordinal)
    left_output.push_back(ordinal);
  std::vector<std::size_t> right_output;
  for (std::size_t ordinal = 0U; ordinal < right_width; ++ordinal)
    right_output.push_back(ordinal);
  auto join = chronos::query::AsofJoinOperator::create(
      std::make_unique<EmptySource>(), std::make_unique<EmptySource>(),
      {.left_input_columns = std::move(left),
       .right_input_columns = std::move(right),
       .equality_keys = std::move(keys),
       .left_timestamp_column_ordinal = left_timestamp,
       .right_timestamp_column_ordinal = right_timestamp,
       .right_physical_ordering_key_ordinals = std::move(physical),
       .right_row_version_first_column_ordinal = suffix_first,
       .left_output_column_ordinals = std::move(left_output),
       .right_output_column_ordinals = std::move(right_output),
       .left_outer = (data[7] & 1U) != 0U},
      {.maximum_left_rows = static_cast<std::uint32_t>(data[0]) + 1U,
       .maximum_right_rows = static_cast<std::uint32_t>(data[1]) + 1U,
       .maximum_equality_keys = static_cast<std::size_t>(data[4]) + 1U,
       .maximum_physical_ordering_keys = static_cast<std::size_t>(data[5]) + 1U,
       .maximum_state_bytes = (data[7] & 4U) != 0U ? static_cast<std::size_t>(data[6]) * 4'096U
                                                   : std::size_t{64U} * 1024U * 1024U,
       .output_limits = {.maximum_rows = static_cast<std::uint32_t>(data[0]) + 1U,
                         .maximum_columns = (data[7] & 2U) != 0U ? static_cast<std::size_t>(data[1])
                                                                 : left_width + right_width + 1U,
                         .maximum_buffer_bytes = static_cast<std::size_t>(data[6]) * 4'096U,
                         .maximum_retained_buffer_bytes =
                             static_cast<std::size_t>(data[6]) * 8'192U}});
  if (!join.has_value())
    return 0;
  auto resources = chronos::query::QueryResourceContext::create(1U << 20U);
  if (resources.has_value())
    static_cast<void>((*join)->next(*resources));
  return 0;
}
