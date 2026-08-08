#include "chronos/columnar/column_vector.hpp"
#include "columnar/columnar_test_support.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <random>
#include <vector>

namespace chronos::columnar {
namespace {

[[nodiscard]] std::array<std::byte, 16> power_of_ten(const std::uint16_t precision) {
  std::array<std::byte, 16> value{};
  value[0] = std::byte{1};
  for (std::uint16_t digit = 0; digit < precision; ++digit) {
    std::uint16_t carry = 0U;
    for (std::byte& limb : value) {
      const auto product =
          static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(limb) * 10U + carry);
      limb = static_cast<std::byte>(product & 0xffU);
      carry = static_cast<std::uint16_t>(product >> 8U);
    }
  }
  return value;
}

void decrement(std::array<std::byte, 16>& value) {
  for (std::byte& limb : value) {
    const std::uint8_t current = std::to_integer<std::uint8_t>(limb);
    limb = static_cast<std::byte>(current - 1U);
    if (current != 0U) {
      return;
    }
  }
}

[[nodiscard]] std::array<std::byte, 16> negate(std::array<std::byte, 16> value) {
  std::uint16_t carry = 1U;
  for (std::byte& limb : value) {
    const auto complemented =
        static_cast<std::uint16_t>(static_cast<std::uint8_t>(~std::to_integer<std::uint8_t>(limb)));
    const auto sum = static_cast<std::uint16_t>(complemented + carry);
    limb = static_cast<std::byte>(sum & 0xffU);
    carry = static_cast<std::uint16_t>(sum >> 8U);
  }
  return value;
}

TEST(ColumnVectorPropertyTest, DeterministicPackedBitmapsRoundTripEveryGeneratedRow) {
  constexpr std::uint32_t kSeed = 0x434f4c31U;
  // A fixed seed makes property failures exactly reproducible.
  // NOLINTNEXTLINE(bugprone-random-generator-seed)
  std::mt19937 generator{kSeed};
  for (std::uint32_t trial = 0; trial < 200U; ++trial) {
    const std::uint32_t rows = static_cast<std::uint32_t>(1U + (generator() % 257U));
    std::vector<std::byte> validity(bitmap_size(rows), std::byte{0});
    std::vector<std::byte> values(bitmap_size(rows), std::byte{0});
    std::uint32_t nulls = 0U;
    for (std::uint32_t row = 0; row < rows; ++row) {
      const std::size_t byte = static_cast<std::size_t>(row) / 8U;
      const auto mask = static_cast<std::uint8_t>(1U << (row % 8U));
      const bool present = (generator() % 5U) != 0U;
      const bool value = (generator() & 1U) != 0U;
      if (present) {
        validity[byte] |= static_cast<std::byte>(mask);
        if (value) {
          values[byte] |= static_cast<std::byte>(mask);
        }
      } else {
        ++nulls;
      }
    }
    const auto vector = ColumnVectorView::create(
        ColumnVectorMetadata{.column_id = test::id<schema::ColumnId>(1),
                             .type = test::type(schema::LogicalTypeKind::kBool),
                             .nullable = true,
                             .row_count = rows,
                             .null_count = nulls},
        ColumnVectorBufferView{.validity = validity, .offsets = {}, .values = values});
    ASSERT_TRUE(vector.has_value())
        << "seed=" << kSeed << " trial=" << trial << ' ' << vector.error().to_string();
    for (std::uint32_t row = 0; row < rows; ++row) {
      const bool present = !vector->is_null(row).value();
      EXPECT_EQ(present, vector->cell(row)->kind() != ColumnCellView::Kind::kNull);
      if (present) {
        const std::size_t byte = static_cast<std::size_t>(row) / 8U;
        const auto mask = static_cast<std::uint8_t>(1U << (row % 8U));
        EXPECT_EQ(vector->cell(row)->boolean().value(),
                  (std::to_integer<std::uint8_t>(values[byte]) & mask) != 0U);
      }
    }
  }
}

TEST(ColumnVectorPropertyTest, DeterministicVariableOffsetsReturnExactGeneratedSlices) {
  constexpr std::uint32_t kSeed = 0x56415231U;
  // A fixed seed makes property failures exactly reproducible.
  // NOLINTNEXTLINE(bugprone-random-generator-seed)
  std::mt19937 generator{kSeed};
  for (std::uint32_t trial = 0; trial < 100U; ++trial) {
    const std::uint32_t rows = static_cast<std::uint32_t>(1U + (generator() % 64U));
    std::vector<std::byte> validity(bitmap_size(rows), std::byte{0});
    std::vector<std::byte> offsets;
    std::vector<std::byte> values;
    std::vector<std::uint32_t> lengths;
    lengths.reserve(rows);
    test::append_u32(offsets, 0U);
    std::uint32_t nulls = 0U;
    for (std::uint32_t row = 0; row < rows; ++row) {
      const bool present = (generator() % 4U) != 0U;
      std::uint32_t length = 0U;
      if (present) {
        const std::size_t byte = static_cast<std::size_t>(row) / 8U;
        validity[byte] |= static_cast<std::byte>(1U << (row % 8U));
        length = static_cast<std::uint32_t>(generator() % 12U);
        for (std::uint32_t index = 0; index < length; ++index) {
          values.push_back(static_cast<std::byte>('a' + (generator() % 26U)));
        }
      } else {
        ++nulls;
      }
      lengths.push_back(length);
      test::append_u32(offsets, static_cast<std::uint32_t>(values.size()));
    }
    const auto vector = ColumnVectorView::create(
        ColumnVectorMetadata{.column_id = test::id<schema::ColumnId>(1),
                             .type = test::type(schema::LogicalTypeKind::kString),
                             .nullable = true,
                             .row_count = rows,
                             .null_count = nulls},
        ColumnVectorBufferView{.validity = validity, .offsets = offsets, .values = values});
    ASSERT_TRUE(vector.has_value())
        << "seed=" << kSeed << " trial=" << trial << ' ' << vector.error().to_string();
    for (std::uint32_t row = 0; row < rows; ++row) {
      if (!vector->is_null(row).value()) {
        EXPECT_EQ(vector->cell(row)->bytes()->size(), lengths[row]);
      }
    }
  }
}

TEST(ColumnVectorPropertyTest, EveryDecimalPrecisionAcceptsOnlyMagnitudesBelowItsPowerOfTen) {
  for (std::uint16_t precision = 1U; precision <= 38U; ++precision) {
    SCOPED_TRACE(precision);
    const schema::LogicalType decimal =
        schema::LogicalType::decimal(precision, precision / 2U).value();
    const ColumnVectorMetadata metadata{.column_id = test::id<schema::ColumnId>(1),
                                        .type = decimal,
                                        .nullable = false,
                                        .row_count = 1U,
                                        .null_count = 0U};
    const std::array<std::byte, 16> limit = power_of_ten(precision);
    std::array<std::byte, 16> accepted = limit;
    decrement(accepted);
    EXPECT_TRUE(
        ColumnVectorView::create(
            metadata, ColumnVectorBufferView{.validity = {}, .offsets = {}, .values = accepted})
            .has_value());
    EXPECT_TRUE(
        ColumnVectorView::create(
            metadata,
            ColumnVectorBufferView{.validity = {}, .offsets = {}, .values = negate(accepted)})
            .has_value());
    EXPECT_FALSE(
        ColumnVectorView::create(
            metadata, ColumnVectorBufferView{.validity = {}, .offsets = {}, .values = limit})
            .has_value());
    EXPECT_FALSE(ColumnVectorView::create(
                     metadata,
                     ColumnVectorBufferView{.validity = {}, .offsets = {}, .values = negate(limit)})
                     .has_value());
  }
}

} // namespace
} // namespace chronos::columnar
