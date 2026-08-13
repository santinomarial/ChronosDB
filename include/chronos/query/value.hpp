#ifndef CHRONOS_QUERY_VALUE_HPP_
#define CHRONOS_QUERY_VALUE_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace chronos::query {

struct Decimal128Value {
  // Canonical signed two's-complement coefficient in little-endian byte order. Scale and
  // precision are carried by ScalarValue::type().
  std::array<std::byte, 16> coefficient{};

  friend bool operator==(const Decimal128Value&, const Decimal128Value&) = default;
};

using ScalarStorage =
    std::variant<std::monostate, bool, std::int64_t, std::uint64_t, float, double, Decimal128Value,
                 std::string, std::vector<std::byte>, common::Uuid>;

// An owned scalar value used by the reference executor. NULL may be typed or temporarily untyped
// while evaluating a NULL literal; every materialized result column is required to be typed.
class ScalarValue {
public:
  ScalarValue() = delete;

  [[nodiscard]] static ScalarValue untyped_null() noexcept;
  [[nodiscard]] static ScalarValue null(schema::LogicalType type) noexcept;
  [[nodiscard]] static common::Result<ScalarValue> boolean(bool value);
  [[nodiscard]] static common::Result<ScalarValue> signed_value(schema::LogicalType type,
                                                                std::int64_t value);
  [[nodiscard]] static common::Result<ScalarValue> unsigned_value(schema::LogicalType type,
                                                                  std::uint64_t value);
  [[nodiscard]] static common::Result<ScalarValue> float32(float value);
  [[nodiscard]] static common::Result<ScalarValue> float64(double value);
  [[nodiscard]] static common::Result<ScalarValue> decimal(schema::LogicalType type,
                                                           Decimal128Value value);
  [[nodiscard]] static common::Result<ScalarValue> text(schema::LogicalType type,
                                                        std::string value);
  [[nodiscard]] static ScalarValue binary(std::vector<std::byte> value);
  [[nodiscard]] static ScalarValue uuid(common::Uuid value);

  // Copies one already validated physical cell into executor-owned storage.
  [[nodiscard]] static common::Result<ScalarValue>
  from_column_cell(schema::LogicalType type, const columnar::ColumnCellView& cell);

  [[nodiscard]] bool is_null() const noexcept;
  [[nodiscard]] const std::optional<schema::LogicalType>& type() const noexcept;
  [[nodiscard]] const ScalarStorage& storage() const noexcept;

private:
  ScalarValue(std::optional<schema::LogicalType> type, ScalarStorage storage) noexcept;

  std::optional<schema::LogicalType> type_;
  ScalarStorage storage_;
};

enum class SqlTruthValue : std::uint8_t { kFalse, kTrue, kUnknown };
enum class ScalarNullPlacement : std::uint8_t { kFirst, kLast };

// SQL equality returns UNKNOWN for either NULL and treats every NaN comparison as false. Total
// comparison is for deterministic ORDER BY/group keys: NaN follows +infinity and NULL placement is
// explicit. Inputs must have compatible bound types.
[[nodiscard]] common::Result<SqlTruthValue> sql_scalar_equal(const ScalarValue& left,
                                                             const ScalarValue& right);
[[nodiscard]] common::Result<int> compare_scalar_values(const ScalarValue& left,
                                                        const ScalarValue& right,
                                                        ScalarNullPlacement null_placement);
// Allocation-free total comparison for two validated cells of one exact physical type. Variable-
// width values are compared directly from their borrowed canonical bytes.
[[nodiscard]] common::Result<int> compare_physical_cells(schema::LogicalType type,
                                                         const columnar::ColumnCellView& left,
                                                         const columnar::ColumnCellView& right,
                                                         ScalarNullPlacement null_placement);
// Allocation-free total comparison for two independently validated canonical scalar byte values.
// NULL values must carry an empty byte view. Variable-width values are compared directly without
// constructing per-row owned ScalarValue objects.
[[nodiscard]] common::Result<int>
compare_canonical_scalar_bytes(schema::LogicalType type, bool left_is_null, common::ByteView left,
                               bool right_is_null, common::ByteView right,
                               ScalarNullPlacement null_placement);

} // namespace chronos::query

#endif // CHRONOS_QUERY_VALUE_HPP_
