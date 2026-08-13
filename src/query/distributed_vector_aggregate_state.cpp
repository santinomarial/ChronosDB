#include "chronos/query/distributed_vector_aggregate_state.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace detail {

struct MergeableVectorAggregateStateWireView {
  VectorAggregateDefinition definition;
  std::uint64_t count;
  std::uint64_t moment_count;
  std::array<std::uint32_t, 8U> exact_sum_magnitude;
  bool exact_sum_negative;
  float float_sum;
  double double_sum;
  double mean;
  double squared_distance;
  const ScalarValue* extremum;
  bool extremum_reservation_valid;
  std::size_t maximum_variable_extremum_bytes;
  bool has_value;
  bool finalized;
};

struct MergeableVectorAggregateStateRestore {
  VectorAggregateDefinition definition;
  std::size_t maximum_variable_extremum_bytes;
  std::uint64_t count;
  std::uint64_t moment_count;
  std::array<std::uint32_t, 8U> exact_sum_magnitude;
  bool exact_sum_negative;
  float float_sum;
  double double_sum;
  double mean;
  double squared_distance;
  std::optional<ScalarValue> extremum;
  QueryMemoryReservation extremum_reservation;
  bool has_value;
};

class MergeableVectorAggregateStateCodecAccess {
public:
  [[nodiscard]] static MergeableVectorAggregateStateWireView
  view(const MergeableVectorAggregateState& state) noexcept {
    return {.definition = state.definition_,
            .count = state.count_,
            .moment_count = state.moment_count_,
            .exact_sum_magnitude = state.exact_sum_magnitude_,
            .exact_sum_negative = state.exact_sum_negative_,
            .float_sum = state.float_sum_,
            .double_sum = state.double_sum_,
            .mean = state.mean_,
            .squared_distance = state.squared_distance_,
            .extremum = state.extremum_.has_value() ? std::addressof(*state.extremum_) : nullptr,
            .extremum_reservation_valid = state.extremum_reservation_.is_valid(),
            .maximum_variable_extremum_bytes = state.maximum_variable_extremum_bytes_,
            .has_value = state.has_value_,
            .finalized = state.finalized_};
  }

  [[nodiscard]] static common::Result<MergeableVectorAggregateState>
  restore(MergeableVectorAggregateStateRestore restored) {
    auto state = MergeableVectorAggregateState::create(restored.definition,
                                                       restored.maximum_variable_extremum_bytes);
    if (!state.has_value())
      return common::make_unexpected(state.error());
    state->count_ = restored.count;
    state->moment_count_ = restored.moment_count;
    state->exact_sum_magnitude_ = restored.exact_sum_magnitude;
    state->exact_sum_negative_ = restored.exact_sum_negative;
    state->float_sum_ = restored.float_sum;
    state->double_sum_ = restored.double_sum;
    state->mean_ = restored.mean;
    state->squared_distance_ = restored.squared_distance;
    state->extremum_ = std::move(restored.extremum);
    state->extremum_reservation_ = std::move(restored.extremum_reservation);
    state->has_value_ = restored.has_value;
    return state;
  }
};

} // namespace detail

namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'A'}, std::byte{'G'},
                                                  std::byte{'S'}, std::byte{'1'}};
inline constexpr std::uint8_t kInputPresent = 1U << 0U;
inline constexpr std::uint8_t kInputNullable = 1U << 1U;
inline constexpr std::uint8_t kValuePresent = 1U << 2U;
inline constexpr std::uint8_t kExactNegative = 1U << 3U;
inline constexpr std::uint8_t kKnownFlags =
    kInputPresent | kInputNullable | kValuePresent | kExactNegative;
inline constexpr std::size_t kHeaderCrcOffset = 108U;
inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

struct DecodedHeader {
  VectorAggregateDefinition definition;
  std::size_t frame_length;
  std::size_t payload_length;
  std::uint8_t flags;
  std::uint64_t count;
  std::array<std::uint32_t, 8U> exact_sum_magnitude;
  std::uint32_t float_bits;
  std::uint64_t primary_bits;
  std::uint64_t secondary_bits;
};

struct DecodedExtremum {
  std::optional<ScalarValue> value;
  QueryMemoryReservation reservation;
};

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool positive_zero(const float value) noexcept {
  return std::bit_cast<std::uint32_t>(value) == 0U;
}

[[nodiscard]] bool positive_zero(const double value) noexcept {
  return std::bit_cast<std::uint64_t>(value) == 0U;
}

[[nodiscard]] bool zero_magnitude(const std::array<std::uint32_t, 8U>& magnitude) noexcept {
  return std::ranges::all_of(magnitude, [](const std::uint32_t limb) { return limb == 0U; });
}

[[nodiscard]] bool exact_sum_type(const schema::LogicalTypeKind kind) noexcept {
  return (kind >= schema::LogicalTypeKind::kInt8 && kind <= schema::LogicalTypeKind::kUInt64) ||
         kind == schema::LogicalTypeKind::kDecimal;
}

[[nodiscard]] std::optional<std::size_t>
fixed_scalar_width(const schema::LogicalTypeKind kind) noexcept {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return std::nullopt;
  }
  return std::nullopt;
}

[[nodiscard]] common::Status
validate_limits(const DistributedVectorAggregateStateDecodeLimits& limits) {
  if (limits.maximum_frame_length <
          distributed_vector_aggregate_state_format::kMinimumFrameLength ||
      limits.maximum_frame_length >
          distributed_vector_aggregate_state_format::kMaximumFrameLength ||
      limits.maximum_variable_extremum_bytes == 0U ||
      limits.maximum_variable_extremum_bytes >
          distributed_vector_aggregate_state_format::kMaximumExtremumBytes) {
    return invalid("distributed vector aggregate state limits are invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_state_view(const detail::MergeableVectorAggregateStateWireView& state) {
  if (state.finalized)
    return invalid("finalized aggregate state cannot be encoded");
  const auto shape = vector_aggregate_output_shape(state.definition);
  if (!shape.has_value())
    return shape.error();
  const VectorAggregateInput* input =
      state.definition.input.has_value() ? std::addressof(state.definition.input.value()) : nullptr;
  if (input != nullptr &&
      input->column_ordinal >
          distributed_vector_aggregate_state_format::kMaximumInputColumnOrdinal) {
    return invalid("aggregate state input ordinal exceeds the wire limit");
  }
  const bool exact_zero = zero_magnitude(state.exact_sum_magnitude);
  const bool common_numeric_zero = state.count == 0U && state.moment_count == 0U && exact_zero &&
                                   !state.exact_sum_negative && positive_zero(state.float_sum) &&
                                   positive_zero(state.double_sum) && positive_zero(state.mean) &&
                                   positive_zero(state.squared_distance);
  const VectorAggregateOperation operation = state.definition.operation;
  if (operation == VectorAggregateOperation::kCountStar ||
      operation == VectorAggregateOperation::kCount) {
    if (state.count > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        state.moment_count != 0U || !exact_zero || state.exact_sum_negative ||
        !positive_zero(state.float_sum) || !positive_zero(state.double_sum) ||
        !positive_zero(state.mean) || !positive_zero(state.squared_distance) ||
        state.extremum != nullptr || state.extremum_reservation_valid || state.has_value) {
      return invalid("COUNT aggregate state is noncanonical");
    }
    return common::Status::ok();
  }
  if (operation == VectorAggregateOperation::kSum) {
    if (input == nullptr)
      return invalid("SUM aggregate state input is missing");
    const schema::LogicalTypeKind kind = input->type.kind();
    const bool common_unused = state.count == 0U && state.moment_count == 0U &&
                               positive_zero(state.mean) && positive_zero(state.squared_distance) &&
                               state.extremum == nullptr && !state.extremum_reservation_valid;
    if (!common_unused)
      return invalid("SUM aggregate state has noncanonical unused fields");
    if (kind == schema::LogicalTypeKind::kFloat32) {
      if (!exact_zero || state.exact_sum_negative || !positive_zero(state.double_sum) ||
          (!state.has_value && !positive_zero(state.float_sum))) {
        return invalid("FLOAT32 SUM aggregate state is noncanonical");
      }
      return common::Status::ok();
    }
    if (kind == schema::LogicalTypeKind::kFloat64) {
      if (!exact_zero || state.exact_sum_negative || !positive_zero(state.float_sum) ||
          (!state.has_value && !positive_zero(state.double_sum))) {
        return invalid("FLOAT64 SUM aggregate state is noncanonical");
      }
      return common::Status::ok();
    }
    if (!exact_sum_type(kind) || !positive_zero(state.float_sum) ||
        !positive_zero(state.double_sum) || (!state.has_value && !exact_zero) ||
        (exact_zero && state.exact_sum_negative)) {
      return invalid("exact SUM aggregate state is noncanonical");
    }
    return common::Status::ok();
  }
  if (operation == VectorAggregateOperation::kAverage) {
    if (state.count != 0U || !exact_zero || state.exact_sum_negative ||
        !positive_zero(state.float_sum) || !positive_zero(state.mean) ||
        !positive_zero(state.squared_distance) || state.extremum != nullptr ||
        state.extremum_reservation_valid || state.has_value ||
        (state.moment_count == 0U && !positive_zero(state.double_sum))) {
      return invalid("AVG aggregate state is noncanonical");
    }
    return common::Status::ok();
  }
  if (operation == VectorAggregateOperation::kVariancePopulation ||
      operation == VectorAggregateOperation::kVarianceSample) {
    if (state.count != 0U || !exact_zero || state.exact_sum_negative ||
        !positive_zero(state.float_sum) || !positive_zero(state.double_sum) ||
        state.extremum != nullptr || state.extremum_reservation_valid || state.has_value ||
        (state.moment_count == 0U &&
         (!positive_zero(state.mean) || !positive_zero(state.squared_distance)))) {
      return invalid("variance aggregate state is noncanonical");
    }
    return common::Status::ok();
  }
  if (operation != VectorAggregateOperation::kMinimum &&
      operation != VectorAggregateOperation::kMaximum) {
    return invalid("aggregate state operation is invalid");
  }
  if (!common_numeric_zero || state.has_value)
    return invalid("extremum aggregate state has noncanonical numeric fields");
  if (input == nullptr)
    return invalid("extremum aggregate state input is missing");
  const bool variable = input->type.is_variable_width();
  if ((state.extremum == nullptr && state.extremum_reservation_valid) ||
      (state.extremum != nullptr && variable != state.extremum_reservation_valid)) {
    return invalid("extremum aggregate state ownership is noncanonical");
  }
  const schema::LogicalType* extremum_type =
      state.extremum != nullptr && state.extremum->type().has_value()
          ? std::addressof(state.extremum->type().value())
          : nullptr;
  if (state.extremum != nullptr &&
      (extremum_type == nullptr || *extremum_type != input->type || state.extremum->is_null())) {
    return invalid("extremum aggregate state value is invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::size_t> scalar_payload_size(const schema::LogicalType type,
                                                              const ScalarValue& value) {
  // The value access is guarded in the same conditional expression.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  const schema::LogicalType* stored_type =
      value.type().has_value() ? std::addressof(value.type().value()) : nullptr;
  // NOLINTEND(bugprone-unchecked-optional-access)
  if (stored_type == nullptr || *stored_type != type || value.is_null())
    return common::make_unexpected(invalid("aggregate extremum type is invalid"));
  if (type.is_variable_width()) {
    if (const auto* text = std::get_if<std::string>(&value.storage()); text != nullptr)
      return text->size();
    if (const auto* binary = std::get_if<std::vector<std::byte>>(&value.storage());
        binary != nullptr) {
      return binary->size();
    }
    return common::make_unexpected(invalid("aggregate extremum storage is invalid"));
  }
  const auto width = fixed_scalar_width(type.kind());
  if (!width.has_value())
    return common::make_unexpected(invalid("aggregate extremum width is invalid"));
  return *width;
}

[[nodiscard]] common::Status write_scalar_payload(common::ByteWriter& writer,
                                                  const schema::LogicalType type,
                                                  const ScalarValue& value) {
  using schema::LogicalTypeKind;
  switch (type.kind()) {
  case LogicalTypeKind::kBool: {
    const auto* stored = std::get_if<bool>(&value.storage());
    return stored == nullptr ? invalid("Boolean extremum storage is invalid")
                             : writer.write_u8(*stored ? 1U : 0U);
  }
  case LogicalTypeKind::kInt8: {
    const auto* stored = std::get_if<std::int64_t>(&value.storage());
    return stored == nullptr ? invalid("signed extremum storage is invalid")
                             : writer.write_i8(static_cast<std::int8_t>(*stored));
  }
  case LogicalTypeKind::kInt16: {
    const auto* stored = std::get_if<std::int64_t>(&value.storage());
    return stored == nullptr ? invalid("signed extremum storage is invalid")
                             : writer.write_i16_le(static_cast<std::int16_t>(*stored));
  }
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate: {
    const auto* stored = std::get_if<std::int64_t>(&value.storage());
    return stored == nullptr ? invalid("signed extremum storage is invalid")
                             : writer.write_i32_le(static_cast<std::int32_t>(*stored));
  }
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs: {
    const auto* stored = std::get_if<std::int64_t>(&value.storage());
    return stored == nullptr ? invalid("signed extremum storage is invalid")
                             : writer.write_i64_le(*stored);
  }
  case LogicalTypeKind::kUInt8: {
    const auto* stored = std::get_if<std::uint64_t>(&value.storage());
    return stored == nullptr ? invalid("unsigned extremum storage is invalid")
                             : writer.write_u8(static_cast<std::uint8_t>(*stored));
  }
  case LogicalTypeKind::kUInt16: {
    const auto* stored = std::get_if<std::uint64_t>(&value.storage());
    return stored == nullptr ? invalid("unsigned extremum storage is invalid")
                             : writer.write_u16_le(static_cast<std::uint16_t>(*stored));
  }
  case LogicalTypeKind::kUInt32: {
    const auto* stored = std::get_if<std::uint64_t>(&value.storage());
    return stored == nullptr ? invalid("unsigned extremum storage is invalid")
                             : writer.write_u32_le(static_cast<std::uint32_t>(*stored));
  }
  case LogicalTypeKind::kUInt64: {
    const auto* stored = std::get_if<std::uint64_t>(&value.storage());
    return stored == nullptr ? invalid("unsigned extremum storage is invalid")
                             : writer.write_u64_le(*stored);
  }
  case LogicalTypeKind::kFloat32: {
    const auto* stored = std::get_if<float>(&value.storage());
    return stored == nullptr ? invalid("FLOAT32 extremum storage is invalid")
                             : writer.write_float32_le(*stored);
  }
  case LogicalTypeKind::kFloat64: {
    const auto* stored = std::get_if<double>(&value.storage());
    return stored == nullptr ? invalid("FLOAT64 extremum storage is invalid")
                             : writer.write_float64_le(*stored);
  }
  case LogicalTypeKind::kDecimal: {
    const auto* stored = std::get_if<Decimal128Value>(&value.storage());
    return stored == nullptr ? invalid("DECIMAL extremum storage is invalid")
                             : writer.write_exact(stored->coefficient);
  }
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString: {
    const auto* stored = std::get_if<std::string>(&value.storage());
    if (stored == nullptr)
      return invalid("text extremum storage is invalid");
    return writer.write_exact(std::as_bytes(std::span{stored->data(), stored->size()}));
  }
  case LogicalTypeKind::kBinary: {
    const auto* stored = std::get_if<std::vector<std::byte>>(&value.storage());
    return stored == nullptr ? invalid("binary extremum storage is invalid")
                             : writer.write_exact(*stored);
  }
  case LogicalTypeKind::kUuid: {
    const auto* stored = std::get_if<common::Uuid>(&value.storage());
    return stored == nullptr ? invalid("UUID extremum storage is invalid")
                             : writer.write_exact(stored->bytes());
  }
  }
  return invalid("aggregate extremum logical type is invalid");
}

[[nodiscard]] common::Result<ScalarValue> decode_fixed_scalar(const schema::LogicalType type,
                                                              const common::ByteView payload) {
  common::ByteReader reader{payload};
  using schema::LogicalTypeKind;
  switch (type.kind()) {
  case LogicalTypeKind::kBool: {
    const auto value = reader.read_u8();
    if (!value.has_value() || *value > 1U)
      return common::make_unexpected(corruption("Boolean extremum payload is invalid"));
    return ScalarValue::boolean(*value != 0U);
  }
  case LogicalTypeKind::kInt8: {
    const auto value = reader.read_i8();
    return value.has_value() ? ScalarValue::signed_value(type, *value)
                             : common::make_unexpected(corruption("INT8 extremum is truncated"));
  }
  case LogicalTypeKind::kInt16: {
    const auto value = reader.read_i16_le();
    return value.has_value() ? ScalarValue::signed_value(type, *value)
                             : common::make_unexpected(corruption("INT16 extremum is truncated"));
  }
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate: {
    const auto value = reader.read_i32_le();
    return value.has_value() ? ScalarValue::signed_value(type, *value)
                             : common::make_unexpected(corruption("INT32 extremum is truncated"));
  }
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs: {
    const auto value = reader.read_i64_le();
    return value.has_value() ? ScalarValue::signed_value(type, *value)
                             : common::make_unexpected(corruption("INT64 extremum is truncated"));
  }
  case LogicalTypeKind::kUInt8: {
    const auto value = reader.read_u8();
    return value.has_value() ? ScalarValue::unsigned_value(type, *value)
                             : common::make_unexpected(corruption("UINT8 extremum is truncated"));
  }
  case LogicalTypeKind::kUInt16: {
    const auto value = reader.read_u16_le();
    return value.has_value() ? ScalarValue::unsigned_value(type, *value)
                             : common::make_unexpected(corruption("UINT16 extremum is truncated"));
  }
  case LogicalTypeKind::kUInt32: {
    const auto value = reader.read_u32_le();
    return value.has_value() ? ScalarValue::unsigned_value(type, *value)
                             : common::make_unexpected(corruption("UINT32 extremum is truncated"));
  }
  case LogicalTypeKind::kUInt64: {
    const auto value = reader.read_u64_le();
    return value.has_value() ? ScalarValue::unsigned_value(type, *value)
                             : common::make_unexpected(corruption("UINT64 extremum is truncated"));
  }
  case LogicalTypeKind::kFloat32: {
    const auto value = reader.read_float32_le();
    return value.has_value() ? ScalarValue::float32(*value)
                             : common::make_unexpected(corruption("FLOAT32 extremum is truncated"));
  }
  case LogicalTypeKind::kFloat64: {
    const auto value = reader.read_float64_le();
    return value.has_value() ? ScalarValue::float64(*value)
                             : common::make_unexpected(corruption("FLOAT64 extremum is truncated"));
  }
  case LogicalTypeKind::kDecimal: {
    if (payload.size() != 16U)
      return common::make_unexpected(corruption("DECIMAL extremum width is invalid"));
    Decimal128Value value;
    std::ranges::copy(payload, value.coefficient.begin());
    auto scalar = ScalarValue::decimal(type, value);
    return scalar.has_value()
               ? std::move(scalar)
               : common::make_unexpected(corruption("DECIMAL extremum is outside its type"));
  }
  case LogicalTypeKind::kUuid: {
    if (payload.size() != common::Uuid::kSize)
      return common::make_unexpected(corruption("UUID extremum width is invalid"));
    common::Uuid::Bytes bytes{};
    std::ranges::copy(payload, bytes.begin());
    return ScalarValue::uuid(common::Uuid{bytes});
  }
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    break;
  }
  return common::make_unexpected(corruption("fixed extremum type is invalid"));
}

[[nodiscard]] common::Result<DecodedExtremum>
decode_extremum(const schema::LogicalType type, const common::ByteView payload,
                const QueryResourceContext& resources,
                const std::size_t maximum_variable_extremum_bytes) {
  if (!type.is_variable_width()) {
    auto value = decode_fixed_scalar(type, payload);
    if (!value.has_value())
      return common::make_unexpected(value.error());
    return DecodedExtremum{.value = std::move(*value), .reservation = {}};
  }
  if (payload.size() > maximum_variable_extremum_bytes)
    return common::make_unexpected(exhausted("aggregate extremum exceeds caller byte limit"));
  const auto doubled = common::checked_multiply(payload.size(), std::size_t{2U});
  const auto charge = doubled.has_value()
                          ? common::checked_add(*doubled, kConservativeAllocationOverheadBytes)
                          : std::nullopt;
  if (!charge.has_value())
    return common::make_unexpected(exhausted("aggregate extremum accounting overflowed"));
  auto reservation = resources.reserve(*charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  std::optional<ScalarValue> value;
  if (type.kind() == schema::LogicalTypeKind::kBinary) {
    value = ScalarValue::binary(std::vector<std::byte>{payload.begin(), payload.end()});
  } else {
    std::string text(payload.size(), '\0');
    if (!payload.empty())
      std::memcpy(text.data(), payload.data(), payload.size());
    auto decoded = ScalarValue::text(type, std::move(text));
    if (!decoded.has_value())
      return common::make_unexpected(corruption("text extremum is not canonical UTF-8"));
    value = std::move(*decoded);
  }
  std::size_t retained_payload{};
  if (const auto* text = std::get_if<std::string>(&value->storage()); text != nullptr) {
    retained_payload = text->capacity();
  } else if (const auto* binary = std::get_if<std::vector<std::byte>>(&value->storage());
             binary != nullptr) {
    retained_payload = binary->capacity();
  }
  if (retained_payload > reservation->bytes())
    return common::make_unexpected(exhausted("aggregate extremum allocation exceeded its charge"));
  return DecodedExtremum{.value = std::move(value), .reservation = std::move(*reservation)};
}

[[nodiscard]] common::Result<DecodedHeader>
decode_header(const common::ByteView bytes,
              const DistributedVectorAggregateStateDecodeLimits& limits,
              const bool require_complete_frame) {
  if (bytes.size() < distributed_vector_aggregate_state_format::kHeaderLength)
    return common::make_unexpected(corruption("aggregate state header is truncated"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("aggregate state magic is invalid"));
  common::ByteReader crc_reader{bytes.subspan(kHeaderCrcOffset, sizeof(std::uint32_t))};
  const auto header_crc = crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)))
    return common::make_unexpected(corruption("aggregate state header checksum is invalid"));

  common::ByteReader reader{bytes.first(distributed_vector_aggregate_state_format::kHeaderLength)};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto operation = reader.read_u8();
  const auto flags = reader.read_u8();
  const auto type_code = reader.read_u16_le();
  const auto parameter_0 = reader.read_u16_le();
  const auto parameter_1 = reader.read_u16_le();
  const auto column_ordinal = reader.read_u32_le();
  const auto payload_length = reader.read_u32_le();
  const auto count = reader.read_u64_le();
  std::array<std::uint32_t, 8U> exact_sum_magnitude{};
  bool exact_complete = true;
  for (std::uint32_t& limb : exact_sum_magnitude) {
    const auto value = reader.read_u32_le();
    if (!value.has_value()) {
      exact_complete = false;
      break;
    }
    limb = *value;
  }
  const auto float_bits = reader.read_u32_le();
  const auto reserved_0 = reader.read_u32_le();
  const auto primary_bits = reader.read_u64_le();
  const auto secondary_bits = reader.read_u64_le();
  const auto reserved_1 = reader.read_u32_le();
  static_cast<void>(reader.skip(sizeof(std::uint32_t)));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !operation.has_value() || !flags.has_value() ||
      !type_code.has_value() || !parameter_0.has_value() || !parameter_1.has_value() ||
      !column_ordinal.has_value() || !payload_length.has_value() || !count.has_value() ||
      !exact_complete || !float_bits.has_value() || !reserved_0.has_value() ||
      !primary_bits.has_value() || !secondary_bits.has_value() || !reserved_1.has_value()) {
    return common::make_unexpected(corruption("aggregate state header fields are truncated"));
  }
  if (*major != distributed_vector_aggregate_state_format::kMajor ||
      *minor != distributed_vector_aggregate_state_format::kMinor) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "aggregate state version is unsupported"});
  }
  if (*header_length != distributed_vector_aggregate_state_format::kHeaderLength ||
      *frame_length < distributed_vector_aggregate_state_format::kMinimumFrameLength ||
      *frame_length > distributed_vector_aggregate_state_format::kMaximumFrameLength ||
      *frame_length != distributed_vector_aggregate_state_format::kHeaderLength +
                           static_cast<std::uint64_t>(*payload_length) +
                           distributed_vector_aggregate_state_format::kTrailerLength ||
      (*flags & ~kKnownFlags) != 0U || *reserved_0 != 0U || *reserved_1 != 0U ||
      *column_ordinal > distributed_vector_aggregate_state_format::kMaximumInputColumnOrdinal) {
    return common::make_unexpected(corruption("aggregate state header is noncanonical"));
  }
  if (*frame_length > limits.maximum_frame_length)
    return common::make_unexpected(exhausted("aggregate state exceeds caller frame limit"));
  if (require_complete_frame && *frame_length != bytes.size())
    return common::make_unexpected(corruption("aggregate state exact length is invalid"));

  const bool input_present = (*flags & kInputPresent) != 0U;
  VectorAggregateDefinition definition{
      .operation = static_cast<VectorAggregateOperation>(*operation), .input = std::nullopt};
  if (input_present) {
    const auto kind = schema::logical_type_kind_from_code(*type_code);
    if (!kind.has_value())
      return common::make_unexpected(kind.error());
    const auto type = schema::LogicalType::create(*kind, *parameter_0, *parameter_1);
    if (!type.has_value())
      return common::make_unexpected(corruption("aggregate state logical type is invalid"));
    definition.input = VectorAggregateInput{.column_ordinal = *column_ordinal,
                                            .type = *type,
                                            .nullable = (*flags & kInputNullable) != 0U};
  } else if ((*flags & kInputNullable) != 0U || *type_code != 0U || *parameter_0 != 0U ||
             *parameter_1 != 0U || *column_ordinal != 0U) {
    return common::make_unexpected(corruption("aggregate state absent input is noncanonical"));
  }
  const auto shape = vector_aggregate_output_shape(definition);
  if (!shape.has_value())
    return common::make_unexpected(corruption("aggregate state definition is invalid"));
  return DecodedHeader{.definition = definition,
                       .frame_length = static_cast<std::size_t>(*frame_length),
                       .payload_length = *payload_length,
                       .flags = *flags,
                       .count = *count,
                       .exact_sum_magnitude = exact_sum_magnitude,
                       .float_bits = *float_bits,
                       .primary_bits = *primary_bits,
                       .secondary_bits = *secondary_bits};
}

[[nodiscard]] common::Status validate_decoded_fields(const DecodedHeader& header) {
  const bool has_value = (header.flags & kValuePresent) != 0U;
  const bool negative = (header.flags & kExactNegative) != 0U;
  const bool exact_zero = zero_magnitude(header.exact_sum_magnitude);
  const bool float_zero = header.float_bits == 0U;
  const bool primary_zero = header.primary_bits == 0U;
  const bool secondary_zero = header.secondary_bits == 0U;
  const VectorAggregateOperation operation = header.definition.operation;
  const VectorAggregateInput* input = header.definition.input.has_value()
                                          ? std::addressof(header.definition.input.value())
                                          : nullptr;
  if (operation == VectorAggregateOperation::kCountStar ||
      operation == VectorAggregateOperation::kCount) {
    if (header.count > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        has_value || negative || !exact_zero || !float_zero || !primary_zero || !secondary_zero ||
        header.payload_length != 0U) {
      return corruption("COUNT aggregate state fields are noncanonical");
    }
    return common::Status::ok();
  }
  if (operation == VectorAggregateOperation::kSum) {
    if (input == nullptr)
      return corruption("SUM aggregate state input is missing");
    const schema::LogicalTypeKind kind = input->type.kind();
    if (header.count != 0U || header.payload_length != 0U || !secondary_zero)
      return corruption("SUM aggregate state fields are noncanonical");
    if (kind == schema::LogicalTypeKind::kFloat32) {
      return (!negative && exact_zero && primary_zero && (has_value || float_zero))
                 ? common::Status::ok()
                 : corruption("FLOAT32 SUM aggregate state fields are noncanonical");
    }
    if (kind == schema::LogicalTypeKind::kFloat64) {
      return (!negative && exact_zero && float_zero && (has_value || primary_zero))
                 ? common::Status::ok()
                 : corruption("FLOAT64 SUM aggregate state fields are noncanonical");
    }
    return (exact_sum_type(kind) && float_zero && primary_zero && (has_value || exact_zero) &&
            (!exact_zero || !negative))
               ? common::Status::ok()
               : corruption("exact SUM aggregate state fields are noncanonical");
  }
  if (operation == VectorAggregateOperation::kAverage) {
    return (!has_value && !negative && exact_zero && float_zero && secondary_zero &&
            header.payload_length == 0U && (header.count != 0U || primary_zero))
               ? common::Status::ok()
               : corruption("AVG aggregate state fields are noncanonical");
  }
  if (operation == VectorAggregateOperation::kVariancePopulation ||
      operation == VectorAggregateOperation::kVarianceSample) {
    return (!has_value && !negative && exact_zero && float_zero && header.payload_length == 0U &&
            (header.count != 0U || (primary_zero && secondary_zero)))
               ? common::Status::ok()
               : corruption("variance aggregate state fields are noncanonical");
  }
  if (operation != VectorAggregateOperation::kMinimum &&
      operation != VectorAggregateOperation::kMaximum) {
    return corruption("aggregate state operation is invalid");
  }
  if (header.count != 0U || negative || !exact_zero || !float_zero || !primary_zero ||
      !secondary_zero || (!has_value && header.payload_length != 0U)) {
    return corruption("extremum aggregate state fields are noncanonical");
  }
  if (!has_value)
    return common::Status::ok();
  if (input == nullptr)
    return corruption("extremum aggregate state input is missing");
  const schema::LogicalType type = input->type;
  if (!type.is_variable_width()) {
    const auto width = fixed_scalar_width(type.kind());
    if (!width.has_value() || *width != header.payload_length)
      return corruption("fixed extremum payload length is invalid");
  }
  return common::Status::ok();
}

} // namespace

EncodedMergeableVectorAggregateState::EncodedMergeableVectorAggregateState(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedMergeableVectorAggregateState::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedMergeableVectorAggregateState>
encode_mergeable_vector_aggregate_state(const MergeableVectorAggregateState& state) {
  const detail::MergeableVectorAggregateStateWireView view =
      detail::MergeableVectorAggregateStateCodecAccess::view(state);
  const common::Status validation = validate_state_view(view);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  const VectorAggregateInput* input =
      view.definition.input.has_value() ? std::addressof(view.definition.input.value()) : nullptr;

  std::size_t payload_length{};
  if (view.extremum != nullptr) {
    if (input == nullptr)
      return common::make_unexpected(invalid("aggregate extremum input is missing"));
    auto size = scalar_payload_size(input->type, *view.extremum);
    if (!size.has_value())
      return common::make_unexpected(size.error());
    payload_length = *size;
  }
  if (payload_length > distributed_vector_aggregate_state_format::kMaximumExtremumBytes ||
      payload_length > view.maximum_variable_extremum_bytes) {
    return common::make_unexpected(invalid("aggregate extremum exceeds the wire byte limit"));
  }
  const std::size_t frame_length = distributed_vector_aggregate_state_format::kHeaderLength +
                                   payload_length +
                                   distributed_vector_aggregate_state_format::kTrailerLength;
  try {
    std::vector<std::byte> bytes(frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_aggregate_state_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_aggregate_state_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_vector_aggregate_state_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(frame_length);
    if (status.is_ok())
      status = writer.write_u8(static_cast<std::uint8_t>(view.definition.operation));
    std::uint8_t flags{};
    if (input != nullptr) {
      flags |= kInputPresent;
      if (input->nullable)
        flags |= kInputNullable;
    }
    if (view.has_value || view.extremum != nullptr)
      flags |= kValuePresent;
    if (view.exact_sum_negative)
      flags |= kExactNegative;
    if (status.is_ok())
      status = writer.write_u8(flags);
    if (status.is_ok())
      status = writer.write_u16_le(input != nullptr ? input->type.code() : 0U);
    if (status.is_ok())
      status = writer.write_u16_le(input != nullptr ? input->type.parameter_0() : 0U);
    if (status.is_ok())
      status = writer.write_u16_le(input != nullptr ? input->type.parameter_1() : 0U);
    if (status.is_ok())
      status = writer.write_u32_le(
          input != nullptr ? static_cast<std::uint32_t>(input->column_ordinal) : 0U);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(payload_length));
    const VectorAggregateOperation operation = view.definition.operation;
    const bool counted = operation == VectorAggregateOperation::kCountStar ||
                         operation == VectorAggregateOperation::kCount;
    const bool moments = operation == VectorAggregateOperation::kAverage ||
                         operation == VectorAggregateOperation::kVariancePopulation ||
                         operation == VectorAggregateOperation::kVarianceSample;
    if (status.is_ok())
      status = writer.write_u64_le(counted ? view.count : (moments ? view.moment_count : 0U));
    const bool exact = operation == VectorAggregateOperation::kSum && input != nullptr &&
                       exact_sum_type(input->type.kind());
    for (const std::uint32_t limb : view.exact_sum_magnitude) {
      if (status.is_ok())
        status = writer.write_u32_le(exact ? limb : 0U);
    }
    const bool float32_sum = operation == VectorAggregateOperation::kSum && input != nullptr &&
                             input->type.kind() == schema::LogicalTypeKind::kFloat32;
    if (status.is_ok())
      status = writer.write_u32_le(float32_sum ? std::bit_cast<std::uint32_t>(view.float_sum) : 0U);
    if (status.is_ok())
      status = writer.zero_fill(sizeof(std::uint32_t));
    std::uint64_t primary_bits{};
    std::uint64_t secondary_bits{};
    if (operation == VectorAggregateOperation::kSum && input != nullptr &&
        input->type.kind() == schema::LogicalTypeKind::kFloat64) {
      primary_bits = std::bit_cast<std::uint64_t>(view.double_sum);
    } else if (operation == VectorAggregateOperation::kAverage) {
      primary_bits = std::bit_cast<std::uint64_t>(view.double_sum);
    } else if (operation == VectorAggregateOperation::kVariancePopulation ||
               operation == VectorAggregateOperation::kVarianceSample) {
      primary_bits = std::bit_cast<std::uint64_t>(view.mean);
      secondary_bits = std::bit_cast<std::uint64_t>(view.squared_distance);
    }
    if (status.is_ok())
      status = writer.write_u64_le(primary_bits);
    if (status.is_ok())
      status = writer.write_u64_le(secondary_bits);
    if (status.is_ok())
      status = writer.zero_fill(sizeof(std::uint32_t));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok() && view.extremum != nullptr)
      status = write_scalar_payload(writer, input->type, *view.extremum);
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "aggregate state frame layout failed"});
    return EncodedMergeableVectorAggregateState{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("aggregate state encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("aggregate state frame exceeds container limits"));
  }
}

common::Result<MergeableVectorAggregateState> decode_mergeable_vector_aggregate_state_exact(
    const common::ByteView bytes, const QueryResourceContext& resources,
    const DistributedVectorAggregateStateDecodeLimits limits) {
  const common::Status limits_status = validate_limits(limits);
  if (!limits_status.is_ok())
    return common::make_unexpected(limits_status);
  if (bytes.size() < distributed_vector_aggregate_state_format::kMinimumFrameLength ||
      bytes.size() > distributed_vector_aggregate_state_format::kMaximumFrameLength) {
    return common::make_unexpected(corruption("aggregate state frame length is invalid"));
  }
  auto header = decode_header(bytes, limits, true);
  if (!header.has_value())
    return common::make_unexpected(header.error());
  common::ByteReader trailer{bytes.last(distributed_vector_aggregate_state_format::kTrailerLength)};
  const auto frame_crc = trailer.read_u32_le();
  if (!frame_crc.has_value() || *frame_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("aggregate state frame checksum is invalid"));
  const common::Status fields_status = validate_decoded_fields(*header);
  if (!fields_status.is_ok())
    return common::make_unexpected(fields_status);
  // The input access is guarded in the same conditional expression.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  const VectorAggregateInput* input = header->definition.input.has_value()
                                          ? std::addressof(header->definition.input.value())
                                          : nullptr;
  // NOLINTEND(bugprone-unchecked-optional-access)

  try {
    DecodedExtremum extremum;
    if ((header->flags & kValuePresent) != 0U &&
        (header->definition.operation == VectorAggregateOperation::kMinimum ||
         header->definition.operation == VectorAggregateOperation::kMaximum)) {
      if (input == nullptr)
        return common::make_unexpected(corruption("aggregate extremum input is missing"));
      auto decoded =
          decode_extremum(input->type,
                          bytes.subspan(distributed_vector_aggregate_state_format::kHeaderLength,
                                        header->payload_length),
                          resources, limits.maximum_variable_extremum_bytes);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      extremum = std::move(*decoded);
    }
    const VectorAggregateOperation operation = header->definition.operation;
    const bool counted = operation == VectorAggregateOperation::kCountStar ||
                         operation == VectorAggregateOperation::kCount;
    const bool moments = operation == VectorAggregateOperation::kAverage ||
                         operation == VectorAggregateOperation::kVariancePopulation ||
                         operation == VectorAggregateOperation::kVarianceSample;
    const bool exact = operation == VectorAggregateOperation::kSum && input != nullptr &&
                       exact_sum_type(input->type.kind());
    const bool float32_sum = operation == VectorAggregateOperation::kSum && input != nullptr &&
                             input->type.kind() == schema::LogicalTypeKind::kFloat32;
    const bool float64_sum = operation == VectorAggregateOperation::kSum && input != nullptr &&
                             input->type.kind() == schema::LogicalTypeKind::kFloat64;
    return detail::MergeableVectorAggregateStateCodecAccess::restore(
        {.definition = header->definition,
         .maximum_variable_extremum_bytes = limits.maximum_variable_extremum_bytes,
         .count = counted ? header->count : 0U,
         .moment_count = moments ? header->count : 0U,
         .exact_sum_magnitude =
             exact ? header->exact_sum_magnitude : std::array<std::uint32_t, 8U>{},
         .exact_sum_negative = exact && (header->flags & kExactNegative) != 0U,
         .float_sum = float32_sum ? std::bit_cast<float>(header->float_bits) : 0.0F,
         .double_sum = (float64_sum || operation == VectorAggregateOperation::kAverage)
                           ? std::bit_cast<double>(header->primary_bits)
                           : 0.0,
         .mean = (operation == VectorAggregateOperation::kVariancePopulation ||
                  operation == VectorAggregateOperation::kVarianceSample)
                     ? std::bit_cast<double>(header->primary_bits)
                     : 0.0,
         .squared_distance = (operation == VectorAggregateOperation::kVariancePopulation ||
                              operation == VectorAggregateOperation::kVarianceSample)
                                 ? std::bit_cast<double>(header->secondary_bits)
                                 : 0.0,
         .extremum = std::move(extremum.value),
         .extremum_reservation = std::move(extremum.reservation),
         .has_value =
             operation == VectorAggregateOperation::kSum && (header->flags & kValuePresent) != 0U});
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("aggregate state decoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("aggregate state decode exceeds container limits"));
  }
}

MergeableVectorAggregateStateReader::MergeableVectorAggregateStateReader(
    QueryResourceContext resources,
    const DistributedVectorAggregateStateDecodeLimits limits) noexcept
    : resources_(std::move(resources)), limits_(limits) {}

common::Result<MergeableVectorAggregateStateReadStep>
MergeableVectorAggregateStateReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const common::Status limits_status = validate_limits(limits_);
  if (!limits_status.is_ok()) {
    failure_ = limits_status;
    return common::make_unexpected(*failure_);
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(
        bytes.size(), distributed_vector_aggregate_state_format::kHeaderLength - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != distributed_vector_aggregate_state_format::kHeaderLength)
      return MergeableVectorAggregateStateReadStep{.consumed_bytes = consumed,
                                                   .state = std::nullopt};

    auto decoded_header = decode_header(header_, limits_, false);
    if (!decoded_header.has_value()) {
      failure_ = decoded_header.error();
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(decoded_header->frame_length);
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("aggregate state reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("aggregate state reader frame exceeds container limits");
      return common::make_unexpected(*failure_);
    }
    std::ranges::copy(header_, frame_.begin());
    frame_bytes_ = header_.size();
  }

  const common::ByteView remainder = bytes.subspan(consumed);
  const std::size_t copied = std::min(remainder.size(), frame_.size() - frame_bytes_);
  std::ranges::copy(remainder.first(copied),
                    frame_.begin() + static_cast<std::ptrdiff_t>(frame_bytes_));
  frame_bytes_ += copied;
  consumed += copied;
  if (frame_bytes_ != frame_.size())
    return MergeableVectorAggregateStateReadStep{.consumed_bytes = consumed, .state = std::nullopt};

  auto decoded = decode_mergeable_vector_aggregate_state_exact(frame_, resources_, limits_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return MergeableVectorAggregateStateReadStep{.consumed_bytes = consumed,
                                               .state = std::move(*decoded)};
}

std::size_t MergeableVectorAggregateStateReader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool MergeableVectorAggregateStateReader::failed() const noexcept {
  return failure_.has_value();
}

MergeableVectorAggregateStateWriteCursor::MergeableVectorAggregateStateWriteCursor(
    EncodedMergeableVectorAggregateState encoded) noexcept
    : encoded_(std::move(encoded)) {}

MergeableVectorAggregateStateWriteCursor::MergeableVectorAggregateStateWriteCursor(
    MergeableVectorAggregateStateWriteCursor&& other) noexcept
    : encoded_(std::move(other.encoded_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_.bytes().size();
}

MergeableVectorAggregateStateWriteCursor& MergeableVectorAggregateStateWriteCursor::operator=(
    MergeableVectorAggregateStateWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = std::move(other.encoded_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_.bytes().size();
  }
  return *this;
}

common::Result<MergeableVectorAggregateStateWriteCursor>
MergeableVectorAggregateStateWriteCursor::create(const MergeableVectorAggregateState& state) {
  auto encoded = encode_mergeable_vector_aggregate_state(state);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return MergeableVectorAggregateStateWriteCursor{std::move(*encoded)};
}

common::ByteView MergeableVectorAggregateStateWriteCursor::pending_write() const noexcept {
  return encoded_.bytes().subspan(written_bytes_);
}

common::Status
MergeableVectorAggregateStateWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > encoded_.bytes().size() - written_bytes_)
    return invalid("written byte count exceeds aggregate state frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t MergeableVectorAggregateStateWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool MergeableVectorAggregateStateWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_.bytes().size();
}

} // namespace chronos::query
