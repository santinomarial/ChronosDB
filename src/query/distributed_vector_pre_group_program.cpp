#include "chronos/query/distributed_vector_pre_group_program.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <variant>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'P'}, std::byte{'G'},
                                                  std::byte{'P'}, std::byte{'1'}};
inline constexpr std::size_t kPayloadCrcOffset = 36U;
inline constexpr std::size_t kHeaderCrcOffset = 40U;
inline constexpr std::uint8_t kFlag = 1U << 0U;

enum class InstructionTag : std::uint8_t { kInput = 1U, kConstant, kUnary, kCast, kBinary };

struct EncodedLogicalType {
  std::uint16_t code;
  std::uint16_t parameter_0;
  std::uint16_t parameter_1;
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

[[nodiscard]] std::optional<std::uint8_t> unary_code(const VectorUnaryOperation operation) {
  switch (operation) {
  case VectorUnaryOperation::kPositive:
    return 1U;
  case VectorUnaryOperation::kNegative:
    return 2U;
  case VectorUnaryOperation::kNot:
    return 3U;
  case VectorUnaryOperation::kIsNull:
    return 4U;
  case VectorUnaryOperation::kIsNotNull:
    return 5U;
  case VectorUnaryOperation::kAbsolute:
    return 6U;
  case VectorUnaryOperation::kLowerAscii:
    return 7U;
  case VectorUnaryOperation::kUpperAscii:
    return 8U;
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<VectorUnaryOperation> unary_operation(const std::uint8_t code) {
  switch (code) {
  case 1U:
    return VectorUnaryOperation::kPositive;
  case 2U:
    return VectorUnaryOperation::kNegative;
  case 3U:
    return VectorUnaryOperation::kNot;
  case 4U:
    return VectorUnaryOperation::kIsNull;
  case 5U:
    return VectorUnaryOperation::kIsNotNull;
  case 6U:
    return VectorUnaryOperation::kAbsolute;
  case 7U:
    return VectorUnaryOperation::kLowerAscii;
  case 8U:
    return VectorUnaryOperation::kUpperAscii;
  default:
    return std::nullopt;
  }
}

[[nodiscard]] std::optional<std::uint8_t> binary_code(const VectorBinaryOperation operation) {
  switch (operation) {
  case VectorBinaryOperation::kAnd:
    return 1U;
  case VectorBinaryOperation::kOr:
    return 2U;
  case VectorBinaryOperation::kEqual:
    return 3U;
  case VectorBinaryOperation::kNotEqual:
    return 4U;
  case VectorBinaryOperation::kLess:
    return 5U;
  case VectorBinaryOperation::kLessEqual:
    return 6U;
  case VectorBinaryOperation::kGreater:
    return 7U;
  case VectorBinaryOperation::kGreaterEqual:
    return 8U;
  case VectorBinaryOperation::kAdd:
    return 9U;
  case VectorBinaryOperation::kSubtract:
    return 10U;
  case VectorBinaryOperation::kMultiply:
    return 11U;
  case VectorBinaryOperation::kDivide:
    return 12U;
  case VectorBinaryOperation::kRemainder:
    return 13U;
  case VectorBinaryOperation::kCoalesce:
    return 14U;
  case VectorBinaryOperation::kTimeBucket:
    return 15U;
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<VectorBinaryOperation> binary_operation(const std::uint8_t code) {
  switch (code) {
  case 1U:
    return VectorBinaryOperation::kAnd;
  case 2U:
    return VectorBinaryOperation::kOr;
  case 3U:
    return VectorBinaryOperation::kEqual;
  case 4U:
    return VectorBinaryOperation::kNotEqual;
  case 5U:
    return VectorBinaryOperation::kLess;
  case 6U:
    return VectorBinaryOperation::kLessEqual;
  case 7U:
    return VectorBinaryOperation::kGreater;
  case 8U:
    return VectorBinaryOperation::kGreaterEqual;
  case 9U:
    return VectorBinaryOperation::kAdd;
  case 10U:
    return VectorBinaryOperation::kSubtract;
  case 11U:
    return VectorBinaryOperation::kMultiply;
  case 12U:
    return VectorBinaryOperation::kDivide;
  case 13U:
    return VectorBinaryOperation::kRemainder;
  case 14U:
    return VectorBinaryOperation::kCoalesce;
  case 15U:
    return VectorBinaryOperation::kTimeBucket;
  default:
    return std::nullopt;
  }
}

[[nodiscard]] common::Status
validate_limits(const DistributedVectorPreGroupProgramDecodeLimits& l) {
  using namespace distributed_vector_pre_group_program_format;
  if (l.maximum_frame_length < kMinimumFrameLength ||
      l.maximum_frame_length > kMaximumFrameLength || l.maximum_expressions == 0U ||
      l.maximum_expressions > kMaximumExpressions || l.maximum_total_instructions == 0U ||
      l.maximum_total_instructions > kMaximumTotalInstructions ||
      l.maximum_instructions_per_expression == 0U ||
      l.maximum_instructions_per_expression > kMaximumVectorExpressionInstructions ||
      l.maximum_constant_payload_bytes > kMaximumConstantPayloadBytes) {
    return invalid("distributed pre-group program limits are invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<schema::LogicalType> decode_type(const EncodedLogicalType encoded) {
  auto kind = schema::logical_type_kind_from_code(encoded.code);
  if (!kind.has_value())
    return common::make_unexpected(corruption("distributed pre-group type code is invalid"));
  auto type = schema::LogicalType::create(*kind, encoded.parameter_0, encoded.parameter_1);
  if (!type.has_value())
    return common::make_unexpected(corruption("distributed pre-group type is invalid"));
  return *type;
}

[[nodiscard]] common::Result<std::size_t>
instruction_payload_size(const VectorExpressionInstruction& instruction) {
  if (const auto* constant = std::get_if<VectorConstantExpression>(&instruction)) {
    auto size = canonical_scalar_value_size(constant->value);
    if (!size.has_value())
      return common::make_unexpected(size.error());
    if (*size > distributed_vector_pre_group_program_format::kMaximumConstantPayloadBytes)
      return common::make_unexpected(exhausted("distributed pre-group constant exceeds its limit"));
    return *size;
  }
  return 0U;
}

[[nodiscard]] common::Status write_instruction(common::ByteWriter& writer,
                                               const VectorExpressionInstruction& instruction) {
  InstructionTag tag = InstructionTag::kInput;
  std::uint8_t flags = 0U;
  std::uint8_t operation = 0U;
  std::uint16_t type_code = 0U;
  std::uint16_t parameter_0 = 0U;
  std::uint16_t parameter_1 = 0U;
  std::uint32_t operand_0 = 0U;
  std::uint32_t operand_1 = 0U;
  std::vector<std::byte> payload;

  if (const auto* input = std::get_if<VectorInputExpression>(&instruction)) {
    if (input->input_column_ordinal > std::numeric_limits<std::uint32_t>::max())
      return invalid("distributed pre-group input ordinal is too large");
    tag = InstructionTag::kInput;
    flags = input->nullable ? kFlag : 0U;
    type_code = input->type.code();
    parameter_0 = input->type.parameter_0();
    parameter_1 = input->type.parameter_1();
    operand_0 = static_cast<std::uint32_t>(input->input_column_ordinal);
  } else if (const auto* constant = std::get_if<VectorConstantExpression>(&instruction)) {
    if (!constant->value.type().has_value())
      return invalid("distributed pre-group constant is untyped");
    tag = InstructionTag::kConstant;
    flags = constant->value.is_null() ? kFlag : 0U;
    type_code = constant->value.type()->code();
    parameter_0 = constant->value.type()->parameter_0();
    parameter_1 = constant->value.type()->parameter_1();
    auto encoded = encode_canonical_scalar_value(constant->value);
    if (!encoded.has_value())
      return encoded.error();
    payload = std::move(*encoded);
  } else if (const auto* unary = std::get_if<VectorUnaryExpression>(&instruction)) {
    if (unary->operand_instruction > std::numeric_limits<std::uint32_t>::max())
      return invalid("distributed pre-group unary operand is too large");
    const auto code = unary_code(unary->operation);
    if (!code.has_value())
      return invalid("distributed pre-group unary operation is invalid");
    tag = InstructionTag::kUnary;
    operation = *code;
    operand_0 = static_cast<std::uint32_t>(unary->operand_instruction);
  } else if (const auto* cast = std::get_if<VectorCastExpression>(&instruction)) {
    if (cast->operand_instruction > std::numeric_limits<std::uint32_t>::max())
      return invalid("distributed pre-group cast operand is too large");
    tag = InstructionTag::kCast;
    type_code = cast->target_type.code();
    parameter_0 = cast->target_type.parameter_0();
    parameter_1 = cast->target_type.parameter_1();
    operand_0 = static_cast<std::uint32_t>(cast->operand_instruction);
  } else {
    const auto& binary = std::get<VectorBinaryExpression>(instruction);
    if (binary.left_instruction > std::numeric_limits<std::uint32_t>::max() ||
        binary.right_instruction > std::numeric_limits<std::uint32_t>::max())
      return invalid("distributed pre-group binary operand is too large");
    const auto code = binary_code(binary.operation);
    if (!code.has_value())
      return invalid("distributed pre-group binary operation is invalid");
    tag = InstructionTag::kBinary;
    operation = *code;
    operand_0 = static_cast<std::uint32_t>(binary.left_instruction);
    operand_1 = static_cast<std::uint32_t>(binary.right_instruction);
  }

  common::Status status = writer.write_u8(static_cast<std::uint8_t>(tag));
  if (status.is_ok())
    status = writer.write_u8(flags);
  if (status.is_ok())
    status = writer.write_u8(operation);
  if (status.is_ok())
    status = writer.zero_fill(1U);
  if (status.is_ok())
    status = writer.write_u16_le(type_code);
  if (status.is_ok())
    status = writer.write_u16_le(parameter_0);
  if (status.is_ok())
    status = writer.write_u16_le(parameter_1);
  if (status.is_ok())
    status = writer.zero_fill(2U);
  if (status.is_ok())
    status = writer.write_u32_le(operand_0);
  if (status.is_ok())
    status = writer.write_u32_le(operand_1);
  if (status.is_ok())
    status = writer.write_u32_le(static_cast<std::uint32_t>(payload.size()));
  if (status.is_ok())
    status = writer.write_u32_le(common::crc32c(payload));
  if (status.is_ok())
    status = writer.zero_fill(4U);
  if (status.is_ok())
    status = writer.write_exact(payload);
  return status;
}

} // namespace

EncodedDistributedVectorPreGroupProgram::EncodedDistributedVectorPreGroupProgram(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedVectorPreGroupProgram::bytes() const noexcept {
  return bytes_;
}

common::Status
validate_distributed_vector_pre_group_program(const DistributedVectorPreGroupProgram& program) {
  using namespace distributed_vector_pre_group_program_format;
  if (program.outputs.empty() || program.outputs.size() > kMaximumExpressions)
    return invalid("distributed pre-group output count is invalid");
  std::size_t total_instructions = 0U;
  for (const VectorExpression& expression : program.outputs) {
    if (expression.instructions().empty() ||
        expression.instructions().size() > kMaximumVectorExpressionInstructions)
      return invalid("distributed pre-group expression instruction count is invalid");
    if (expression.retained_configuration_bytes() > kMaximumFrameLength)
      return exhausted("distributed pre-group expression configuration exceeds its limit");
    const auto next = common::checked_add(total_instructions, expression.instructions().size());
    if (!next.has_value() || *next > kMaximumTotalInstructions)
      return exhausted("distributed pre-group instruction count exceeds its limit");
    total_instructions = *next;
    for (const VectorExpressionInstruction& instruction : expression.instructions()) {
      auto payload_size = instruction_payload_size(instruction);
      if (!payload_size.has_value())
        return payload_size.error();
    }
  }
  return common::Status::ok();
}

common::Result<EncodedDistributedVectorPreGroupProgram>
encode_distributed_vector_pre_group_program(const DistributedVectorPreGroupProgram& program) {
  using namespace distributed_vector_pre_group_program_format;
  const common::Status validation = validate_distributed_vector_pre_group_program(program);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  try {
    std::size_t payload_length = 0U;
    std::size_t total_instructions = 0U;
    for (const VectorExpression& expression : program.outputs) {
      auto next = common::checked_add(payload_length, kExpressionHeaderLength);
      if (!next.has_value())
        return common::make_unexpected(exhausted("distributed pre-group size overflowed"));
      payload_length = *next;
      total_instructions += expression.instructions().size();
      for (const VectorExpressionInstruction& instruction : expression.instructions()) {
        auto size = instruction_payload_size(instruction);
        if (!size.has_value())
          return common::make_unexpected(size.error());
        next = common::checked_add(payload_length, kInstructionHeaderLength + *size);
        if (!next.has_value())
          return common::make_unexpected(exhausted("distributed pre-group size overflowed"));
        payload_length = *next;
      }
    }
    const auto frame_length = common::checked_add(kHeaderLength + kTrailerLength, payload_length);
    if (!frame_length.has_value() || *frame_length > kMaximumFrameLength)
      return common::make_unexpected(exhausted("distributed pre-group frame exceeds its limit"));
    std::vector<std::byte> bytes(*frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(*frame_length);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(program.outputs.size()));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(total_instructions));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(payload_length));
    if (status.is_ok())
      status = writer.zero_fill(28U);
    for (const VectorExpression& expression : program.outputs) {
      std::size_t expression_length = 0U;
      for (const VectorExpressionInstruction& instruction : expression.instructions()) {
        auto size = instruction_payload_size(instruction);
        if (!size.has_value())
          return common::make_unexpected(size.error());
        expression_length += kInstructionHeaderLength + *size;
      }
      const std::size_t expression_start = writer.offset() + kExpressionHeaderLength;
      if (status.is_ok())
        status = writer.write_u32_le(static_cast<std::uint32_t>(expression.instructions().size()));
      if (status.is_ok())
        status = writer.write_u32_le(static_cast<std::uint32_t>(expression_length));
      if (status.is_ok())
        status = writer.zero_fill(8U);
      for (const VectorExpressionInstruction& instruction : expression.instructions()) {
        if (status.is_ok())
          status = write_instruction(writer, instruction);
      }
      if (status.is_ok() && writer.offset() != expression_start + expression_length)
        status = invalid("distributed pre-group expression layout failed");
      if (status.is_ok()) {
        common::ByteWriter crc_writer{
            common::MutableByteView{bytes}.subspan(expression_start - 8U, 4U)};
        status = crc_writer.write_u32_le(
            common::crc32c(common::ByteView{bytes}.subspan(expression_start, expression_length)));
      }
    }
    if (status.is_ok())
      status = writer.zero_fill(kTrailerLength);
    if (!status.is_ok())
      return common::make_unexpected(status);
    if (!writer.full())
      return common::make_unexpected(invalid("distributed pre-group frame layout failed"));
    common::ByteWriter payload_crc_writer{
        common::MutableByteView{bytes}.subspan(kPayloadCrcOffset, 4U)};
    status = payload_crc_writer.write_u32_le(
        common::crc32c(common::ByteView{bytes}.subspan(kHeaderLength, payload_length)));
    common::ByteWriter header_crc_writer{
        common::MutableByteView{bytes}.subspan(kHeaderCrcOffset, 4U)};
    if (status.is_ok())
      status = header_crc_writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    common::ByteWriter frame_crc_writer{common::MutableByteView{bytes}.last(kTrailerLength)};
    if (status.is_ok())
      status = frame_crc_writer.write_u32_le(
          common::crc32c(common::ByteView{bytes}.first(bytes.size() - kTrailerLength)));
    if (!status.is_ok())
      return common::make_unexpected(status);
    return EncodedDistributedVectorPreGroupProgram{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed pre-group encode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed pre-group encode exceeds limits"));
  }
}

common::Result<DistributedVectorPreGroupProgram> decode_distributed_vector_pre_group_program_exact(
    const common::ByteView bytes, const DistributedVectorPreGroupProgramDecodeLimits limits) {
  using namespace distributed_vector_pre_group_program_format;
  const common::Status limit_status = validate_limits(limits);
  if (!limit_status.is_ok())
    return common::make_unexpected(limit_status);
  if (bytes.size() < kMinimumFrameLength || bytes.size() > kMaximumFrameLength)
    return common::make_unexpected(corruption("distributed pre-group frame length is invalid"));
  if (bytes.size() > limits.maximum_frame_length)
    return common::make_unexpected(exhausted("distributed pre-group frame exceeds caller limit"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("distributed pre-group magic is invalid"));
  common::ByteReader stored_header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto stored_header_crc = stored_header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)))
    return common::make_unexpected(corruption("distributed pre-group header checksum differs"));
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto expression_count = reader.read_u32_le();
  const auto total_instructions = reader.read_u32_le();
  const auto payload_length = reader.read_u32_le();
  const auto payload_crc = reader.read_u32_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_exact(20U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !expression_count.has_value() ||
      !total_instructions.has_value() || !payload_length.has_value() || !payload_crc.has_value() ||
      !reserved.has_value())
    return common::make_unexpected(corruption("distributed pre-group header is truncated"));
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "distributed pre-group version is unsupported"});
  const auto minimum_payload =
      common::checked_multiply(static_cast<std::size_t>(*expression_count),
                               kExpressionHeaderLength + kInstructionHeaderLength);
  if (*header_length != kHeaderLength || *frame_length != bytes.size() || *expression_count == 0U ||
      *expression_count > kMaximumExpressions || *total_instructions < *expression_count ||
      *total_instructions > kMaximumTotalInstructions || !minimum_payload.has_value() ||
      *payload_length < *minimum_payload ||
      *payload_length != bytes.size() - kHeaderLength - kTrailerLength ||
      !std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{}; }))
    return common::make_unexpected(corruption("distributed pre-group header is invalid"));
  if (*expression_count > limits.maximum_expressions ||
      *total_instructions > limits.maximum_total_instructions)
    return common::make_unexpected(
        exhausted("distributed pre-group program exceeds caller limits"));
  common::ByteReader trailer{bytes.last(kTrailerLength)};
  const auto frame_crc = trailer.read_u32_le();
  if (!frame_crc.has_value() ||
      *frame_crc != common::crc32c(bytes.first(bytes.size() - kTrailerLength)))
    return common::make_unexpected(corruption("distributed pre-group frame checksum differs"));
  const common::ByteView payload = bytes.subspan(kHeaderLength, *payload_length);
  if (*payload_crc != common::crc32c(payload))
    return common::make_unexpected(corruption("distributed pre-group payload checksum differs"));

  try {
    DistributedVectorPreGroupProgram program;
    program.outputs.reserve(*expression_count);
    common::ByteReader payload_reader{payload};
    std::size_t observed_instructions = 0U;
    for (std::uint32_t expression_index = 0U; expression_index < *expression_count;
         ++expression_index) {
      const auto instruction_count = payload_reader.read_u32_le();
      const auto expression_length = payload_reader.read_u32_le();
      const auto expression_crc = payload_reader.read_u32_le();
      const auto expression_reserved = payload_reader.read_u32_le();
      if (!instruction_count.has_value() || !expression_length.has_value() ||
          !expression_crc.has_value() || !expression_reserved.has_value() ||
          *instruction_count == 0U || *instruction_count > kMaximumVectorExpressionInstructions ||
          *expression_reserved != 0U ||
          *expression_length <
              static_cast<std::size_t>(*instruction_count) * kInstructionHeaderLength)
        return common::make_unexpected(
            corruption("distributed pre-group expression header is invalid"));
      if (*instruction_count > limits.maximum_instructions_per_expression)
        return common::make_unexpected(
            exhausted("distributed pre-group expression exceeds caller instruction limit"));
      const auto expression_bytes = payload_reader.read_exact(*expression_length);
      if (!expression_bytes.has_value() || *expression_crc != common::crc32c(*expression_bytes))
        return common::make_unexpected(corruption("distributed pre-group expression is damaged"));
      observed_instructions += *instruction_count;
      if (observed_instructions > *total_instructions)
        return common::make_unexpected(
            corruption("distributed pre-group instruction total differs"));
      common::ByteReader expression_reader{*expression_bytes};
      std::vector<VectorExpressionInstruction> instructions;
      instructions.reserve(*instruction_count);
      for (std::uint32_t instruction_index = 0U; instruction_index < *instruction_count;
           ++instruction_index) {
        const auto tag = expression_reader.read_u8();
        const auto flags = expression_reader.read_u8();
        const auto operation = expression_reader.read_u8();
        const auto reserved_0 = expression_reader.read_u8();
        const auto type_code = expression_reader.read_u16_le();
        const auto parameter_0 = expression_reader.read_u16_le();
        const auto parameter_1 = expression_reader.read_u16_le();
        const auto reserved_1 = expression_reader.read_u16_le();
        const auto operand_0 = expression_reader.read_u32_le();
        const auto operand_1 = expression_reader.read_u32_le();
        const auto constant_length = expression_reader.read_u32_le();
        const auto constant_crc = expression_reader.read_u32_le();
        const auto reserved_2 = expression_reader.read_u32_le();
        if (!tag.has_value() || !flags.has_value() || !operation.has_value() ||
            !reserved_0.has_value() || !type_code.has_value() || !parameter_0.has_value() ||
            !parameter_1.has_value() || !reserved_1.has_value() || !operand_0.has_value() ||
            !operand_1.has_value() || !constant_length.has_value() || !constant_crc.has_value() ||
            !reserved_2.has_value() || *reserved_0 != 0U || *reserved_1 != 0U ||
            *reserved_2 != 0U || *constant_length > kMaximumConstantPayloadBytes)
          return common::make_unexpected(
              corruption("distributed pre-group instruction header is invalid"));
        if (*constant_length > limits.maximum_constant_payload_bytes)
          return common::make_unexpected(
              exhausted("distributed pre-group constant exceeds caller limit"));
        const auto constant = expression_reader.read_exact(*constant_length);
        if (!constant.has_value() || *constant_crc != common::crc32c(*constant))
          return common::make_unexpected(corruption("distributed pre-group constant is damaged"));

        if (*tag == static_cast<std::uint8_t>(InstructionTag::kInput)) {
          if ((*flags & ~kFlag) != 0U || *operation != 0U || *operand_1 != 0U ||
              *constant_length != 0U)
            return common::make_unexpected(
                corruption("distributed pre-group input is noncanonical"));
          auto type = decode_type({*type_code, *parameter_0, *parameter_1});
          if (!type.has_value())
            return common::make_unexpected(type.error());
          instructions.emplace_back(
              VectorInputExpression{*operand_0, *type, (*flags & kFlag) != 0U});
        } else if (*tag == static_cast<std::uint8_t>(InstructionTag::kConstant)) {
          if ((*flags & ~kFlag) != 0U || *operation != 0U || *operand_0 != 0U || *operand_1 != 0U)
            return common::make_unexpected(
                corruption("distributed pre-group constant is noncanonical"));
          auto type = decode_type({*type_code, *parameter_0, *parameter_1});
          if (!type.has_value())
            return common::make_unexpected(type.error());
          auto value = decode_canonical_scalar_value(*type, (*flags & kFlag) != 0U, *constant);
          if (!value.has_value()) {
            if (value.error().code() == common::StatusCode::kResourceExhausted)
              return common::make_unexpected(value.error());
            return common::make_unexpected(corruption("distributed pre-group constant is invalid"));
          }
          instructions.emplace_back(VectorConstantExpression{std::move(*value)});
        } else if (*tag == static_cast<std::uint8_t>(InstructionTag::kUnary)) {
          const auto unary = unary_operation(*operation);
          if (*flags != 0U || !unary.has_value() || *type_code != 0U || *parameter_0 != 0U ||
              *parameter_1 != 0U || *operand_1 != 0U || *constant_length != 0U)
            return common::make_unexpected(
                corruption("distributed pre-group unary is noncanonical"));
          instructions.emplace_back(VectorUnaryExpression{*unary, *operand_0});
        } else if (*tag == static_cast<std::uint8_t>(InstructionTag::kCast)) {
          if (*flags != 0U || *operation != 0U || *operand_1 != 0U || *constant_length != 0U)
            return common::make_unexpected(
                corruption("distributed pre-group cast is noncanonical"));
          auto type = decode_type({*type_code, *parameter_0, *parameter_1});
          if (!type.has_value())
            return common::make_unexpected(type.error());
          instructions.emplace_back(VectorCastExpression{*operand_0, *type});
        } else if (*tag == static_cast<std::uint8_t>(InstructionTag::kBinary)) {
          const auto binary = binary_operation(*operation);
          if (*flags != 0U || !binary.has_value() || *type_code != 0U || *parameter_0 != 0U ||
              *parameter_1 != 0U || *constant_length != 0U)
            return common::make_unexpected(
                corruption("distributed pre-group binary is noncanonical"));
          instructions.emplace_back(VectorBinaryExpression{*binary, *operand_0, *operand_1});
        } else {
          return common::make_unexpected(
              corruption("distributed pre-group instruction tag is invalid"));
        }
      }
      if (!expression_reader.empty())
        return common::make_unexpected(
            corruption("distributed pre-group expression has trailing bytes"));
      auto expression = VectorExpression::create(
          std::move(instructions),
          {.maximum_instructions = limits.maximum_instructions_per_expression,
           .maximum_retained_configuration_bytes = kMaximumFrameLength});
      if (!expression.has_value()) {
        if (expression.error().code() == common::StatusCode::kResourceExhausted)
          return common::make_unexpected(expression.error());
        return common::make_unexpected(corruption("distributed pre-group expression is invalid"));
      }
      program.outputs.push_back(std::move(*expression));
    }
    if (!payload_reader.empty() || observed_instructions != *total_instructions)
      return common::make_unexpected(
          corruption("distributed pre-group payload has trailing bytes"));
    return program;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed pre-group decode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed pre-group decode exceeds limits"));
  }
}

} // namespace chronos::query
