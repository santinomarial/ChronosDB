#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_PRE_GROUP_PROGRAM_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_PRE_GROUP_PROGRAM_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/vector_expression.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::query {

namespace distributed_vector_pre_group_program_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 64U;
inline constexpr std::size_t kExpressionHeaderLength = 16U;
inline constexpr std::size_t kInstructionHeaderLength = 32U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::uint32_t kMaximumExpressions = 4096U;
inline constexpr std::uint32_t kMaximumTotalInstructions = 65'536U;
inline constexpr std::uint32_t kMaximumConstantPayloadBytes = 1024U * 1024U;
inline constexpr std::size_t kMaximumFrameLength = std::size_t{4U} * 1024U * 1024U;
inline constexpr std::size_t kMinimumFrameLength =
    kHeaderLength + kExpressionHeaderLength + kInstructionHeaderLength + kTrailerLength;
} // namespace distributed_vector_pre_group_program_format

// Ordered, owned output expressions evaluated over the worker's exact source schema. Expression
// input ordinals name source-schema columns; the output ordinal is the expression's position here.
struct DistributedVectorPreGroupProgram {
  std::vector<VectorExpression> outputs;

  friend bool operator==(const DistributedVectorPreGroupProgram&,
                         const DistributedVectorPreGroupProgram&) = default;
};

struct DistributedVectorPreGroupProgramDecodeLimits {
  std::size_t maximum_frame_length{
      distributed_vector_pre_group_program_format::kMaximumFrameLength};
  std::uint32_t maximum_expressions{
      distributed_vector_pre_group_program_format::kMaximumExpressions};
  std::uint32_t maximum_total_instructions{
      distributed_vector_pre_group_program_format::kMaximumTotalInstructions};
  std::uint32_t maximum_instructions_per_expression{
      static_cast<std::uint32_t>(kMaximumVectorExpressionInstructions)};
  std::uint32_t maximum_constant_payload_bytes{
      distributed_vector_pre_group_program_format::kMaximumConstantPayloadBytes};
};

class EncodedDistributedVectorPreGroupProgram {
public:
  EncodedDistributedVectorPreGroupProgram() = delete;
  EncodedDistributedVectorPreGroupProgram(const EncodedDistributedVectorPreGroupProgram&) = delete;
  EncodedDistributedVectorPreGroupProgram&
  operator=(const EncodedDistributedVectorPreGroupProgram&) = delete;
  EncodedDistributedVectorPreGroupProgram(EncodedDistributedVectorPreGroupProgram&&) noexcept =
      default;
  EncodedDistributedVectorPreGroupProgram&
  operator=(EncodedDistributedVectorPreGroupProgram&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorPreGroupProgram(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorPreGroupProgram>
  encode_distributed_vector_pre_group_program(const DistributedVectorPreGroupProgram&);
};

[[nodiscard]] common::Status
validate_distributed_vector_pre_group_program(const DistributedVectorPreGroupProgram& program);

[[nodiscard]] common::Result<EncodedDistributedVectorPreGroupProgram>
encode_distributed_vector_pre_group_program(const DistributedVectorPreGroupProgram& program);

[[nodiscard]] common::Result<DistributedVectorPreGroupProgram>
decode_distributed_vector_pre_group_program_exact(
    common::ByteView bytes, DistributedVectorPreGroupProgramDecodeLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_PRE_GROUP_PROGRAM_HPP_
