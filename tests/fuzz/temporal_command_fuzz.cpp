#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/columnar/columnar_batch_format.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/query/temporal_command.hpp"
#include "chronos/wal/application.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ranges>
#include <span>
#include <vector>

namespace {

[[nodiscard]] std::uint8_t input_byte(const chronos::common::ByteView input,
                                      const std::size_t offset, const std::uint8_t fallback = 0U) {
  return offset < input.size() ? std::to_integer<std::uint8_t>(input[offset]) : fallback;
}

[[nodiscard]] std::int64_t signed_input_byte(const chronos::common::ByteView input,
                                             const std::size_t offset) {
  const std::uint8_t value = input_byte(input, offset);
  return value <= 127U ? static_cast<std::int64_t>(value) : static_cast<std::int64_t>(value) - 256;
}

void store_u32_le(const chronos::common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void refresh_nested_batch_checksums(const chronos::common::MutableByteView batch) {
  using namespace chronos::columnar;
  if (batch.size() < format::kBatchHeaderLength + format::kBatchTrailerLength)
    std::abort();
  store_u32_le(
      batch, format::kHeaderCrc32cOffset,
      chronos::common::crc32c(chronos::common::ByteView{batch}.first(format::kHeaderCrc32cOffset)));
  const std::size_t trailer = batch.size() - format::kBatchTrailerLength;
  store_u32_le(batch, trailer,
               chronos::common::crc32c(chronos::common::ByteView{batch}.first(trailer)));
}

void refresh_temporal_checksums(std::vector<std::byte>& bytes, const std::size_t batch_size,
                                const std::size_t metadata_size, const bool refresh_nested_batch) {
  using namespace chronos;
  constexpr std::size_t kBatchCrcOffset = 80U;
  constexpr std::size_t kMetadataCrcOffset = 84U;
  constexpr std::size_t kHeaderCrcOffset = 88U;
  const std::size_t body_offset = wal::kApplicationBodyOffset;
  const std::size_t batch_offset = body_offset + query::kTemporalCommandHeaderSize;
  const std::size_t metadata_offset = batch_offset + batch_size;
  if (metadata_offset > bytes.size() || metadata_size > bytes.size() - metadata_offset ||
      bytes.size() <
          body_offset + query::kTemporalCommandHeaderSize + query::kTemporalCommandTrailerSize)
    std::abort();
  common::MutableByteView output{bytes};
  const common::MutableByteView batch = output.subspan(batch_offset, batch_size);
  if (refresh_nested_batch)
    refresh_nested_batch_checksums(batch);
  store_u32_le(output, body_offset + kBatchCrcOffset, common::crc32c(batch));
  store_u32_le(output, body_offset + kMetadataCrcOffset,
               common::crc32c(output.subspan(metadata_offset, metadata_size)));
  store_u32_le(output, body_offset + kHeaderCrcOffset, 0U);
  store_u32_le(output, body_offset + kHeaderCrcOffset,
               common::crc32c(common::ByteView{output}.subspan(body_offset,
                                                               query::kTemporalCommandHeaderSize)));
  const std::size_t trailer = bytes.size() - query::kTemporalCommandTrailerSize;
  store_u32_le(
      output, trailer,
      common::crc32c(common::ByteView{output}.subspan(body_offset, trailer - body_offset)));
}

void exercise(const chronos::common::ByteView bytes) {
  using namespace chronos::query;
  const auto decoded = decode_temporal_command_v1(bytes);
  if (!decoded.has_value())
    return;
  if (decoded->batch().row_count() == 0U ||
      decoded->batch().row_count() != decoded->mutations().size())
    std::abort();
  for (const DecodedTemporalMutationDescriptor& mutation : decoded->mutations()) {
    if (mutation.logical_identity.empty() || mutation.kind < TemporalMutationKind::kOriginal ||
        mutation.kind > TemporalMutationKind::kTombstone)
      std::abort();
  }
  for (std::size_t left = 0U; left < decoded->mutations().size(); ++left) {
    for (std::size_t right = left + 1U; right < decoded->mutations().size(); ++right) {
      if (std::ranges::equal(decoded->mutations()[left].logical_identity,
                             decoded->mutations()[right].logical_identity))
        std::abort();
    }
  }
}

void exercise_structured(const chronos::common::ByteView input) {
  using namespace chronos;
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns());
  if (!batch.has_value())
    std::abort();
  const auto first_kind = static_cast<query::TemporalMutationKind>(
      1U + static_cast<std::uint8_t>(input_byte(input, 0U) % 4U));
  const auto second_kind = static_cast<query::TemporalMutationKind>(
      1U + static_cast<std::uint8_t>(input_byte(input, 1U) % 4U));
  const std::vector<query::TemporalMutationDescriptor> mutations{
      {{std::byte{1U}, static_cast<std::byte>(input_byte(input, 2U))},
       signed_input_byte(input, 3U),
       signed_input_byte(input, 4U),
       first_kind},
      {{std::byte{2U}, static_cast<std::byte>(input_byte(input, 5U))},
       signed_input_byte(input, 6U),
       signed_input_byte(input, 7U),
       second_kind}};
  const std::int64_t commit_time = signed_input_byte(input, 8U);
  auto encoded = query::encode_temporal_command_v1(*batch, mutations, commit_time);
  if (!encoded.has_value())
    std::abort();
  exercise(encoded->bytes());
  auto decoded = query::decode_temporal_command_v1(encoded->bytes());
  if (!decoded.has_value() || decoded->system_commit_time_ns() != commit_time ||
      decoded->mutations().size() != mutations.size())
    std::abort();

  query::TemporalCommandLimits lower_limits;
  lower_limits.maximum_mutations = 1U;
  if (query::decode_temporal_command_v1(encoded->bytes(), lower_limits).has_value())
    std::abort();

  auto encoded_batch = columnar::encode_columnar_batch_v1(*batch);
  if (!encoded_batch.has_value())
    std::abort();
  std::size_t metadata_size = 0U;
  for (const query::TemporalMutationDescriptor& mutation : mutations)
    metadata_size += query::kTemporalMutationMetadataSize + mutation.logical_identity.size();
  std::vector<std::byte> mutated{encoded->bytes().begin(), encoded->bytes().end()};
  const std::size_t selector = static_cast<std::size_t>(input_byte(input, 9U)) |
                               (static_cast<std::size_t>(input_byte(input, 10U)) << 8U);
  const std::size_t offset = selector % mutated.size();
  const std::uint8_t supplied_mask = input_byte(input, 11U, 1U);
  mutated[offset] ^= static_cast<std::byte>(supplied_mask == 0U ? 1U : supplied_mask);
  const std::uint8_t repair_mode = input_byte(input, 12U) & 3U;
  if (repair_mode != 0U)
    refresh_temporal_checksums(mutated, encoded_batch->size(), metadata_size, repair_mode >= 2U);
  exercise(mutated);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes =
      chronos::common::byte_view(std::span<const std::uint8_t>{data, size});
  exercise(bytes);
  exercise_structured(bytes);
  return 0;
}
