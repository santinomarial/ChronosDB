#include "chronos/query/temporal_command.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <set>
#include <span>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'N'}, std::byte{'T'}, std::byte{'M'},
                                           std::byte{'P'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}
[[nodiscard]] common::Status corruption(const char* message) {
  return common::Status{common::StatusCode::kCorruption, message};
}

[[nodiscard]] bool valid_kind(const TemporalMutationKind kind) {
  return kind >= TemporalMutationKind::kOriginal && kind <= TemporalMutationKind::kTombstone;
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  return value;
}

} // namespace

EncodedTemporalCommand::EncodedTemporalCommand(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}
common::ByteView EncodedTemporalCommand::bytes() const noexcept {
  return bytes_;
}

DecodedTemporalCommandView::DecodedTemporalCommandView(
    columnar::DecodedColumnarBatchView batch,
    std::vector<DecodedTemporalMutationDescriptor> mutations,
    const std::int64_t system_commit_time_ns) noexcept
    : batch_(std::move(batch)), mutations_(std::move(mutations)),
      system_commit_time_ns_(system_commit_time_ns) {}
const columnar::DecodedColumnarBatchView& DecodedTemporalCommandView::batch() const noexcept {
  return batch_;
}
std::span<const DecodedTemporalMutationDescriptor>
DecodedTemporalCommandView::mutations() const noexcept {
  return mutations_;
}
std::int64_t DecodedTemporalCommandView::system_commit_time_ns() const noexcept {
  return system_commit_time_ns_;
}

common::Result<EncodedTemporalCommand>
encode_temporal_command_v1(const columnar::OwnedColumnarBatch& batch,
                           const std::vector<TemporalMutationDescriptor>& mutations,
                           const std::int64_t system_commit_time_ns,
                           const TemporalCommandLimits limits) {
  if (mutations.empty() || mutations.size() != batch.row_count() ||
      mutations.size() > limits.maximum_mutations || limits.maximum_identity_bytes == 0U ||
      limits.maximum_metadata_bytes == 0U) {
    return common::make_unexpected(invalid("temporal command mutation count or limits invalid"));
  }
  std::size_t metadata_size = 0U;
  std::set<std::vector<std::byte>> identities;
  for (const TemporalMutationDescriptor& mutation : mutations) {
    if (!valid_kind(mutation.kind) || mutation.logical_identity.empty() ||
        mutation.logical_identity.size() > limits.maximum_identity_bytes) {
      return common::make_unexpected(invalid("temporal mutation descriptor is invalid"));
    }
    if (!identities.insert(mutation.logical_identity).second)
      return common::make_unexpected(invalid("temporal command repeats a logical identity"));
    auto size =
        common::checked_add(kTemporalMutationMetadataSize, mutation.logical_identity.size());
    if (!size.has_value())
      return common::make_unexpected(invalid("temporal metadata size overflows"));
    auto total = common::checked_add(metadata_size, *size);
    if (!total.has_value() || *total > limits.maximum_metadata_bytes)
      return common::make_unexpected(invalid("temporal metadata exceeds configured limit"));
    metadata_size = *total;
  }
  auto encoded_batch = columnar::encode_columnar_batch_v1(batch);
  if (!encoded_batch.has_value())
    return common::make_unexpected(encoded_batch.error());
  auto body_size = common::checked_add(kTemporalCommandHeaderSize, encoded_batch->size());
  if (body_size.has_value())
    body_size = common::checked_add(*body_size, metadata_size);
  if (body_size.has_value())
    body_size = common::checked_add(*body_size, kTemporalCommandTrailerSize);
  if (!body_size.has_value() || *body_size > std::numeric_limits<std::uint32_t>::max() ||
      encoded_batch->size() > std::numeric_limits<std::uint32_t>::max() ||
      metadata_size > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(invalid("temporal command size exceeds v1 bounds"));
  }
  const std::size_t command_size = body_size.value();
  std::vector<std::byte> body(command_size, std::byte{0U});
  const std::size_t metadata_offset = kTemporalCommandHeaderSize + encoded_batch->size();
  std::copy(encoded_batch->bytes().begin(), encoded_batch->bytes().end(),
            body.begin() + static_cast<std::ptrdiff_t>(kTemporalCommandHeaderSize));
  common::ByteWriter metadata{
      common::MutableByteView{body}.subspan(metadata_offset, metadata_size)};
  for (const TemporalMutationDescriptor& mutation : mutations) {
    for (const common::Status& status :
         {metadata.write_u8(static_cast<std::uint8_t>(mutation.kind)), metadata.zero_fill(3U),
          metadata.write_u32_le(static_cast<std::uint32_t>(mutation.logical_identity.size())),
          metadata.write_i64_le(mutation.event_time_ns),
          metadata.write_i64_le(mutation.receive_time_ns),
          metadata.write_exact(mutation.logical_identity)}) {
      if (!status.is_ok())
        return common::make_unexpected(status);
    }
  }
  common::ByteWriter header{common::MutableByteView{body}.first(kTemporalCommandHeaderSize)};
  for (const common::Status& status :
       {header.write_exact(kMagic), header.write_u16_le(kMajor), header.write_u16_le(kMinor),
        header.write_u32_le(kTemporalCommandHeaderSize),
        header.write_u32_le(static_cast<std::uint32_t>(command_size)),
        header.write_u32_le(static_cast<std::uint32_t>(encoded_batch->size())),
        header.write_u32_le(static_cast<std::uint32_t>(mutations.size())),
        header.write_u32_le(static_cast<std::uint32_t>(metadata_size)),
        header.write_exact(batch.schema().table_id().uuid().bytes()),
        header.write_exact(batch.schema().schema_id().uuid().bytes()),
        header.write_u64_le(batch.schema().version().value()),
        header.write_i64_le(system_commit_time_ns),
        header.write_u32_le(common::crc32c(encoded_batch->bytes())),
        header.write_u32_le(
            common::crc32c(common::ByteView{body}.subspan(metadata_offset, metadata_size))),
        header.write_u32_le(0U), header.write_u32_le(0U)}) {
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  common::ByteWriter header_crc{common::MutableByteView{body}.subspan(88U, 4U)};
  if (auto status = header_crc.write_u32_le(
          common::crc32c(common::ByteView{body}.first(kTemporalCommandHeaderSize)));
      !status.is_ok())
    return common::make_unexpected(status);
  common::ByteWriter trailer{common::MutableByteView{body}.last(kTemporalCommandTrailerSize)};
  if (auto status = trailer.write_u32_le(
          common::crc32c(common::ByteView{body}.first(body.size() - kTemporalCommandTrailerSize)));
      !status.is_ok())
    return common::make_unexpected(status);
  auto envelope = wal::encode_application_payload({.application_format = kTemporalApplicationFormat,
                                                   .application_kind = kTemporalApplicationKind,
                                                   .application_flags = 0U,
                                                   .application_body = body});
  if (!envelope.has_value())
    return common::make_unexpected(envelope.error());
  return EncodedTemporalCommand{
      std::vector<std::byte>{envelope->bytes().begin(), envelope->bytes().end()}};
}

common::Result<DecodedTemporalCommandView>
decode_temporal_command_v1(const common::ByteView bytes, const TemporalCommandLimits limits) {
  auto envelope = wal::decode_application_payload(bytes);
  if (!envelope.has_value())
    return common::make_unexpected(envelope.error());
  if (envelope->application_format != kTemporalApplicationFormat ||
      envelope->application_kind != kTemporalApplicationKind || envelope->application_flags != 0U)
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "application payload is not temporal v1"});
  const common::ByteView body = envelope->application_body;
  if (body.size() < kTemporalCommandHeaderSize + kTemporalCommandTrailerSize)
    return common::make_unexpected(corruption("temporal command is shorter than framing"));
  std::array<std::byte, kTemporalCommandHeaderSize> checked_header{};
  std::copy_n(body.begin(), kTemporalCommandHeaderSize, checked_header.begin());
  std::fill(checked_header.begin() + 88, checked_header.begin() + 92, std::byte{0U});
  if (common::crc32c(checked_header) != load_u32(body, 88U) ||
      common::crc32c(body.first(body.size() - kTemporalCommandTrailerSize)) !=
          load_u32(body, body.size() - kTemporalCommandTrailerSize)) {
    return common::make_unexpected(corruption("temporal command checksum mismatch"));
  }
  common::ByteReader header{body.first(kTemporalCommandHeaderSize)};
  auto magic = header.read_exact(8U);
  auto major = header.read_u16_le();
  auto minor = header.read_u16_le();
  auto header_size = header.read_u32_le();
  auto total_size = header.read_u32_le();
  auto batch_size = header.read_u32_le();
  auto count = header.read_u32_le();
  auto metadata_size = header.read_u32_le();
  auto table = header.read_exact(16U);
  auto schema = header.read_exact(16U);
  auto schema_version = header.read_u64_le();
  auto commit_time = header.read_i64_le();
  auto batch_crc = header.read_u32_le();
  auto metadata_crc = header.read_u32_le();
  auto ignored_header_crc = header.read_u32_le();
  auto reserved = header.read_u32_le();
  static_cast<void>(ignored_header_crc);
  if (!magic || !major || !minor || !header_size || !total_size || !batch_size || !count ||
      !metadata_size || !table || !schema || !schema_version || !commit_time || !batch_crc ||
      !metadata_crc || !reserved || !std::ranges::equal(*magic, kMagic) || *major != kMajor ||
      *minor != kMinor || *header_size != kTemporalCommandHeaderSize ||
      *total_size != body.size() || *count == 0U || *count > limits.maximum_mutations ||
      *metadata_size > limits.maximum_metadata_bytes || *reserved != 0U ||
      *batch_size > body.size() - kTemporalCommandHeaderSize - kTemporalCommandTrailerSize ||
      *metadata_size !=
          body.size() - kTemporalCommandHeaderSize - *batch_size - kTemporalCommandTrailerSize) {
    return common::make_unexpected(corruption("temporal command header is invalid"));
  }
  const common::ByteView batch_bytes = body.subspan(kTemporalCommandHeaderSize, *batch_size);
  const common::ByteView metadata =
      body.subspan(kTemporalCommandHeaderSize + *batch_size, *metadata_size);
  if (common::crc32c(batch_bytes) != *batch_crc || common::crc32c(metadata) != *metadata_crc)
    return common::make_unexpected(corruption("temporal command payload checksum mismatch"));
  auto batch = columnar::decode_columnar_batch_v1_exact(batch_bytes, limits.batch);
  if (!batch.has_value())
    return common::make_unexpected(batch.error().status());
  if (batch->row_count() != *count || batch->schema_version().value() != *schema_version ||
      !std::ranges::equal(*table, batch->table_id().uuid().bytes()) ||
      !std::ranges::equal(*schema, batch->schema_id().uuid().bytes())) {
    return common::make_unexpected(corruption("temporal command batch identity mismatch"));
  }
  common::ByteReader reader{metadata};
  std::vector<DecodedTemporalMutationDescriptor> mutations;
  mutations.reserve(*count);
  std::set<std::vector<std::byte>> identities;
  for (std::uint32_t index = 0U; index < *count; ++index) {
    auto kind = reader.read_u8();
    auto zeros = reader.read_exact(3U);
    auto identity_size = reader.read_u32_le();
    auto event_time = reader.read_i64_le();
    auto receive_time = reader.read_i64_le();
    if (!kind || !zeros || !identity_size || !event_time || !receive_time || *identity_size == 0U ||
        *identity_size > limits.maximum_identity_bytes ||
        std::ranges::any_of(*zeros, [](const std::byte value) { return value != std::byte{0U}; }))
      return common::make_unexpected(corruption("temporal mutation metadata is invalid"));
    auto identity = reader.read_exact(*identity_size);
    const auto mutation_kind = static_cast<TemporalMutationKind>(*kind);
    if (!identity || !valid_kind(mutation_kind))
      return common::make_unexpected(corruption("temporal mutation identity or kind is invalid"));
    if (!identities.emplace(identity->begin(), identity->end()).second)
      return common::make_unexpected(corruption("temporal command repeats a logical identity"));
    mutations.push_back({*identity, *event_time, *receive_time, mutation_kind});
  }
  if (!reader.empty())
    return common::make_unexpected(corruption("temporal metadata has trailing bytes"));
  return DecodedTemporalCommandView{std::move(*batch), std::move(mutations), *commit_time};
}

} // namespace chronos::query
