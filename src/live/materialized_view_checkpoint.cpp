#include "chronos/live/materialized_view_checkpoint.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'M'},
                                           std::byte{'V'}, std::byte{'C'}, std::byte{'P'},
                                           std::byte{'1'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kTotalSizeOffset = 24U;
constexpr std::size_t kTabletIdOffset = 32U;
constexpr std::size_t kWalIdOffset = 48U;
constexpr std::size_t kAppliedSequenceOffset = 64U;
constexpr std::size_t kWidthOffset = 72U;
constexpr std::size_t kSlideOffset = 80U;
constexpr std::size_t kAllowedLatenessOffset = 88U;
constexpr std::size_t kMaximumWindowsOffset = 96U;
constexpr std::size_t kMaximumRowsOffset = 104U;
constexpr std::size_t kWatermarkOffset = 112U;
constexpr std::size_t kRowCountOffset = 120U;
constexpr std::size_t kWindowCountOffset = 128U;
constexpr std::size_t kContributionCountOffset = 136U;
constexpr std::size_t kHeaderCrcOffset = 144U;
constexpr std::size_t kReservedOffset = 148U;

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status unsupported(std::string message) {
  return common::Status{common::StatusCode::kNotSupported, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] bool valid_limits(const MaterializedViewCheckpointCodecLimits limits) noexcept {
  return limits.maximum_checkpoint_bytes >=
             kMaterializedViewCheckpointHeaderSize + kMaterializedViewCheckpointTrailerSize &&
         limits.maximum_checkpoint_bytes <= kMaximumMaterializedViewCheckpointSize &&
         limits.maximum_rows > 0U && limits.maximum_windows > 0U &&
         limits.maximum_window_contributions > 0U;
}

void store_u32(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  std::uint16_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes, const std::size_t offset) {
  std::uint64_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::int64_t load_i64(const common::ByteView bytes, const std::size_t offset) {
  return std::bit_cast<std::int64_t>(load_u64(bytes, offset));
}

[[nodiscard]] common::Result<WindowedMaterializedViewCheckpoint>
canonicalize(const WindowedMaterializedViewCheckpoint& checkpoint, const bool encoding) {
  try {
    auto restored = WindowedMaterializedView::restore(checkpoint);
    if (!restored.has_value()) {
      if (encoding && restored.error().code() == common::StatusCode::kCorruption) {
        return common::make_unexpected(invalid("materialized-view checkpoint is noncanonical"));
      }
      return common::make_unexpected(restored.error());
    }
    return restored->checkpoint();
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("materialized-view checkpoint allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint exceeded container limits"));
  }
}

[[nodiscard]] common::Result<std::pair<std::size_t, std::size_t>>
encoded_size(const WindowedMaterializedViewCheckpoint& checkpoint,
             const MaterializedViewCheckpointCodecLimits limits) {
  if (checkpoint.rows.size() > limits.maximum_rows ||
      checkpoint.windows.size() > limits.maximum_windows ||
      checkpoint.definition.maximum_rows > limits.maximum_rows ||
      checkpoint.definition.maximum_windows > limits.maximum_windows) {
    return common::make_unexpected(exhausted("materialized-view checkpoint counts exceed limits"));
  }
  std::size_t contribution_count = 0U;
  for (const MaterializedWindowCheckpoint& window : checkpoint.windows) {
    const auto next = common::checked_add(contribution_count, window.aggregate.rows.size());
    if (!next.has_value() || *next > limits.maximum_window_contributions) {
      return common::make_unexpected(
          exhausted("materialized-view checkpoint contribution count exceeds limit"));
    }
    contribution_count = *next;
  }
  const auto row_bytes =
      common::checked_multiply(checkpoint.rows.size(), kMaterializedViewCheckpointRowSize);
  const auto window_bytes =
      common::checked_multiply(checkpoint.windows.size(), kMaterializedViewCheckpointWindowSize);
  const auto contribution_bytes =
      common::checked_multiply(contribution_count, kMaterializedViewCheckpointRowSize);
  if (!row_bytes.has_value() || !window_bytes.has_value() || !contribution_bytes.has_value()) {
    return common::make_unexpected(exhausted("materialized-view checkpoint size overflow"));
  }
  auto total = common::checked_add(kMaterializedViewCheckpointHeaderSize, *row_bytes);
  total = total.has_value() ? common::checked_add(*total, *window_bytes) : std::nullopt;
  total = total.has_value() ? common::checked_add(*total, *contribution_bytes) : std::nullopt;
  total = total.has_value() ? common::checked_add(*total, kMaterializedViewCheckpointTrailerSize)
                            : std::nullopt;
  if (!total.has_value() || *total > limits.maximum_checkpoint_bytes) {
    return common::make_unexpected(exhausted("materialized-view checkpoint exceeds size limit"));
  }
  return std::pair{*total, contribution_count};
}

[[nodiscard]] common::Status write_row(common::ByteWriter& writer, const AggregateInput& row) {
  common::Status status = writer.write_u64_le(row.row_identity);
  if (status.is_ok())
    status = writer.write_i64_le(row.event_time);
  if (status.is_ok())
    status = writer.write_u64_le(row.source_order);
  if (status.is_ok())
    status = writer.write_float64_le(row.value);
  if (status.is_ok())
    status = writer.write_float64_le(row.weight);
  return status;
}

[[nodiscard]] common::Result<AggregateInput> read_row(common::ByteReader& reader) {
  auto identity = reader.read_u64_le();
  auto event_time = reader.read_i64_le();
  auto source_order = reader.read_u64_le();
  auto value = reader.read_float64_le();
  auto weight = reader.read_float64_le();
  if (!identity.has_value() || !event_time.has_value() || !source_order.has_value() ||
      !value.has_value() || !weight.has_value()) {
    return common::make_unexpected(corruption("materialized-view checkpoint row is truncated"));
  }
  return AggregateInput{*identity, *event_time, *source_order, *value, *weight};
}

} // namespace

common::Result<std::vector<std::byte>> encode_windowed_materialized_view_checkpoint_v1(
    const WindowedMaterializedViewCheckpoint& checkpoint,
    const MaterializedViewCheckpointCodecLimits limits) {
  if (!valid_limits(limits)) {
    return common::make_unexpected(
        invalid("materialized-view checkpoint codec limits are invalid"));
  }
  auto canonical = canonicalize(checkpoint, true);
  if (!canonical.has_value()) {
    return common::make_unexpected(canonical.error());
  }
  auto layout = encoded_size(*canonical, limits);
  if (!layout.has_value()) {
    return common::make_unexpected(layout.error());
  }
  try {
    std::vector<std::byte> bytes(layout->first, std::byte{0U});
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kMaterializedViewCheckpointHeaderSize);
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (status.is_ok())
      status = writer.write_u64_le(bytes.size());
    if (status.is_ok())
      status = writer.write_exact(canonical->position.tablet_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(canonical->position.wal_id.bytes);
    if (status.is_ok())
      status = writer.write_u64_le(canonical->position.record_sequence);
    if (status.is_ok())
      status = writer.write_i64_le(canonical->definition.width);
    if (status.is_ok())
      status = writer.write_i64_le(canonical->definition.slide);
    if (status.is_ok())
      status = writer.write_i64_le(canonical->definition.allowed_lateness);
    if (status.is_ok())
      status = writer.write_u64_le(canonical->definition.maximum_windows);
    if (status.is_ok())
      status = writer.write_u64_le(canonical->definition.maximum_rows);
    if (status.is_ok())
      status = writer.write_i64_le(canonical->watermark);
    if (status.is_ok())
      status = writer.write_u64_le(canonical->rows.size());
    if (status.is_ok())
      status = writer.write_u64_le(canonical->windows.size());
    if (status.is_ok())
      status = writer.write_u64_le(layout->second);
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (status.is_ok())
      status = writer.zero_fill(12U);
    for (const AggregateInput& row : canonical->rows) {
      if (status.is_ok())
        status = write_row(writer, row);
    }
    for (const MaterializedWindowCheckpoint& window : canonical->windows) {
      if (status.is_ok())
        status = writer.write_i64_le(window.window.start);
      if (status.is_ok())
        status = writer.write_i64_le(window.window.end);
      if (status.is_ok())
        status = writer.write_u64_le(window.revision);
      const std::uint32_t flags = (window.emitted ? 1U : 0U) | (window.finalized ? 2U : 0U);
      if (status.is_ok())
        status = writer.write_u32_le(flags);
      if (status.is_ok())
        status = writer.write_u32_le(0U);
      if (status.is_ok())
        status = writer.write_u64_le(window.aggregate.rows.size());
      if (status.is_ok())
        status = writer.write_u64_le(window.aggregate.count);
      if (status.is_ok())
        status = writer.write_float64_le(window.aggregate.sum);
      if (status.is_ok())
        status = writer.write_float64_le(window.aggregate.weighted_sum);
      if (status.is_ok())
        status = writer.write_float64_le(window.aggregate.weight_sum);
      if (status.is_ok())
        status = writer.write_float64_le(window.aggregate.mean);
      if (status.is_ok())
        status = writer.write_float64_le(window.aggregate.m2);
      for (const AggregateInput& row : window.aggregate.rows) {
        if (status.is_ok())
          status = write_row(writer, row);
      }
    }
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         "materialized-view checkpoint layout disagrees with writer"});
    }
    store_u32(bytes, kHeaderCrcOffset,
              common::crc32c(common::ByteView{bytes}.first(kMaterializedViewCheckpointHeaderSize)));
    store_u32(bytes, bytes.size() - kMaterializedViewCheckpointTrailerSize,
              common::crc32c(common::ByteView{bytes}.first(
                  bytes.size() - kMaterializedViewCheckpointTrailerSize)));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("materialized-view checkpoint allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint exceeded container limits"));
  }
}

common::Result<WindowedMaterializedViewCheckpoint> decode_windowed_materialized_view_checkpoint_v1(
    const common::ByteView bytes, const MaterializedViewCheckpointCodecLimits limits) {
  if (!valid_limits(limits)) {
    return common::make_unexpected(
        invalid("materialized-view checkpoint codec limits are invalid"));
  }
  if (bytes.size() <
      kMaterializedViewCheckpointHeaderSize + kMaterializedViewCheckpointTrailerSize) {
    return common::make_unexpected(
        corruption("materialized-view checkpoint is shorter than framing"));
  }
  if (bytes.size() > limits.maximum_checkpoint_bytes) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint exceeds decode size limit"));
  }
  std::array<std::byte, kMaterializedViewCheckpointHeaderSize> header{};
  std::ranges::copy(bytes.first(header.size()), header.begin());
  const std::uint32_t stored_header_crc = load_u32(bytes, kHeaderCrcOffset);
  store_u32(header, kHeaderCrcOffset, 0U);
  if (common::crc32c(header) != stored_header_crc) {
    return common::make_unexpected(
        corruption("materialized-view checkpoint header checksum mismatch"));
  }
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic) || load_u16(bytes, 8U) != kMajor) {
    return common::make_unexpected(
        unsupported("materialized-view checkpoint magic or major is unknown"));
  }
  if (load_u16(bytes, 10U) != kMinor ||
      load_u32(bytes, 12U) != kMaterializedViewCheckpointHeaderSize) {
    return common::make_unexpected(
        unsupported("materialized-view checkpoint minor or header is unknown"));
  }
  const std::uint64_t total_size = load_u64(bytes, kTotalSizeOffset);
  const std::uint64_t row_count = load_u64(bytes, kRowCountOffset);
  const std::uint64_t window_count = load_u64(bytes, kWindowCountOffset);
  const std::uint64_t contribution_count = load_u64(bytes, kContributionCountOffset);
  if (total_size != bytes.size() || load_u32(bytes, 16U) != 0U || load_u32(bytes, 20U) != 0U ||
      std::ranges::any_of(bytes.subspan(kReservedOffset, 12U),
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      common::crc32c(bytes.first(bytes.size() - kMaterializedViewCheckpointTrailerSize)) !=
          load_u32(bytes, bytes.size() - kMaterializedViewCheckpointTrailerSize)) {
    return common::make_unexpected(corruption("materialized-view checkpoint framing is invalid"));
  }
  if (row_count > limits.maximum_rows || window_count > limits.maximum_windows ||
      contribution_count > limits.maximum_window_contributions ||
      row_count > std::numeric_limits<std::size_t>::max() ||
      window_count > std::numeric_limits<std::size_t>::max() ||
      contribution_count > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint counts exceed decode limits"));
  }
  const auto row_bytes = common::checked_multiply(static_cast<std::size_t>(row_count),
                                                  kMaterializedViewCheckpointRowSize);
  const auto window_bytes = common::checked_multiply(static_cast<std::size_t>(window_count),
                                                     kMaterializedViewCheckpointWindowSize);
  const auto contribution_bytes = common::checked_multiply(
      static_cast<std::size_t>(contribution_count), kMaterializedViewCheckpointRowSize);
  auto expected_size = row_bytes.has_value()
                           ? common::checked_add(kMaterializedViewCheckpointHeaderSize, *row_bytes)
                           : std::nullopt;
  expected_size = expected_size.has_value() && window_bytes.has_value()
                      ? common::checked_add(*expected_size, *window_bytes)
                      : std::nullopt;
  expected_size = expected_size.has_value() && contribution_bytes.has_value()
                      ? common::checked_add(*expected_size, *contribution_bytes)
                      : std::nullopt;
  expected_size = expected_size.has_value()
                      ? common::checked_add(*expected_size, kMaterializedViewCheckpointTrailerSize)
                      : std::nullopt;
  if (!expected_size.has_value() || *expected_size != bytes.size()) {
    return common::make_unexpected(
        corruption("materialized-view checkpoint counts disagree with size"));
  }

  common::Uuid::Bytes tablet_bytes{};
  std::ranges::copy(bytes.subspan(kTabletIdOffset, tablet_bytes.size()), tablet_bytes.begin());
  auto tablet_id = schema::TabletId::from_bytes(tablet_bytes);
  wal::WalId wal_id;
  std::ranges::copy(bytes.subspan(kWalIdOffset, wal_id.bytes.size()), wal_id.bytes.begin());
  const std::uint64_t maximum_windows = load_u64(bytes, kMaximumWindowsOffset);
  const std::uint64_t maximum_rows = load_u64(bytes, kMaximumRowsOffset);
  if (!tablet_id.has_value() || !wal_id.is_valid()) {
    return common::make_unexpected(corruption("materialized-view checkpoint identity is invalid"));
  }
  if (maximum_windows > limits.maximum_windows || maximum_rows > limits.maximum_rows ||
      maximum_windows > std::numeric_limits<std::size_t>::max() ||
      maximum_rows > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint declared bounds exceed decode limits"));
  }

  WindowedMaterializedViewCheckpoint checkpoint{
      .definition = {.width = load_i64(bytes, kWidthOffset),
                     .slide = load_i64(bytes, kSlideOffset),
                     .allowed_lateness = load_i64(bytes, kAllowedLatenessOffset),
                     .maximum_windows = static_cast<std::size_t>(maximum_windows),
                     .maximum_rows = static_cast<std::size_t>(maximum_rows)},
      .position = SourcePosition::wal(*tablet_id, wal_id, load_u64(bytes, kAppliedSequenceOffset)),
      .watermark = load_i64(bytes, kWatermarkOffset),
      .rows = {},
      .windows = {}};
  try {
    checkpoint.rows.reserve(static_cast<std::size_t>(row_count));
    checkpoint.windows.reserve(static_cast<std::size_t>(window_count));
    common::ByteReader reader{bytes.subspan(kMaterializedViewCheckpointHeaderSize,
                                            bytes.size() - kMaterializedViewCheckpointHeaderSize -
                                                kMaterializedViewCheckpointTrailerSize)};
    for (std::uint64_t index = 0U; index < row_count; ++index) {
      auto row = read_row(reader);
      if (!row.has_value())
        return common::make_unexpected(row.error());
      checkpoint.rows.push_back(*row);
    }
    std::size_t decoded_contributions = 0U;
    for (std::uint64_t index = 0U; index < window_count; ++index) {
      auto start = reader.read_i64_le();
      auto end = reader.read_i64_le();
      auto revision = reader.read_u64_le();
      auto flags = reader.read_u32_le();
      auto reserved = reader.read_u32_le();
      auto aggregate_rows = reader.read_u64_le();
      auto count = reader.read_u64_le();
      auto sum = reader.read_float64_le();
      auto weighted_sum = reader.read_float64_le();
      auto weight_sum = reader.read_float64_le();
      auto mean = reader.read_float64_le();
      auto m2 = reader.read_float64_le();
      if (!start.has_value() || !end.has_value() || !revision.has_value() || !flags.has_value() ||
          !reserved.has_value() || !aggregate_rows.has_value() || !count.has_value() ||
          !sum.has_value() || !weighted_sum.has_value() || !weight_sum.has_value() ||
          !mean.has_value() || !m2.has_value() || *reserved != 0U || (*flags & ~3U) != 0U ||
          *aggregate_rows > contribution_count - decoded_contributions) {
        return common::make_unexpected(
            corruption("materialized-view checkpoint window is invalid"));
      }
      MaterializedWindowCheckpoint window{.window = {*start, *end},
                                          .revision = *revision,
                                          .emitted = (*flags & 1U) != 0U,
                                          .finalized = (*flags & 2U) != 0U,
                                          .aggregate = {.rows = {},
                                                        .count = *count,
                                                        .sum = *sum,
                                                        .weighted_sum = *weighted_sum,
                                                        .weight_sum = *weight_sum,
                                                        .mean = *mean,
                                                        .m2 = *m2}};
      window.aggregate.rows.reserve(static_cast<std::size_t>(*aggregate_rows));
      for (std::uint64_t row_index = 0U; row_index < *aggregate_rows; ++row_index) {
        auto row = read_row(reader);
        if (!row.has_value())
          return common::make_unexpected(row.error());
        window.aggregate.rows.push_back(*row);
      }
      decoded_contributions += static_cast<std::size_t>(*aggregate_rows);
      checkpoint.windows.push_back(std::move(window));
    }
    if (!reader.empty() || decoded_contributions != contribution_count) {
      return common::make_unexpected(
          corruption("materialized-view checkpoint body has trailing bytes"));
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint decode allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint decode exceeded container limits"));
  }
  auto canonical = canonicalize(checkpoint, false);
  if (!canonical.has_value()) {
    return common::make_unexpected(canonical.error());
  }
  return canonical;
}

} // namespace chronos::live
