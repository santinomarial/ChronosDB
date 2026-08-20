#include "chronos/common/crc32c.hpp"
#include "chronos/live/materialized_view_checkpoint.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <vector>

namespace {

constexpr std::size_t kHeaderCrcOffset = 144U;

[[nodiscard]] std::uint8_t input_byte(const chronos::common::ByteView input,
                                      const std::size_t index,
                                      const std::uint8_t fallback) noexcept {
  return index < input.size() ? std::to_integer<std::uint8_t>(input[index]) : fallback;
}

[[nodiscard]] chronos::common::Uuid uuid(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  bytes.front() = static_cast<std::byte>(seed == 0U ? 1U : seed);
  return chronos::common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::uint8_t seed) {
  auto result = Identifier::from_uuid(uuid(seed));
  if (!result.has_value())
    std::abort();
  return *result;
}

[[nodiscard]] chronos::wal::WalId wal_id(const std::uint8_t seed) {
  chronos::wal::WalId result{};
  result.bytes = uuid(seed).bytes();
  return result;
}

void store_crc(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t crc) {
  for (std::size_t index = 0U; index < sizeof(crc); ++index)
    bytes[offset + index] = static_cast<std::byte>((crc >> (index * 8U)) & 0xffU);
}

void refresh_checkpoint_crc(std::span<std::byte> bytes) {
  using namespace chronos::live;
  if (bytes.size() < kMaterializedViewCheckpointHeaderSize + kMaterializedViewCheckpointTrailerSize)
    return;
  store_crc(bytes, kHeaderCrcOffset, 0U);
  store_crc(bytes, kHeaderCrcOffset,
            chronos::common::crc32c(
                chronos::common::ByteView{bytes}.first(kMaterializedViewCheckpointHeaderSize)));
  const std::size_t trailer_offset = bytes.size() - kMaterializedViewCheckpointTrailerSize;
  store_crc(bytes, trailer_offset,
            chronos::common::crc32c(chronos::common::ByteView{bytes}.first(trailer_offset)));
}

void refresh_bound_crc(std::vector<std::byte>& bytes) {
  using namespace chronos::live;
  if (bytes.size() <
      kBoundMaterializedViewCheckpointHeaderSize + kMaterializedViewCheckpointHeaderSize +
          kMaterializedViewCheckpointTrailerSize + kBoundMaterializedViewCheckpointTrailerSize)
    return;
  auto all = std::span<std::byte>{bytes};
  auto nested = all.subspan(kBoundMaterializedViewCheckpointHeaderSize,
                            all.size() - kBoundMaterializedViewCheckpointHeaderSize -
                                kBoundMaterializedViewCheckpointTrailerSize);
  refresh_checkpoint_crc(nested);
  store_crc(all, kHeaderCrcOffset, 0U);
  store_crc(all, kHeaderCrcOffset,
            chronos::common::crc32c(
                chronos::common::ByteView{all}.first(kBoundMaterializedViewCheckpointHeaderSize)));
  const std::size_t trailer_offset = bytes.size() - kBoundMaterializedViewCheckpointTrailerSize;
  store_crc(all, trailer_offset,
            chronos::common::crc32c(chronos::common::ByteView{all}.first(trailer_offset)));
}

void exercise_checkpoint_bytes(const chronos::common::ByteView bytes) {
  const auto decoded = chronos::live::decode_windowed_materialized_view_checkpoint_v1(bytes);
  if (!decoded.has_value())
    return;
  const auto encoded = chronos::live::encode_windowed_materialized_view_checkpoint_v1(*decoded);
  if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
    std::abort();
}

void exercise_bound_bytes(const chronos::common::ByteView bytes) {
  const auto decoded = chronos::live::decode_bound_materialized_view_checkpoint_v1(bytes);
  if (!decoded.has_value())
    return;
  const auto encoded = chronos::live::encode_bound_materialized_view_checkpoint_v1(*decoded);
  if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
    std::abort();
}

[[nodiscard]] chronos::live::WindowedMaterializedViewCheckpoint
structured_checkpoint(const chronos::common::ByteView input) {
  using namespace chronos;
  const schema::TabletId tablet = identifier<schema::TabletId>(0x11U);
  const wal::WalId wal = wal_id(0x12U);
  auto view = live::WindowedMaterializedView::create(tablet, wal, {10, 5, 2, 32U, 32U});
  if (!view.has_value())
    std::abort();
  const std::size_t row_count = 1U + static_cast<std::size_t>(input_byte(input, 0U, 1U) % 8U);
  for (std::size_t index = 0U; index < row_count; ++index) {
    const std::uint64_t sequence = index + 1U;
    const double value = static_cast<double>(input_byte(input, 1U + index, 10U));
    const double weight = 1.0 + static_cast<double>(input_byte(input, 9U + index, 1U) % 4U);
    if (!view->apply_committed(
                 live::SourcePosition{tablet, wal, sequence},
                 live::MaterializedViewInput{
                     {sequence, static_cast<std::int64_t>(index * 5U), sequence, value, weight},
                     false})
             .has_value())
      std::abort();
  }
  if (!view->advance_watermark(input_byte(input, 17U, 0U) % 40U).has_value())
    std::abort();
  auto checkpoint = view->checkpoint();
  if (!checkpoint.has_value())
    std::abort();
  return std::move(*checkpoint);
}

void mutate_checkpoint(std::vector<std::byte> bytes, const chronos::common::ByteView input) {
  if (bytes.size() <= chronos::live::kMaterializedViewCheckpointTrailerSize)
    std::abort();
  const std::size_t mutable_size =
      bytes.size() - chronos::live::kMaterializedViewCheckpointTrailerSize;
  const std::size_t selector = static_cast<std::size_t>(input_byte(input, 18U, 0U)) |
                               (static_cast<std::size_t>(input_byte(input, 19U, 0U)) << 8U);
  const std::size_t offset = selector % mutable_size;
  const std::uint8_t supplied_mask = input_byte(input, 20U, 1U);
  bytes[offset] ^= static_cast<std::byte>(supplied_mask == 0U ? 1U : supplied_mask);
  refresh_checkpoint_crc(bytes);
  exercise_checkpoint_bytes(bytes);
}

void mutate_bound(std::vector<std::byte> bytes, const chronos::common::ByteView input) {
  if (bytes.size() <= chronos::live::kBoundMaterializedViewCheckpointTrailerSize)
    std::abort();
  const std::size_t mutable_size =
      bytes.size() - chronos::live::kBoundMaterializedViewCheckpointTrailerSize;
  const std::size_t selector = static_cast<std::size_t>(input_byte(input, 21U, 0U)) |
                               (static_cast<std::size_t>(input_byte(input, 22U, 0U)) << 8U);
  const std::size_t offset = selector % mutable_size;
  const std::uint8_t supplied_mask = input_byte(input, 23U, 1U);
  bytes[offset] ^= static_cast<std::byte>(supplied_mask == 0U ? 1U : supplied_mask);
  refresh_bound_crc(bytes);
  exercise_bound_bytes(bytes);
}

void exercise_structured(const chronos::common::ByteView input) {
  using namespace chronos::live;
  const WindowedMaterializedViewCheckpoint checkpoint = structured_checkpoint(input);
  auto encoded = encode_windowed_materialized_view_checkpoint_v1(checkpoint);
  if (!encoded.has_value())
    std::abort();
  exercise_checkpoint_bytes(*encoded);
  mutate_checkpoint(*encoded, input);

  MaterializedViewCheckpointCodecLimits limits;
  limits.maximum_rows = checkpoint.definition.maximum_rows - 1U;
  if (decode_windowed_materialized_view_checkpoint_v1(*encoded, limits).has_value())
    std::abort();

  PlanFingerprint plan{};
  plan.fill(static_cast<std::byte>(input_byte(input, 24U, 0x37U)));
  const BoundMaterializedViewCheckpoint bound{
      {.database_id = uuid(0x21U),
       .view_id = uuid(0x22U),
       .table_id = identifier<chronos::schema::TableId>(0x23U),
       .schema_id = identifier<chronos::schema::SchemaId>(0x24U),
       .schema_version = chronos::schema::SchemaVersion::initial(),
       .plan_fingerprint = plan},
      1U + static_cast<std::uint64_t>(input_byte(input, 25U, 0U)),
      checkpoint};
  auto bound_bytes = encode_bound_materialized_view_checkpoint_v1(bound);
  if (!bound_bytes.has_value())
    std::abort();
  exercise_bound_bytes(*bound_bytes);
  mutate_bound(*bound_bytes, input);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes = std::as_bytes(std::span{data, size});
  exercise_checkpoint_bytes(bytes);
  exercise_bound_bytes(bytes);
  exercise_structured(bytes);
  return 0;
}
