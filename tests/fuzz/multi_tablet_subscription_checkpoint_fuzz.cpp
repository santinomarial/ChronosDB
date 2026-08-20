#include "chronos/common/crc32c.hpp"
#include "chronos/live/multi_tablet_subscription_checkpoint.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <vector>

namespace {

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

void store_crc(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t crc) {
  for (std::size_t index = 0U; index < sizeof(crc); ++index)
    bytes[offset + index] = static_cast<std::byte>((crc >> (index * 8U)) & 0xffU);
}

void refresh_checkpoint_crc(std::vector<std::byte>& bytes) {
  if (bytes.size() < chronos::live::kMultiTabletSubscriptionCheckpointTrailerSize)
    return;
  const std::size_t crc_offset =
      bytes.size() - chronos::live::kMultiTabletSubscriptionCheckpointTrailerSize;
  store_crc(bytes, crc_offset,
            chronos::common::crc32c(chronos::common::ByteView{bytes}.first(crc_offset)));
}

void refresh_bound_crc(std::vector<std::byte>& bytes) {
  using namespace chronos::live;
  if (bytes.size() < kBoundMultiTabletSubscriptionCheckpointHeaderSize +
                         kMultiTabletSubscriptionCheckpointTrailerSize +
                         kBoundMultiTabletSubscriptionCheckpointTrailerSize)
    return;
  const std::size_t outer_crc_offset =
      bytes.size() - kBoundMultiTabletSubscriptionCheckpointTrailerSize;
  const std::size_t nested_crc_offset =
      outer_crc_offset - kMultiTabletSubscriptionCheckpointTrailerSize;
  const auto all_bytes = chronos::common::ByteView{bytes};
  const auto nested_prefix =
      all_bytes.subspan(kBoundMultiTabletSubscriptionCheckpointHeaderSize,
                        nested_crc_offset - kBoundMultiTabletSubscriptionCheckpointHeaderSize);
  store_crc(bytes, nested_crc_offset, chronos::common::crc32c(nested_prefix));
  store_crc(bytes, outer_crc_offset,
            chronos::common::crc32c(chronos::common::ByteView{bytes}.first(outer_crc_offset)));
}

void exercise_checkpoint_bytes(const chronos::common::ByteView bytes) {
  using namespace chronos::live;
  const auto v1 = decode_multi_tablet_subscription_checkpoint_v1(bytes);
  const auto v2 = decode_multi_tablet_subscription_checkpoint_v2(bytes);
  const auto compatible = decode_multi_tablet_subscription_checkpoint(bytes);

  if (v1.has_value() && v2.has_value())
    std::abort();
  if (compatible.has_value()) {
    if (v1.has_value() == v2.has_value())
      std::abort();
    const MultiTabletSubscriptionCheckpoint& explicit_checkpoint = v1.has_value() ? *v1 : *v2;
    if (*compatible != explicit_checkpoint)
      std::abort();
  } else if (v1.has_value() || v2.has_value()) {
    std::abort();
  }

  if (v1.has_value()) {
    const auto encoded = encode_multi_tablet_subscription_checkpoint_v1(*v1);
    if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
      std::abort();
  }
  if (v2.has_value()) {
    const auto encoded = encode_multi_tablet_subscription_checkpoint_v2(*v2);
    if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
      std::abort();
  }
}

void exercise_bound_bytes(const chronos::common::ByteView bytes) {
  using namespace chronos::live;
  const auto v1 = decode_bound_multi_tablet_subscription_checkpoint_v1(bytes);
  const auto v2 = decode_bound_multi_tablet_subscription_checkpoint_v2(bytes);
  const auto compatible = decode_bound_multi_tablet_subscription_checkpoint(bytes);

  if (v1.has_value() && v2.has_value())
    std::abort();
  if (compatible.has_value()) {
    if (v1.has_value() == v2.has_value())
      std::abort();
    const BoundMultiTabletSubscriptionCheckpoint& explicit_checkpoint = v1.has_value() ? *v1 : *v2;
    if (*compatible != explicit_checkpoint)
      std::abort();
  } else if (v1.has_value() || v2.has_value()) {
    std::abort();
  }

  if (v1.has_value()) {
    const auto encoded = encode_bound_multi_tablet_subscription_checkpoint_v1(*v1);
    if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
      std::abort();
  }
  if (v2.has_value()) {
    const auto encoded = encode_bound_multi_tablet_subscription_checkpoint_v2(*v2);
    if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
      std::abort();
  }
}

[[nodiscard]] chronos::live::SourcePosition position(const chronos::schema::TabletId tablet,
                                                     const std::uint8_t identity_seed,
                                                     const std::uint64_t sequence,
                                                     const bool raft) {
  if (raft)
    return chronos::live::SourcePosition::raft(tablet, uuid(identity_seed), sequence);
  return chronos::live::SourcePosition::wal(tablet, wal_id(identity_seed), sequence);
}

[[nodiscard]] chronos::live::MultiTabletSubscriptionCheckpoint
structured_checkpoint(const chronos::common::ByteView input, const bool source_tagged) {
  using namespace chronos;
  const std::size_t source_count = 1U + static_cast<std::size_t>(input_byte(input, 0U, 1U) % 4U);
  const bool terminal = (input_byte(input, 1U, 0U) & 8U) != 0U;
  const schema::SchemaId schema_id = identifier<schema::SchemaId>(0x51U);
  live::PlanFingerprint plan{};
  plan.fill(static_cast<std::byte>(input_byte(input, 2U, 0x37U)));

  live::MultiTabletSubscriptionCheckpoint checkpoint{
      uuid(0x41U), identifier<schema::TableId>(0x42U), plan,
      schema_id,   schema::SchemaVersion::initial(),   {},
      {}};
  checkpoint.plan_schema_compatible = !terminal;
  checkpoint.sources.reserve(source_count);
  std::vector<std::uint8_t> retained_counts;
  retained_counts.reserve(source_count);
  for (std::size_t index = 0U; index < source_count; ++index) {
    const std::uint8_t retained = terminal ? 0U : input_byte(input, 3U + index, 2U) % 4U;
    const std::uint64_t expired = input_byte(input, 7U + index, 0U) % 16U;
    const std::uint64_t latest = expired + retained;
    const schema::TabletId tablet =
        identifier<schema::TabletId>(static_cast<std::uint8_t>(0x10U + index));
    const bool raft = source_tagged && (input_byte(input, 11U + index, 0U) & 1U) != 0U;
    checkpoint.sources.push_back(
        {position(tablet, static_cast<std::uint8_t>(0x20U + index), latest, raft), latest});
    if (!terminal)
      checkpoint.sources.back().expired_through_sequence = expired;
    retained_counts.push_back(retained);
  }

  std::vector<std::uint8_t> emitted(source_count, 0U);
  bool remaining = true;
  std::size_t admission = 0U;
  while (remaining) {
    remaining = false;
    for (std::size_t index = 0U; index < source_count; ++index) {
      if (emitted[index] == retained_counts[index])
        continue;
      remaining = true;
      ++emitted[index];
      const auto& source = checkpoint.sources[index];
      auto change_position = source.latest_position;
      change_position.record_sequence =
          source.expired_through_sequence + static_cast<std::uint64_t>(emitted[index]);
      const std::uint8_t selector = input_byte(input, 15U + admission, 1U);
      const bool deletion = (selector & 1U) != 0U;
      const std::size_t key_size = 1U + static_cast<std::size_t>((selector >> 1U) % 4U);
      const std::size_t payload_size =
          deletion ? 0U : static_cast<std::size_t>((selector >> 3U) % 5U);
      std::vector<std::byte> key(key_size, static_cast<std::byte>(selector));
      std::vector<std::byte> payload(payload_size, static_cast<std::byte>(selector ^ 0xa5U));
      checkpoint.retained_changes.push_back(
          {change_position, schema_id, schema::SchemaVersion::initial(),
           deletion ? live::LogicalChangeOperation::kDelete : live::LogicalChangeOperation::kUpsert,
           std::move(key), std::move(payload)});
      ++admission;
    }
  }
  return checkpoint;
}

void mutate_checkpoint(std::vector<std::byte> bytes, const chronos::common::ByteView input) {
  if (bytes.size() <= chronos::live::kMultiTabletSubscriptionCheckpointTrailerSize)
    std::abort();
  const std::size_t mutable_size =
      bytes.size() - chronos::live::kMultiTabletSubscriptionCheckpointTrailerSize;
  const std::size_t selector = static_cast<std::size_t>(input_byte(input, 31U, 0U)) |
                               (static_cast<std::size_t>(input_byte(input, 32U, 0U)) << 8U);
  const std::size_t offset = selector % mutable_size;
  const std::uint8_t supplied_mask = input_byte(input, 33U, 1U);
  bytes[offset] ^= static_cast<std::byte>(supplied_mask == 0U ? 1U : supplied_mask);
  refresh_checkpoint_crc(bytes);
  exercise_checkpoint_bytes(bytes);
}

void mutate_bound(std::vector<std::byte> bytes, const chronos::common::ByteView input) {
  if (bytes.size() <= chronos::live::kBoundMultiTabletSubscriptionCheckpointTrailerSize)
    std::abort();
  const std::size_t mutable_size =
      bytes.size() - chronos::live::kBoundMultiTabletSubscriptionCheckpointTrailerSize;
  const std::size_t selector = static_cast<std::size_t>(input_byte(input, 34U, 0U)) |
                               (static_cast<std::size_t>(input_byte(input, 35U, 0U)) << 8U);
  const std::size_t offset = selector % mutable_size;
  const std::uint8_t supplied_mask = input_byte(input, 36U, 1U);
  bytes[offset] ^= static_cast<std::byte>(supplied_mask == 0U ? 1U : supplied_mask);
  refresh_bound_crc(bytes);
  exercise_bound_bytes(bytes);
}

void exercise_structured(const chronos::common::ByteView input, const bool source_tagged) {
  using namespace chronos::live;
  const MultiTabletSubscriptionCheckpoint checkpoint = structured_checkpoint(input, source_tagged);
  auto encoded = source_tagged ? encode_multi_tablet_subscription_checkpoint_v2(checkpoint)
                               : encode_multi_tablet_subscription_checkpoint_v1(checkpoint);
  if (!encoded.has_value())
    std::abort();
  exercise_checkpoint_bytes(*encoded);

  if (checkpoint.sources.size() > 1U) {
    MultiTabletSubscriptionCheckpointCodecLimits limits;
    limits.maximum_sources = checkpoint.sources.size() - 1U;
    if (decode_multi_tablet_subscription_checkpoint(*encoded, limits).has_value())
      std::abort();
  }
  mutate_checkpoint(*encoded, input);

  const BoundMultiTabletSubscriptionCheckpoint bound{
      1U + static_cast<std::uint64_t>(input_byte(input, 37U, 0U)), checkpoint};
  auto bound_bytes = source_tagged ? encode_bound_multi_tablet_subscription_checkpoint_v2(bound)
                                   : encode_bound_multi_tablet_subscription_checkpoint_v1(bound);
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
  exercise_structured(bytes, false);
  exercise_structured(bytes, true);
  return 0;
}
