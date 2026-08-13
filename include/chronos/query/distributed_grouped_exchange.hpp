#ifndef CHRONOS_QUERY_DISTRIBUTED_GROUPED_EXCHANGE_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_GROUPED_EXCHANGE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace chronos::query {

struct GroupedFloat64ExchangeMessage {
  common::Uuid query_id;
  schema::TabletId tablet_id;
  std::uint64_t sequence{};
  // SQL NULL is absent. Non-NULL signed zero and NaN representations are canonicalized on encode.
  std::optional<double> group_key;
  MergeableAggregateState partial;
  bool terminal{};
};

namespace grouped_float64_exchange_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kFrameLength = 136U;
inline constexpr std::uint64_t kCanonicalQuietNanBits = 0x7ff8'0000'0000'0000ULL;
} // namespace grouped_float64_exchange_format

class EncodedGroupedFloat64ExchangeMessage {
public:
  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedGroupedFloat64ExchangeMessage(
      std::array<std::byte, grouped_float64_exchange_format::kFrameLength> bytes) noexcept;

  std::array<std::byte, grouped_float64_exchange_format::kFrameLength> bytes_{};

  friend common::Result<EncodedGroupedFloat64ExchangeMessage>
  encode_grouped_float64_exchange_message(const GroupedFloat64ExchangeMessage&);
};

// Canonical one-key grouping-state frame. It is a distinct protocol from the frozen ungrouped v1
// exchange and does not imply grouped planning, multi-key tuples, or coordinator merge semantics.
[[nodiscard]] common::Result<EncodedGroupedFloat64ExchangeMessage>
encode_grouped_float64_exchange_message(const GroupedFloat64ExchangeMessage& message);

[[nodiscard]] common::Result<GroupedFloat64ExchangeMessage>
decode_grouped_float64_exchange_message_exact(common::ByteView bytes);

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_GROUPED_EXCHANGE_HPP_
