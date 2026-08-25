#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_EXCHANGE_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_EXCHANGE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/distributed_vector_aggregate_state.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/schema/identity.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace chronos::query {

namespace distributed_vector_grouped_aggregate_exchange_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 128U;
inline constexpr std::size_t kKeyHeaderLength = 16U;
inline constexpr std::size_t kStateHeaderLength = 16U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMaximumFrameLength = std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumKeyPayloadBytes = std::size_t{1024U} * 1024U;
inline constexpr std::uint32_t kMaximumGroups =
    static_cast<std::uint32_t>(kMaximumGroupedAggregateGroups);
inline constexpr std::uint32_t kMaximumGroupKeys =
    static_cast<std::uint32_t>(kMaximumGroupedAggregateKeys);
inline constexpr std::uint32_t kMaximumAggregates =
    static_cast<std::uint32_t>(kMaximumGroupedAggregateWidth);
inline constexpr std::size_t kMinimumFrameLength = kHeaderLength + kTrailerLength;
} // namespace distributed_vector_grouped_aggregate_exchange_format

struct DistributedVectorGroupedAggregateExchangePosition {
  common::Uuid query_id;
  schema::TabletId tablet_id;
  std::uint64_t sequence{};
  std::uint32_t group_ordinal{};
  std::uint32_t group_count{};
  bool terminal{};
  bool empty{};
};

struct DistributedVectorGroupedAggregateExchangeDecodeLimits;

// One complete group owns an exact multi-column key tuple and one sufficient state per fragment-
// authorized aggregate. The distinct empty terminal owns neither keys nor states. Decoded variable
// key bytes retain query credit for the lifetime of this move-only value; nested extrema retain
// their own independently bounded credit.
class DistributedVectorGroupedAggregateExchangeMessage {
public:
  DistributedVectorGroupedAggregateExchangeMessage(
      DistributedVectorGroupedAggregateExchangePosition position, std::vector<ScalarValue> keys,
      std::vector<MergeableVectorAggregateState> states) noexcept;
  DistributedVectorGroupedAggregateExchangeMessage(
      const DistributedVectorGroupedAggregateExchangeMessage&) = delete;
  DistributedVectorGroupedAggregateExchangeMessage&
  operator=(const DistributedVectorGroupedAggregateExchangeMessage&) = delete;
  DistributedVectorGroupedAggregateExchangeMessage(
      DistributedVectorGroupedAggregateExchangeMessage&&) noexcept = default;
  DistributedVectorGroupedAggregateExchangeMessage&
  operator=(DistributedVectorGroupedAggregateExchangeMessage&&) noexcept = default;

  [[nodiscard]] const DistributedVectorGroupedAggregateExchangePosition& position() const noexcept;
  [[nodiscard]] std::span<const ScalarValue> keys() const noexcept;
  [[nodiscard]] std::span<const MergeableVectorAggregateState> states() const noexcept;
  [[nodiscard]] std::vector<MergeableVectorAggregateState> take_states() && noexcept;

private:
  DistributedVectorGroupedAggregateExchangeMessage(
      DistributedVectorGroupedAggregateExchangePosition position, std::vector<ScalarValue> keys,
      std::vector<MergeableVectorAggregateState> states,
      QueryMemoryReservation key_reservation) noexcept;

  DistributedVectorGroupedAggregateExchangePosition position_;
  std::vector<ScalarValue> keys_;
  std::vector<MergeableVectorAggregateState> states_;
  QueryMemoryReservation key_reservation_;

  friend common::Result<DistributedVectorGroupedAggregateExchangeMessage>
  decode_distributed_vector_grouped_aggregate_exchange_message_exact(
      common::ByteView, std::span<const VectorGroupKeyDefinition>,
      std::span<const VectorAggregateDefinition>, const QueryResourceContext&,
      DistributedVectorGroupedAggregateExchangeDecodeLimits);
};

struct DistributedVectorGroupedAggregateExchangeDecodeLimits {
  std::size_t maximum_frame_length{
      distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength};
  std::size_t maximum_key_payload_bytes{
      distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes};
  std::uint32_t maximum_groups{
      distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::uint32_t maximum_group_keys{
      distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys};
  std::uint32_t maximum_aggregates{
      distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates};
  DistributedVectorAggregateStateDecodeLimits state;
};

class EncodedDistributedVectorGroupedAggregateExchangeMessage {
public:
  EncodedDistributedVectorGroupedAggregateExchangeMessage() = delete;
  EncodedDistributedVectorGroupedAggregateExchangeMessage(
      const EncodedDistributedVectorGroupedAggregateExchangeMessage&) = delete;
  EncodedDistributedVectorGroupedAggregateExchangeMessage&
  operator=(const EncodedDistributedVectorGroupedAggregateExchangeMessage&) = delete;
  EncodedDistributedVectorGroupedAggregateExchangeMessage(
      EncodedDistributedVectorGroupedAggregateExchangeMessage&&) noexcept = default;
  EncodedDistributedVectorGroupedAggregateExchangeMessage&
  operator=(EncodedDistributedVectorGroupedAggregateExchangeMessage&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorGroupedAggregateExchangeMessage(
      std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorGroupedAggregateExchangeMessage>
  encode_distributed_vector_grouped_aggregate_exchange_message(
      const DistributedVectorGroupedAggregateExchangeMessage&,
      std::span<const VectorGroupKeyDefinition>, std::span<const VectorAggregateDefinition>);
};

[[nodiscard]] common::Status validate_distributed_vector_grouped_aggregate_authority(
    std::span<const VectorGroupKeyDefinition> key_definitions,
    std::span<const VectorAggregateDefinition> aggregate_definitions,
    std::uint32_t maximum_group_keys =
        distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys,
    std::uint32_t maximum_aggregates =
        distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates);

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateExchangeMessage>
encode_distributed_vector_grouped_aggregate_exchange_message(
    const DistributedVectorGroupedAggregateExchangeMessage& message,
    std::span<const VectorGroupKeyDefinition> expected_keys,
    std::span<const VectorAggregateDefinition> expected_aggregates);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateExchangeMessage>
decode_distributed_vector_grouped_aggregate_exchange_message_exact(
    common::ByteView bytes, std::span<const VectorGroupKeyDefinition> expected_keys,
    std::span<const VectorAggregateDefinition> expected_aggregates,
    const QueryResourceContext& resources,
    DistributedVectorGroupedAggregateExchangeDecodeLimits limits = {});

struct DistributedVectorGroupedAggregateExchangeReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorGroupedAggregateExchangeMessage> message;
};

// Header-first single-frame owner. Allocation-driving lengths and fragment authority pass before
// exact frame retention. It consumes at most one frame, leaves a coalesced suffix caller-owned,
// and makes every failure sticky.
class DistributedVectorGroupedAggregateExchangeReader {
public:
  DistributedVectorGroupedAggregateExchangeReader(
      std::vector<VectorGroupKeyDefinition>&& expected_keys,
      std::vector<VectorAggregateDefinition>&& expected_aggregates, QueryResourceContext resources,
      DistributedVectorGroupedAggregateExchangeDecodeLimits limits = {}) noexcept;
  DistributedVectorGroupedAggregateExchangeReader(
      const DistributedVectorGroupedAggregateExchangeReader&) = delete;
  DistributedVectorGroupedAggregateExchangeReader&
  operator=(const DistributedVectorGroupedAggregateExchangeReader&) = delete;
  DistributedVectorGroupedAggregateExchangeReader(
      DistributedVectorGroupedAggregateExchangeReader&&) = delete;
  DistributedVectorGroupedAggregateExchangeReader&
  operator=(DistributedVectorGroupedAggregateExchangeReader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateExchangeReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::vector<VectorGroupKeyDefinition> expected_keys_;
  std::vector<VectorAggregateDefinition> expected_aggregates_;
  QueryResourceContext resources_;
  DistributedVectorGroupedAggregateExchangeDecodeLimits limits_;
  std::array<std::byte, distributed_vector_grouped_aggregate_exchange_format::kHeaderLength>
      header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

class DistributedVectorGroupedAggregateExchangeWriteCursor {
public:
  DistributedVectorGroupedAggregateExchangeWriteCursor() = delete;
  DistributedVectorGroupedAggregateExchangeWriteCursor(
      const DistributedVectorGroupedAggregateExchangeWriteCursor&) = delete;
  DistributedVectorGroupedAggregateExchangeWriteCursor&
  operator=(const DistributedVectorGroupedAggregateExchangeWriteCursor&) = delete;
  DistributedVectorGroupedAggregateExchangeWriteCursor(
      DistributedVectorGroupedAggregateExchangeWriteCursor&& other) noexcept;
  DistributedVectorGroupedAggregateExchangeWriteCursor&
  operator=(DistributedVectorGroupedAggregateExchangeWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateExchangeWriteCursor>
  create(const DistributedVectorGroupedAggregateExchangeMessage& message,
         std::span<const VectorGroupKeyDefinition> expected_keys,
         std::span<const VectorAggregateDefinition> expected_aggregates);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateExchangeWriteCursor(
      EncodedDistributedVectorGroupedAggregateExchangeMessage encoded) noexcept;
  EncodedDistributedVectorGroupedAggregateExchangeMessage encoded_;
  std::size_t written_bytes_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_EXCHANGE_HPP_
