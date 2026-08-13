#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_AGGREGATE_EXCHANGE_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_AGGREGATE_EXCHANGE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/distributed_vector_aggregate_state.hpp"
#include "chronos/query/distributed_vector_plan.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/schema/identity.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace chronos::query {

namespace distributed_vector_aggregate_exchange_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 96U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::uint32_t kMaximumAggregates =
    static_cast<std::uint32_t>(kMaximumUngroupedAggregateWidth);
inline constexpr std::size_t kMinimumFrameLength =
    kHeaderLength + distributed_vector_aggregate_state_format::kMinimumFrameLength + kTrailerLength;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + distributed_vector_aggregate_state_format::kMaximumFrameLength + kTrailerLength;
} // namespace distributed_vector_aggregate_exchange_format

// Validates the complete fragment-bound definition authority even when no exchange payload is
// present (for example, while decoding a correlated failure response).
[[nodiscard]] common::Status validate_distributed_vector_aggregate_definitions(
    std::span<const VectorAggregateDefinition> definitions,
    std::uint32_t maximum_aggregates =
        distributed_vector_aggregate_exchange_format::kMaximumAggregates);

// Derives the exact state definitions authorized by one Fragment-v2 ungrouped aggregate shape.
// The result schema is checked at the same boundary even though SQL names are not repeated in each
// state frame.
[[nodiscard]] common::Result<std::vector<VectorAggregateDefinition>>
bind_distributed_vector_ungrouped_aggregate_definitions(
    const DistributedVectorPlanIntent& intent,
    std::span<const PhysicalColumnShape> projected_inputs,
    const DistributedVectorResultSchema& result_schema);

struct DistributedVectorAggregateExchangePosition {
  common::Uuid query_id;
  schema::TabletId tablet_id;
  std::uint64_t sequence{};
  std::uint32_t aggregate_ordinal{};
  bool terminal{};
};

struct DistributedVectorAggregateExchangeMessage {
  common::Uuid query_id;
  schema::TabletId tablet_id;
  std::uint64_t sequence{};
  std::uint32_t aggregate_ordinal{};
  bool terminal{};
  MergeableVectorAggregateState state;

  DistributedVectorAggregateExchangeMessage(DistributedVectorAggregateExchangePosition position,
                                            MergeableVectorAggregateState state_value) noexcept;
  DistributedVectorAggregateExchangeMessage(const DistributedVectorAggregateExchangeMessage&) =
      delete;
  DistributedVectorAggregateExchangeMessage&
  operator=(const DistributedVectorAggregateExchangeMessage&) = delete;
  DistributedVectorAggregateExchangeMessage(DistributedVectorAggregateExchangeMessage&&) noexcept =
      default;
  DistributedVectorAggregateExchangeMessage&
  operator=(DistributedVectorAggregateExchangeMessage&&) noexcept = default;
};

struct DistributedVectorAggregateExchangeDecodeLimits {
  std::size_t maximum_frame_length{
      distributed_vector_aggregate_exchange_format::kMaximumFrameLength};
  std::uint32_t maximum_aggregates{
      distributed_vector_aggregate_exchange_format::kMaximumAggregates};
  DistributedVectorAggregateStateDecodeLimits state;
};

class EncodedDistributedVectorAggregateExchangeMessage {
public:
  EncodedDistributedVectorAggregateExchangeMessage() = delete;
  EncodedDistributedVectorAggregateExchangeMessage(
      const EncodedDistributedVectorAggregateExchangeMessage&) = delete;
  EncodedDistributedVectorAggregateExchangeMessage&
  operator=(const EncodedDistributedVectorAggregateExchangeMessage&) = delete;
  EncodedDistributedVectorAggregateExchangeMessage(
      EncodedDistributedVectorAggregateExchangeMessage&&) noexcept = default;
  EncodedDistributedVectorAggregateExchangeMessage&
  operator=(EncodedDistributedVectorAggregateExchangeMessage&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorAggregateExchangeMessage(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorAggregateExchangeMessage>
  encode_distributed_vector_aggregate_exchange_message(
      const DistributedVectorAggregateExchangeMessage&, std::span<const VectorAggregateDefinition>);
};

[[nodiscard]] common::Result<EncodedDistributedVectorAggregateExchangeMessage>
encode_distributed_vector_aggregate_exchange_message(
    const DistributedVectorAggregateExchangeMessage& message,
    std::span<const VectorAggregateDefinition> expected_definitions);

[[nodiscard]] common::Result<DistributedVectorAggregateExchangeMessage>
decode_distributed_vector_aggregate_exchange_message_exact(
    common::ByteView bytes, std::span<const VectorAggregateDefinition> expected_definitions,
    const QueryResourceContext& resources,
    DistributedVectorAggregateExchangeDecodeLimits limits = {});

struct DistributedVectorAggregateExchangeReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorAggregateExchangeMessage> message;
};

// One owner supplies the fragment-bound definitions and query resource context. The reader retains
// one bounded frame, leaves a coalesced successor caller-owned, and makes every failure sticky.
class DistributedVectorAggregateExchangeReader {
public:
  DistributedVectorAggregateExchangeReader(
      std::vector<VectorAggregateDefinition>&& expected_definitions, QueryResourceContext resources,
      DistributedVectorAggregateExchangeDecodeLimits limits = {}) noexcept;
  DistributedVectorAggregateExchangeReader(const DistributedVectorAggregateExchangeReader&) =
      delete;
  DistributedVectorAggregateExchangeReader&
  operator=(const DistributedVectorAggregateExchangeReader&) = delete;
  DistributedVectorAggregateExchangeReader(DistributedVectorAggregateExchangeReader&&) = delete;
  DistributedVectorAggregateExchangeReader&
  operator=(DistributedVectorAggregateExchangeReader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorAggregateExchangeReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::vector<VectorAggregateDefinition> expected_definitions_;
  QueryResourceContext resources_;
  DistributedVectorAggregateExchangeDecodeLimits limits_;
  std::array<std::byte, distributed_vector_aggregate_exchange_format::kHeaderLength> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

class DistributedVectorAggregateExchangeWriteCursor {
public:
  DistributedVectorAggregateExchangeWriteCursor() = delete;
  DistributedVectorAggregateExchangeWriteCursor(
      const DistributedVectorAggregateExchangeWriteCursor&) = delete;
  DistributedVectorAggregateExchangeWriteCursor&
  operator=(const DistributedVectorAggregateExchangeWriteCursor&) = delete;
  DistributedVectorAggregateExchangeWriteCursor(
      DistributedVectorAggregateExchangeWriteCursor&& other) noexcept;
  DistributedVectorAggregateExchangeWriteCursor&
  operator=(DistributedVectorAggregateExchangeWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorAggregateExchangeWriteCursor>
  create(const DistributedVectorAggregateExchangeMessage& message,
         std::span<const VectorAggregateDefinition> expected_definitions);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorAggregateExchangeWriteCursor(
      EncodedDistributedVectorAggregateExchangeMessage encoded) noexcept;
  EncodedDistributedVectorAggregateExchangeMessage encoded_;
  std::size_t written_bytes_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_AGGREGATE_EXCHANGE_HPP_
