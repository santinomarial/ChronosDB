#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_AGGREGATE_STATE_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_AGGREGATE_STATE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/aggregate.hpp"
#include "chronos/query/resource_context.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::query {

namespace distributed_vector_aggregate_state_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 112U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::uint32_t kMaximumInputColumnOrdinal = 4095U;
inline constexpr std::size_t kMaximumExtremumBytes = kDefaultAggregateExtremumByteLimit;
inline constexpr std::size_t kMinimumFrameLength = kHeaderLength + kTrailerLength;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + kMaximumExtremumBytes + kTrailerLength;
} // namespace distributed_vector_aggregate_state_format

struct DistributedVectorAggregateStateDecodeLimits {
  std::size_t maximum_frame_length{distributed_vector_aggregate_state_format::kMaximumFrameLength};
  std::size_t maximum_variable_extremum_bytes{kDefaultAggregateExtremumByteLimit};
};

// Owns one canonical, checksummed partial-state frame. This is a nested state value: a later
// exchange envelope must still bind query/tablet identity, sequencing, grouping, and terminality.
class EncodedMergeableVectorAggregateState {
public:
  EncodedMergeableVectorAggregateState() = delete;
  EncodedMergeableVectorAggregateState(const EncodedMergeableVectorAggregateState&) = delete;
  EncodedMergeableVectorAggregateState&
  operator=(const EncodedMergeableVectorAggregateState&) = delete;
  EncodedMergeableVectorAggregateState(EncodedMergeableVectorAggregateState&&) noexcept = default;
  EncodedMergeableVectorAggregateState&
  operator=(EncodedMergeableVectorAggregateState&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedMergeableVectorAggregateState(std::vector<std::byte> bytes) noexcept;

  std::vector<std::byte> bytes_;

  friend common::Result<EncodedMergeableVectorAggregateState>
  encode_mergeable_vector_aggregate_state(const MergeableVectorAggregateState&);
};

[[nodiscard]] common::Result<EncodedMergeableVectorAggregateState>
encode_mergeable_vector_aggregate_state(const MergeableVectorAggregateState& state);

[[nodiscard]] common::Result<MergeableVectorAggregateState>
decode_mergeable_vector_aggregate_state_exact(
    common::ByteView bytes, const QueryResourceContext& resources,
    DistributedVectorAggregateStateDecodeLimits limits = {});

struct MergeableVectorAggregateStateReadStep {
  std::size_t consumed_bytes{};
  std::optional<MergeableVectorAggregateState> state;
};

// Header-first single-frame reader. It retains no unchecked variable-length allocation, consumes
// at most one frame per call, leaves a coalesced successor with the caller, and fails sticky.
class MergeableVectorAggregateStateReader {
public:
  explicit MergeableVectorAggregateStateReader(
      QueryResourceContext resources,
      DistributedVectorAggregateStateDecodeLimits limits = {}) noexcept;
  MergeableVectorAggregateStateReader(const MergeableVectorAggregateStateReader&) = delete;
  MergeableVectorAggregateStateReader&
  operator=(const MergeableVectorAggregateStateReader&) = delete;
  MergeableVectorAggregateStateReader(MergeableVectorAggregateStateReader&&) = delete;
  MergeableVectorAggregateStateReader& operator=(MergeableVectorAggregateStateReader&&) = delete;

  [[nodiscard]] common::Result<MergeableVectorAggregateStateReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  QueryResourceContext resources_;
  DistributedVectorAggregateStateDecodeLimits limits_;
  std::array<std::byte, distributed_vector_aggregate_state_format::kHeaderLength> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

// Move-only complete-frame owner exposing only the unwritten suffix. Moving makes the source
// complete, and an over-acknowledgement fails before progress changes.
class MergeableVectorAggregateStateWriteCursor {
public:
  MergeableVectorAggregateStateWriteCursor() = delete;
  MergeableVectorAggregateStateWriteCursor(const MergeableVectorAggregateStateWriteCursor&) =
      delete;
  MergeableVectorAggregateStateWriteCursor&
  operator=(const MergeableVectorAggregateStateWriteCursor&) = delete;
  MergeableVectorAggregateStateWriteCursor(
      MergeableVectorAggregateStateWriteCursor&& other) noexcept;
  MergeableVectorAggregateStateWriteCursor&
  operator=(MergeableVectorAggregateStateWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<MergeableVectorAggregateStateWriteCursor>
  create(const MergeableVectorAggregateState& state);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit MergeableVectorAggregateStateWriteCursor(
      EncodedMergeableVectorAggregateState encoded) noexcept;

  EncodedMergeableVectorAggregateState encoded_;
  std::size_t written_bytes_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_AGGREGATE_STATE_HPP_
