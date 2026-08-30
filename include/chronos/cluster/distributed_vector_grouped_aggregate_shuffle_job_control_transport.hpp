#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TRANSPORT_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"

#include <array>
#include <cstddef>
#include <optional>
#include <vector>

namespace chronos::cluster {

// Validates the complete fixed header before a stream reader allocates for its declared frame.
[[nodiscard]] common::Result<std::size_t>
distributed_vector_grouped_aggregate_shuffle_job_control_request_frame_length_v1(
    common::ByteView header,
    DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits = {});

[[nodiscard]] common::Result<std::size_t>
distributed_vector_grouped_aggregate_shuffle_job_control_request_frame_length_v2(
    common::ByteView header,
    DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits = {});

struct DistributedVectorGroupedAggregateShuffleJobControlRequestReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorGroupedAggregateShuffleJobControlRequest> request;
};

class DistributedVectorGroupedAggregateShuffleJobControlRequestReader {
public:
  DistributedVectorGroupedAggregateShuffleJobControlRequestReader() = delete;
  DistributedVectorGroupedAggregateShuffleJobControlRequestReader(
      const DistributedVectorGroupedAggregateShuffleJobControlRequestReader&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlRequestReader&
  operator=(const DistributedVectorGroupedAggregateShuffleJobControlRequestReader&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlRequestReader(
      DistributedVectorGroupedAggregateShuffleJobControlRequestReader&& other) noexcept;
  DistributedVectorGroupedAggregateShuffleJobControlRequestReader&
  operator=(DistributedVectorGroupedAggregateShuffleJobControlRequestReader&& other) noexcept;

  [[nodiscard]] static common::Result<
      DistributedVectorGroupedAggregateShuffleJobControlRequestReader>
  create(DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits = {});
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequestReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateShuffleJobControlRequestReader(
      DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits) noexcept;
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequestReadStep>
  fail(common::Status status);
  void reset_frame() noexcept;

  DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits_;
  std::array<std::byte,
             distributed_vector_grouped_aggregate_shuffle_job_control_format::kHeaderLength>
      header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
  bool version_two_{};
};

struct DistributedVectorGroupedAggregateShuffleJobControlResponseReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorGroupedAggregateShuffleJobControlResponse> response;
};

class DistributedVectorGroupedAggregateShuffleJobControlResponseReader {
public:
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader() = default;
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader(
      const DistributedVectorGroupedAggregateShuffleJobControlResponseReader&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader&
  operator=(const DistributedVectorGroupedAggregateShuffleJobControlResponseReader&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader(
      DistributedVectorGroupedAggregateShuffleJobControlResponseReader&& other) noexcept;
  DistributedVectorGroupedAggregateShuffleJobControlResponseReader&
  operator=(DistributedVectorGroupedAggregateShuffleJobControlResponseReader&& other) noexcept;

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponseReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte,
             distributed_vector_grouped_aggregate_shuffle_job_control_format::kResponseFrameLength>
      frame_{};
  std::size_t buffered_bytes_{};
  std::optional<common::Status> failure_;
};

// Each cursor owns one canonical frame. Moving transfers the only write obligation and leaves the
// source complete.
class DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor {
public:
  DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor() = delete;
  DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor(
      const DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor&
  operator=(const DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor(
      DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor&& other) noexcept;
  DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor&
  operator=(DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor&& other) noexcept;

  [[nodiscard]] static DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor
  create(EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest frame) noexcept;
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor(
      EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest frame) noexcept;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest frame_;
  std::size_t written_bytes_{};
};

class DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor {
public:
  DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor() = delete;
  DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor(
      const DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor&
  operator=(const DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor(
      DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor&& other) noexcept;
  DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor&
  operator=(DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor&& other) noexcept;

  [[nodiscard]] static DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor
  create(EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse frame) noexcept;
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor(
      EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse frame) noexcept;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse frame_;
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_TRANSPORT_HPP_
