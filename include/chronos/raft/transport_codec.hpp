#ifndef CHRONOS_RAFT_TRANSPORT_CODEC_HPP_
#define CHRONOS_RAFT_TRANSPORT_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/multi_raft.hpp"

#include <array>
#include <cstddef>
#include <optional>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kRaftTransportHeaderSize = 96U;
inline constexpr std::size_t kRaftTransportTrailerSize = 4U;
inline constexpr std::size_t kMaximumRaftTransportFrameSize = std::size_t{64U} * 1024U * 1024U;

struct RaftTransportCodecLimits {
  std::size_t maximum_frame_bytes{kMaximumRaftTransportFrameSize};
  std::size_t maximum_append_entries{1024U};
  std::size_t maximum_entry_bytes{std::size_t{16U} * 1024U * 1024U};
  std::size_t maximum_snapshot_voters{31U};
};

struct RaftTransportEnvelope {
  GroupId group_id;
  NodeId source{};
  NodeId destination{};
  Message message;

  friend bool operator==(const RaftTransportEnvelope&, const RaftTransportEnvelope&) = default;
};

// Canonical group-scoped Raft bytes for an already authenticated cluster carrier. CRC32C detects
// accidental damage but does not authenticate the claimed source. The receiver must authorize the
// transport principal for source and exact-match destination before calling MultiRaftRuntime.
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_raft_transport_envelope_v1(const RaftTransportEnvelope& envelope,
                                  RaftTransportCodecLimits limits = {});

[[nodiscard]] common::Result<RaftTransportEnvelope>
decode_raft_transport_envelope_v1(common::ByteView bytes, RaftTransportCodecLimits limits = {});

// Applies every value and configured-size check used by the encoder without allocating bytes.
[[nodiscard]] common::Result<std::size_t>
raft_transport_encoded_length_v1(const RaftTransportEnvelope& envelope,
                                 RaftTransportCodecLimits limits = {});

// Validates one complete fixed header before returning its exact bounded frame length. This is the
// allocation gate for stream carriers; payload and trailer integrity still require full decode.
[[nodiscard]] common::Result<std::size_t>
raft_transport_frame_length_v1(common::ByteView header, RaftTransportCodecLimits limits = {});

struct RaftTransportReadStep {
  std::size_t consumed_bytes{};
  std::optional<RaftTransportEnvelope> envelope;
};

// Incremental one-frame-at-a-time reader for persistent byte streams. It retains only the fixed
// header until header integrity and the declared bound pass, then allocates the exact frame. A
// successful step resets the reader for the next frame and leaves any coalesced suffix to caller.
// Failure is sticky.
class RaftTransportFrameReader {
public:
  RaftTransportFrameReader() = delete;
  RaftTransportFrameReader(const RaftTransportFrameReader&) = delete;
  RaftTransportFrameReader& operator=(const RaftTransportFrameReader&) = delete;
  RaftTransportFrameReader(RaftTransportFrameReader&&) noexcept = default;
  RaftTransportFrameReader& operator=(RaftTransportFrameReader&&) noexcept = default;

  [[nodiscard]] static common::Result<RaftTransportFrameReader>
  create(RaftTransportCodecLimits limits = {});
  [[nodiscard]] common::Result<RaftTransportReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  explicit RaftTransportFrameReader(RaftTransportCodecLimits limits) noexcept;
  [[nodiscard]] common::Result<RaftTransportReadStep> fail(common::Status status);
  void reset_frame() noexcept;

  RaftTransportCodecLimits limits_;
  std::array<std::byte, kRaftTransportHeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
};

// Owns one fully validated frame and exposes only its unwritten suffix. Moving transfers the sole
// write obligation and leaves the source complete.
class RaftTransportFrameWriteCursor {
public:
  RaftTransportFrameWriteCursor() = delete;
  RaftTransportFrameWriteCursor(const RaftTransportFrameWriteCursor&) = delete;
  RaftTransportFrameWriteCursor& operator=(const RaftTransportFrameWriteCursor&) = delete;
  RaftTransportFrameWriteCursor(RaftTransportFrameWriteCursor&& other) noexcept;
  RaftTransportFrameWriteCursor& operator=(RaftTransportFrameWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<RaftTransportFrameWriteCursor>
  create(std::vector<std::byte> encoded_frame, RaftTransportCodecLimits limits = {});
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit RaftTransportFrameWriteCursor(std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TRANSPORT_CODEC_HPP_
