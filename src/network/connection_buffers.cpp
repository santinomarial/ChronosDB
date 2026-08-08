#include "chronos/network/connection_buffers.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <new>
#include <string>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

} // namespace

ConnectionBuffers::ConnectionBuffers(ConnectionBufferConfig config) noexcept : config_(config) {}

common::Result<ConnectionBuffers> ConnectionBuffers::create(const ConnectionBufferConfig& config) {
  if (const common::Status status = validate_protocol_limits(config.protocol); !status.is_ok())
    return common::make_unexpected(status);
  const auto maximum_frame =
      encoded_frame_size(config.protocol.maximum_payload_size, config.protocol);
  if (!maximum_frame.has_value())
    return common::make_unexpected(maximum_frame.error());
  if (config.maximum_inbound_buffer_bytes < *maximum_frame ||
      config.maximum_outbound_buffer_bytes < kFrameHeaderSize ||
      config.maximum_outbound_frames == 0U)
    return common::make_unexpected(invalid("connection buffer limits cannot hold required frames"));
  try {
    ConnectionBuffers result{config};
    result.inbound_.reserve(std::min(config.maximum_inbound_buffer_bytes, *maximum_frame));
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("connection buffer allocation failed"));
  }
}

common::Result<std::vector<Frame>> ConnectionBuffers::receive(const common::ByteView bytes) {
  const auto new_size = common::checked_add(inbound_.size(), bytes.size());
  if (!new_size.has_value() || *new_size > config_.maximum_inbound_buffer_bytes)
    return common::make_unexpected(exhausted("connection inbound buffer limit exceeded"));
  try {
    inbound_.insert(inbound_.end(), bytes.begin(), bytes.end());
    std::vector<Frame> frames;
    std::size_t consumed = 0U;
    while (inbound_.size() - consumed >= kFrameHeaderSize) {
      const common::ByteView remaining{inbound_.data() + consumed, inbound_.size() - consumed};
      const auto header = decode_frame_header(remaining.first(kFrameHeaderSize), config_.protocol);
      if (!header.has_value())
        return common::make_unexpected(header.error());
      const auto size = encoded_frame_size(header->payload_size, config_.protocol);
      if (!size.has_value())
        return common::make_unexpected(size.error());
      if (remaining.size() < *size)
        break;
      auto decoded = decode_frame(remaining.first(*size), config_.protocol);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      frames.push_back(std::move(*decoded));
      consumed += *size;
    }
    if (consumed != 0U)
      inbound_.erase(inbound_.begin(), inbound_.begin() + static_cast<std::ptrdiff_t>(consumed));
    return frames;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("connection receive allocation failed"));
  }
}

common::Status ConnectionBuffers::enqueue(std::vector<std::byte> encoded_frame) {
  if (!decode_frame(encoded_frame, config_.protocol).has_value())
    return invalid("outbound bytes are not one canonical Protocol v1 frame");
  const auto total = common::checked_add(outbound_bytes_, encoded_frame.size());
  if (!total.has_value() || *total > config_.maximum_outbound_buffer_bytes ||
      outbound_.size() == config_.maximum_outbound_frames)
    return exhausted("connection outbound buffer limit exceeded");
  try {
    outbound_.push_back(std::move(encoded_frame));
    outbound_bytes_ = *total;
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return exhausted("connection outbound queue allocation failed");
  }
}

common::ByteView ConnectionBuffers::pending_write() const noexcept {
  if (outbound_.empty())
    return {};
  return common::ByteView{outbound_.front()}.subspan(outbound_offset_);
}

common::Status ConnectionBuffers::consume_written(const std::size_t bytes) noexcept {
  if (outbound_.empty() || bytes > outbound_.front().size() - outbound_offset_)
    return invalid("written byte count exceeds the current outbound frame");
  outbound_offset_ += bytes;
  outbound_bytes_ -= bytes;
  if (outbound_offset_ == outbound_.front().size()) {
    outbound_.pop_front();
    outbound_offset_ = 0U;
  }
  return common::Status::ok();
}

void ConnectionBuffers::clear() noexcept {
  inbound_.clear();
  outbound_.clear();
  outbound_offset_ = 0U;
  outbound_bytes_ = 0U;
}
std::size_t ConnectionBuffers::inbound_buffer_bytes() const noexcept {
  return inbound_.size();
}
std::size_t ConnectionBuffers::outbound_buffer_bytes() const noexcept {
  return outbound_bytes_;
}
std::size_t ConnectionBuffers::outbound_frames() const noexcept {
  return outbound_.size();
}

} // namespace chronos::network
