#ifndef CHRONOS_WAL_APPLICATION_HPP_
#define CHRONOS_WAL_APPLICATION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::wal {

inline constexpr std::size_t kApplicationFormatOffset = 0U;
inline constexpr std::size_t kApplicationKindOffset = 4U;
inline constexpr std::size_t kApplicationFlagsOffset = 8U;
inline constexpr std::size_t kApplicationBodyOffset = 16U;

struct ApplicationEnvelopeInput {
  std::uint32_t application_format;
  std::uint32_t application_kind;
  std::uint64_t application_flags;
  common::ByteView application_body;
};

// Owns exactly one complete WAL application payload: the 16-byte envelope and its body.
class EncodedApplicationPayload {
public:
  EncodedApplicationPayload() = delete;
  EncodedApplicationPayload(const EncodedApplicationPayload&) = delete;
  EncodedApplicationPayload& operator=(const EncodedApplicationPayload&) = delete;
  EncodedApplicationPayload(EncodedApplicationPayload&&) noexcept = default;
  EncodedApplicationPayload& operator=(EncodedApplicationPayload&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;

private:
  explicit EncodedApplicationPayload(std::vector<std::byte> bytes) noexcept;

  std::vector<std::byte> bytes_;

  friend common::Result<EncodedApplicationPayload>
  encode_application_payload(const ApplicationEnvelopeInput& input);
};

// Borrows a complete application payload. The body view remains valid only while encoded_bytes
// remains alive and immutable. Generic decoding proves envelope structure, not kind support.
struct DecodedApplicationEnvelope {
  std::uint32_t application_format;
  std::uint32_t application_kind;
  std::uint64_t application_flags;
  common::ByteView application_body;
};

[[nodiscard]] common::Result<EncodedApplicationPayload>
encode_application_payload(const ApplicationEnvelopeInput& input);

// The supplied bytes are one exact payload; every byte after the envelope belongs to the body.
// Truncation returns OUT_OF_RANGE, while zero format/kind values return CORRUPTION.
[[nodiscard]] common::Result<DecodedApplicationEnvelope>
decode_application_payload(common::ByteView encoded_bytes);

} // namespace chronos::wal

#endif // CHRONOS_WAL_APPLICATION_HPP_
