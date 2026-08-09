#ifndef CHRONOS_QUERY_TEMPORAL_COMMAND_HPP_
#define CHRONOS_QUERY_TEMPORAL_COMMAND_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/temporal_snapshot.hpp"
#include "chronos/wal/application.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::query {

inline constexpr std::uint32_t kTemporalApplicationFormat = 1U;
inline constexpr std::uint32_t kTemporalApplicationKind = 3U;
inline constexpr std::size_t kTemporalCommandHeaderSize = 96U;
inline constexpr std::size_t kTemporalMutationMetadataSize = 24U;
inline constexpr std::size_t kTemporalCommandTrailerSize = 4U;

struct TemporalMutationDescriptor {
  std::vector<std::byte> logical_identity;
  std::int64_t event_time_ns{};
  std::int64_t receive_time_ns{};
  TemporalMutationKind kind{TemporalMutationKind::kOriginal};
};

struct TemporalCommandLimits {
  std::uint32_t maximum_mutations{1U << 20U};
  std::size_t maximum_identity_bytes{1024U};
  std::size_t maximum_metadata_bytes{64U * 1024U * 1024U};
  columnar::ColumnarBatchDecodeLimits batch;
};

class EncodedTemporalCommand {
public:
  EncodedTemporalCommand() = delete;
  EncodedTemporalCommand(const EncodedTemporalCommand&) = delete;
  EncodedTemporalCommand& operator=(const EncodedTemporalCommand&) = delete;
  EncodedTemporalCommand(EncodedTemporalCommand&&) noexcept = default;
  EncodedTemporalCommand& operator=(EncodedTemporalCommand&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedTemporalCommand(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedTemporalCommand>
  encode_temporal_command_v1(const columnar::OwnedColumnarBatch&,
                             std::vector<TemporalMutationDescriptor>, std::int64_t,
                             TemporalCommandLimits);
};

struct DecodedTemporalMutationDescriptor {
  common::ByteView logical_identity;
  std::int64_t event_time_ns{};
  std::int64_t receive_time_ns{};
  TemporalMutationKind kind{TemporalMutationKind::kOriginal};
};

class DecodedTemporalCommandView {
public:
  DecodedTemporalCommandView() = delete;

  [[nodiscard]] const columnar::DecodedColumnarBatchView& batch() const noexcept;
  [[nodiscard]] std::span<const DecodedTemporalMutationDescriptor> mutations() const noexcept;
  [[nodiscard]] std::int64_t system_commit_time_ns() const noexcept;

private:
  DecodedTemporalCommandView(columnar::DecodedColumnarBatchView batch,
                             std::vector<DecodedTemporalMutationDescriptor> mutations,
                             std::int64_t system_commit_time_ns) noexcept;
  columnar::DecodedColumnarBatchView batch_;
  std::vector<DecodedTemporalMutationDescriptor> mutations_;
  std::int64_t system_commit_time_ns_{};

  friend common::Result<DecodedTemporalCommandView>
      decode_temporal_command_v1(common::ByteView, TemporalCommandLimits);
};

[[nodiscard]] common::Result<EncodedTemporalCommand>
encode_temporal_command_v1(const columnar::OwnedColumnarBatch& batch,
                           std::vector<TemporalMutationDescriptor> mutations,
                           std::int64_t system_commit_time_ns, TemporalCommandLimits limits = {});

[[nodiscard]] common::Result<DecodedTemporalCommandView>
decode_temporal_command_v1(common::ByteView bytes, TemporalCommandLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_TEMPORAL_COMMAND_HPP_
