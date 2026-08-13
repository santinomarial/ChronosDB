#ifndef CHRONOS_QUERY_DISTRIBUTED_GROUPED_EXCHANGE_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_GROUPED_EXCHANGE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

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

struct GroupedExchangeTerminalMessage {
  common::Uuid query_id;
  schema::TabletId tablet_id;
  std::uint64_t sequence{};
};

namespace grouped_exchange_terminal_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kFrameLength = 64U;
} // namespace grouped_exchange_terminal_format

class EncodedGroupedExchangeTerminalMessage {
public:
  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedGroupedExchangeTerminalMessage(
      std::array<std::byte, grouped_exchange_terminal_format::kFrameLength> bytes) noexcept;

  std::array<std::byte, grouped_exchange_terminal_format::kFrameLength> bytes_{};

  friend common::Result<EncodedGroupedExchangeTerminalMessage>
  encode_grouped_exchange_terminal_message(const GroupedExchangeTerminalMessage&);
};

struct GroupedExchangeTerminalFrameReadStep {
  std::size_t consumed_bytes{};
  std::optional<GroupedExchangeTerminalMessage> message;
};

class GroupedExchangeTerminalFrameReader {
public:
  GroupedExchangeTerminalFrameReader() = default;
  GroupedExchangeTerminalFrameReader(const GroupedExchangeTerminalFrameReader&) = delete;
  GroupedExchangeTerminalFrameReader& operator=(const GroupedExchangeTerminalFrameReader&) = delete;
  GroupedExchangeTerminalFrameReader(GroupedExchangeTerminalFrameReader&&) = delete;
  GroupedExchangeTerminalFrameReader& operator=(GroupedExchangeTerminalFrameReader&&) = delete;

  [[nodiscard]] common::Result<GroupedExchangeTerminalFrameReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte, grouped_exchange_terminal_format::kFrameLength> bytes_{};
  std::size_t buffered_bytes_{};
  std::optional<common::Status> failure_;
};

class GroupedExchangeTerminalFrameWriteCursor {
public:
  GroupedExchangeTerminalFrameWriteCursor() = delete;
  GroupedExchangeTerminalFrameWriteCursor(const GroupedExchangeTerminalFrameWriteCursor&) = delete;
  GroupedExchangeTerminalFrameWriteCursor&
  operator=(const GroupedExchangeTerminalFrameWriteCursor&) = delete;
  GroupedExchangeTerminalFrameWriteCursor(GroupedExchangeTerminalFrameWriteCursor&& other) noexcept;
  GroupedExchangeTerminalFrameWriteCursor&
  operator=(GroupedExchangeTerminalFrameWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<GroupedExchangeTerminalFrameWriteCursor>
  create(const GroupedExchangeTerminalMessage& message);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit GroupedExchangeTerminalFrameWriteCursor(
      EncodedGroupedExchangeTerminalMessage encoded) noexcept;

  EncodedGroupedExchangeTerminalMessage encoded_;
  std::size_t written_bytes_{};
};

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

struct GroupedFloat64ExchangeFrameReadStep {
  std::size_t consumed_bytes{};
  std::optional<GroupedFloat64ExchangeMessage> message;
};

class GroupedFloat64ExchangeFrameReader {
public:
  GroupedFloat64ExchangeFrameReader() = default;
  GroupedFloat64ExchangeFrameReader(const GroupedFloat64ExchangeFrameReader&) = delete;
  GroupedFloat64ExchangeFrameReader& operator=(const GroupedFloat64ExchangeFrameReader&) = delete;
  GroupedFloat64ExchangeFrameReader(GroupedFloat64ExchangeFrameReader&&) = delete;
  GroupedFloat64ExchangeFrameReader& operator=(GroupedFloat64ExchangeFrameReader&&) = delete;

  [[nodiscard]] common::Result<GroupedFloat64ExchangeFrameReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte, grouped_float64_exchange_format::kFrameLength> bytes_{};
  std::size_t buffered_bytes_{};
  std::optional<common::Status> failure_;
};

class GroupedFloat64ExchangeFrameWriteCursor {
public:
  GroupedFloat64ExchangeFrameWriteCursor() = delete;
  GroupedFloat64ExchangeFrameWriteCursor(const GroupedFloat64ExchangeFrameWriteCursor&) = delete;
  GroupedFloat64ExchangeFrameWriteCursor&
  operator=(const GroupedFloat64ExchangeFrameWriteCursor&) = delete;
  GroupedFloat64ExchangeFrameWriteCursor(GroupedFloat64ExchangeFrameWriteCursor&& other) noexcept;
  GroupedFloat64ExchangeFrameWriteCursor&
  operator=(GroupedFloat64ExchangeFrameWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<GroupedFloat64ExchangeFrameWriteCursor>
  create(const GroupedFloat64ExchangeMessage& message);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit GroupedFloat64ExchangeFrameWriteCursor(
      EncodedGroupedFloat64ExchangeMessage encoded) noexcept;

  EncodedGroupedFloat64ExchangeMessage encoded_;
  std::size_t written_bytes_{};
};

struct GroupedFloat64AggregateResult {
  std::optional<double> group_key;
  MergeableAggregateState aggregate;
};

enum class DistributedGroupedFloat64ResultDirection : std::uint8_t {
  kAscending = 1,
  kDescending = 2,
};

enum class DistributedGroupedFloat64NullPlacement : std::uint8_t {
  kFirst = 1,
  kLast = 2,
};

struct DistributedGroupedFloat64ResultOptions {
  DistributedGroupedFloat64ResultDirection direction{
      DistributedGroupedFloat64ResultDirection::kAscending};
  DistributedGroupedFloat64NullPlacement null_placement{
      DistributedGroupedFloat64NullPlacement::kFirst};
  std::optional<std::uint64_t> limit;
};

// Single-owner grouped coordinator. The owner serializes admissions, failures, and finish. It
// retains a bounded canonical retry history and exposes no partial result before every tablet has
// accepted exactly one terminal form.
class DistributedGroupedFloat64Coordinator {
public:
  DistributedGroupedFloat64Coordinator() = delete;
  ~DistributedGroupedFloat64Coordinator();
  DistributedGroupedFloat64Coordinator(const DistributedGroupedFloat64Coordinator&) = delete;
  DistributedGroupedFloat64Coordinator&
  operator=(const DistributedGroupedFloat64Coordinator&) = delete;
  DistributedGroupedFloat64Coordinator(DistributedGroupedFloat64Coordinator&&) noexcept;
  DistributedGroupedFloat64Coordinator& operator=(DistributedGroupedFloat64Coordinator&&) noexcept;

  [[nodiscard]] static common::Result<DistributedGroupedFloat64Coordinator>
  create(common::Uuid query_id, std::vector<schema::TabletId> tablets,
         DistributedCoordinatorLimits limits = {},
         DistributedGroupedFloat64ResultOptions result_options = {});
  [[nodiscard]] common::Status accept(const GroupedFloat64ExchangeMessage& message);
  [[nodiscard]] common::Status accept_terminal(const GroupedExchangeTerminalMessage& message);
  [[nodiscard]] common::Status worker_failed(const schema::TabletId& tablet_id,
                                             common::Status failure);
  [[nodiscard]] common::Result<std::vector<GroupedFloat64AggregateResult>> finish() const;

private:
  class Impl;
  explicit DistributedGroupedFloat64Coordinator(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

// Canonical one-key grouping-state frame. It is a distinct protocol from the frozen ungrouped v1
// exchange and does not imply grouped planning, multi-key tuples, or coordinator merge semantics.
[[nodiscard]] common::Result<EncodedGroupedFloat64ExchangeMessage>
encode_grouped_float64_exchange_message(const GroupedFloat64ExchangeMessage& message);

[[nodiscard]] common::Result<GroupedFloat64ExchangeMessage>
decode_grouped_float64_exchange_message_exact(common::ByteView bytes);

// Distinct terminal-only frame for an empty grouped tablet stream. It cannot be represented by a
// NULL group key because NULL is a real SQL group identity.
[[nodiscard]] common::Result<EncodedGroupedExchangeTerminalMessage>
encode_grouped_exchange_terminal_message(const GroupedExchangeTerminalMessage& message);

[[nodiscard]] common::Result<GroupedExchangeTerminalMessage>
decode_grouped_exchange_terminal_message_exact(common::ByteView bytes);

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_GROUPED_EXCHANGE_HPP_
