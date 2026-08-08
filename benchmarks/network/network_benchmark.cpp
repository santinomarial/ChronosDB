#include "chronos/network/client_session.hpp"
#include "chronos/network/connection_buffers.hpp"
#include "chronos/network/epoll_reactor.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/schema/logical_type.hpp"
#include "support/counting_allocator.hpp"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <arpa/inet.h>
#include <cerrno>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace chronos::network {
namespace {

[[nodiscard]] std::vector<std::byte> payload(const std::size_t size) {
  std::vector<std::byte> bytes(size);
  for (std::size_t index = 0U; index < size; ++index)
    bytes[index] = static_cast<std::byte>((index * 131U) & 0xffU);
  return bytes;
}

void frame_round_trip(benchmark::State& state) {
  const std::size_t payload_size = static_cast<std::size_t>(state.range(0));
  const std::vector<std::byte> source = payload(payload_size);
  std::size_t measured_allocations = 0U;
  std::size_t measured_allocated_bytes = 0U;
  {
    benchmark_support::ScopedAllocationCounting counting;
    auto encoded = encode_frame({.message_type = MessageType::kQueryResult, .request_id = 7U},
                                source);
    auto decoded = encoded.has_value() ? decode_frame(*encoded) : common::Result<Frame>{
                                                                  common::make_unexpected(
                                                                      encoded.error())};
    const benchmark_support::AllocationCounts counts = counting.stop();
    measured_allocations = counts.allocations;
    measured_allocated_bytes = counts.allocated_bytes;
    if (!decoded.has_value()) {
      state.SkipWithError(decoded.error().to_string());
      return;
    }
  }
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = encode_frame({.message_type = MessageType::kQueryResult, .request_id = 7U},
                                source);
    if (!encoded.has_value()) {
      state.SkipWithError(encoded.error().to_string());
      return;
    }
    auto decoded = decode_frame(*encoded);
    if (!decoded.has_value()) {
      state.SkipWithError(decoded.error().to_string());
      return;
    }
    benchmark::DoNotOptimize(decoded->payload.data());
  }
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(payload_size) * 2);
  state.counters["alloc_bytes"] = static_cast<double>(measured_allocated_bytes);
  state.counters["allocations"] = static_cast<double>(measured_allocations);
  state.SetLabel("encode + checked decode; payload bytes counted in both directions");
}

[[nodiscard]] std::vector<QueryResultCell> result_cells(const std::uint32_t rows,
                                                        const common::ByteView value) {
  std::vector<QueryResultCell> cells;
  cells.reserve(static_cast<std::size_t>(rows) * 2U);
  for (std::uint32_t row = 0U; row < rows; ++row) {
    cells.push_back({.is_null = false, .value = value});
    cells.push_back({.is_null = (row % 8U) == 0U,
                     .value = (row % 8U) == 0U ? common::ByteView{} : value});
  }
  return cells;
}

void query_result_round_trip(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 2> columns{
      QueryResultColumn{.name = "event_time", .type = type, .nullable = false},
      QueryResultColumn{.name = "value", .type = type, .nullable = true}};
  const std::array<std::byte, sizeof(std::int64_t)> value{};
  const std::vector<QueryResultCell> cells = result_cells(rows, value);
  const auto example = encode_query_result_batch(rows, columns, cells);
  if (!example.has_value()) {
    state.SkipWithError(example.error().to_string());
    return;
  }
  std::size_t measured_allocations = 0U;
  std::size_t measured_allocated_bytes = 0U;
  {
    benchmark_support::ScopedAllocationCounting counting;
    auto encoded = encode_query_result_batch(rows, columns, cells);
    auto decoded = encoded.has_value() ? decode_query_result_batch(*encoded)
                                       : common::Result<QueryResultBatchView>{
                                             common::make_unexpected(encoded.error())};
    const benchmark_support::AllocationCounts counts = counting.stop();
    measured_allocations = counts.allocations;
    measured_allocated_bytes = counts.allocated_bytes;
    if (!decoded.has_value()) {
      state.SkipWithError(decoded.error().to_string());
      return;
    }
  }
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = encode_query_result_batch(rows, columns, cells);
    if (!encoded.has_value()) {
      state.SkipWithError(encoded.error().to_string());
      return;
    }
    auto decoded = decode_query_result_batch(*encoded);
    if (!decoded.has_value()) {
      state.SkipWithError(decoded.error().to_string());
      return;
    }
    benchmark::DoNotOptimize(decoded->cell(rows == 0U ? 0U : rows - 1U, 0U));
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * rows);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(example->size()) * 2);
  state.counters["alloc_bytes"] = static_cast<double>(measured_allocated_bytes);
  state.counters["allocations"] = static_cast<double>(measured_allocations);
  state.counters["encoded_bytes"] = static_cast<double>(example->size());
  state.SetLabel("two INT64 columns; one-eighth NULL in nullable column");
}

void fragmented_connection_receive(benchmark::State& state) {
  const std::size_t payload_size = static_cast<std::size_t>(state.range(0));
  const std::size_t fragment_size = static_cast<std::size_t>(state.range(1));
  const std::vector<std::byte> source = payload(payload_size);
  const std::vector<std::byte> encoded =
      encode_frame({.message_type = MessageType::kQueryResult, .request_id = 11U}, source).value();
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    ConnectionBuffers buffers = ConnectionBuffers::create().value();
    state.ResumeTiming();
    std::size_t decoded_frames = 0U;
    for (std::size_t offset = 0U; offset < encoded.size(); offset += fragment_size) {
      const std::size_t count = std::min(fragment_size, encoded.size() - offset);
      auto frames = buffers.receive(common::ByteView{encoded}.subspan(offset, count));
      if (!frames.has_value()) {
        state.SkipWithError(frames.error().to_string());
        return;
      }
      decoded_frames += frames->size();
    }
    if (decoded_frames != 1U) {
      state.SkipWithError("fragmented receive did not produce exactly one frame");
      return;
    }
  }
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(encoded.size()));
  state.counters["fragments"] =
      static_cast<double>((encoded.size() + fragment_size - 1U) / fragment_size);
  state.SetLabel("buffer construction excluded; append, header validation, CRC, and compaction");
}

[[nodiscard]] NetworkTask queue_task(const std::uint64_t request_id) {
  return {.connection_id = 1U,
          .principal_id = 2U,
          .frame = {.header = {.message_type = MessageType::kQueryRequest,
                              .request_id = request_id},
                    .payload = {}}};
}

void spsc_push_pop(benchmark::State& state) {
  const std::size_t capacity = static_cast<std::size_t>(state.range(0));
  SpscNetworkTaskQueue queue = SpscNetworkTaskQueue::create(capacity).value();
  std::uint64_t request_id = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    if (!queue.try_push(queue_task(++request_id))) {
      state.SkipWithError("empty SPSC queue rejected a push");
      return;
    }
    auto task = queue.try_pop();
    if (!task.has_value()) {
      state.SkipWithError("SPSC queue lost a pushed task");
      return;
    }
    benchmark::DoNotOptimize(task->frame.header.request_id);
  }
  state.SetItemsProcessed(state.iterations());
  state.counters["capacity"] = static_cast<double>(capacity);
  state.SetLabel("single-thread release/acquire push-pop pair; zero steady-state allocation");
}

void spsc_saturation_cycle(benchmark::State& state) {
  const std::size_t capacity = static_cast<std::size_t>(state.range(0));
  SpscNetworkTaskQueue queue = SpscNetworkTaskQueue::create(capacity).value();
  std::uint64_t request_id = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    for (std::size_t index = 0U; index < capacity; ++index) {
      if (!queue.try_push(queue_task(++request_id))) {
        state.SkipWithError("SPSC queue rejected before its declared capacity");
        return;
      }
    }
    if (queue.try_push(queue_task(++request_id))) {
      state.SkipWithError("SPSC queue accepted work beyond its declared capacity");
      return;
    }
    for (std::size_t index = 0U; index < capacity; ++index) {
      if (!queue.try_pop().has_value()) {
        state.SkipWithError("SPSC saturation drain lost work");
        return;
      }
    }
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(capacity));
  state.counters["capacity"] = static_cast<double>(capacity);
  state.counters["rejections_per_cycle"] = 1.0;
  state.SetLabel("fill, one explicit overload rejection, and complete drain");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(frame_round_trip)->Arg(64)->Arg(4'096)->Arg(65'536)->Arg(1'048'576);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(query_result_round_trip)->Arg(0)->Arg(16)->Arg(256)->Arg(4'096);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(fragmented_connection_receive)
    ->Args({4'096, 1})
    ->Args({4'096, 64})
    ->Args({65'536, 1'500})
    ->Args({1'048'576, 65'536});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(spsc_push_pop)->Arg(1)->Arg(64)->Arg(1'024);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(spsc_saturation_cycle)->Arg(1)->Arg(64)->Arg(1'024);

#if defined(__linux__)

class SocketHandle {
public:
  explicit SocketHandle(const int descriptor = -1) noexcept : descriptor_(descriptor) {}
  ~SocketHandle() {
    if (descriptor_ >= 0)
      static_cast<void>(::close(descriptor_));
  }
  SocketHandle(const SocketHandle&) = delete;
  SocketHandle& operator=(const SocketHandle&) = delete;
  SocketHandle(SocketHandle&& other) noexcept
      : descriptor_(std::exchange(other.descriptor_, -1)) {}
  SocketHandle& operator=(SocketHandle&&) = delete;
  [[nodiscard]] int get() const noexcept { return descriptor_; }

private:
  int descriptor_;
};

[[nodiscard]] SocketHandle connect_client(const std::uint16_t port) {
  SocketHandle socket{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
  if (socket.get() < 0)
    return socket;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::connect(socket.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0)
    return SocketHandle{};
  return socket;
}

[[nodiscard]] bool send_pending(const int socket, NativeClientSession& client) {
  const common::ByteView pending = client.pending_write();
  std::size_t offset = 0U;
  while (offset < pending.size()) {
    const ssize_t sent =
        ::send(socket, pending.data() + offset, pending.size() - offset, MSG_NOSIGNAL);
    if (sent <= 0)
      return false;
    offset += static_cast<std::size_t>(sent);
  }
  return client.consume_written(pending.size()).is_ok();
}

[[nodiscard]] common::Result<std::vector<std::byte>> receive_available(const int socket) {
  std::vector<std::byte> bytes;
  std::array<std::byte, 65'536> buffer{};
  for (;;) {
    const ssize_t count = ::recv(socket, buffer.data(), buffer.size(), MSG_DONTWAIT);
    if (count < 0 && errno == EAGAIN)
      return bytes;
    if (count < 0)
      return common::make_unexpected(
          common::Status{common::StatusCode::kIoError, "loopback receive failed"});
    if (count == 0)
      return bytes;
    bytes.insert(bytes.end(), buffer.begin(),
                 buffer.begin() + static_cast<std::ptrdiff_t>(count));
  }
}

[[nodiscard]] bool drive_handshake(EpollReactor& reactor, const int socket,
                                   NativeClientSession& client) {
  if (!client.queue_handshake().is_ok() || !send_pending(socket, client))
    return false;
  for (std::size_t attempt = 0U; attempt < 128U; ++attempt) {
    if (!reactor.poll_once(std::chrono::milliseconds{1}).is_ok())
      return false;
    auto bytes = receive_available(socket);
    if (!bytes.has_value())
      return false;
    if (!bytes->empty() && !client.receive(*bytes).has_value())
      return false;
    if (client.phase() == ClientSessionPhase::kActive)
      return true;
  }
  return false;
}

void epoll_connection_churn(benchmark::State& state) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(64U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(64U).value();
  EpollServerConfig config;
  config.maximum_connections = 64U;
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();
  for ([[maybe_unused]] auto iteration : state) {
    SocketHandle client = connect_client(reactor.bound_port());
    if (client.get() < 0 || !reactor.poll_once(std::chrono::milliseconds{1}).is_ok()) {
      state.SkipWithError("loopback connection admission failed");
      return;
    }
  }
  if (!reactor.poll_once(std::chrono::milliseconds{0}).is_ok()) {
    state.SkipWithError("final loopback connection detach failed");
    return;
  }
  const EpollServerMetrics metrics = reactor.metrics();
  state.SetItemsProcessed(state.iterations());
  state.counters["accepted"] = static_cast<double>(metrics.accepted_connections);
  state.counters["closed"] = static_cast<double>(metrics.closed_connections);
  state.SetLabel("TCP connect, epoll admission, peer close, and descriptor detach");
  if (!reactor.shutdown().is_ok())
    state.SkipWithError("reactor shutdown failed");
}

struct LoopbackClient {
  SocketHandle socket;
  NativeClientSession session;

  LoopbackClient(SocketHandle socket_value, NativeClientSession session_value)
      : socket(std::move(socket_value)), session(std::move(session_value)) {}
  LoopbackClient(LoopbackClient&&) noexcept = default;
  LoopbackClient& operator=(LoopbackClient&&) = delete;
};

[[nodiscard]] common::Status complete_query_round(EpollReactor& reactor,
                                                  SpscNetworkTaskQueue& requests,
                                                  SpscNetworkTaskQueue& responses,
                                                  std::vector<LoopbackClient>& clients,
                                                  const common::ByteView result_payload) {
  for (LoopbackClient& client : clients) {
    if (!client.session.queue_query("SELECT 1").has_value() ||
        !send_pending(client.socket.get(), client.session))
      return {common::StatusCode::kInternal, "loopback client could not send a query"};
  }

  std::vector<NetworkTask> dispatched;
  dispatched.reserve(clients.size());
  for (std::size_t attempt = 0U; attempt < 4'096U && dispatched.size() < clients.size(); ++attempt) {
    if (!reactor.poll_once(std::chrono::milliseconds{0}).is_ok())
      return {common::StatusCode::kInternal, "reactor failed while dispatching queries"};
    while (auto task = requests.try_pop())
      dispatched.push_back(std::move(*task));
  }
  if (dispatched.size() != clients.size())
    return {common::StatusCode::kInternal, "reactor did not dispatch every client query"};

  for (NetworkTask& request : dispatched) {
    const std::uint64_t request_id = request.frame.header.request_id;
    if (!responses.try_push(
            {.connection_id = request.connection_id,
             .frame = {.header = {.message_type = MessageType::kQueryResult,
                                  .flags = kFrameFlagEndStream,
                                  .request_id = request_id},
                       .payload = std::vector<std::byte>{result_payload.begin(),
                                                         result_payload.end()}}}) ||
        !responses.try_push(
            {.connection_id = request.connection_id,
             .frame = {.header = {.message_type = MessageType::kQueryEnd,
                                  .request_id = request_id},
                       .payload = {}}}))
      return {common::StatusCode::kInternal, "response queue rejected a query terminal pair"};
  }
  if (!reactor.notify_response_ready().is_ok())
    return {common::StatusCode::kInternal, "response wakeup failed"};

  for (std::size_t attempt = 0U; attempt < 4'096U; ++attempt) {
    if (!reactor.poll_once(std::chrono::milliseconds{0}).is_ok())
      return {common::StatusCode::kInternal, "reactor failed while delivering responses"};
    std::size_t unfinished = 0U;
    for (LoopbackClient& client : clients) {
      auto bytes = receive_available(client.socket.get());
      if (!bytes.has_value())
        return {common::StatusCode::kInternal, "loopback client receive failed"};
      if (!bytes->empty()) {
        auto frames = client.session.receive(*bytes);
        if (!frames.has_value())
          return frames.error();
      }
      unfinished += client.session.in_flight_requests();
    }
    if (unfinished == 0U)
      return common::Status::ok();
  }
  return {common::StatusCode::kInternal, "not every client received QUERY_END"};
}

void epoll_equal_connection_round(benchmark::State& state) {
  const std::size_t connection_count = static_cast<std::size_t>(state.range(0));
  const std::size_t queue_capacity = connection_count * 2U + 1U;
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(queue_capacity).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(queue_capacity).value();
  EpollServerConfig config;
  config.maximum_connections = connection_count;
  config.maximum_events_per_poll = std::max<std::size_t>(connection_count * 2U, 16U);
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();
  std::vector<LoopbackClient> clients;
  clients.reserve(connection_count);
  for (std::size_t index = 0U; index < connection_count; ++index) {
    SocketHandle socket = connect_client(reactor.bound_port());
    NativeClientSession session = NativeClientSession::create().value();
    if (socket.get() < 0 || !drive_handshake(reactor, socket.get(), session)) {
      state.SkipWithError("loopback handshake failed");
      return;
    }
    clients.emplace_back(std::move(socket), std::move(session));
  }
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 1> columns{
      QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  const std::vector<std::byte> result_payload =
      encode_query_result_batch(0U, columns, {}).value();

  for ([[maybe_unused]] auto iteration : state) {
    const common::Status status =
        complete_query_round(reactor, requests, responses, clients, result_payload);
    if (!status.is_ok()) {
      state.SkipWithError(status.to_string());
      return;
    }
  }
  const EpollServerMetrics metrics = reactor.metrics();
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(connection_count));
  state.counters["connections"] = static_cast<double>(connection_count);
  state.counters["queue_capacity"] = static_cast<double>(queue_capacity);
  state.counters["wire_bytes_per_request"] =
      static_cast<double>(metrics.bytes_read + metrics.bytes_written) /
      static_cast<double>(std::max<std::uint64_t>(metrics.dispatched_requests, 1U));
  state.SetLabel("one query per connection; round ends only after every client receives QUERY_END");
  if (!reactor.shutdown().is_ok())
    state.SkipWithError("reactor shutdown failed");
}

// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(epoll_connection_churn);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(epoll_equal_connection_round)->Arg(1)->Arg(8)->Arg(32);

#endif

} // namespace
} // namespace chronos::network
