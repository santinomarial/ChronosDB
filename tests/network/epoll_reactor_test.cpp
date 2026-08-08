#include "chronos/network/epoll_reactor.hpp"
#include "chronos/network/messages.hpp"

#include <gtest/gtest.h>

#if defined(__linux__)
#include <arpa/inet.h>
#include <cerrno>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#endif

namespace chronos::network {
namespace {

TEST(EpollReactorTest, PlatformBoundaryIsExplicit) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  auto reactor = EpollReactor::start({}, {.requests = &requests, .responses = &responses});
#if defined(__linux__)
  ASSERT_TRUE(reactor.has_value()) << reactor.error().to_string();
  EXPECT_NE(reactor->bound_port(), 0U);
  EXPECT_TRUE(reactor->is_running());
  EXPECT_TRUE(reactor->shutdown().is_ok());
#else
  ASSERT_FALSE(reactor.has_value());
  EXPECT_EQ(reactor.error().code(), common::StatusCode::kNotSupported);
#endif
}

#if defined(__linux__)
[[nodiscard]] int connect_client(const std::uint16_t port) {
  const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (client < 0)
    return -1;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::connect(client, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(client);
    return -1;
  }
  return client;
}

void send_all(const int socket, const common::ByteView bytes) {
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const ssize_t count =
        ::send(socket, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    ASSERT_GT(count, 0);
    offset += static_cast<std::size_t>(count);
  }
}

[[nodiscard]] std::vector<std::byte> receive_available(const int socket) {
  std::vector<std::byte> bytes;
  std::array<std::byte, 4096> buffer{};
  for (;;) {
    const ssize_t count = ::recv(socket, buffer.data(), buffer.size(), MSG_DONTWAIT);
    if (count < 0 && errno == EAGAIN)
      break;
    if (count <= 0)
      break;
    bytes.insert(bytes.end(), buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(count));
  }
  return bytes;
}

TEST(EpollReactorTest, RealSocketsHandshakeDispatchRespondAndExposeQueueOverload) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(1U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  EpollServerConfig config;
  config.read_chunk_bytes = 3U;
  config.maximum_connections = 4U;
  config.maximum_events_per_poll = 16U;
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();
  const int client = connect_client(reactor.bound_port());
  ASSERT_GE(client, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{10}).is_ok());

  const auto hello_payload = encode_client_hello({}).value();
  const auto hello =
      encode_frame({.message_type = MessageType::kClientHello}, hello_payload).value();
  send_all(client, hello);
  for (std::size_t attempt = 0U; attempt < 64U; ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const std::vector<std::byte> hello_response = receive_available(client);
  const auto decoded_hello = decode_frame(hello_response);
  ASSERT_TRUE(decoded_hello.has_value()) << decoded_hello.error().to_string();
  EXPECT_EQ(decoded_hello->header.message_type, MessageType::kServerHello);

  const auto query_payload = encode_query_request("SELECT 1").value();
  const auto query =
      encode_frame({.message_type = MessageType::kQueryRequest, .request_id = 1U}, query_payload)
          .value();
  send_all(client, query);
  for (std::size_t attempt = 0U; attempt < 64U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  auto dispatched = requests.try_pop();
  ASSERT_TRUE(dispatched.has_value());
  EXPECT_EQ(dispatched->frame.header.request_id, 1U); // NOLINT(bugprone-unchecked-optional-access)

  ASSERT_TRUE(responses.try_push(
      {.connection_id = dispatched->connection_id,
       .frame = {.header = {.message_type = MessageType::kQueryEnd, .request_id = 1U},
                 .payload = {}}}));
  for (std::size_t attempt = 0U; attempt < 4U; ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto end = decode_frame(receive_available(client));
  ASSERT_TRUE(end.has_value());
  EXPECT_EQ(end->header.message_type, MessageType::kQueryEnd);

  ASSERT_TRUE(requests.try_push({.connection_id = 999U, .frame = {}}));
  const auto second_query =
      encode_frame({.message_type = MessageType::kQueryRequest, .request_id = 2U}, query_payload)
          .value();
  send_all(client, second_query);
  for (std::size_t attempt = 0U; attempt < 64U; ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto overloaded = decode_frame(receive_available(client));
  ASSERT_TRUE(overloaded.has_value());
  EXPECT_EQ(overloaded->header.message_type, MessageType::kError);
  EXPECT_EQ(reactor.metrics().queue_overloads, 1U);
  ::close(client);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

TEST(EpollReactorTest, SlowHandshakeTimesOutAndConnectionAdmissionIsBounded) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  EpollServerConfig config;
  config.maximum_connections = 1U;
  config.handshake_timeout = std::chrono::milliseconds{2};
  config.idle_timeout = std::chrono::milliseconds{100};
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();
  const int slow = connect_client(reactor.bound_port());
  ASSERT_GE(slow, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());

  const auto hello_payload = encode_client_hello({}).value();
  const auto hello =
      encode_frame({.message_type = MessageType::kClientHello}, hello_payload).value();
  send_all(slow, common::ByteView{hello}.first(7U));
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());

  const int excess = connect_client(reactor.bound_port());
  ASSERT_GE(excess, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  EXPECT_EQ(reactor.metrics().rejected_connections, 1U);
  ::close(excess);

  std::this_thread::sleep_for(std::chrono::milliseconds{3});
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(reactor.metrics().timed_out_connections, 1U);
  EXPECT_EQ(reactor.metrics().active_connections, 0U);
  ::close(slow);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}
#endif

} // namespace
} // namespace chronos::network
