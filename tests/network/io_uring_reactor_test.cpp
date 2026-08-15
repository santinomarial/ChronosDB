#include "chronos/network/io_uring_reactor.hpp"
#include "chronos/network/messages.hpp"

#include <gtest/gtest.h>

#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#endif

namespace chronos::network {
namespace {

TEST(IoUringReactorTest, BuildAndKernelBoundaryIsExplicit) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  auto reactor = IoUringReactor::start({}, {.requests = &requests, .responses = &responses});
#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)
  if (!reactor.has_value() && reactor.error().code() == common::StatusCode::kNotSupported)
    GTEST_SKIP() << reactor.error().to_string();
  ASSERT_TRUE(reactor.has_value()) << reactor.error().to_string();
  EXPECT_NE(reactor->bound_port(), 0U);
  EXPECT_TRUE(reactor->is_running());
  EXPECT_TRUE(reactor->shutdown().is_ok());
#else
  ASSERT_FALSE(reactor.has_value());
  EXPECT_EQ(reactor.error().code(), common::StatusCode::kNotSupported);
  IoUringReactor empty;
  EXPECT_EQ(empty.poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kNotSupported);
  EXPECT_EQ(empty.notify_response_ready().code(), common::StatusCode::kNotSupported);
  EXPECT_TRUE(empty.shutdown().is_ok());
  EXPECT_EQ(empty.bound_port(), 0U);
  EXPECT_EQ(empty.metrics().active_connections, 0U);
  EXPECT_FALSE(empty.is_running());
#endif
}

#if defined(__linux__) && defined(CHRONOS_HAS_LIBURING)

class IoUringTestAuthenticator final : public ConnectionAuthenticator {
public:
  common::Result<PeerAuthenticationResult> authenticate(const PeerAuthenticationRequest&) override {
    return PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }
};

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

void send_fragmented(const int socket, IoUringReactor& reactor, const common::ByteView bytes) {
  for (const std::byte value : bytes) {
    ASSERT_EQ(::send(socket, &value, 1U, MSG_NOSIGNAL), 1);
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  }
}

[[nodiscard]] std::vector<std::byte> receive_frame(const int socket, IoUringReactor& reactor) {
  std::vector<std::byte> bytes;
  std::array<std::byte, 4096U> buffer{};
  for (std::size_t attempt = 0U; attempt < 128U; ++attempt) {
    EXPECT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
    for (;;) {
      const ssize_t count = ::recv(socket, buffer.data(), buffer.size(), MSG_DONTWAIT);
      if (count < 0 && errno == EAGAIN)
        break;
      if (count <= 0)
        return bytes;
      bytes.insert(bytes.end(), buffer.begin(),
                   buffer.begin() + static_cast<std::ptrdiff_t>(count));
    }
    if (bytes.size() >= kFrameHeaderSize) {
      auto header = decode_frame_header(common::ByteView{bytes}.first(kFrameHeaderSize));
      if (header.has_value() && bytes.size() == kFrameHeaderSize + header->payload_size)
        return bytes;
    }
  }
  return bytes;
}

TEST(IoUringReactorTest, SocketOperationsPreserveFragmentedProtocolAndShardRouting) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  EpollServerConfig config;
  config.maximum_connections = 4U;
  config.maximum_events_per_poll = 8U;
  config.read_chunk_bytes = 3U;
  IoUringTestAuthenticator authenticator;
  config.security.authenticator = &authenticator;
  auto started = IoUringReactor::start(config, {.requests = &requests, .responses = &responses});
  if (!started.has_value() && started.error().code() == common::StatusCode::kNotSupported)
    GTEST_SKIP() << started.error().to_string();
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  IoUringReactor reactor = std::move(*started);
  const int client = connect_client(reactor.bound_port());
  ASSERT_GE(client, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{10}).is_ok());

  const auto hello =
      encode_frame({.message_type = MessageType::kClientHello}, *encode_client_hello({}));
  ASSERT_TRUE(hello.has_value());
  send_fragmented(client, reactor, *hello);
  const auto hello_response = decode_frame(receive_frame(client, reactor));
  ASSERT_TRUE(hello_response.has_value()) << hello_response.error().to_string();
  EXPECT_EQ(hello_response->header.message_type, MessageType::kServerHello);

  const auto query = encode_frame({.message_type = MessageType::kQueryRequest, .request_id = 7U},
                                  *encode_query_request("SELECT 1"));
  ASSERT_TRUE(query.has_value());
  send_fragmented(client, reactor, *query);
  for (std::size_t attempt = 0U; attempt < 64U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  auto dispatched = requests.try_pop();
  ASSERT_TRUE(dispatched.has_value());
  EXPECT_EQ(dispatched->principal_id, 91U);
  EXPECT_EQ(dispatched->frame.header.request_id, 7U);

  const schema::LogicalType result_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 1U> result_columns{
      QueryResultColumn{.name = "value", .type = result_type, .nullable = false}};
  const auto empty_result = encode_query_result_batch(0U, result_columns, {});
  ASSERT_TRUE(empty_result.has_value());
  ASSERT_TRUE(responses.try_push({.connection_id = dispatched->connection_id,
                                  .principal_id = dispatched->principal_id,
                                  .frame = {.header = {.message_type = MessageType::kQueryResult,
                                                       .flags = kFrameFlagEndStream,
                                                       .request_id = 7U},
                                            .payload = std::move(*empty_result)}}));
  ASSERT_TRUE(reactor.notify_response_ready().is_ok());
  const auto result = decode_frame(receive_frame(client, reactor));
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->header.message_type, MessageType::kQueryResult);

  ASSERT_TRUE(responses.try_push(
      {.connection_id = dispatched->connection_id,
       .principal_id = dispatched->principal_id,
       .frame = {.header = {.message_type = MessageType::kQueryEnd, .request_id = 7U},
                 .payload = {}}}));
  ASSERT_TRUE(reactor.notify_response_ready().is_ok());
  const auto query_end = decode_frame(receive_frame(client, reactor));
  ASSERT_TRUE(query_end.has_value()) << query_end.error().to_string();
  EXPECT_EQ(query_end->header.message_type, MessageType::kQueryEnd);
  EXPECT_EQ(reactor.metrics().response_wakeups, 2U);

  ::close(client);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

#endif

} // namespace
} // namespace chronos::network
