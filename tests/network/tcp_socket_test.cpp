#include "chronos/network/tcp_socket.hpp"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/tcp.h>
#include <optional>
#include <poll.h>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace chronos::network {
namespace {

TEST(TcpSocketTest, ParsesOnlyCanonicalNonzeroIpv4Endpoints) {
  const auto parsed = parse_ipv4_endpoint("127.0.0.1:7441");
  ASSERT_TRUE(parsed.has_value()) << parsed.error().to_string();
  EXPECT_EQ(*parsed, (Ipv4Endpoint{{127U, 0U, 0U, 1U}, 7441U}));
  EXPECT_EQ(*parse_ipv4_endpoint("255.255.255.255:65535"),
            (Ipv4Endpoint{{255U, 255U, 255U, 255U}, 65535U}));

  for (const std::string_view invalid :
       {"", "127.0.0.1", "127.0.0.1:", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:01",
        "127.00.0.1:1", "0.0.0.0:1", "256.0.0.1:1", "1.2.3:1", "1.2.3.4.5:1", "node.example:1",
        " 127.0.0.1:1", "127.0.0.1:1 "}) {
    EXPECT_EQ(parse_ipv4_endpoint(invalid).error().code(), common::StatusCode::kInvalidArgument)
        << invalid;
  }
}

void wait_for_connection(TcpListener& listener, TcpSocket& client,
                         std::optional<TcpSocket>& accepted) {
  for (std::size_t attempt = 0U; attempt < 32U && !accepted.has_value(); ++attempt) {
    std::array<pollfd, 2> descriptors{{{.fd = listener.descriptor(), .events = POLLIN},
                                       {.fd = client.descriptor(), .events = POLLOUT}}};
    ASSERT_GE(::poll(descriptors.data(), descriptors.size(), 100), 0);
    if (client.connect_state() == TcpConnectState::kInProgress &&
        (descriptors[1].revents & (POLLOUT | POLLERR | POLLHUP)) != 0) {
      auto finished = client.finish_connect();
      ASSERT_TRUE(finished.has_value()) << finished.error().message();
    }
    if ((descriptors[0].revents & POLLIN) != 0) {
      auto next = listener.accept_one();
      ASSERT_TRUE(next.has_value()) << next.error().message();
      if (next->has_value())
        accepted.emplace(std::move(**next));
    }
  }
}

TEST(TcpSocketTest, EstablishesOwnedNonblockingLoopbackConnection) {
  auto listener = TcpListener::bind();
  ASSERT_TRUE(listener.has_value()) << listener.error().message();
  ASSERT_TRUE(listener->valid());
  const Ipv4Endpoint bound = listener->bound_endpoint();
  EXPECT_EQ(bound.address, (std::array<std::uint8_t, 4>{127U, 0U, 0U, 1U}));
  EXPECT_NE(bound.port, 0U);
  auto initially_empty = listener->accept_one();
  ASSERT_TRUE(initially_empty.has_value());
  EXPECT_FALSE(initially_empty->has_value());

  auto client = TcpSocket::begin_connect(bound);
  ASSERT_TRUE(client.has_value()) << client.error().message();
  std::optional<TcpSocket> accepted;
  wait_for_connection(*listener, *client, accepted);
  ASSERT_TRUE(accepted.has_value());
  if (client->connect_state() == TcpConnectState::kInProgress) {
    auto finished = client->finish_connect();
    ASSERT_TRUE(finished.has_value()) << finished.error().message();
  }
  EXPECT_EQ(client->finish_connect(), TcpConnectState::kConnected);

  const auto client_local = client->local_endpoint();
  const auto client_peer = client->peer_endpoint();
  const auto server_local = accepted->local_endpoint();
  const auto server_peer = accepted->peer_endpoint();
  ASSERT_TRUE(client_local.has_value());
  ASSERT_TRUE(client_peer.has_value());
  ASSERT_TRUE(server_local.has_value());
  ASSERT_TRUE(server_peer.has_value());
  EXPECT_EQ(*client_peer, bound);
  EXPECT_EQ(*server_local, bound);
  EXPECT_EQ(*server_peer, *client_local);

  for (const int descriptor :
       {listener->descriptor(), client->descriptor(), accepted->descriptor()}) {
    EXPECT_NE(::fcntl(descriptor, F_GETFL, 0) & O_NONBLOCK, 0);
    EXPECT_NE(::fcntl(descriptor, F_GETFD, 0) & FD_CLOEXEC, 0);
  }
  for (const int descriptor : {client->descriptor(), accepted->descriptor()}) {
    int no_delay{};
    socklen_t size = sizeof(no_delay);
    ASSERT_EQ(::getsockopt(descriptor, IPPROTO_TCP, TCP_NODELAY, &no_delay, &size), 0);
    EXPECT_NE(no_delay, 0);
  }

  constexpr std::array<std::byte, 3> sent{std::byte{'t'}, std::byte{'c'}, std::byte{'p'}};
  ASSERT_EQ(::send(client->descriptor(), sent.data(), sent.size(), 0),
            static_cast<ssize_t>(sent.size()));
  pollfd readable{.fd = accepted->descriptor(), .events = POLLIN};
  ASSERT_GT(::poll(&readable, 1U, 100), 0);
  std::array<std::byte, 3> received{};
  ASSERT_EQ(::recv(accepted->descriptor(), received.data(), received.size(), 0),
            static_cast<ssize_t>(received.size()));
  EXPECT_EQ(received, sent);
}

TEST(TcpSocketTest, RejectsInvalidConfigurationAndClosesExactlyOwnedDescriptors) {
  EXPECT_EQ(TcpSocket::begin_connect({}).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(TcpListener::bind({.backlog = 0}).error().code(), common::StatusCode::kInvalidArgument);
  TcpSocket empty;
  EXPECT_FALSE(empty.valid());
  EXPECT_EQ(empty.finish_connect().error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(empty.close().is_ok());

  auto listener = TcpListener::bind();
  ASSERT_TRUE(listener.has_value());
  const int descriptor = listener->descriptor();
  TcpListener moved = std::move(*listener);
  EXPECT_FALSE(listener->valid());
  EXPECT_EQ(listener->descriptor(), -1);
  EXPECT_TRUE(moved.close().is_ok());
  errno = 0;
  EXPECT_EQ(::fcntl(descriptor, F_GETFD, 0), -1);
  EXPECT_EQ(errno, EBADF);
  EXPECT_TRUE(moved.close().is_ok());
}

TEST(TcpSocketTest, RefusedConnectCannotBecomeSuccessfulOnRetry) {
  auto listener = TcpListener::bind();
  ASSERT_TRUE(listener.has_value());
  const Ipv4Endpoint endpoint = listener->bound_endpoint();
  ASSERT_TRUE(listener->close().is_ok());

  auto refused = TcpSocket::begin_connect(endpoint);
  if (!refused.has_value()) {
    EXPECT_EQ(refused.error().code(), common::StatusCode::kIoError);
    return;
  }
  pollfd writable{.fd = refused->descriptor(), .events = POLLOUT};
  ASSERT_GT(::poll(&writable, 1U, 100), 0);
  const auto completion = refused->finish_connect();
  ASSERT_FALSE(completion.has_value());
  EXPECT_EQ(completion.error().code(), common::StatusCode::kIoError);
  EXPECT_FALSE(refused->valid());
  EXPECT_EQ(refused->finish_connect().error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::network
