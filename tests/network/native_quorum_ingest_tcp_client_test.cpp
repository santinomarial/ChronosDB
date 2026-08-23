#include "chronos/network/connection_buffers.hpp"
#include "chronos/network/connection_state.hpp"
#include "chronos/network/native_quorum_ingest_tcp_client.hpp"
#include "chronos/network/native_quorum_ingest_tcp_execution.hpp"

#include "gtest/gtest.h"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <optional>
#include <poll.h>
#include <utility>
#include <vector>

namespace chronos::network {
namespace {

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] TlsServerConfig tls_server_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] TlsClientConfig tls_client_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] common::Uuid group() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{9U};
  return common::Uuid{bytes};
}

class Authenticator final : public ConnectionAuthenticator {
public:
  common::Result<PeerAuthenticationResult>
  authenticate(const PeerAuthenticationRequest& request) override {
    if (!request.transport_authenticated || !request.peer_certificate_sha256.has_value())
      return common::make_unexpected(
          status(common::StatusCode::kUnauthenticated, "test peer is not authenticated"));
    ++calls;
    return PeerAuthenticationResult{.authorized = true, .principal_id = 77U};
  }

  std::size_t calls{};
};

class NodeAuthorizer final : public NativeNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const std::uint64_t node_id) const override {
    observed_nodes.push_back(node_id);
    return allow && principal_id == 77U && (node_id == 1U || node_id == 2U);
  }

  bool allow{true};
  mutable std::vector<std::uint64_t> observed_nodes;
};

enum class ServerReply : std::uint8_t { kRedirect, kAcknowledge, kClose };

class ScriptedNativeServer {
public:
  ScriptedNativeServer(TcpListener owned_listener, TlsServerContext owned_context,
                       ConnectionBuffers owned_buffers, ServerConnectionState owned_state,
                       const ServerReply configured_reply)
      : listener_(std::move(owned_listener)), context_(std::move(owned_context)),
        buffers_(std::move(owned_buffers)), protocol_state_(std::move(owned_state)),
        reply_(configured_reply) {}

  [[nodiscard]] static common::Result<ScriptedNativeServer> create(const ServerReply reply) {
    auto listener = TcpListener::bind();
    if (!listener.has_value())
      return common::make_unexpected(listener.error());
    auto context = TlsServerContext::create(tls_server_config());
    if (!context.has_value())
      return common::make_unexpected(context.error());
    auto buffers = ConnectionBuffers::create();
    if (!buffers.has_value())
      return common::make_unexpected(buffers.error());
    auto protocol_state =
        ServerConnectionState::create({.maximum_in_flight_requests = 1U,
                                       .maximum_protocol_major = kProtocolV2Major,
                                       .supported_feature_bits = kProtocolV2QuorumSyncFeature |
                                                                 kProtocolV2LeaderRedirectFeature});
    if (!protocol_state.has_value())
      return common::make_unexpected(protocol_state.error());
    return ScriptedNativeServer{std::move(*listener), std::move(*context), std::move(*buffers),
                                std::move(*protocol_state), reply};
  }

  [[nodiscard]] Ipv4Endpoint endpoint() const noexcept {
    return listener_.bound_endpoint();
  }

  [[nodiscard]] common::Status poll_once() {
    if (closed_after_request_)
      return common::Status::ok();
    if (!connection_.has_value()) {
      auto accepted = listener_.accept_one();
      if (!accepted.has_value())
        return accepted.error();
      TcpSocket* candidate = optional_pointer(*accepted);
      if (candidate == nullptr)
        return common::Status::ok();
      connection_.emplace(std::move(*candidate));
      TcpSocket* connection = optional_pointer(connection_);
      auto tls = TlsSocket::accept(context_, connection->descriptor());
      if (!tls.has_value())
        return tls.error();
      tls_.emplace(std::move(*tls));
    }
    TlsSocket* tls = optional_pointer(tls_);
    if (tls == nullptr)
      return status(common::StatusCode::kInternal, "test TLS server is missing its session");
    if (!tls->handshake_complete()) {
      auto handshake = tls->handshake();
      if (!handshake.has_value())
        return handshake.error();
      if (handshake->state == TlsIoState::kClosed)
        return status(common::StatusCode::kUnavailable, "test TLS handshake closed");
      return common::Status::ok();
    }
    if (!output_.empty())
      return write_output(*tls);
    return read_input(*tls);
  }

  [[nodiscard]] std::size_t requests() const noexcept {
    return request_ids_.size();
  }
  [[nodiscard]] const std::vector<std::uint64_t>& request_ids() const noexcept {
    return request_ids_;
  }
  [[nodiscard]] const std::vector<std::vector<std::byte>>& commands() const noexcept {
    return commands_;
  }

private:
  [[nodiscard]] common::Status queue(const Frame& response) {
    if (!output_.empty())
      return status(common::StatusCode::kInternal, "test server output is already occupied");
    auto encoded = encode_frame({.protocol_major = response.header.protocol_major,
                                 .protocol_minor = response.header.protocol_minor,
                                 .message_type = response.header.message_type,
                                 .flags = response.header.flags,
                                 .request_id = response.header.request_id},
                                response.payload);
    if (!encoded.has_value())
      return encoded.error();
    output_ = std::move(*encoded);
    output_offset_ = 0U;
    return common::Status::ok();
  }

  [[nodiscard]] common::Status handle(Frame frame) {
    auto action = protocol_state_.accept(frame);
    if (!action.has_value())
      return action.error();
    if (action->kind == InboundActionKind::kHandshake) {
      auto payload =
          encode_server_hello({.selected_major = action->negotiated_major,
                               .selected_minor = action->negotiated_minor,
                               .feature_bits = action->negotiated_feature_bits,
                               .maximum_payload_size = action->negotiated_maximum_payload_size});
      if (!payload.has_value())
        return payload.error();
      return queue({.header = {.protocol_major = kProtocolMajor,
                               .message_type = MessageType::kServerHello,
                               .request_id = 0U},
                    .payload = std::move(*payload)});
    }
    if (action->kind != InboundActionKind::kIngest)
      return status(common::StatusCode::kInvalidArgument,
                    "test server received an unexpected action");
    auto ingest = decode_ingest_request(
        frame.payload, {.protocol_major = protocol_state_.negotiated_major(),
                        .protocol_minor = protocol_state_.negotiated_minor(),
                        .feature_bits = protocol_state_.negotiated_feature_bits()});
    if (!ingest.has_value())
      return ingest.error();
    request_ids_.push_back(frame.header.request_id);
    commands_.emplace_back(ingest->encoded_columnar_append.begin(),
                           ingest->encoded_columnar_append.end());
    if (reply_ == ServerReply::kClose) {
      tls_.reset();
      TcpSocket* connection = optional_pointer(connection_);
      if (connection != nullptr)
        static_cast<void>(connection->close());
      closed_after_request_ = true;
      return common::Status::ok();
    }

    std::vector<std::byte> payload;
    MessageType type = MessageType::kLeaderRedirect;
    if (reply_ == ServerReply::kRedirect) {
      auto encoded = encode_leader_redirect(
          {.group_id = group(), .leader_node_id = 2U, .leader_term = 7U, .placement_epoch = 4U});
      if (!encoded.has_value())
        return encoded.error();
      payload = std::move(*encoded);
    } else {
      type = MessageType::kQuorumSyncIngestAcknowledgement;
      auto encoded =
          encode_quorum_sync_ingest_acknowledgement({.group_id = group(),
                                                     .leader_node_id = 2U,
                                                     .leader_term = 8U,
                                                     .log_index = 10U,
                                                     .entry_term = 8U,
                                                     .local_durable_physical_sequence = 12U});
      if (!encoded.has_value())
        return encoded.error();
      payload = std::move(*encoded);
    }
    Frame response{.header = {.protocol_major = kProtocolV2Major,
                              .message_type = type,
                              .request_id = frame.header.request_id},
                   .payload = std::move(payload)};
    const common::Status accepted = protocol_state_.accept_response(response);
    return accepted.is_ok() ? queue(response) : accepted;
  }

  [[nodiscard]] common::Status read_input(TlsSocket& tls) {
    std::array<std::byte, 4096U> input{};
    auto progress = tls.read(input);
    if (!progress.has_value())
      return progress.error();
    if (progress->state == TlsIoState::kClosed)
      return common::Status::ok();
    if (progress->state != TlsIoState::kComplete || progress->bytes_transferred == 0U)
      return common::Status::ok();
    auto frames = buffers_.receive(common::ByteView{input}.first(progress->bytes_transferred));
    if (!frames.has_value())
      return frames.error();
    for (Frame& frame : *frames) {
      const common::Status handled = handle(std::move(frame));
      if (!handled.is_ok())
        return handled;
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status write_output(TlsSocket& tls) {
    auto progress = tls.write(common::ByteView{output_}.subspan(output_offset_));
    if (!progress.has_value())
      return progress.error();
    if (progress->state == TlsIoState::kClosed)
      return status(common::StatusCode::kUnavailable, "test TLS response closed");
    if (progress->state != TlsIoState::kComplete || progress->bytes_transferred == 0U)
      return common::Status::ok();
    output_offset_ += progress->bytes_transferred;
    if (output_offset_ == output_.size()) {
      output_.clear();
      output_offset_ = 0U;
    }
    return common::Status::ok();
  }

  TcpListener listener_;
  TlsServerContext context_;
  ConnectionBuffers buffers_;
  ServerConnectionState protocol_state_;
  std::optional<TcpSocket> connection_;
  std::optional<TlsSocket> tls_;
  ServerReply reply_;
  std::vector<std::byte> output_;
  std::size_t output_offset_{};
  std::vector<std::uint64_t> request_ids_;
  std::vector<std::vector<std::byte>> commands_;
  bool closed_after_request_{};
};

[[nodiscard]] NativeQuorumIngestTcpClientConfig
client_config(const TlsClientContext& context, const Ipv4Endpoint first, const Ipv4Endpoint second,
              Authenticator& authenticator, const NodeAuthorizer& authorizer,
              const std::size_t io_chunk_bytes = std::size_t{64U} * 1024U) {
  return {.retry = {.routing = {.group_id = group(),
                                .initial_node_id = 1U,
                                .minimum_placement_epoch = 4U,
                                .routes = {{1U, first, &context}, {2U, second, &context}},
                                .limits = {.maximum_routes = 2U, .maximum_redirects = 2U}}},
          .authenticator = &authenticator,
          .node_authorizer = &authorizer,
          .limits = {.connect_timeout = std::chrono::milliseconds{5000},
                     .handshake_timeout = std::chrono::milliseconds{5000},
                     .exchange_timeout = std::chrono::milliseconds{5000},
                     .maximum_io_chunk_bytes = io_chunk_bytes}};
}

void drive_until_terminal(NativeQuorumIngestTcpClient& client,
                          const std::initializer_list<ScriptedNativeServer*> servers) {
  for (std::size_t iteration = 0U; iteration < 20'000U; ++iteration) {
    for (ScriptedNativeServer* server : servers) {
      const common::Status server_status = server->poll_once();
      ASSERT_TRUE(server_status.is_ok()) << server_status.to_string();
    }
    if (client.state() == NativeQuorumIngestTcpClientState::kComplete ||
        client.state() == NativeQuorumIngestTcpClientState::kFailed) {
      return;
    }
    const NativeQuorumIngestTcpInterest interest = client.interest();
    pollfd descriptor{.fd = client.descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    const int ready = ::poll(&descriptor, 1U, 1);
    ASSERT_GE(ready, 0);
    const bool readable =
        (descriptor.revents & static_cast<short>(POLLIN | POLLERR | POLLHUP)) != 0;
    const bool writable =
        (descriptor.revents & static_cast<short>(POLLOUT | POLLERR | POLLHUP)) != 0;
    static_cast<void>(
        client.on_ready(readable, writable, NativeQuorumIngestTcpClient::TimePoint::clock::now()));
  }
  FAIL() << "native QUORUM_SYNC TCP client did not reach a terminal state";
}

TEST(NativeQuorumIngestTcpClientTest, ReconnectsThroughRealMutualTlsAndFragmentsEveryByte) {
  auto first = ScriptedNativeServer::create(ServerReply::kRedirect);
  auto second = ScriptedNativeServer::create(ServerReply::kAcknowledge);
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const std::vector command{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  auto client = NativeQuorumIngestTcpClient::begin(
      client_config(*context, first->endpoint(), second->endpoint(), authenticator, authorizer, 1U),
      command, NativeQuorumIngestTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  drive_until_terminal(*client, {&*first, &*second});

  ASSERT_EQ(client->state(), NativeQuorumIngestTcpClientState::kComplete)
      << client->failure().to_string();
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_FALSE(client->interest().want_read);
  EXPECT_FALSE(client->interest().want_write);
  EXPECT_FALSE(client->deadline().has_value());
  EXPECT_EQ(client->attempts_started(), 2U);
  EXPECT_EQ(client->current_route().node_id, 2U);
  ASSERT_EQ(first->requests(), 1U);
  ASSERT_EQ(second->requests(), 1U);
  EXPECT_EQ(first->request_ids(), std::vector<std::uint64_t>{1U});
  EXPECT_EQ(second->request_ids(), std::vector<std::uint64_t>{1U});
  EXPECT_EQ(first->commands(), std::vector<std::vector<std::byte>>{command});
  EXPECT_EQ(second->commands(), std::vector<std::vector<std::byte>>{command});
  EXPECT_EQ(authenticator.calls, 2U);
  EXPECT_EQ(authorizer.observed_nodes, (std::vector<std::uint64_t>{1U, 2U}));
  const QuorumSyncIngestAcknowledgement expected{.group_id = group(),
                                                 .leader_node_id = 2U,
                                                 .leader_term = 8U,
                                                 .log_index = 10U,
                                                 .entry_term = 8U,
                                                 .local_durable_physical_sequence = 12U};
  EXPECT_EQ(*client->result(), expected);
}

TEST(NativeQuorumIngestTcpClientTest, DoesNotReplayAnAmbiguousTransportClose) {
  auto server = ScriptedNativeServer::create(ServerReply::kClose);
  auto unused = ScriptedNativeServer::create(ServerReply::kAcknowledge);
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(unused.has_value()) << unused.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  auto client = NativeQuorumIngestTcpClient::begin(
      client_config(*context, server->endpoint(), unused->endpoint(), authenticator, authorizer),
      {std::byte{4U}}, NativeQuorumIngestTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  drive_until_terminal(*client, {&*server});

  EXPECT_EQ(client->state(), NativeQuorumIngestTcpClientState::kFailed);
  EXPECT_EQ(client->failure().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->attempts_started(), 1U);
  EXPECT_EQ(client->current_route().node_id, 1U);
  EXPECT_EQ(server->requests(), 1U);
  EXPECT_EQ(unused->requests(), 0U);
  EXPECT_FALSE(client->result().has_value());
}

TEST(NativeQuorumIngestTcpClientTest, RejectsWrongNodePrincipalBeforeProtocolBytes) {
  auto server = ScriptedNativeServer::create(ServerReply::kAcknowledge);
  auto unused = ScriptedNativeServer::create(ServerReply::kAcknowledge);
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(unused.has_value()) << unused.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  authorizer.allow = false;
  auto client = NativeQuorumIngestTcpClient::begin(
      client_config(*context, server->endpoint(), unused->endpoint(), authenticator, authorizer),
      {std::byte{5U}}, NativeQuorumIngestTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  drive_until_terminal(*client, {&*server});

  EXPECT_EQ(client->state(), NativeQuorumIngestTcpClientState::kFailed);
  EXPECT_EQ(client->failure().code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(server->requests(), 0U);
  EXPECT_EQ(authorizer.observed_nodes, std::vector<std::uint64_t>{1U});
}

TEST(NativeQuorumIngestTcpClientTest, ValidatesBoundsAndExpiresConnectExactly) {
  auto listener = TcpListener::bind();
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const auto start = NativeQuorumIngestTcpClient::TimePoint{};
  auto invalid = client_config(*context, listener->bound_endpoint(), listener->bound_endpoint(),
                               authenticator, authorizer, 0U);
  EXPECT_EQ(
      NativeQuorumIngestTcpClient::begin(std::move(invalid), {std::byte{6U}}, start).error().code(),
      common::StatusCode::kInvalidArgument);

  auto config = client_config(*context, listener->bound_endpoint(), {{127U, 0U, 0U, 1U}, 1U},
                              authenticator, authorizer);
  config.limits.connect_timeout = std::chrono::milliseconds{5};
  auto client = NativeQuorumIngestTcpClient::begin(std::move(config), {std::byte{7U}}, start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  ASSERT_EQ(client->deadline(), start + std::chrono::milliseconds{5});
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status timed_out =
      client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(timed_out.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), NativeQuorumIngestTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_FALSE(client->deadline().has_value());
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), timed_out);
}

TEST(NativeQuorumIngestTcpClientTest, ExpiresHandshakeExactly) {
  auto listener = TcpListener::bind();
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const auto start = NativeQuorumIngestTcpClient::TimePoint{};
  auto config = client_config(*context, listener->bound_endpoint(), {{127U, 0U, 0U, 1U}, 1U},
                              authenticator, authorizer);
  config.limits.handshake_timeout = std::chrono::milliseconds{7};
  auto client = NativeQuorumIngestTcpClient::begin(std::move(config), {std::byte{8U}}, start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 64U && client->state() == NativeQuorumIngestTcpClientState::kConnecting;
       ++iteration) {
    pollfd descriptor{.fd = client->descriptor(), .events = POLLOUT};
    ASSERT_GE(::poll(&descriptor, 1U, 10), 0);
    ASSERT_TRUE(client->on_ready(false, (descriptor.revents & POLLOUT) != 0, start).is_ok());
  }
  ASSERT_EQ(client->state(), NativeQuorumIngestTcpClientState::kHandshaking);
  ASSERT_EQ(client->deadline(), start + std::chrono::milliseconds{7});
  const common::Status timed_out =
      client->on_ready(false, false, start + std::chrono::milliseconds{7});
  EXPECT_EQ(timed_out.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), NativeQuorumIngestTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
}

TEST(NativeQuorumIngestTcpClientTest, ExpiresAuthenticatedExchangeExactly) {
  auto server = ScriptedNativeServer::create(ServerReply::kAcknowledge);
  auto unused = ScriptedNativeServer::create(ServerReply::kAcknowledge);
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(unused.has_value()) << unused.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  auto config =
      client_config(*context, server->endpoint(), unused->endpoint(), authenticator, authorizer);
  config.limits.exchange_timeout = std::chrono::milliseconds{9};
  auto client = NativeQuorumIngestTcpClient::begin(
      std::move(config), {std::byte{9U}}, NativeQuorumIngestTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 1024U && client->state() != NativeQuorumIngestTcpClientState::kExchanging;
       ++iteration) {
    ASSERT_TRUE(server->poll_once().is_ok());
    const NativeQuorumIngestTcpInterest interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 10), 0);
    const bool readable =
        (descriptor.revents & static_cast<short>(POLLIN | POLLERR | POLLHUP)) != 0;
    const bool writable =
        (descriptor.revents & static_cast<short>(POLLOUT | POLLERR | POLLHUP)) != 0;
    ASSERT_TRUE(
        client->on_ready(readable, writable, NativeQuorumIngestTcpClient::TimePoint::clock::now())
            .is_ok())
        << client->failure().to_string();
  }
  ASSERT_EQ(client->state(), NativeQuorumIngestTcpClientState::kExchanging);
  auto deadline = client->deadline();
  const NativeQuorumIngestTcpClient::TimePoint* deadline_value = optional_pointer(deadline);
  ASSERT_NE(deadline_value, nullptr);
  const common::Status timed_out = client->on_ready(false, false, *deadline_value);
  EXPECT_EQ(timed_out.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), NativeQuorumIngestTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_EQ(server->requests(), 0U);
}

TEST(NativeQuorumIngestTcpExecutionTest, PollsACompleteRedirectedOperation) {
  auto first = ScriptedNativeServer::create(ServerReply::kRedirect);
  auto second = ScriptedNativeServer::create(ServerReply::kAcknowledge);
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const auto start = NativeQuorumIngestTcpExecution::TimePoint::clock::now();
  const std::vector command{std::byte{10U}, std::byte{11U}};
  auto execution = NativeQuorumIngestTcpExecution::begin(
      {.client = client_config(*context, first->endpoint(), second->endpoint(), authenticator,
                               authorizer, 1U),
       .operation_deadline = start + std::chrono::seconds{5}},
      command);
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  for (std::size_t iteration = 0U;
       iteration < 20'000U && execution->state() == NativeQuorumIngestTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(first->poll_once().is_ok());
    ASSERT_TRUE(second->poll_once().is_ok());
    const common::Status polled = execution->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(polled.is_ok()) << polled.to_string();
  }

  ASSERT_EQ(execution->state(), NativeQuorumIngestTcpExecutionState::kComplete)
      << execution->failure().to_string();
  const auto metrics = execution->metrics();
  EXPECT_GT(metrics.poll_calls, 0U);
  EXPECT_GT(metrics.readiness_events, 0U);
  EXPECT_EQ(metrics.attempts_started, 2U);
  EXPECT_EQ(metrics.redirects_followed, 1U);
  EXPECT_FALSE(metrics.active_client);
  EXPECT_FALSE(execution->next_deadline().has_value());
  EXPECT_EQ(execution->current_route().node_id, 2U);
  EXPECT_EQ(first->commands(), std::vector<std::vector<std::byte>>{command});
  EXPECT_EQ(second->commands(), std::vector<std::vector<std::byte>>{command});
  const QuorumSyncIngestAcknowledgement expected{.group_id = group(),
                                                 .leader_node_id = 2U,
                                                 .leader_term = 8U,
                                                 .log_index = 10U,
                                                 .entry_term = 8U,
                                                 .local_durable_physical_sequence = 12U};
  EXPECT_EQ(*execution->result(), expected);
  EXPECT_TRUE(execution->poll_once(std::chrono::milliseconds{100}).is_ok());
}

TEST(NativeQuorumIngestTcpExecutionTest, CancelsExplicitlyAndPreservesTheFirstStatus) {
  auto listener = TcpListener::bind();
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const auto start = NativeQuorumIngestTcpExecution::TimePoint::clock::now();
  auto execution = NativeQuorumIngestTcpExecution::begin(
      {.client = client_config(*context, listener->bound_endpoint(), {{127U, 0U, 0U, 1U}, 1U},
                               authenticator, authorizer),
       .operation_deadline = start + std::chrono::seconds{5}},
      {std::byte{12U}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->poll_once(std::chrono::milliseconds{-1}).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(execution->state(), NativeQuorumIngestTcpExecutionState::kRunning);

  const common::Status cancelled = execution->cancel();
  EXPECT_EQ(cancelled.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(execution->state(), NativeQuorumIngestTcpExecutionState::kCancelled);
  EXPECT_EQ(execution->failure(), cancelled);
  EXPECT_EQ(execution->poll_once(std::chrono::milliseconds{0}), cancelled);
  EXPECT_EQ(execution->cancel(), cancelled);
  EXPECT_EQ(execution->result().error(), cancelled);
  EXPECT_FALSE(execution->metrics().active_client);
  EXPECT_EQ(execution->metrics().attempts_started, 1U);
}

TEST(NativeQuorumIngestTcpExecutionTest, RejectsAnExpiredDeadlineBeforeOpeningASocket) {
  auto listener = TcpListener::bind();
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const auto start = NativeQuorumIngestTcpExecution::TimePoint::clock::now();
  auto execution = NativeQuorumIngestTcpExecution::begin(
      {.client = client_config(*context, listener->bound_endpoint(), {{127U, 0U, 0U, 1U}, 1U},
                               authenticator, authorizer),
       .operation_deadline = start},
      {std::byte{13U}});
  ASSERT_FALSE(execution.has_value());
  EXPECT_EQ(execution.error().code(), common::StatusCode::kCancelled);
  auto accepted = listener->accept_one();
  ASSERT_TRUE(accepted.has_value()) << accepted.error().to_string();
  EXPECT_FALSE(accepted->has_value());
}

TEST(NativeQuorumIngestTcpExecutionTest, BoundsTheKernelWaitByTheOperationDeadline) {
  auto listener = TcpListener::bind();
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const auto start = NativeQuorumIngestTcpExecution::TimePoint::clock::now();
  const auto operation_deadline = start + std::chrono::milliseconds{30};
  auto execution = NativeQuorumIngestTcpExecution::begin(
      {.client = client_config(*context, listener->bound_endpoint(), {{127U, 0U, 0U, 1U}, 1U},
                               authenticator, authorizer),
       .operation_deadline = operation_deadline},
      {std::byte{14U}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->next_deadline(), operation_deadline);
  common::Status progress = common::Status::ok();
  for (std::size_t iteration = 0U;
       iteration < 128U && execution->state() == NativeQuorumIngestTcpExecutionState::kRunning;
       ++iteration) {
    progress = execution->poll_once(std::chrono::milliseconds{100});
  }
  EXPECT_EQ(execution->state(), NativeQuorumIngestTcpExecutionState::kCancelled);
  EXPECT_EQ(progress.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(execution->failure(), progress);
  EXPECT_FALSE(execution->metrics().active_client);
  EXPECT_FALSE(execution->next_deadline().has_value());
  EXPECT_LT(NativeQuorumIngestTcpExecution::TimePoint::clock::now(),
            operation_deadline + std::chrono::seconds{1});
}

} // namespace
} // namespace chronos::network
