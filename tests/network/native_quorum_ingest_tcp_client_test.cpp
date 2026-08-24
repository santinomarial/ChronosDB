#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/network/connection_buffers.hpp"
#include "chronos/network/connection_state.hpp"
#include "chronos/network/native_query_tcp_client.hpp"
#include "chronos/network/native_query_tcp_execution.hpp"
#include "chronos/network/native_quorum_ingest_tcp_client.hpp"
#include "chronos/network/native_quorum_ingest_tcp_execution.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include "gtest/gtest.h"
#include <algorithm>
#include <array>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <optional>
#include <poll.h>
#include <span>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <thread>
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

[[nodiscard]] std::filesystem::path fixture(const std::string_view name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / std::filesystem::path{name};
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronosctl-process-XXXXXX").string();
    if (char* created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] bool write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  if (!bytes.empty()) {
    // Character streams are permitted to access an object's byte representation.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
  }
  output.close();
  return static_cast<bool>(output);
}

[[nodiscard]] std::vector<std::byte> canonical_append() {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto append = ingest::encode_columnar_append_v1(
                          {.client_id = ingest::test::request_id<ingest::ClientId>(9U),
                           .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(10U),
                           .tablet_id = columnar::test::id<schema::TabletId>(90U)},
                          encoded_batch)
                          .value();
  return {append.bytes().begin(), append.bytes().end()};
}

[[nodiscard]] TlsServerConfig tls_server_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] TlsServerConfig node_tls_server_config(const std::string_view node) {
  return {.certificate_chain_file = fixture(std::string{node} + ".pem").string(),
          .private_key_file = fixture(std::string{node} + "-key.pem").string(),
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

enum class ServerReply : std::uint8_t {
  kRedirect,
  kAcknowledge,
  kClose,
  kQueryRedirect,
  kQueryResult,
};

class ScriptedNativeServer {
public:
  ScriptedNativeServer(TcpListener owned_listener, TlsServerContext owned_context,
                       ConnectionBuffers owned_buffers, ServerConnectionState owned_state,
                       const ServerReply configured_reply)
      : listener_(std::move(owned_listener)), context_(std::move(owned_context)),
        buffers_(std::move(owned_buffers)), protocol_state_(std::move(owned_state)),
        reply_(configured_reply) {}

  [[nodiscard]] static common::Result<ScriptedNativeServer>
  create(const ServerReply reply, const TlsServerConfig& server_config = tls_server_config()) {
    auto listener = TcpListener::bind();
    if (!listener.has_value())
      return common::make_unexpected(listener.error());
    auto context = TlsServerContext::create(server_config);
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
  [[nodiscard]] const std::vector<std::vector<std::byte>>& queries() const noexcept {
    return queries_;
  }

private:
  [[nodiscard]] common::Status queue(const Frame& response) {
    auto encoded = encode_frame({.protocol_major = response.header.protocol_major,
                                 .protocol_minor = response.header.protocol_minor,
                                 .message_type = response.header.message_type,
                                 .flags = response.header.flags,
                                 .request_id = response.header.request_id},
                                response.payload);
    if (!encoded.has_value())
      return encoded.error();
    output_.insert(output_.end(), encoded->begin(), encoded->end());
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
    if (action->kind != InboundActionKind::kIngest && action->kind != InboundActionKind::kQuery)
      return status(common::StatusCode::kInvalidArgument,
                    "test server received an unexpected action");
    if (action->kind == InboundActionKind::kQuery)
      return handle_query(std::move(frame));
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

  [[nodiscard]] common::Status handle_query(Frame frame) {
    auto sql = decode_query_request(frame.payload);
    if (!sql.has_value())
      return sql.error();
    request_ids_.push_back(frame.header.request_id);
    queries_.emplace_back(sql->begin(), sql->end());
    if (reply_ == ServerReply::kClose) {
      tls_.reset();
      TcpSocket* connection = optional_pointer(connection_);
      if (connection != nullptr)
        static_cast<void>(connection->close());
      closed_after_request_ = true;
      return common::Status::ok();
    }
    if (reply_ == ServerReply::kQueryRedirect) {
      auto payload = encode_leader_redirect(
          {.group_id = group(), .leader_node_id = 2U, .leader_term = 7U, .placement_epoch = 4U});
      if (!payload.has_value())
        return payload.error();
      Frame response{.header = {.protocol_major = kProtocolV2Major,
                                .message_type = MessageType::kLeaderRedirect,
                                .request_id = frame.header.request_id},
                     .payload = std::move(*payload)};
      const common::Status accepted = protocol_state_.accept_response(response);
      return accepted.is_ok() ? queue(response) : accepted;
    }
    if (reply_ != ServerReply::kQueryResult)
      return status(common::StatusCode::kInvalidArgument, "test query server reply is invalid");
    const std::array columns{QueryResultColumn{
        .name = "value",
        .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
        .nullable = false}};
    auto payload = encode_query_result_batch(0U, columns, {});
    if (!payload.has_value())
      return payload.error();
    Frame result{.header = {.protocol_major = kProtocolV2Major,
                            .message_type = MessageType::kQueryResult,
                            .flags = kFrameFlagEndStream,
                            .request_id = frame.header.request_id},
                 .payload = std::move(*payload)};
    if (const common::Status accepted = protocol_state_.accept_response(result); !accepted.is_ok())
      return accepted;
    if (const common::Status queued = queue(result); !queued.is_ok())
      return queued;
    Frame end{.header = {.protocol_major = kProtocolV2Major,
                         .message_type = MessageType::kQueryEnd,
                         .request_id = frame.header.request_id}};
    const common::Status accepted = protocol_state_.accept_response(end);
    return accepted.is_ok() ? queue(end) : accepted;
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
  std::vector<std::vector<std::byte>> queries_;
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

[[nodiscard]] NativeQueryTcpClientConfig
query_client_config(const TlsClientContext& context, const Ipv4Endpoint first,
                    const Ipv4Endpoint second, Authenticator& authenticator,
                    const NodeAuthorizer& authorizer,
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

void drive_until_terminal(NativeQueryTcpClient& client,
                          const std::initializer_list<ScriptedNativeServer*> servers) {
  for (std::size_t iteration = 0U; iteration < 20'000U; ++iteration) {
    for (ScriptedNativeServer* server : servers) {
      const common::Status server_status = server->poll_once();
      ASSERT_TRUE(server_status.is_ok()) << server_status.to_string();
    }
    if (client.state() == NativeQueryTcpClientState::kComplete ||
        client.state() == NativeQueryTcpClientState::kFailed) {
      return;
    }
    const NativeQueryTcpInterest interest = client.interest();
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
        client.on_ready(readable, writable, NativeQueryTcpClient::TimePoint::clock::now()));
  }
  FAIL() << "native query TCP client did not reach a terminal state";
}

TEST(NativeQueryTcpClientTest, ReconnectsThroughRealMutualTlsAndPublishesCompleteResult) {
  auto first = ScriptedNativeServer::create(ServerReply::kQueryRedirect);
  auto second = ScriptedNativeServer::create(ServerReply::kQueryResult);
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const std::string sql = "SELECT value FROM events";
  auto client = NativeQueryTcpClient::begin(query_client_config(*context, first->endpoint(),
                                                                second->endpoint(), authenticator,
                                                                authorizer, 1U),
                                            sql, NativeQueryTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  drive_until_terminal(*client, {&*first, &*second});

  ASSERT_EQ(client->state(), NativeQueryTcpClientState::kComplete) << client->failure().to_string();
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_EQ(client->attempts_started(), 2U);
  EXPECT_EQ(client->current_route().node_id, 2U);
  const auto sql_bytes = std::as_bytes(std::span{sql.data(), sql.size()});
  const std::vector<std::byte> expected_sql(sql_bytes.begin(), sql_bytes.end());
  EXPECT_EQ(first->queries(), std::vector<std::vector<std::byte>>{expected_sql});
  EXPECT_EQ(second->queries(), std::vector<std::vector<std::byte>>{expected_sql});
  EXPECT_EQ(authenticator.calls, 2U);
  EXPECT_EQ(authorizer.observed_nodes, (std::vector<std::uint64_t>{1U, 2U}));
  ASSERT_TRUE(client->result().has_value());
  EXPECT_EQ(client->result()->row_count, 0U);
  ASSERT_EQ(client->result()->encoded_batches.size(), 1U);
  EXPECT_TRUE(decode_query_result_batch(client->result()->encoded_batches.front()).has_value());
}

TEST(NativeQueryTcpClientTest, DoesNotReplayAnAmbiguousTransportClose) {
  auto server = ScriptedNativeServer::create(ServerReply::kClose);
  auto unused = ScriptedNativeServer::create(ServerReply::kQueryResult);
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(unused.has_value()) << unused.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  auto client = NativeQueryTcpClient::begin(
      query_client_config(*context, server->endpoint(), unused->endpoint(), authenticator,
                          authorizer),
      "SELECT 1", NativeQueryTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  drive_until_terminal(*client, {&*server});

  EXPECT_EQ(client->state(), NativeQueryTcpClientState::kFailed);
  EXPECT_EQ(client->failure().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->attempts_started(), 1U);
  EXPECT_EQ(server->requests(), 1U);
  EXPECT_EQ(unused->requests(), 0U);
  EXPECT_FALSE(client->result().has_value());
}

TEST(NativeQueryTcpClientTest, ValidatesBoundsAndExpiresConnectExactly) {
  auto listener = TcpListener::bind();
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const auto start = NativeQueryTcpClient::TimePoint{};
  auto invalid = query_client_config(*context, listener->bound_endpoint(),
                                     listener->bound_endpoint(), authenticator, authorizer, 0U);
  EXPECT_EQ(NativeQueryTcpClient::begin(std::move(invalid), "SELECT 1", start).error().code(),
            common::StatusCode::kInvalidArgument);

  auto config = query_client_config(*context, listener->bound_endpoint(), {{127U, 0U, 0U, 1U}, 1U},
                                    authenticator, authorizer);
  config.limits.connect_timeout = std::chrono::milliseconds{5};
  auto client = NativeQueryTcpClient::begin(std::move(config), "SELECT 2", start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status timed_out =
      client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(timed_out.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), NativeQueryTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_FALSE(client->result().has_value());
}

TEST(NativeQueryTcpExecutionTest, PollsACompleteRedirectedOperation) {
  auto first = ScriptedNativeServer::create(ServerReply::kQueryRedirect);
  auto second = ScriptedNativeServer::create(ServerReply::kQueryResult);
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const auto start = NativeQueryTcpExecution::TimePoint::clock::now();
  const std::string sql = "SELECT value FROM events";
  auto execution = NativeQueryTcpExecution::begin(
      {.client = query_client_config(*context, first->endpoint(), second->endpoint(), authenticator,
                                     authorizer, 1U),
       .operation_deadline = start + std::chrono::seconds{5}},
      sql);
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();

  for (std::size_t iteration = 0U;
       iteration < 20'000U && execution->state() == NativeQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(first->poll_once().is_ok());
    ASSERT_TRUE(second->poll_once().is_ok());
    const common::Status polled = execution->poll_once(std::chrono::milliseconds{1});
    ASSERT_TRUE(polled.is_ok()) << polled.to_string();
  }

  ASSERT_EQ(execution->state(), NativeQueryTcpExecutionState::kComplete)
      << execution->failure().to_string();
  const auto metrics = execution->metrics();
  EXPECT_GT(metrics.poll_calls, 0U);
  EXPECT_GT(metrics.readiness_events, 0U);
  EXPECT_EQ(metrics.attempts_started, 2U);
  EXPECT_EQ(metrics.redirects_followed, 1U);
  EXPECT_FALSE(metrics.active_client);
  EXPECT_FALSE(execution->next_deadline().has_value());
  EXPECT_EQ(execution->current_route().node_id, 2U);
  ASSERT_TRUE(execution->result().has_value());
  EXPECT_EQ(execution->result()->encoded_batches.size(), 1U);
  EXPECT_TRUE(execution->poll_once(std::chrono::milliseconds{100}).is_ok());
}

TEST(NativeQueryTcpExecutionTest, CancelsExplicitlyAndPreservesTheFirstStatus) {
  auto listener = TcpListener::bind();
  auto context = TlsClientContext::create(tls_client_config());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator authenticator;
  NodeAuthorizer authorizer;
  const auto start = NativeQueryTcpExecution::TimePoint::clock::now();
  auto execution = NativeQueryTcpExecution::begin(
      {.client = query_client_config(*context, listener->bound_endpoint(), {{127U, 0U, 0U, 1U}, 1U},
                                     authenticator, authorizer),
       .operation_deadline = start + std::chrono::seconds{5}},
      "SELECT 1");
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->poll_once(std::chrono::milliseconds{-1}).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(execution->state(), NativeQueryTcpExecutionState::kRunning);

  const common::Status cancelled = execution->cancel();
  EXPECT_EQ(cancelled.code(), common::StatusCode::kCancelled);
  EXPECT_EQ(execution->state(), NativeQueryTcpExecutionState::kCancelled);
  EXPECT_EQ(execution->failure(), cancelled);
  EXPECT_EQ(execution->poll_once(std::chrono::milliseconds{0}), cancelled);
  EXPECT_EQ(execution->cancel(), cancelled);
  EXPECT_FALSE(execution->result().has_value());
  EXPECT_FALSE(execution->metrics().active_client);
  EXPECT_EQ(execution->metrics().attempts_started, 1U);
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

TEST(NativeQuorumIngestTcpExecutionTest, PackagedChronosctlObtainsJsonReceiptThroughRealMutualTls) {
  constexpr std::string_view kServerFingerprint =
      "e79120b0ee5e55f91ea4cb4a29d3ca20aaa36a4abdbeb74d06f97751e61368d1";
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  auto server = ScriptedNativeServer::create(ServerReply::kAcknowledge);
  ASSERT_TRUE(server.has_value()) << server.error().to_string();

  const std::filesystem::path route_file = directory.path() / "routes.conf";
  {
    std::ofstream output{route_file, std::ios::binary | std::ios::trunc};
    output << "CHRONOSDB_NATIVE_CLIENT_ROUTES_V1\n2=127.0.0.1:" << server->endpoint().port
           << ",127.0.0.1," << kServerFingerprint << '\n';
    output.close();
    ASSERT_TRUE(output);
  }
  const std::filesystem::path private_key = directory.path() / "client-key.pem";
  std::error_code filesystem_error;
  std::filesystem::copy_file(fixture("client-key.pem"), private_key,
                             std::filesystem::copy_options::overwrite_existing, filesystem_error);
  ASSERT_FALSE(filesystem_error) << filesystem_error.message();
  std::filesystem::permissions(
      private_key, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
      std::filesystem::perm_options::replace, filesystem_error);
  ASSERT_FALSE(filesystem_error) << filesystem_error.message();
  const std::filesystem::path append_file = directory.path() / "append.bin";
  const std::vector<std::byte> append = canonical_append();
  ASSERT_TRUE(write_bytes(append_file, append));

  std::array<int, 2U> output_pipe{-1, -1};
  ASSERT_EQ(::pipe(output_pipe.data()), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::dup2(output_pipe[1], STDOUT_FILENO));
    static_cast<void>(::dup2(output_pipe[1], STDERR_FILENO));
    ::close(output_pipe[0]);
    ::close(output_pipe[1]);
    const std::string routes = route_file.string();
    const std::string certificate = fixture("client.pem").string();
    const std::string key = private_key.string();
    const std::string trust_store = fixture("ca.pem").string();
    const std::string append_path = append_file.string();
    ::execl(CHRONOSCTL_PATH, CHRONOSCTL_PATH, "quorum-sync", "--json", "--group",
            "09000000-0000-0000-0000-000000000000", "--initial-node", "2",
            "--minimum-placement-epoch", "1", "--routes", routes.c_str(), "--tls-cert",
            certificate.c_str(), "--tls-key", key.c_str(), "--tls-ca", trust_store.c_str(),
            "--append-file", append_path.c_str(), "--timeout-ms", "5000", nullptr);
    std::_Exit(127);
  }
  ::close(output_pipe[1]);
  output_pipe[1] = -1;

  int child_status{};
  pid_t waited{};
  common::Status server_status = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 10'000U; ++iteration) {
    server_status = server->poll_once();
    if (!server_status.is_ok())
      break;
    waited = ::waitpid(child, &child_status, WNOHANG);
    if (waited < 0 && errno == EINTR) {
      waited = 0;
      continue;
    }
    if (waited == child || waited < 0)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  if (waited != child) {
    static_cast<void>(::kill(child, SIGKILL));
    waited = ::waitpid(child, &child_status, 0);
  }

  std::string command_output;
  std::array<char, 1024U> buffer{};
  for (;;) {
    const ssize_t count = ::read(output_pipe[0], buffer.data(), buffer.size());
    if (count <= 0)
      break;
    command_output.append(buffer.data(), static_cast<std::size_t>(count));
  }
  ::close(output_pipe[0]);

  ASSERT_TRUE(server_status.is_ok()) << server_status.to_string();
  ASSERT_EQ(waited, child);
  ASSERT_TRUE(WIFEXITED(child_status));
  EXPECT_EQ(WEXITSTATUS(child_status), 0) << command_output;
  EXPECT_EQ(server->requests(), 1U);
  EXPECT_EQ(server->commands(), std::vector<std::vector<std::byte>>{append});
  EXPECT_EQ(command_output,
            "{\"command\":\"quorum-sync\",\"status\":\"ok\",\"outcome\":\"APPLIED\","
            "\"group_id\":\"09000000-0000-0000-0000-000000000000\",\"leader_node_id\":2,"
            "\"leader_term\":8,\"log_index\":10,\"entry_term\":8,"
            "\"local_durable_physical_sequence\":12,\"attempts\":1,\"redirects\":0}\n");
}

TEST(NativeQueryTcpExecutionTest, PackagedChronosctlReplaysRoutedSqlThroughRealMutualTls) {
  constexpr std::string_view kNodeOneFingerprint =
      "7145018d7511b2e2af9e5531e01e9061af0a43e0b193621be717906b20e253a9";
  constexpr std::string_view kNodeTwoFingerprint =
      "baf82073b1ad1f131414b65c6b302bd1d09b7f3bbb224916e19f305f201b091f";
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  auto first =
      ScriptedNativeServer::create(ServerReply::kQueryRedirect, node_tls_server_config("node1"));
  auto second =
      ScriptedNativeServer::create(ServerReply::kQueryResult, node_tls_server_config("node2"));
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();

  const std::filesystem::path route_file = directory.path() / "routes.conf";
  {
    std::ofstream output{route_file, std::ios::binary | std::ios::trunc};
    output << "CHRONOSDB_NATIVE_CLIENT_ROUTES_V1\n1=127.0.0.1:" << first->endpoint().port
           << ",127.0.0.1," << kNodeOneFingerprint << "\n2=127.0.0.1:" << second->endpoint().port
           << ",127.0.0.1," << kNodeTwoFingerprint << '\n';
    output.close();
    ASSERT_TRUE(output);
  }
  const std::filesystem::path private_key = directory.path() / "client-key.pem";
  std::error_code filesystem_error;
  std::filesystem::copy_file(fixture("client-key.pem"), private_key,
                             std::filesystem::copy_options::overwrite_existing, filesystem_error);
  ASSERT_FALSE(filesystem_error) << filesystem_error.message();
  std::filesystem::permissions(
      private_key, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
      std::filesystem::perm_options::replace, filesystem_error);
  ASSERT_FALSE(filesystem_error) << filesystem_error.message();

  std::array<int, 2U> output_pipe{-1, -1};
  ASSERT_EQ(::pipe(output_pipe.data()), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    static_cast<void>(::dup2(output_pipe[1], STDOUT_FILENO));
    static_cast<void>(::dup2(output_pipe[1], STDERR_FILENO));
    ::close(output_pipe[0]);
    ::close(output_pipe[1]);
    const std::string routes = route_file.string();
    const std::string certificate = fixture("client.pem").string();
    const std::string key = private_key.string();
    const std::string trust_store = fixture("cluster-ca.pem").string();
    ::execl(CHRONOSCTL_PATH, CHRONOSCTL_PATH, "routed-sql", "--group",
            "09000000-0000-0000-0000-000000000000", "--initial-node", "1",
            "--minimum-placement-epoch", "4", "--routes", routes.c_str(), "--tls-cert",
            certificate.c_str(), "--tls-key", key.c_str(), "--tls-ca", trust_store.c_str(),
            "--execute", "SELECT value FROM events", "--timeout-ms", "5000", nullptr);
    std::_Exit(127);
  }
  ::close(output_pipe[1]);
  output_pipe[1] = -1;

  int child_status{};
  pid_t waited{};
  common::Status first_status = common::Status::ok();
  common::Status second_status = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 10'000U; ++iteration) {
    first_status = first->poll_once();
    second_status = second->poll_once();
    if (!first_status.is_ok() || !second_status.is_ok())
      break;
    waited = ::waitpid(child, &child_status, WNOHANG);
    if (waited < 0 && errno == EINTR) {
      waited = 0;
      continue;
    }
    if (waited == child || waited < 0)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  if (waited != child) {
    static_cast<void>(::kill(child, SIGKILL));
    waited = ::waitpid(child, &child_status, 0);
  }

  std::string command_output;
  std::array<char, 1024U> buffer{};
  for (;;) {
    const ssize_t count = ::read(output_pipe[0], buffer.data(), buffer.size());
    if (count <= 0)
      break;
    command_output.append(buffer.data(), static_cast<std::size_t>(count));
  }
  ::close(output_pipe[0]);

  ASSERT_TRUE(first_status.is_ok()) << first_status.to_string();
  ASSERT_TRUE(second_status.is_ok()) << second_status.to_string();
  ASSERT_EQ(waited, child);
  ASSERT_TRUE(WIFEXITED(child_status));
  EXPECT_EQ(WEXITSTATUS(child_status), 0) << command_output;
  const std::string sql = "SELECT value FROM events";
  const auto sql_bytes = std::as_bytes(std::span{sql.data(), sql.size()});
  const std::vector<std::byte> expected_sql(sql_bytes.begin(), sql_bytes.end());
  EXPECT_EQ(first->request_ids(), std::vector<std::uint64_t>{1U});
  EXPECT_EQ(second->request_ids(), std::vector<std::uint64_t>{1U});
  EXPECT_EQ(first->queries(), std::vector<std::vector<std::byte>>{expected_sql});
  EXPECT_EQ(second->queries(), std::vector<std::vector<std::byte>>{expected_sql});
  EXPECT_EQ(command_output, "value\n");
}

} // namespace
} // namespace chronos::network
