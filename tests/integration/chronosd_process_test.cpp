#include "chronos/common/byte_reader.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/subscription_messages.hpp"

#include <arpa/inet.h>
#include <array>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <span>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::integration {
namespace {

class ChildProcess {
public:
  ChildProcess() = default;
  ~ChildProcess() {
    static_cast<void>(stop());
  }
  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;

  [[nodiscard]] bool start(const std::string& data_directory = {},
                           const std::string& subscription_sql = {},
                           const std::string& subscription_key_file = {}) {
    int output[2]{};
    if (::pipe(output) != 0)
      return false;
    if (::fcntl(output[0], F_SETFD, FD_CLOEXEC) != 0 ||
        ::fcntl(output[1], F_SETFD, FD_CLOEXEC) != 0) {
      ::close(output[0]);
      ::close(output[1]);
      return false;
    }
    pid_ = ::fork();
    if (pid_ == 0) {
      static_cast<void>(::dup2(output[1], STDOUT_FILENO));
      ::close(output[0]);
      ::close(output[1]);
      if (data_directory.empty())
        ::execl(CHRONOSD_PATH, CHRONOSD_PATH, "--port", "0", nullptr);
      else if (subscription_sql.empty())
        ::execl(CHRONOSD_PATH, CHRONOSD_PATH, "--port", "0", "--data-dir", data_directory.c_str(),
                nullptr);
      else
        ::execl(CHRONOSD_PATH, CHRONOSD_PATH, "--port", "0", "--data-dir", data_directory.c_str(),
                "--subscription-sql", subscription_sql.c_str(), "--subscription-key-file",
                subscription_key_file.c_str(), nullptr);
      std::_Exit(127);
    }
    ::close(output[1]);
    if (pid_ < 0) {
      ::close(output[0]);
      return false;
    }
    output_ = output[0];
    return true;
  }

  [[nodiscard]] std::string read_startup_line() const {
    pollfd descriptor{.fd = output_, .events = POLLIN, .revents = 0};
    if (::poll(&descriptor, 1, 5000) <= 0)
      return {};
    std::string line;
    for (;;) {
      char value{};
      const ssize_t count = ::read(output_, &value, 1U);
      if (count != 1 || value == '\n')
        return line;
      line.push_back(value);
    }
  }

  [[nodiscard]] int stop() noexcept {
    if (output_ >= 0) {
      ::close(output_);
      output_ = -1;
    }
    if (pid_ <= 0)
      return -1;
    static_cast<void>(::kill(pid_, SIGTERM));
    int status{};
    static_cast<void>(::waitpid(pid_, &status, 0));
    pid_ = -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
  }

private:
  pid_t pid_{-1};
  int output_{-1};
};

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronosd-process-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::string& path() const noexcept {
    return path_;
  }

private:
  std::string path_;
};

[[nodiscard]] std::uint16_t parse_port(const std::string_view line) {
  constexpr std::string_view prefix{"chronosd listening on 127.0.0.1:"};
  const std::size_t end = line.find(' ', prefix.size());
  if (!line.starts_with(prefix) || end == std::string_view::npos)
    return 0U;
  const std::string port{line.substr(prefix.size(), end - prefix.size())};
  const unsigned long value = std::strtoul(port.c_str(), nullptr, 10);
  return value <= 65'535UL ? static_cast<std::uint16_t>(value) : 0U;
}

[[nodiscard]] int connect_client(const std::uint16_t port) {
  const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
  if (socket < 0)
    return -1;
  if (::fcntl(socket, F_SETFD, FD_CLOEXEC) != 0) {
    ::close(socket);
    return -1;
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  // POSIX requires the generic sockaddr view of the initialized IPv4 address.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(socket);
    return -1;
  }
  const timeval timeout{.tv_sec = 5, .tv_usec = 0};
  static_cast<void>(::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
  return socket;
}

[[nodiscard]] bool send_all(const int socket, const std::vector<std::byte>& bytes) {
  std::size_t offset{};
  while (offset < bytes.size()) {
    const ssize_t count =
        ::send(socket, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    if (count <= 0)
      return false;
    offset += static_cast<std::size_t>(count);
  }
  return true;
}

[[nodiscard]] std::vector<std::byte> receive_frame(const int socket) {
  std::vector<std::byte> bytes(network::kFrameHeaderSize);
  std::size_t offset{};
  while (offset < bytes.size()) {
    const ssize_t count = ::recv(socket, bytes.data() + offset, bytes.size() - offset, 0);
    if (count <= 0)
      return {};
    offset += static_cast<std::size_t>(count);
  }
  const auto header = network::decode_frame_header(bytes);
  if (!header.has_value())
    return {};
  bytes.resize(network::kFrameHeaderSize + header->payload_size);
  while (offset < bytes.size()) {
    const ssize_t count = ::recv(socket, bytes.data() + offset, bytes.size() - offset, 0);
    if (count <= 0)
      return {};
    offset += static_cast<std::size_t>(count);
  }
  return bytes;
}

[[nodiscard]] std::string byte_string(const common::ByteView bytes) {
  std::string value(bytes.size(), '\0');
  std::memcpy(value.data(), bytes.data(), bytes.size());
  return value;
}

void handshake(const int client) {
  const auto hello_payload = network::encode_client_hello({}).value();
  ASSERT_TRUE(
      send_all(client, network::encode_frame({.message_type = network::MessageType::kClientHello},
                                             hello_payload)
                           .value()));
  const auto response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  EXPECT_EQ(response->header.message_type, network::MessageType::kServerHello);
}

void handshake_subscriptions(const int client) {
  const auto hello_payload =
      network::encode_client_hello(
          {.maximum_minor = 1U, .feature_bits = network::kProtocolV1SubscriptionFeature})
          .value();
  ASSERT_TRUE(
      send_all(client, network::encode_frame({.message_type = network::MessageType::kClientHello},
                                             hello_payload)
                           .value()));
  const auto response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  ASSERT_EQ(response->header.message_type, network::MessageType::kServerHello);
  const auto hello = network::decode_server_hello(response->payload);
  ASSERT_TRUE(hello.has_value());
  EXPECT_EQ(hello->selected_minor, 1U);
  EXPECT_EQ(hello->feature_bits, network::kProtocolV1SubscriptionFeature);
}

[[nodiscard]] network::Frame send_query(const int client, const std::uint64_t request_id,
                                        const std::string_view sql) {
  const auto payload = network::encode_query_request(sql).value();
  EXPECT_TRUE(send_all(
      client,
      network::encode_frame(
          {.message_type = network::MessageType::kQueryRequest, .request_id = request_id}, payload)
          .value()));
  auto response = network::decode_frame(receive_frame(client));
  EXPECT_TRUE(response.has_value());
  return response.value_or(network::Frame{});
}

TEST(ChronosdProcessTest, NegotiatesPongsAndRejectsUnconfiguredDataPlane) {
  ChildProcess child;
  ASSERT_TRUE(child.start());
  const std::string startup = child.read_startup_line();
  EXPECT_NE(startup.find("data_plane=unconfigured"), std::string::npos);
  const std::uint16_t port = parse_port(startup);
  ASSERT_NE(port, 0U);

  const int client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake(client);

  ASSERT_TRUE(send_all(
      client, network::encode_frame({.message_type = network::MessageType::kPing}, {}).value()));
  auto response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  EXPECT_EQ(response->header.message_type, network::MessageType::kPong);

  const auto query_payload = network::encode_query_request("SELECT 1").value();
  ASSERT_TRUE(send_all(
      client,
      network::encode_frame({.message_type = network::MessageType::kQueryRequest, .request_id = 1U},
                            query_payload)
          .value()));
  response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  ASSERT_EQ(response->header.message_type, network::MessageType::kError);
  const auto error = network::decode_error_message(response->payload);
  ASSERT_TRUE(error.has_value());
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kExecutionFailure);
  EXPECT_EQ(byte_string(error->message), "chronosd data plane is not configured");

  ::close(client);
  EXPECT_EQ(child.stop(), 0);
}

TEST(ChronosdProcessTest, CreatesQueriesAndRecoversAConfiguredDatabase) {
  constexpr std::string_view create_sql =
      "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, price "
      "DECIMAL(20, 8) NOT NULL, note STRING) EVENT TIME ts ORDER KEY (symbol, ts) "
      "PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) DEDUP KEY "
      "(symbol, ts) RETENTION INTERVAL '30 days' SYSTEM HISTORY RETENTION INTERVAL "
      "'7 days' ALLOWED LATENESS INTERVAL '0 seconds'";
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ChildProcess child;
  ASSERT_TRUE(child.start(directory.path()));
  std::string startup = child.read_startup_line();
  EXPECT_NE(startup.find("data_plane=configured"), std::string::npos);
  std::uint16_t port = parse_port(startup);
  ASSERT_NE(port, 0U);

  int client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake(client);
  auto response = send_query(client, 1U, create_sql);
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  auto ddl = network::decode_query_result_batch(response.payload);
  ASSERT_TRUE(ddl.has_value()) << ddl.error().to_string();
  EXPECT_EQ(ddl->row_count(), 1U);
  EXPECT_EQ(ddl->columns().size(), 5U);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kQueryEnd);

  response = send_query(client, 2U, "SELECT count(*) AS rows FROM trades");
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  auto count = network::decode_query_result_batch(response.payload);
  ASSERT_TRUE(count.has_value()) << count.error().to_string();
  ASSERT_NE(count->cell(0U, 0U), nullptr);
  common::ByteReader zero{count->cell(0U, 0U)->value};
  EXPECT_EQ(zero.read_i64_le().value(), 0);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  ::close(client);
  EXPECT_EQ(child.stop(), 0);

  ASSERT_TRUE(child.start(directory.path()));
  startup = child.read_startup_line();
  EXPECT_NE(startup.find("data_plane=configured"), std::string::npos);
  port = parse_port(startup);
  ASSERT_NE(port, 0U);
  client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake(client);
  response = send_query(client, 3U, "SELECT count(*) AS rows FROM trades");
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  count = network::decode_query_result_batch(response.payload);
  ASSERT_TRUE(count.has_value()) << count.error().to_string();
  common::ByteReader recovered{count->cell(0U, 0U)->value};
  EXPECT_EQ(recovered.read_i64_le().value(), 0);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  ::close(client);
  EXPECT_EQ(child.stop(), 0);
}

TEST(ChronosdProcessTest, StreamsAcknowledgesAndResumesAConfiguredSubscription) {
  constexpr std::string_view create_sql =
      "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, price "
      "DECIMAL(20, 8) NOT NULL, note STRING) EVENT TIME ts ORDER KEY (symbol, ts) "
      "PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) DEDUP KEY "
      "(symbol, ts) RETENTION INTERVAL '30 days' SYSTEM HISTORY RETENTION INTERVAL "
      "'7 days' ALLOWED LATENESS INTERVAL '0 seconds'";
  constexpr std::string_view subscription_sql = "SUBSCRIBE SELECT ts FROM trades";
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ChildProcess child;
  ASSERT_TRUE(child.start(directory.path()));
  std::uint16_t port = parse_port(child.read_startup_line());
  ASSERT_NE(port, 0U);
  int client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake(client);
  auto response = send_query(client, 1U, create_sql);
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  ::close(client);
  ASSERT_EQ(child.stop(), 0);

  const std::string key_path = directory.path() + "/subscription.key";
  const int key_file = ::open(key_path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(key_file, 0);
  std::array<std::byte, 32U> key{};
  key.fill(std::byte{0x5a});
  ASSERT_EQ(::write(key_file, key.data(), key.size()), static_cast<ssize_t>(key.size()));
  ASSERT_EQ(::fsync(key_file), 0);
  ASSERT_EQ(::close(key_file), 0);

  ASSERT_TRUE(child.start(directory.path(), std::string{subscription_sql}, key_path));
  const std::string startup = child.read_startup_line();
  EXPECT_NE(startup.find("subscriptions=configured"), std::string::npos);
  port = parse_port(startup);
  ASSERT_NE(port, 0U);
  client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake_subscriptions(client);

  common::Uuid::Bytes subscription_bytes{};
  subscription_bytes.front() = std::byte{1};
  const common::Uuid subscription_id{subscription_bytes};
  auto subscribe = network::encode_subscription_request(
      {.mode = network::SubscriptionStartMode::kNewQuery,
       .subscription_id = subscription_id,
       .body = std::as_bytes(std::span{subscription_sql.data(), subscription_sql.size()})});
  ASSERT_TRUE(subscribe.has_value());
  ASSERT_TRUE(send_all(
      client,
      network::encode_frame(
          {.message_type = network::MessageType::kSubscribeRequest, .request_id = 10U}, *subscribe)
          .value()));
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  EXPECT_NE(response.header.flags & network::kFrameFlagEndStream, 0U);
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kSubscriptionReady);

  response = send_query(client, 11U,
                        "INSERT INTO trades VALUES "
                        "(TIMESTAMP '1970-01-01 00:00:00.000000001Z', "
                        "CAST('A' AS SYMBOL), 1, NULL)");
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kSubscriptionChange);
  auto change = network::decode_subscription_change(response.payload);
  ASSERT_TRUE(change.has_value()) << change.error().to_string();
  EXPECT_EQ(change->delivery_sequence, 1U);
  auto nested = network::decode_query_result_batch(change->payload);
  ASSERT_TRUE(nested.has_value()) << nested.error().to_string();
  EXPECT_EQ(nested->row_count(), 1U);

  auto acknowledgement = network::encode_subscription_acknowledgement({1U});
  ASSERT_TRUE(acknowledgement.has_value());
  ASSERT_TRUE(send_all(
      client, network::encode_frame({.message_type = network::MessageType::kSubscriptionAcknowledge,
                                     .request_id = 10U},
                                    *acknowledgement)
                  .value()));
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kSubscriptionCheckpoint);
  auto checkpoint = network::decode_subscription_checkpoint(response.payload);
  ASSERT_TRUE(checkpoint.has_value());
  std::vector<std::byte> resume_token{checkpoint->resume_token.begin(),
                                      checkpoint->resume_token.end()};
  ::close(client);
  ASSERT_EQ(child.stop(), 0);

  ASSERT_TRUE(child.start(directory.path(), std::string{subscription_sql}, key_path));
  port = parse_port(child.read_startup_line());
  ASSERT_NE(port, 0U);
  client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake_subscriptions(client);
  auto resume =
      network::encode_subscription_request({.mode = network::SubscriptionStartMode::kResume,
                                            .subscription_id = subscription_id,
                                            .body = resume_token});
  ASSERT_TRUE(resume.has_value());
  ASSERT_TRUE(send_all(
      client,
      network::encode_frame(
          {.message_type = network::MessageType::kSubscribeRequest, .request_id = 20U}, *resume)
          .value()));
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  EXPECT_NE(response.header.flags & network::kFrameFlagEndStream, 0U);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kSubscriptionReady);
  ::close(client);
  EXPECT_EQ(child.stop(), 0);
}

} // namespace
} // namespace chronos::integration
