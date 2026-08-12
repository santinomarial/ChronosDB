#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"

#include <arpa/inet.h>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
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

  [[nodiscard]] bool start() {
    int output[2]{};
    if (::pipe2(output, O_CLOEXEC) != 0)
      return false;
    pid_ = ::fork();
    if (pid_ == 0) {
      static_cast<void>(::dup2(output[1], STDOUT_FILENO));
      ::close(output[0]);
      ::close(output[1]);
      ::execl(CHRONOSD_PATH, CHRONOSD_PATH, "--port", "0", nullptr);
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
  const int socket = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (socket < 0)
    return -1;
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

TEST(ChronosdProcessTest, NegotiatesPongsAndRejectsUnconfiguredDataPlane) {
  ChildProcess child;
  ASSERT_TRUE(child.start());
  const std::string startup = child.read_startup_line();
  EXPECT_NE(startup.find("data_plane=unconfigured"), std::string::npos);
  const std::uint16_t port = parse_port(startup);
  ASSERT_NE(port, 0U);

  const int client = connect_client(port);
  ASSERT_GE(client, 0);
  const auto hello_payload = network::encode_client_hello({}).value();
  ASSERT_TRUE(
      send_all(client, network::encode_frame({.message_type = network::MessageType::kClientHello},
                                             hello_payload)
                           .value()));
  auto response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  EXPECT_EQ(response->header.message_type, network::MessageType::kServerHello);

  ASSERT_TRUE(send_all(
      client, network::encode_frame({.message_type = network::MessageType::kPing}, {}).value()));
  response = network::decode_frame(receive_frame(client));
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
  EXPECT_EQ(common::as_string_view(error->message), "chronosd data plane is not configured");

  ::close(client);
  EXPECT_EQ(child.stop(), 0);
}

} // namespace
} // namespace chronos::integration
