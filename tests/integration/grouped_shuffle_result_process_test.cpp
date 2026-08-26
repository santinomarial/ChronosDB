#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <optional>
#include <poll.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::integration {
namespace {

class ChildProcess {
public:
  ChildProcess() = default;
  ~ChildProcess() {
    static_cast<void>(kill_abruptly());
  }
  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;

  [[nodiscard]] bool start(std::vector<std::string> arguments) {
    arguments.insert(arguments.begin(), GROUPED_SHUFFLE_RESULT_PROCESS_CHILD_PATH);
    std::vector<char*> raw_arguments;
    raw_arguments.reserve(arguments.size() + 1U);
    for (std::string& argument : arguments)
      raw_arguments.push_back(argument.data());
    raw_arguments.push_back(nullptr);
    std::array<int, 2U> output{};
    if (::pipe(output.data()) != 0)
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
      static_cast<void>(::dup2(output[1], STDERR_FILENO));
      ::close(output[0]);
      ::close(output[1]);
      ::execv(raw_arguments.front(), raw_arguments.data());
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

  [[nodiscard]] std::optional<std::string> read_line(const std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string line;
    for (;;) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline)
        return std::nullopt;
      const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
      pollfd descriptor{.fd = output_, .events = POLLIN, .revents = 0};
      if (::poll(&descriptor, 1, static_cast<int>(remaining.count())) <= 0)
        return std::nullopt;
      char value{};
      const ssize_t count = ::read(output_, &value, 1U);
      if (count != 1)
        return std::nullopt;
      if (value == '\n')
        return line;
      line.push_back(value);
    }
  }

  [[nodiscard]] int wait_for_exit(const std::chrono::milliseconds timeout) noexcept {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    int status{};
    while (std::chrono::steady_clock::now() < deadline) {
      const pid_t result = ::waitpid(pid_, &status, WNOHANG);
      if (result == pid_) {
        pid_ = -1;
        close_output();
        return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
      }
      if (result < 0 && errno != EINTR)
        break;
      std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    return -1;
  }

  [[nodiscard]] bool kill_abruptly() noexcept {
    close_output();
    if (pid_ <= 0)
      return false;
    if (::kill(pid_, SIGKILL) != 0)
      return false;
    int status{};
    const bool killed =
        ::waitpid(pid_, &status, 0) == pid_ && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    pid_ = -1;
    return killed;
  }

private:
  void close_output() noexcept {
    if (output_ >= 0) {
      ::close(output_);
      output_ = -1;
    }
  }

  pid_t pid_{-1};
  int output_{-1};
};

[[nodiscard]] std::uint16_t reserve_then_release_loopback_port() {
  const int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
  if (descriptor < 0)
    return 0U;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = 0U;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  // POSIX requires the generic sockaddr view of the initialized IPv4 address.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::bind(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(descriptor);
    return 0U;
  }
  socklen_t size = sizeof(address);
  // POSIX requires the generic sockaddr view for the initialized IPv4 address.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::getsockname(descriptor, reinterpret_cast<sockaddr*>(&address), &size) != 0) {
    ::close(descriptor);
    return 0U;
  }
  const std::uint16_t port = ntohs(address.sin_port);
  ::close(descriptor);
  return port;
}

[[nodiscard]] std::uint16_t ready_port(const std::string_view line) {
  constexpr std::string_view prefix{"READY "};
  if (!line.starts_with(prefix))
    return 0U;
  const std::string value{line.substr(prefix.size())};
  const unsigned long port = std::strtoul(value.c_str(), nullptr, 10);
  return port <= 65'535UL ? static_cast<std::uint16_t>(port) : 0U;
}

void expect_successful_query() {
  ChildProcess coordinator;
  ASSERT_TRUE(coordinator.start({"coordinator", "0", "5000"}));
  const auto ready = coordinator.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(ready.has_value());
  const std::uint16_t coordinator_port = ready_port(*ready);
  ASSERT_NE(coordinator_port, 0U) << *ready;
  const std::uint16_t refused_port = reserve_then_release_loopback_port();
  ASSERT_NE(refused_port, 0U);

  ChildProcess east;
  ChildProcess west;
  ASSERT_TRUE(east.start({"reducer", std::to_string(coordinator_port), std::to_string(refused_port),
                          "0", "east", "1"}));
  ASSERT_TRUE(west.start({"reducer", std::to_string(coordinator_port), "0", "1", "west", "2"}));
  const auto east_sent = east.read_line(std::chrono::seconds{5});
  const auto west_sent = west.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(east_sent.has_value());
  ASSERT_TRUE(west_sent.has_value());
  EXPECT_EQ(*east_sent, "SENT 0 attempts=2 retries=1");
  EXPECT_EQ(*west_sent, "SENT 1 attempts=1 retries=0");
  EXPECT_EQ(east.wait_for_exit(std::chrono::seconds{5}), 0);
  EXPECT_EQ(west.wait_for_exit(std::chrono::seconds{5}), 0);

  const auto result = coordinator.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "RESULT west 2");
  EXPECT_EQ(coordinator.wait_for_exit(std::chrono::seconds{5}), 0);
}

TEST(GroupedShuffleResultProcessTest,
     IndependentReducersRetryAndCoordinatorPublishesOnlyTheGlobalResult) {
  expect_successful_query();
}

TEST(GroupedShuffleResultProcessTest,
     AbruptReducerLossWithholdsPartialResultAndAWholeNewAttemptSucceeds) {
  ChildProcess coordinator;
  ASSERT_TRUE(coordinator.start({"coordinator", "0", "750"}));
  const auto ready = coordinator.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(ready.has_value());
  const std::uint16_t coordinator_port = ready_port(*ready);
  ASSERT_NE(coordinator_port, 0U) << *ready;

  ChildProcess east;
  ChildProcess lost_west;
  ASSERT_TRUE(east.start({"reducer", std::to_string(coordinator_port), "0", "0", "east", "1"}));
  ASSERT_TRUE(lost_west.start({"stall-reducer", "1"}));
  const auto east_sent = east.read_line(std::chrono::seconds{5});
  const auto stalled = lost_west.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(east_sent.has_value());
  ASSERT_TRUE(stalled.has_value());
  EXPECT_EQ(*east_sent, "SENT 0 attempts=1 retries=0");
  EXPECT_EQ(*stalled, "STALLING 1");
  EXPECT_EQ(east.wait_for_exit(std::chrono::seconds{5}), 0);
  EXPECT_TRUE(lost_west.kill_abruptly());

  const auto cancelled = coordinator.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(cancelled.has_value());
  EXPECT_EQ(*cancelled, "CANCELLED");
  EXPECT_EQ(coordinator.wait_for_exit(std::chrono::seconds{5}), 3);

  expect_successful_query();
}

} // namespace
} // namespace chronos::integration
