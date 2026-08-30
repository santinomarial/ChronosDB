#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
#include <string>
#include <string_view>
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
      if (::poll(&descriptor, 1U, static_cast<int>(remaining.count())) <= 0)
        return std::nullopt;
      char value{};
      if (::read(output_, &value, 1U) != 1)
        return std::nullopt;
      if (value == '\n')
        return line;
      line.push_back(value);
    }
  }

  [[nodiscard]] std::optional<std::string> read_until(const std::string_view expected,
                                                      const std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline)
        return std::nullopt;
      const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
      auto line = read_line(remaining);
      if (!line.has_value())
        return std::nullopt;
      if (*line == expected)
        return line;
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

[[nodiscard]] int run_command(std::vector<std::string> arguments, const bool quiet = false) {
  std::vector<char*> raw_arguments;
  raw_arguments.reserve(arguments.size() + 1U);
  for (std::string& argument : arguments)
    raw_arguments.push_back(argument.data());
  raw_arguments.push_back(nullptr);
  const pid_t pid = ::fork();
  if (pid == 0) {
    if (quiet) {
      const int null_output = ::open("/dev/null", O_WRONLY);
      if (null_output < 0 || ::dup2(null_output, STDOUT_FILENO) < 0 ||
          ::dup2(null_output, STDERR_FILENO) < 0) {
        std::_Exit(126);
      }
      ::close(null_output);
    }
    ::execv(raw_arguments.front(), raw_arguments.data());
    std::_Exit(127);
  }
  if (pid < 0)
    return -1;
  int status{};
  pid_t result{};
  do {
    result = ::waitpid(pid, &status, 0);
  } while (result < 0 && errno == EINTR);
  if (result != pid)
    return -1;
  return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
}

[[nodiscard]] std::string iptables_path() {
  const char* configured = std::getenv("CHRONOS_IPTABLES_PATH");
  return configured == nullptr || *configured == '\0' ? "/usr/sbin/iptables" : configured;
}

enum class PacketDirection : std::uint8_t {
  kCoordinatorToReducer = 1,
  kReducerToCoordinator = 2,
};

class PacketDropRule {
public:
  PacketDropRule(std::string executable, const PacketDirection direction,
                 const std::uint16_t reducer_port)
      : executable_(std::move(executable)), direction_(direction),
        port_(std::to_string(reducer_port)) {}
  ~PacketDropRule() noexcept {
    try {
      if (!remove()) {
        constexpr std::string_view message{"packet-fault rule cleanup failed\n"};
        static_cast<void>(::write(STDERR_FILENO, message.data(), message.size()));
      }
    } catch (...) {
      constexpr std::string_view message{"packet-fault rule cleanup threw\n"};
      static_cast<void>(::write(STDERR_FILENO, message.data(), message.size()));
    }
  }
  PacketDropRule(const PacketDropRule&) = delete;
  PacketDropRule& operator=(const PacketDropRule&) = delete;

  [[nodiscard]] bool install() {
    if (installed_)
      return true;
    installed_ = run_command(command("-I", true)) == 0;
    return installed_;
  }

  [[nodiscard]] bool remove() {
    if (!installed_)
      return true;
    const bool removed = run_command(command("-D", false)) == 0;
    if (removed)
      installed_ = false;
    return removed;
  }

private:
  [[nodiscard]] std::vector<std::string> command(const std::string_view action,
                                                 const bool insert) const {
    std::vector<std::string> arguments{executable_, "-w", "2", std::string{action}, "OUTPUT"};
    if (insert)
      arguments.emplace_back("1");
    arguments.insert(arguments.end(),
                     {"-p", "tcp", "-d", "127.0.0.1/32",
                      direction_ == PacketDirection::kCoordinatorToReducer ? "--dport" : "--sport",
                      port_, "-j", "DROP"});
    return arguments;
  }

  std::string executable_;
  PacketDirection direction_;
  std::string port_;
  bool installed_{};
};

[[nodiscard]] std::uint16_t job_reducer_port(const std::string_view line) {
  constexpr std::string_view prefix{"JOB_REDUCER_READY "};
  if (!line.starts_with(prefix))
    return 0U;
  const std::string value{line.substr(prefix.size())};
  const unsigned long port = std::strtoul(value.c_str(), nullptr, 10);
  return port <= 65'535UL ? static_cast<std::uint16_t>(port) : 0U;
}

void expect_directional_packet_loss_and_healing(const PacketDirection direction) {
#if !defined(__linux__)
  GTEST_SKIP() << "packet-fault qualification requires Linux netfilter";
#endif
  const std::string iptables = iptables_path();
  const char* enabled = std::getenv("CHRONOS_RUN_PACKET_FAULT_TESTS");
  if (enabled == nullptr || std::string_view{enabled} != "1")
    GTEST_SKIP() << "set CHRONOS_RUN_PACKET_FAULT_TESTS=1 in an isolated Linux network namespace";
  if (::geteuid() != 0)
    GTEST_SKIP() << "packet-fault qualification requires root in an isolated namespace";
  if (::access(iptables.c_str(), X_OK) != 0)
    GTEST_SKIP() << "iptables executable is unavailable: " << iptables;
  if (run_command({iptables, "-w", "2", "-L", "OUTPUT", "-n"}, true) != 0)
    GTEST_SKIP() << "the current namespace does not grant netfilter administration";

  ChildProcess isolated_reducer;
  ChildProcess healthy_reducer;
  ASSERT_TRUE(isolated_reducer.start({"job-reducer", "2", "none"}));
  ASSERT_TRUE(healthy_reducer.start({"job-reducer", "3", "none"}));
  const auto isolated_ready = isolated_reducer.read_line(std::chrono::seconds{5});
  const auto healthy_ready = healthy_reducer.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(isolated_ready.has_value());
  ASSERT_TRUE(healthy_ready.has_value());
  const std::uint16_t isolated_port = job_reducer_port(*isolated_ready);
  const std::uint16_t healthy_port = job_reducer_port(*healthy_ready);
  ASSERT_NE(isolated_port, 0U) << *isolated_ready;
  ASSERT_NE(healthy_port, 0U) << *healthy_ready;

  ChildProcess partitioned_coordinator;
  ASSERT_TRUE(
      partitioned_coordinator.start({"job-coordinator-two", std::to_string(isolated_port),
                                     std::to_string(healthy_port), "1", "5000", "300", "hold"}));
  const auto leased = partitioned_coordinator.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(leased.has_value());
  ASSERT_EQ(*leased, "LEASED 1");
  ASSERT_TRUE(isolated_reducer.read_until("RENEWED 1", std::chrono::seconds{5}).has_value());
  ASSERT_TRUE(healthy_reducer.read_until("RENEWED 1", std::chrono::seconds{5}).has_value());

  PacketDropRule partition{iptables, direction, isolated_port};
  ASSERT_TRUE(partition.install());
  ASSERT_TRUE(isolated_reducer.read_until("EXPIRED 1", std::chrono::seconds{3}).has_value());
  ASSERT_TRUE(healthy_reducer.read_until("CANCEL_REQUESTS 1", std::chrono::seconds{3}).has_value());
  ASSERT_TRUE(partitioned_coordinator.read_until("FAILED 1", std::chrono::seconds{12}).has_value());
  const int failed_exit = partitioned_coordinator.wait_for_exit(std::chrono::seconds{12});
  ASSERT_EQ(failed_exit, 6);
  ASSERT_TRUE(partition.remove());

  ChildProcess replacement;
  ASSERT_TRUE(replacement.start({"job-coordinator-two", std::to_string(isolated_port),
                                 std::to_string(healthy_port), "2", "5000", "300", "cancel"}));
  const auto replacement_leased = replacement.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(replacement_leased.has_value());
  EXPECT_EQ(*replacement_leased, "LEASED 2");
  ASSERT_TRUE(isolated_reducer.read_until("ACTIVE 2", std::chrono::seconds{5}).has_value());
  ASSERT_TRUE(healthy_reducer.read_until("ACTIVE 2", std::chrono::seconds{5}).has_value());
  const auto replacement_cancelled = replacement.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(replacement_cancelled.has_value());
  EXPECT_EQ(*replacement_cancelled, "CANCELLED 2");
  EXPECT_EQ(replacement.wait_for_exit(std::chrono::seconds{5}), 0);
  EXPECT_TRUE(
      isolated_reducer.read_until("CANCEL_REQUESTS 1", std::chrono::seconds{5}).has_value());
  EXPECT_TRUE(healthy_reducer.read_until("CANCEL_REQUESTS 2", std::chrono::seconds{5}).has_value());
  EXPECT_TRUE(isolated_reducer.kill_abruptly());
  EXPECT_TRUE(healthy_reducer.kill_abruptly());
}

TEST(GroupedShufflePacketFaultProcessTest,
     DropsCoordinatorPacketsToOneReducerAndHealsForAFreshLifecycle) {
  expect_directional_packet_loss_and_healing(PacketDirection::kCoordinatorToReducer);
}

TEST(GroupedShufflePacketFaultProcessTest, DropsOneReducerResponsesAndHealsForAFreshLifecycle) {
  expect_directional_packet_loss_and_healing(PacketDirection::kReducerToCoordinator);
}

} // namespace
} // namespace chronos::integration
