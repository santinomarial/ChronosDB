#include "chronos/ingest/sha256.hpp"
#include "chronos/tiering/object_store.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <gtest/gtest.h>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <poll.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {

[[nodiscard]] std::string lower(std::string value) {
  std::ranges::transform(value, value.begin(), [](const unsigned char character) {
    return static_cast<char>(character >= 'A' && character <= 'Z' ? character + ('a' - 'A')
                                                                  : character);
  });
  return value;
}

[[nodiscard]] std::string future_http_date(const char* format) {
  const std::time_t future = std::time(nullptr) + 2;
  std::tm utc{};
  if (::gmtime_r(&future, &utc) == nullptr)
    return {};
  std::array<char, 64U> encoded{};
  const std::size_t length = std::strftime(encoded.data(), encoded.size(), format, &utc);
  return std::string{encoded.data(), length};
}

[[nodiscard]] std::string future_iso_expiration() {
  const std::time_t future = std::time(nullptr) + 3'600;
  std::tm utc{};
  if (::gmtime_r(&future, &utc) == nullptr)
    return {};
  std::array<char, 64U> encoded{};
  const std::size_t length =
      std::strftime(encoded.data(), encoded.size(), "%Y-%m-%dT%H:%M:%SZ", &utc);
  return std::string{encoded.data(), length};
}

struct RecordedRequest {
  std::string method;
  std::string target;
  std::map<std::string, std::string> headers;
  std::vector<std::byte> body;
};

struct LocalS3Behavior {
  std::string access_key_id{"test-access"};
  std::optional<std::string> session_token{"test-token"};
  std::size_t transient_put_failures{};
  std::optional<std::string> transient_retry_after;
  std::optional<std::size_t> fail_multipart_part;
  bool embedded_multipart_completion_error{};
  std::optional<std::string> head_encryption_override;
  std::optional<std::string> container_credential_response;
  std::optional<std::string> container_authorization;
};

class LocalS3Server final {
public:
  explicit LocalS3Server(LocalS3Behavior behavior = {}) : behavior_(std::move(behavior)) {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener_ < 0) {
      failure_ = "socket creation failed";
      return;
    }
    const int reuse = 1;
    static_cast<void>(::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0U;
    if (::bind(listener_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener_, 8) != 0) {
      failure_ = "listener bind failed";
      ::close(listener_);
      listener_ = -1;
      return;
    }
    socklen_t address_length = sizeof(address);
    if (::getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &address_length) != 0) {
      failure_ = "listener endpoint lookup failed";
      ::close(listener_);
      listener_ = -1;
      return;
    }
    port_ = ntohs(address.sin_port);
    worker_ = std::jthread{[this](const std::stop_token stop) { serve(stop); }};
  }

  ~LocalS3Server() {
    worker_.request_stop();
    if (listener_ >= 0) {
      static_cast<void>(::shutdown(listener_, SHUT_RDWR));
      ::close(listener_);
      if (worker_.joinable())
        worker_.join();
      listener_ = -1;
    }
  }

  LocalS3Server(const LocalS3Server&) = delete;
  LocalS3Server& operator=(const LocalS3Server&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return listener_ >= 0;
  }

  [[nodiscard]] std::uint16_t port() const noexcept {
    return port_;
  }

  [[nodiscard]] std::vector<RecordedRequest> requests() const {
    std::scoped_lock lock{mutex_};
    return requests_;
  }

  [[nodiscard]] std::string failure() const {
    std::scoped_lock lock{mutex_};
    return failure_;
  }

private:
  [[nodiscard]] static bool send_all(const int descriptor, const std::string_view bytes) {
    std::size_t sent{};
    while (sent < bytes.size()) {
      const ssize_t result = ::send(descriptor, bytes.data() + sent, bytes.size() - sent, 0);
      if (result <= 0)
        return false;
      sent += static_cast<std::size_t>(result);
    }
    return true;
  }

  [[nodiscard]] static std::optional<RecordedRequest> read_request(const int descriptor) {
    std::vector<char> bytes;
    bytes.reserve(4096U);
    std::array<char, 2048U> fragment{};
    std::size_t header_end = std::string::npos;
    std::size_t content_length{};
    while (bytes.size() <= 64U * 1024U) {
      const ssize_t received = ::recv(descriptor, fragment.data(), fragment.size(), 0);
      if (received <= 0)
        return std::nullopt;
      bytes.insert(bytes.end(), fragment.begin(), fragment.begin() + received);
      const std::string_view view{bytes.data(), bytes.size()};
      header_end = view.find("\r\n\r\n");
      if (header_end == std::string::npos)
        continue;

      RecordedRequest request;
      const std::size_t request_line_end = view.find("\r\n");
      if (request_line_end == std::string::npos)
        return std::nullopt;
      const std::string_view request_line = view.substr(0U, request_line_end);
      const std::size_t first_space = request_line.find(' ');
      const std::size_t second_space = request_line.find(' ', first_space + 1U);
      if (first_space == std::string::npos || second_space == std::string::npos)
        return std::nullopt;
      request.method = request_line.substr(0U, first_space);
      request.target = request_line.substr(first_space + 1U, second_space - first_space - 1U);

      std::size_t line_begin = request_line_end + 2U;
      while (line_begin < header_end) {
        const std::size_t line_end = view.find("\r\n", line_begin);
        if (line_end == std::string::npos || line_end > header_end)
          return std::nullopt;
        const std::string_view line = view.substr(line_begin, line_end - line_begin);
        const std::size_t colon = line.find(':');
        if (colon == std::string::npos)
          return std::nullopt;
        std::string name = lower(std::string{line.substr(0U, colon)});
        std::string_view value = line.substr(colon + 1U);
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
          value.remove_prefix(1U);
        request.headers.emplace(std::move(name), std::string{value});
        line_begin = line_end + 2U;
      }
      const auto length = request.headers.find("content-length");
      if (length != request.headers.end()) {
        try {
          content_length = std::stoull(length->second);
        } catch (...) {
          return std::nullopt;
        }
      }
      const std::size_t body_begin = header_end + 4U;
      while (bytes.size() - body_begin < content_length) {
        const ssize_t body_received = ::recv(descriptor, fragment.data(), fragment.size(), 0);
        if (body_received <= 0)
          return std::nullopt;
        bytes.insert(bytes.end(), fragment.begin(), fragment.begin() + body_received);
      }
      const auto* body = reinterpret_cast<const std::byte*>(bytes.data() + body_begin);
      request.body.assign(body, body + content_length);
      return request;
    }
    return std::nullopt;
  }

  void record_failure(std::string message) {
    std::scoped_lock lock{mutex_};
    if (failure_.empty())
      failure_ = std::move(message);
  }

  void handle(const int descriptor) {
    auto request = read_request(descriptor);
    if (!request.has_value()) {
      record_failure("request parsing failed");
      return;
    }
    {
      std::scoped_lock lock{mutex_};
      requests_.push_back(*request);
    }
    if (behavior_.container_credential_response.has_value()) {
      const auto authorization = request->headers.find("authorization");
      const bool authorized = behavior_.container_authorization.has_value()
                                  ? authorization != request->headers.end() &&
                                        authorization->second == *behavior_.container_authorization
                                  : authorization == request->headers.end();
      if (request->method != "GET" || !authorized) {
        static_cast<void>(
            send_all(descriptor,
                     "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
        return;
      }
      const std::string& body = *behavior_.container_credential_response;
      static_cast<void>(send_all(descriptor, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                                             "Content-Length: " +
                                                 std::to_string(body.size()) +
                                                 "\r\nConnection: close\r\n\r\n" + body));
      return;
    }
    const auto authorization = request->headers.find("authorization");
    const auto signed_date = request->headers.find("x-amz-date");
    const auto token = request->headers.find("x-amz-security-token");
    const bool token_matches =
        behavior_.session_token.has_value()
            ? token != request->headers.end() && token->second == *behavior_.session_token
            : token == request->headers.end();
    if (authorization == request->headers.end() ||
        !authorization->second.starts_with("AWS4-HMAC-SHA256 ") ||
        !authorization->second.contains("Credential=" + behavior_.access_key_id + "/") ||
        !authorization->second.contains("SignedHeaders=") ||
        signed_date == request->headers.end() || !token_matches) {
      static_cast<void>(send_all(
          descriptor, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
      return;
    }

    if (request->method == "POST" && request->target.ends_with("?uploads")) {
      const auto checksum = request->headers.find("x-amz-meta-chronos-sha256");
      if (checksum == request->headers.end() || checksum->second.empty()) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: "
                                   "close\r\n\r\n"));
        return;
      }
      multipart_upload_active_ = true;
      multipart_checksum_hex_ = checksum->second;
      const auto encryption = request->headers.find("x-amz-server-side-encryption");
      const auto kms_key = request->headers.find("x-amz-server-side-encryption-aws-kms-key-id");
      multipart_encryption_ = encryption == request->headers.end()
                                  ? std::nullopt
                                  : std::optional<std::string>{encryption->second};
      multipart_kms_key_id_ = kms_key == request->headers.end()
                                  ? std::nullopt
                                  : std::optional<std::string>{kms_key->second};
      multipart_parts_.clear();
      const std::string body =
          "<InitiateMultipartUploadResult><Bucket>chronos-test</Bucket><Key>parts/multipart"
          "</Key><UploadId>fixture-upload&amp;id</UploadId></InitiateMultipartUploadResult>";
      const std::string response =
          "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(body.size()) +
          "\r\nConnection: close\r\n\r\n" + body;
      static_cast<void>(send_all(descriptor, response));
      return;
    }
    if (request->method == "PUT" && request->target.contains("?partNumber=")) {
      constexpr std::string_view upload_query{"&uploadId=fixture-upload%26id"};
      const std::size_t part_begin = request->target.find("?partNumber=");
      const std::size_t upload_begin = request->target.find(upload_query, part_begin);
      const auto content_digest = request->headers.find("x-amz-content-sha256");
      if (!multipart_upload_active_ || part_begin == std::string::npos ||
          upload_begin == std::string::npos || content_digest == request->headers.end() ||
          content_digest->second.empty()) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: "
                                   "close\r\n\r\n"));
        return;
      }
      const std::string_view encoded_part{request->target.data() +
                                              static_cast<std::ptrdiff_t>(part_begin + 12U),
                                          upload_begin - (part_begin + 12U)};
      std::size_t part_number{};
      const auto parsed = std::from_chars(encoded_part.data(),
                                          encoded_part.data() + encoded_part.size(), part_number);
      if (parsed.ec != std::errc{} || parsed.ptr != encoded_part.data() + encoded_part.size() ||
          part_number == 0U) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: "
                                   "close\r\n\r\n"));
        return;
      }
      if (behavior_.fail_multipart_part == part_number) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: "
                                   "close\r\n\r\n"));
        return;
      }
      multipart_parts_[part_number] = request->body;
      const std::string response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"part-" +
                                   std::to_string(part_number) + "\"\r\nConnection: close\r\n\r\n";
      static_cast<void>(send_all(descriptor, response));
      return;
    }
    if (request->method == "POST" && request->target.ends_with("?uploadId=fixture-upload%26id")) {
      const auto condition = request->headers.find("if-none-match");
      const std::string_view body{reinterpret_cast<const char*>(request->body.data()),
                                  request->body.size()};
      if (!multipart_upload_active_ || condition == request->headers.end() ||
          condition->second != "*" || !body.contains("<PartNumber>1</PartNumber>") ||
          !body.contains("<ETag>\"part-1\"</ETag>")) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: "
                                   "close\r\n\r\n"));
        return;
      }
      if (object_.has_value()) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 412 Precondition Failed\r\nContent-Length: "
                                   "0\r\nConnection: close\r\n\r\n"));
        return;
      }
      if (behavior_.embedded_multipart_completion_error) {
        const std::string result =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InternalError</Code>"
            "<Message>fixture completion failure</Message></Error>";
        const std::string response =
            "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(result.size()) +
            "\r\nConnection: close\r\n\r\n" + result;
        static_cast<void>(send_all(descriptor, response));
        return;
      }
      std::vector<std::byte> assembled;
      for (std::size_t part_number = 1U; part_number <= multipart_parts_.size(); ++part_number) {
        const auto found = multipart_parts_.find(part_number);
        if (found == multipart_parts_.end()) {
          static_cast<void>(send_all(
              descriptor,
              "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
          return;
        }
        assembled.insert(assembled.end(), found->second.begin(), found->second.end());
      }
      object_ = std::move(assembled);
      checksum_hex_ = multipart_checksum_hex_;
      server_side_encryption_ = multipart_encryption_;
      kms_key_id_ = multipart_kms_key_id_;
      multipart_upload_active_ = false;
      multipart_parts_.clear();
      const std::string result =
          "<CompleteMultipartUploadResult><Bucket>chronos-test</Bucket><Key>parts/multipart"
          "</Key><ETag>\"complete-etag\"</ETag></CompleteMultipartUploadResult>";
      const std::string response =
          "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(result.size()) +
          "\r\nConnection: close\r\n\r\n" + result;
      static_cast<void>(send_all(descriptor, response));
      return;
    }
    if (request->method == "DELETE" && request->target.ends_with("?uploadId=fixture-upload%26id")) {
      multipart_upload_active_ = false;
      multipart_parts_.clear();
      static_cast<void>(send_all(
          descriptor, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
      return;
    }

    if (request->method == "PUT") {
      if (behavior_.transient_put_failures > 0U) {
        --behavior_.transient_put_failures;
        const std::string retry_after =
            behavior_.transient_retry_after.has_value()
                ? "Retry-After: " + *behavior_.transient_retry_after + "\r\n"
                : std::string{};
        static_cast<void>(
            send_all(descriptor, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n" +
                                     retry_after + "Connection: close\r\n\r\n"));
        return;
      }
      const auto condition = request->headers.find("if-none-match");
      const auto digest = request->headers.find("x-amz-meta-chronos-sha256");
      const auto content_digest = request->headers.find("x-amz-content-sha256");
      const auto transport_digest = request->headers.find("x-amz-checksum-sha256");
      if (condition == request->headers.end() || condition->second != "*" ||
          digest == request->headers.end() || content_digest == request->headers.end() ||
          transport_digest == request->headers.end() || content_digest->second != digest->second ||
          transport_digest->second.empty()) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: "
                                   "close\r\n\r\n"));
        return;
      }
      if (object_.has_value()) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 412 Precondition Failed\r\nContent-Length: "
                                   "0\r\nConnection: close\r\n\r\n"));
        return;
      }
      object_ = request->body;
      checksum_hex_ = digest->second;
      const auto encryption = request->headers.find("x-amz-server-side-encryption");
      const auto kms_key = request->headers.find("x-amz-server-side-encryption-aws-kms-key-id");
      server_side_encryption_ = encryption == request->headers.end()
                                    ? std::nullopt
                                    : std::optional<std::string>{encryption->second};
      kms_key_id_ = kms_key == request->headers.end() ? std::nullopt
                                                      : std::optional<std::string>{kms_key->second};
      static_cast<void>(send_all(
          descriptor, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
      return;
    }
    if (!object_.has_value()) {
      static_cast<void>(send_all(
          descriptor, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
      return;
    }
    if (request->method == "HEAD") {
      const std::optional<std::string> encryption = behavior_.head_encryption_override.has_value()
                                                        ? behavior_.head_encryption_override
                                                        : server_side_encryption_;
      const std::string encryption_headers =
          encryption.has_value()
              ? "\r\nx-amz-server-side-encryption: " + *encryption +
                    (*encryption == "aws:kms" && kms_key_id_.has_value()
                         ? "\r\nx-amz-server-side-encryption-aws-kms-key-id: " + *kms_key_id_
                         : std::string{})
              : std::string{};
      const std::string response =
          "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(object_->size()) +
          "\r\nx-amz-meta-chronos-sha256: " + checksum_hex_ + encryption_headers +
          "\r\nETag: \"fixture-etag\"\r\nConnection: close\r\n\r\n";
      static_cast<void>(send_all(descriptor, response));
      return;
    }
    if (request->method == "GET") {
      const auto range = request->headers.find("range");
      if (range == request->headers.end() || range->second != "bytes=1-2") {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: "
                                   "0\r\nConnection: close\r\n\r\n"));
        return;
      }
      const std::string header = "HTTP/1.1 206 Partial Content\r\nContent-Length: "
                                 "2\r\nContent-Range: bytes 1-2/3\r\nConnection: close\r\n\r\n";
      static_cast<void>(send_all(descriptor, header));
      const std::array<char, 2U> body{
          static_cast<char>(std::to_integer<unsigned char>((*object_)[1])),
          static_cast<char>(std::to_integer<unsigned char>((*object_)[2]))};
      static_cast<void>(send_all(descriptor, std::string_view{body.data(), body.size()}));
      return;
    }
    if (request->method == "DELETE") {
      const auto condition = request->headers.find("if-match");
      if (condition == request->headers.end() || condition->second != "\"fixture-etag\"") {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 412 Precondition Failed\r\nContent-Length: "
                                   "0\r\nConnection: close\r\n\r\n"));
        return;
      }
      object_.reset();
      checksum_hex_.clear();
      static_cast<void>(send_all(
          descriptor, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
      return;
    }
    static_cast<void>(send_all(
        descriptor,
        "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
  }

  void serve(const std::stop_token stop) {
    while (!stop.stop_requested()) {
      pollfd readiness{.fd = listener_, .events = POLLIN};
      const int ready = ::poll(&readiness, 1U, 50);
      if (ready < 0) {
        if (errno == EINTR)
          continue;
        if (!stop.stop_requested())
          record_failure("listener poll failed");
        return;
      }
      if (ready == 0)
        continue;
      const int connection = ::accept(listener_, nullptr, nullptr);
      if (connection < 0) {
        if (!stop.stop_requested())
          record_failure("listener accept failed");
        return;
      }
      handle(connection);
      ::close(connection);
    }
  }

  mutable std::mutex mutex_;
  int listener_{-1};
  std::uint16_t port_{};
  std::jthread worker_;
  std::vector<RecordedRequest> requests_;
  std::string failure_;
  std::optional<std::vector<std::byte>> object_;
  std::string checksum_hex_;
  std::optional<std::string> server_side_encryption_;
  std::optional<std::string> kms_key_id_;
  LocalS3Behavior behavior_;
  bool multipart_upload_active_{};
  std::string multipart_checksum_hex_;
  std::optional<std::string> multipart_encryption_;
  std::optional<std::string> multipart_kms_key_id_;
  std::map<std::size_t, std::vector<std::byte>> multipart_parts_;
};

class RefreshingCredentialProvider final : public S3CredentialProvider {
public:
  [[nodiscard]] common::Result<S3Credentials> acquire(const S3CredentialRequest request) override {
    std::scoped_lock lock{mutex_};
    requests_.push_back(request);
    if (request == S3CredentialRequest::kRefresh)
      refreshed_ = true;
    if (!refreshed_) {
      return S3Credentials{.access_key_id = "expired-access",
                           .secret_access_key = "expired-secret",
                           .session_token = "expired-token"};
    }
    return S3Credentials{.access_key_id = "fresh-access",
                         .secret_access_key = "fresh-secret",
                         .session_token = "fresh-token"};
  }

  [[nodiscard]] std::vector<S3CredentialRequest> requests() const {
    std::scoped_lock lock{mutex_};
    return requests_;
  }

private:
  mutable std::mutex mutex_;
  bool refreshed_{};
  std::vector<S3CredentialRequest> requests_;
};

class MultipartConcurrencyCredentialProvider final : public S3CredentialProvider {
public:
  [[nodiscard]] common::Result<S3Credentials> acquire(S3CredentialRequest request) override {
    if (request != S3CredentialRequest::kCurrent)
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnauthenticated, "fixture refresh is unsupported"});
    std::unique_lock lock{mutex_};
    ++calls_;
    if (calls_ == 3U || calls_ == 4U) {
      ++active_part_acquires_;
      maximum_active_part_acquires_ =
          std::max(maximum_active_part_acquires_, active_part_acquires_);
      if (active_part_acquires_ == 2U)
        part_workers_released_ = true;
      condition_.notify_all();
      if (!condition_.wait_for(lock, std::chrono::seconds{2},
                               [this] { return part_workers_released_; })) {
        --active_part_acquires_;
        return common::make_unexpected(
            common::Status{common::StatusCode::kUnavailable, "multipart workers did not overlap"});
      }
      --active_part_acquires_;
      condition_.notify_all();
    }
    return S3Credentials{.access_key_id = "test-access",
                         .secret_access_key = "test-secret",
                         .session_token = "test-token"};
  }

  [[nodiscard]] std::size_t maximum_active_part_acquires() const {
    std::scoped_lock lock{mutex_};
    return maximum_active_part_acquires_;
  }

private:
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::size_t calls_{};
  std::size_t active_part_acquires_{};
  std::size_t maximum_active_part_acquires_{};
  bool part_workers_released_{};
};

class ConcurrentPutCredentialProvider final : public S3CredentialProvider {
public:
  [[nodiscard]] common::Result<S3Credentials> acquire(S3CredentialRequest request) override {
    if (request != S3CredentialRequest::kCurrent)
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnauthenticated, "fixture refresh is unsupported"});
    std::unique_lock lock{mutex_};
    ++calls_;
    if (calls_ <= 2U) {
      ++ready_;
      if (ready_ == 2U)
        released_ = true;
      condition_.notify_all();
      if (!condition_.wait_for(lock, std::chrono::seconds{2}, [this] { return released_; })) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kUnavailable, "concurrent PUTs did not overlap"});
      }
    }
    return S3Credentials{.access_key_id = "test-access",
                         .secret_access_key = "test-secret",
                         .session_token = "test-token"};
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  std::size_t calls_{};
  std::size_t ready_{};
  bool released_{};
};

class ScriptedCredentialProvider final : public S3CredentialProvider {
public:
  ScriptedCredentialProvider(common::Result<S3Credentials> current,
                             common::Result<S3Credentials> refresh)
      : current_(std::move(current)), refresh_(std::move(refresh)) {}

  [[nodiscard]] common::Result<S3Credentials> acquire(S3CredentialRequest request) override {
    std::scoped_lock lock{mutex_};
    requests_.push_back(request);
    return request == S3CredentialRequest::kRefresh ? refresh_ : current_;
  }

  [[nodiscard]] std::vector<S3CredentialRequest> requests() const {
    std::scoped_lock lock{mutex_};
    return requests_;
  }

private:
  mutable std::mutex mutex_;
  common::Result<S3Credentials> current_;
  common::Result<S3Credentials> refresh_;
  std::vector<S3CredentialRequest> requests_;
};

class ScopedAwsEnvironment final {
public:
  explicit ScopedAwsEnvironment(std::array<std::optional<std::string>, 3U> values)
      : values_(std::move(values)) {
    for (std::size_t index = 0U; index < names_.size(); ++index) {
      if (const char* existing = std::getenv(names_[index].data()); existing != nullptr)
        previous_[index] = existing;
      const int result = values_[index].has_value()
                             ? ::setenv(names_[index].data(), values_[index]->c_str(), 1)
                             : ::unsetenv(names_[index].data());
      valid_ = valid_ && result == 0;
    }
  }

  ~ScopedAwsEnvironment() {
    for (std::size_t index = 0U; index < names_.size(); ++index) {
      if (previous_[index].has_value())
        static_cast<void>(::setenv(names_[index].data(), previous_[index]->c_str(), 1));
      else
        static_cast<void>(::unsetenv(names_[index].data()));
    }
  }

  ScopedAwsEnvironment(const ScopedAwsEnvironment&) = delete;
  ScopedAwsEnvironment& operator=(const ScopedAwsEnvironment&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return valid_;
  }

private:
  static constexpr std::array<std::string_view, 3U> names_{
      "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"};
  std::array<std::optional<std::string>, 3U> values_;
  std::array<std::optional<std::string>, 3U> previous_;
  bool valid_{true};
};

TEST(MemoryObjectStoreTest, ImmutablePutIsIdempotentAndRangesAreBounded) {
  MemoryObjectStore store;
  const std::vector<std::byte> bytes{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  const auto checksum = ingest::sha256(bytes).value();
  EXPECT_TRUE(store.put_if_absent("part/a", bytes, checksum).has_value());
  EXPECT_TRUE(store.put_if_absent("part/a", bytes, checksum).has_value());
  EXPECT_EQ(store.object_count(), 1U);
  const auto range = store.get_range("part/a", 1U, 2U);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(*range, (std::vector<std::byte>{std::byte{2U}, std::byte{3U}}));
  const std::vector<std::byte> other{std::byte{9U}};
  EXPECT_FALSE(store.put_if_absent("part/a", other, ingest::sha256(other).value()).has_value());
  auto wrong_delete = store.remove_if_exact("part/a", bytes.size(), ingest::sha256(other).value());
  ASSERT_FALSE(wrong_delete.has_value());
  EXPECT_EQ(wrong_delete.error().code(), common::StatusCode::kAlreadyExists);
  EXPECT_EQ(store.object_count(), 1U);
  auto removed = store.remove_if_exact("part/a", bytes.size(), checksum);
  ASSERT_TRUE(removed.has_value());
  EXPECT_TRUE(removed->removed);
  EXPECT_FALSE(removed->already_absent);
  EXPECT_EQ(store.object_count(), 0U);
  auto retry = store.remove_if_exact("part/a", bytes.size(), checksum);
  ASSERT_TRUE(retry.has_value());
  EXPECT_FALSE(retry->removed);
  EXPECT_TRUE(retry->already_absent);
}

TEST(S3EnvironmentCredentialProviderTest, SnapshotsStandardEnvironmentAndSignsRequests) {
  ScopedAwsEnvironment environment{
      {"environment-access", "environment-secret", "environment-token"}};
  ASSERT_TRUE(environment.valid());
  LocalS3Server server{
      LocalS3Behavior{.access_key_id = "environment-access", .session_token = "environment-token"}};
  ASSERT_TRUE(server.valid()) << server.failure();
  auto provider = S3EnvironmentCredentialProvider::create();
  ASSERT_TRUE(provider.has_value()) << provider.error().to_string();
  ASSERT_EQ(::setenv("AWS_ACCESS_KEY_ID", "rotated-access", 1), 0);
  ASSERT_EQ(::setenv("AWS_SECRET_ACCESS_KEY", "rotated-secret", 1), 0);
  ASSERT_EQ(::setenv("AWS_SESSION_TOKEN", "rotated-token", 1), 0);
  auto current = (*provider)->acquire(S3CredentialRequest::kCurrent);
  ASSERT_TRUE(current.has_value());
  EXPECT_EQ(current->access_key_id, "environment-access");
  EXPECT_EQ(current->secret_access_key, "environment-secret");
  ASSERT_TRUE(current->session_token.has_value());
  EXPECT_EQ(*current->session_token, "environment-token");

  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .credential_provider = *provider,
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_response_bytes = 1024U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();
  const std::vector<std::byte> bytes{std::byte{0x31U}};
  const auto uploaded =
      (*store)->put_if_absent("parts/environment", bytes, ingest::sha256(bytes).value());
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 1U);
  EXPECT_TRUE(
      requests.front().headers.at("authorization").contains("Credential=environment-access/"));
  EXPECT_EQ(requests.front().headers.at("x-amz-security-token"), "environment-token");

  const auto refresh = (*provider)->acquire(S3CredentialRequest::kRefresh);
  ASSERT_FALSE(refresh.has_value());
  EXPECT_EQ(refresh.error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3EnvironmentCredentialProviderTest, RejectsIncompleteEnvironmentWithoutLeakingSecrets) {
  {
    ScopedAwsEnvironment environment{{"environment-access", std::nullopt, std::nullopt}};
    ASSERT_TRUE(environment.valid());
    auto provider = S3EnvironmentCredentialProvider::create();
    ASSERT_FALSE(provider.has_value());
    EXPECT_EQ(provider.error().code(), common::StatusCode::kUnauthenticated);
    EXPECT_FALSE(provider.error().to_string().contains("environment-access"));
  }
  {
    const std::string overbound_access_key(1025U, 'x');
    ScopedAwsEnvironment environment{{overbound_access_key, "environment-secret", std::nullopt}};
    ASSERT_TRUE(environment.valid());
    auto provider = S3EnvironmentCredentialProvider::create();
    ASSERT_FALSE(provider.has_value());
    EXPECT_EQ(provider.error().code(), common::StatusCode::kUnauthenticated);
    EXPECT_FALSE(provider.error().to_string().contains(overbound_access_key));
  }
}

TEST(S3CredentialProviderChainTest, SelectsInOrderAndPinsRefreshToTheWinningIdentity) {
  const common::Status absent{common::StatusCode::kNotFound, "fixture identity is absent"};
  const common::Status stale{common::StatusCode::kUnauthenticated, "fixture identity was rejected"};
  auto first = std::make_shared<ScriptedCredentialProvider>(common::make_unexpected(absent),
                                                            common::make_unexpected(absent));
  auto second = std::make_shared<ScriptedCredentialProvider>(
      S3Credentials{.access_key_id = "selected-access", .secret_access_key = "selected-secret"},
      common::make_unexpected(stale));
  auto third = std::make_shared<ScriptedCredentialProvider>(
      S3Credentials{.access_key_id = "fallback-access", .secret_access_key = "fallback-secret"},
      S3Credentials{.access_key_id = "fallback-refresh", .secret_access_key = "fallback-secret"});
  auto chain = S3CredentialProviderChain::create({first, second, third});
  ASSERT_TRUE(chain.has_value()) << chain.error().to_string();

  auto current = (*chain)->acquire(S3CredentialRequest::kCurrent);
  ASSERT_TRUE(current.has_value()) << current.error().to_string();
  EXPECT_EQ(current->access_key_id, "selected-access");
  auto repeated = (*chain)->acquire(S3CredentialRequest::kCurrent);
  ASSERT_TRUE(repeated.has_value());
  EXPECT_EQ(repeated->access_key_id, "selected-access");
  auto refresh = (*chain)->acquire(S3CredentialRequest::kRefresh);
  ASSERT_FALSE(refresh.has_value());
  EXPECT_EQ(refresh.error().code(), common::StatusCode::kUnauthenticated);

  EXPECT_EQ(first->requests(), (std::vector{S3CredentialRequest::kCurrent}));
  EXPECT_EQ(second->requests(),
            (std::vector{S3CredentialRequest::kCurrent, S3CredentialRequest::kCurrent,
                         S3CredentialRequest::kRefresh}));
  EXPECT_TRUE(third->requests().empty());
}

TEST(S3CredentialProviderChainTest, StopsOnProviderFailureAndRejectsInvalidComposition) {
  const common::Status unavailable{common::StatusCode::kUnavailable,
                                   "fixture identity service is unavailable"};
  auto failing = std::make_shared<ScriptedCredentialProvider>(common::make_unexpected(unavailable),
                                                              common::make_unexpected(unavailable));
  auto fallback = std::make_shared<ScriptedCredentialProvider>(
      S3Credentials{.access_key_id = "fallback-access", .secret_access_key = "fallback-secret"},
      S3Credentials{.access_key_id = "fallback-access", .secret_access_key = "fallback-secret"});
  auto chain = S3CredentialProviderChain::create({failing, fallback});
  ASSERT_TRUE(chain.has_value());
  auto current = (*chain)->acquire(S3CredentialRequest::kCurrent);
  ASSERT_FALSE(current.has_value());
  EXPECT_EQ(current.error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(fallback->requests().empty());

  EXPECT_FALSE(S3CredentialProviderChain::create({}).has_value());
  EXPECT_FALSE(S3CredentialProviderChain::create({nullptr}).has_value());
}

TEST(S3ContainerCredentialProviderTest, CachesRefreshesAndSignsWithTemporaryCredentials) {
  const std::string response =
      "{\"AccessKeyId\":\"container-access\",\"SecretAccessKey\":\"container-secret\","
      "\"Token\":\"container-token\",\"Expiration\":\"" +
      future_iso_expiration() + "\"}";
  LocalS3Server credential_server{LocalS3Behavior{.container_credential_response = response,
                                                  .container_authorization = "Bearer fixture"}};
  ASSERT_TRUE(credential_server.valid()) << credential_server.failure();
  S3ContainerCredentialProviderConfig provider_config{
      .endpoint = "http://127.0.0.1:" + std::to_string(credential_server.port()) + "/credentials",
      .authorization_token = "Bearer fixture",
      .connect_timeout = std::chrono::milliseconds{1'000},
      .request_timeout = std::chrono::milliseconds{2'000},
      .require_tls = false};
  auto provider = S3ContainerCredentialProvider::create(provider_config);
  ASSERT_TRUE(provider.has_value()) << provider.error().to_string();

  auto first = (*provider)->acquire(S3CredentialRequest::kCurrent);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  EXPECT_EQ(first->access_key_id, "container-access");
  ASSERT_TRUE(first->session_token.has_value());
  EXPECT_EQ(*first->session_token, "container-token");
  ASSERT_TRUE((*provider)->acquire(S3CredentialRequest::kCurrent).has_value());
  EXPECT_EQ(credential_server.requests().size(), 1U);
  ASSERT_TRUE((*provider)->acquire(S3CredentialRequest::kRefresh).has_value());
  EXPECT_EQ(credential_server.requests().size(), 2U);
  EXPECT_EQ(credential_server.requests().front().headers.at("authorization"), "Bearer fixture");

  LocalS3Server s3_server{
      LocalS3Behavior{.access_key_id = "container-access", .session_token = "container-token"}};
  ASSERT_TRUE(s3_server.valid()) << s3_server.failure();
  S3ObjectStoreConfig store_config{.endpoint =
                                       "http://127.0.0.1:" + std::to_string(s3_server.port()),
                                   .region = "us-east-1",
                                   .bucket = "chronos-test",
                                   .credential_provider = *provider,
                                   .connect_timeout = std::chrono::milliseconds{1'000},
                                   .request_timeout = std::chrono::milliseconds{2'000},
                                   .maximum_response_bytes = 1024U,
                                   .require_tls = false};
  auto store = S3ObjectStore::create(std::move(store_config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();
  const std::vector<std::byte> bytes{std::byte{0x51U}};
  ASSERT_TRUE(
      (*store)->put_if_absent("parts/container", bytes, ingest::sha256(bytes).value()).has_value());
  EXPECT_EQ(credential_server.requests().size(), 2U);
  EXPECT_TRUE(s3_server.failure().empty()) << s3_server.failure();
  EXPECT_TRUE(credential_server.failure().empty()) << credential_server.failure();
}

TEST(S3ContainerCredentialProviderTest, RejectsMalformedResponseAndInsecureDefault) {
  const std::string secret{"do-not-leak-container-secret"};
  LocalS3Server credential_server{
      LocalS3Behavior{.container_credential_response =
                          "{\"AccessKeyId\":\"container-access\",\"SecretAccessKey\":\"" + secret +
                          "\",\"Expiration\":\"" + future_iso_expiration() + "\"}"}};
  ASSERT_TRUE(credential_server.valid()) << credential_server.failure();
  S3ContainerCredentialProviderConfig config{
      .endpoint = "http://127.0.0.1:" + std::to_string(credential_server.port()) + "/credentials",
      .connect_timeout = std::chrono::milliseconds{1'000},
      .request_timeout = std::chrono::milliseconds{2'000}};
  EXPECT_FALSE(S3ContainerCredentialProvider::create(config).has_value());
  config.require_tls = false;
  auto provider = S3ContainerCredentialProvider::create(config);
  ASSERT_TRUE(provider.has_value()) << provider.error().to_string();
  auto credentials = (*provider)->acquire(S3CredentialRequest::kCurrent);
  ASSERT_FALSE(credentials.has_value());
  EXPECT_EQ(credentials.error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_FALSE(credentials.error().to_string().contains(secret));

  config.endpoint = "http://user:secret@127.0.0.1/credentials";
  auto rejected = S3ContainerCredentialProvider::create(std::move(config));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_FALSE(rejected.error().to_string().contains("secret"));
  EXPECT_TRUE(credential_server.failure().empty()) << credential_server.failure();
}

TEST(S3ObjectStoreTest, SignsConditionalPutVerifiesRetryAndReadsExactRange) {
  LocalS3Server server;
  ASSERT_TRUE(server.valid()) << server.failure();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_response_bytes = 1024U};
  EXPECT_FALSE(S3ObjectStore::create(config).has_value());
  config.require_tls = false;
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  const auto checksum = ingest::sha256(bytes).value();
  auto uploaded = (*store)->put_if_absent("parts/a b", bytes, checksum);
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  auto retried = (*store)->put_if_absent("parts/a b", bytes, checksum);
  ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
  EXPECT_EQ(*retried, *uploaded);
  const std::vector<std::byte> conflicting_bytes{std::byte{9U}};
  auto conflict = (*store)->put_if_absent("parts/a b", conflicting_bytes,
                                          ingest::sha256(conflicting_bytes).value());
  ASSERT_FALSE(conflict.has_value());
  EXPECT_EQ(conflict.error().code(), common::StatusCode::kAlreadyExists);

  auto metadata = (*store)->stat("parts/a b");
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  EXPECT_EQ(metadata->size, 3U);
  EXPECT_EQ(metadata->checksum, checksum);
  auto range = (*store)->get_range("parts/a b", 1U, 2U);
  ASSERT_TRUE(range.has_value()) << range.error().to_string();
  EXPECT_EQ(*range, (std::vector<std::byte>{std::byte{2U}, std::byte{3U}}));

  auto wrong_delete = (*store)->remove_if_exact("parts/a b", bytes.size(),
                                                ingest::sha256(conflicting_bytes).value());
  ASSERT_FALSE(wrong_delete.has_value());
  EXPECT_EQ(wrong_delete.error().code(), common::StatusCode::kAlreadyExists);
  auto removed = (*store)->remove_if_exact("parts/a b", bytes.size(), checksum);
  ASSERT_TRUE(removed.has_value()) << removed.error().to_string();
  EXPECT_TRUE(removed->removed);
  EXPECT_FALSE(removed->already_absent);
  auto deletion_retry = (*store)->remove_if_exact("parts/a b", bytes.size(), checksum);
  ASSERT_TRUE(deletion_retry.has_value());
  EXPECT_FALSE(deletion_retry->removed);
  EXPECT_TRUE(deletion_retry->already_absent);

  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 11U);
  EXPECT_EQ(requests[0].method, "PUT");
  EXPECT_EQ(requests[0].target, "/chronos-test/parts/a%20b");
  EXPECT_EQ(requests[1].method, "PUT");
  EXPECT_EQ(requests[2].method, "HEAD");
  EXPECT_EQ(requests[3].method, "PUT");
  EXPECT_EQ(requests[4].method, "HEAD");
  EXPECT_EQ(requests[5].method, "HEAD");
  EXPECT_EQ(requests[6].method, "GET");
  EXPECT_EQ(requests[7].method, "HEAD");
  EXPECT_EQ(requests[8].method, "HEAD");
  EXPECT_EQ(requests[9].method, "DELETE");
  EXPECT_EQ(requests[9].headers.at("if-match"), "\"fixture-etag\"");
  EXPECT_EQ(requests[10].method, "HEAD");
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, IgnoresProcessProxyAndRejectsCredentialBearingProxyConfiguration) {
  LocalS3Server server;
  ASSERT_TRUE(server.valid()) << server.failure();
  const char* previous = std::getenv("http_proxy");
  const std::optional<std::string> saved =
      previous == nullptr ? std::nullopt : std::optional<std::string>{previous};
  ASSERT_EQ(::setenv("http_proxy", "http://127.0.0.1:1", 1), 0);
  struct ProxyEnvironmentRestore {
    std::optional<std::string> value;
    ~ProxyEnvironmentRestore() {
      if (value.has_value())
        static_cast<void>(::setenv("http_proxy", value->c_str(), 1));
      else
        static_cast<void>(::unsetenv("http_proxy"));
    }
  } restore{saved};
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_response_bytes = 1024U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(config);
  ASSERT_TRUE(store.has_value()) << store.error().to_string();
  const std::vector<std::byte> bytes{std::byte{0x44U}};
  ASSERT_TRUE(
      (*store)->put_if_absent("parts/no-proxy", bytes, ingest::sha256(bytes).value()).has_value());

  config.proxy_url = "http://user:secret@proxy.example";
  auto rejected = S3ObjectStore::create(std::move(config));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(rejected.error().to_string().contains("secret"));
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, RequestsAndVerifiesS3ManagedEncryption) {
  LocalS3Server server;
  ASSERT_TRUE(server.valid()) << server.failure();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_response_bytes = 1024U,
                             .server_side_encryption = S3ServerSideEncryption::kS3ManagedAes256,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{0x45U}};
  auto uploaded = (*store)->put_if_absent("parts/encrypted", bytes, ingest::sha256(bytes).value());
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();

  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 2U);
  EXPECT_EQ(requests[0].method, "PUT");
  EXPECT_EQ(requests[0].headers.at("x-amz-server-side-encryption"), "AES256");
  EXPECT_FALSE(requests[0].headers.contains("x-amz-server-side-encryption-aws-kms-key-id"));
  EXPECT_EQ(requests[1].method, "HEAD");
  EXPECT_FALSE(requests[1].headers.contains("x-amz-server-side-encryption"));
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, RejectsEncryptionMetadataMismatchAndInvalidKmsConfiguration) {
  LocalS3Server server{LocalS3Behavior{.head_encryption_override = "AES256"}};
  ASSERT_TRUE(server.valid()) << server.failure();
  const std::string kms_key_arn{
      "arn:aws:kms:us-east-1:123456789012:key/01234567-89ab-cdef-0123-456789abcdef"};
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_response_bytes = 1024U,
                             .server_side_encryption = S3ServerSideEncryption::kKms,
                             .kms_key_id = kms_key_arn,
                             .require_tls = false};
  auto store = S3ObjectStore::create(config);
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{0x46U}};
  auto uploaded =
      (*store)->put_if_absent("parts/kms-mismatch", bytes, ingest::sha256(bytes).value());
  ASSERT_FALSE(uploaded.has_value());
  EXPECT_EQ(uploaded.error().code(), common::StatusCode::kCorruption);
  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 2U);
  EXPECT_EQ(requests[0].headers.at("x-amz-server-side-encryption"), "aws:kms");
  EXPECT_EQ(requests[0].headers.at("x-amz-server-side-encryption-aws-kms-key-id"), kms_key_arn);

  config.kms_key_id.reset();
  EXPECT_FALSE(S3ObjectStore::create(config).has_value());
  config.server_side_encryption = S3ServerSideEncryption::kS3ManagedAes256;
  config.kms_key_id = kms_key_arn;
  EXPECT_FALSE(S3ObjectStore::create(std::move(config)).has_value());
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, RefreshesRejectedCredentialsAndRetriesTransientConditionalPut) {
  LocalS3Server server{LocalS3Behavior{.access_key_id = "fresh-access",
                                       .session_token = "fresh-token",
                                       .transient_put_failures = 1U}};
  ASSERT_TRUE(server.valid()) << server.failure();
  auto provider = std::make_shared<RefreshingCredentialProvider>();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .credential_provider = provider,
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_attempts = 3U,
                             .initial_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_response_bytes = 1024U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{4U}, std::byte{5U}, std::byte{6U}};
  const auto checksum = ingest::sha256(bytes).value();
  auto uploaded = (*store)->put_if_absent("parts/retried", bytes, checksum);
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  EXPECT_EQ(uploaded->size, bytes.size());
  EXPECT_EQ(uploaded->checksum, checksum);

  EXPECT_EQ(provider->requests(),
            (std::vector{S3CredentialRequest::kCurrent, S3CredentialRequest::kRefresh,
                         S3CredentialRequest::kCurrent}));
  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 3U);
  EXPECT_TRUE(requests[0].headers.at("authorization").contains("Credential=expired-access/"));
  EXPECT_TRUE(requests[1].headers.at("authorization").contains("Credential=fresh-access/"));
  EXPECT_TRUE(requests[2].headers.at("authorization").contains("Credential=fresh-access/"));
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, ConcurrentConditionalWritersConvergeWithoutOverwrite) {
  const auto run_race = [](const bool same_content) {
    LocalS3Server server;
    ASSERT_TRUE(server.valid()) << server.failure();
    auto provider = std::make_shared<ConcurrentPutCredentialProvider>();
    const auto make_store = [&]() {
      S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                                 .region = "us-east-1",
                                 .bucket = "chronos-test",
                                 .credential_provider = provider,
                                 .connect_timeout = std::chrono::milliseconds{1'000},
                                 .request_timeout = std::chrono::milliseconds{2'000},
                                 .maximum_response_bytes = 1024U,
                                 .require_tls = false};
      return S3ObjectStore::create(std::move(config));
    };
    auto first_store = make_store();
    auto second_store = make_store();
    ASSERT_TRUE(first_store.has_value()) << first_store.error().to_string();
    ASSERT_TRUE(second_store.has_value()) << second_store.error().to_string();

    const std::vector<std::byte> first_bytes{std::byte{0x21U}, std::byte{0x22U}};
    const std::vector<std::byte> different_bytes{std::byte{0x31U}, std::byte{0x32U}};
    const auto& second_bytes = same_content ? first_bytes : different_bytes;
    std::optional<common::Result<ObjectMetadata>> first_result;
    std::optional<common::Result<ObjectMetadata>> second_result;
    std::jthread first_writer{[&] {
      first_result =
          (*first_store)
              ->put_if_absent("parts/concurrent", first_bytes, ingest::sha256(first_bytes).value());
    }};
    std::jthread second_writer{[&] {
      second_result = (*second_store)
                          ->put_if_absent("parts/concurrent", second_bytes,
                                          ingest::sha256(second_bytes).value());
    }};
    first_writer.join();
    second_writer.join();
    ASSERT_TRUE(first_result.has_value());
    ASSERT_TRUE(second_result.has_value());

    if (same_content) {
      ASSERT_TRUE(first_result->has_value()) << first_result->error().to_string();
      ASSERT_TRUE(second_result->has_value()) << second_result->error().to_string();
      EXPECT_EQ(**first_result, **second_result);
    } else {
      const bool first_won = first_result->has_value();
      const bool second_won = second_result->has_value();
      EXPECT_NE(first_won, second_won);
      const auto& loser = first_won ? *second_result : *first_result;
      ASSERT_FALSE(loser.has_value());
      EXPECT_EQ(loser.error().code(), common::StatusCode::kAlreadyExists);
    }

    auto stored = (*first_store)->stat("parts/concurrent");
    ASSERT_TRUE(stored.has_value()) << stored.error().to_string();
    const auto first_checksum = ingest::sha256(first_bytes).value();
    const auto second_checksum = ingest::sha256(second_bytes).value();
    if (same_content) {
      EXPECT_EQ(stored->checksum, first_checksum);
    } else {
      EXPECT_TRUE(stored->checksum == first_checksum || stored->checksum == second_checksum);
    }
    const auto requests = server.requests();
    EXPECT_EQ(std::ranges::count_if(
                  requests, [](const RecordedRequest& request) { return request.method == "PUT"; }),
              2);
    EXPECT_TRUE(server.failure().empty()) << server.failure();
  };

  run_race(true);
  run_race(false);
}

TEST(S3ObjectStoreTest, StopsAtTheConfiguredRetryAttemptLimit) {
  LocalS3Server server{LocalS3Behavior{.transient_put_failures = 3U}};
  ASSERT_TRUE(server.valid()) << server.failure();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_attempts = 2U,
                             .initial_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_response_bytes = 1024U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{7U}};
  auto uploaded = (*store)->put_if_absent("parts/exhausted", bytes, ingest::sha256(bytes).value());
  ASSERT_FALSE(uploaded.has_value());
  EXPECT_EQ(uploaded.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(server.requests().size(), 2U);
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, HonorsRetryAfterWithinConfiguredBackoffCeiling) {
  LocalS3Server server{LocalS3Behavior{.transient_put_failures = 1U, .transient_retry_after = "1"}};
  ASSERT_TRUE(server.valid()) << server.failure();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_attempts = 2U,
                             .initial_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_retry_backoff = std::chrono::milliseconds{20},
                             .maximum_retry_jitter = std::chrono::milliseconds{0},
                             .maximum_response_bytes = 1024U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{0x17U}};
  const auto started = std::chrono::steady_clock::now();
  auto uploaded =
      (*store)->put_if_absent("parts/retry-after", bytes, ingest::sha256(bytes).value());
  const auto elapsed = std::chrono::steady_clock::now() - started;
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  EXPECT_GE(elapsed, std::chrono::milliseconds{15});
  EXPECT_LT(elapsed, std::chrono::seconds{2});
  EXPECT_EQ(server.requests().size(), 2U);
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, HonorsEveryHttpDateRetryAfterFormWithinBackoffCeiling) {
  constexpr std::array<const char*, 3U> formats{
      "%a, %d %b %Y %H:%M:%S GMT", "%A, %d-%b-%y %H:%M:%S GMT", "%a %b %e %H:%M:%S %Y"};
  for (const char* format : formats) {
    const std::string retry_after = future_http_date(format);
    ASSERT_FALSE(retry_after.empty());
    LocalS3Server server{
        LocalS3Behavior{.transient_put_failures = 1U, .transient_retry_after = retry_after}};
    ASSERT_TRUE(server.valid()) << server.failure();
    S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                               .region = "us-east-1",
                               .bucket = "chronos-test",
                               .access_key_id = "test-access",
                               .secret_access_key = "test-secret",
                               .session_token = "test-token",
                               .connect_timeout = std::chrono::milliseconds{1'000},
                               .request_timeout = std::chrono::milliseconds{2'000},
                               .maximum_attempts = 2U,
                               .initial_retry_backoff = std::chrono::milliseconds{0},
                               .maximum_retry_backoff = std::chrono::milliseconds{20},
                               .maximum_retry_jitter = std::chrono::milliseconds{0},
                               .maximum_response_bytes = 1024U,
                               .require_tls = false};
    auto store = S3ObjectStore::create(std::move(config));
    ASSERT_TRUE(store.has_value()) << store.error().to_string();

    const std::vector<std::byte> bytes{std::byte{0x18U}};
    const auto started = std::chrono::steady_clock::now();
    auto uploaded =
        (*store)->put_if_absent("parts/http-date", bytes, ingest::sha256(bytes).value());
    const auto elapsed = std::chrono::steady_clock::now() - started;
    ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
    EXPECT_GE(elapsed, std::chrono::milliseconds{15}) << retry_after;
    EXPECT_LT(elapsed, std::chrono::seconds{2}) << retry_after;
    EXPECT_EQ(server.requests().size(), 2U);
    EXPECT_TRUE(server.failure().empty()) << server.failure();
  }
}

TEST(S3ObjectStoreTest, IgnoresInvalidHttpDateRetryAfter) {
  LocalS3Server server{LocalS3Behavior{.transient_put_failures = 1U,
                                       .transient_retry_after = "Sun, 31 Feb 2099 25:61:61 GMT"}};
  ASSERT_TRUE(server.valid()) << server.failure();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_attempts = 2U,
                             .initial_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_retry_backoff = std::chrono::milliseconds{200},
                             .maximum_retry_jitter = std::chrono::milliseconds{0},
                             .maximum_response_bytes = 1024U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{0x19U}};
  const auto started = std::chrono::steady_clock::now();
  auto uploaded =
      (*store)->put_if_absent("parts/invalid-http-date", bytes, ingest::sha256(bytes).value());
  const auto elapsed = std::chrono::steady_clock::now() - started;
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  EXPECT_LT(elapsed, std::chrono::milliseconds{180});
  EXPECT_EQ(server.requests().size(), 2U);
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, AppliesDeterministicJitterWithoutExceedingBackoffCeiling) {
  LocalS3Server server{LocalS3Behavior{.transient_put_failures = 1U}};
  ASSERT_TRUE(server.valid()) << server.failure();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_attempts = 2U,
                             .initial_retry_backoff = std::chrono::milliseconds{20},
                             .maximum_retry_backoff = std::chrono::milliseconds{100},
                             .maximum_retry_jitter = std::chrono::milliseconds{80},
                             .retry_jitter_seed = 1U,
                             .maximum_response_bytes = 1024U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(config);
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{0x1AU}};
  const auto started = std::chrono::steady_clock::now();
  auto uploaded = (*store)->put_if_absent("parts/jitter", bytes, ingest::sha256(bytes).value());
  const auto elapsed = std::chrono::steady_clock::now() - started;
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  EXPECT_GE(elapsed, std::chrono::milliseconds{60});
  EXPECT_LT(elapsed, std::chrono::milliseconds{180});

  config.maximum_retry_jitter = std::chrono::milliseconds{-1};
  auto rejected = S3ObjectStore::create(std::move(config));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, MultipartUploadCompletesConditionallyAndVerifiesExactObject) {
  LocalS3Server server;
  ASSERT_TRUE(server.valid()) << server.failure();
  constexpr std::size_t part_bytes = 5U * 1024U * 1024U;
  const std::string kms_key_arn{
      "arn:aws:kms:us-east-1:123456789012:key/01234567-89ab-cdef-0123-456789abcdef"};
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{5'000},
                             .multipart_threshold_bytes = part_bytes,
                             .multipart_part_bytes = part_bytes,
                             .maximum_response_bytes = part_bytes + 3U,
                             .server_side_encryption = S3ServerSideEncryption::kKms,
                             .kms_key_id = kms_key_arn,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  std::vector<std::byte> bytes(part_bytes + 3U, std::byte{0x5A});
  bytes[part_bytes] = std::byte{1U};
  bytes[part_bytes + 1U] = std::byte{2U};
  bytes[part_bytes + 2U] = std::byte{3U};
  const auto checksum = ingest::sha256(bytes).value();
  auto uploaded = (*store)->put_if_absent("parts/multipart", bytes, checksum);
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  EXPECT_EQ(uploaded->size, bytes.size());
  EXPECT_EQ(uploaded->checksum, checksum);

  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 6U);
  EXPECT_EQ(requests[0].method, "HEAD");
  EXPECT_EQ(requests[1].method, "POST");
  EXPECT_TRUE(requests[1].target.ends_with("?uploads"));
  EXPECT_EQ(requests[1].headers.at("x-amz-server-side-encryption"), "aws:kms");
  EXPECT_EQ(requests[1].headers.at("x-amz-server-side-encryption-aws-kms-key-id"), kms_key_arn);
  const auto first_part = std::ranges::find_if(requests, [](const RecordedRequest& request) {
    return request.target.ends_with("?partNumber=1&uploadId=fixture-upload%26id");
  });
  const auto second_part = std::ranges::find_if(requests, [](const RecordedRequest& request) {
    return request.target.ends_with("?partNumber=2&uploadId=fixture-upload%26id");
  });
  ASSERT_NE(first_part, requests.end());
  ASSERT_NE(second_part, requests.end());
  EXPECT_EQ(first_part->method, "PUT");
  EXPECT_EQ(first_part->body.size(), part_bytes);
  EXPECT_EQ(second_part->method, "PUT");
  EXPECT_EQ(second_part->body.size(), 3U);
  EXPECT_EQ(requests[4].method, "POST");
  EXPECT_TRUE(requests[4].target.ends_with("?uploadId=fixture-upload%26id"));
  EXPECT_EQ(requests[4].headers.at("if-none-match"), "*");
  EXPECT_EQ(requests[5].method, "HEAD");
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, BoundsParallelPartWorkersAndPreservesCompletionOrder) {
  LocalS3Server server;
  ASSERT_TRUE(server.valid()) << server.failure();
  constexpr std::size_t part_bytes = 5U * 1024U * 1024U;
  auto provider = std::make_shared<MultipartConcurrencyCredentialProvider>();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .credential_provider = provider,
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{5'000},
                             .multipart_threshold_bytes = part_bytes,
                             .multipart_part_bytes = part_bytes,
                             .multipart_maximum_concurrency = 2U,
                             .maximum_response_bytes = part_bytes + 3U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  std::vector<std::byte> bytes(part_bytes + 3U, std::byte{0x6A});
  auto uploaded = (*store)->put_if_absent("parts/multipart", bytes, ingest::sha256(bytes).value());
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  EXPECT_EQ(provider->maximum_active_part_acquires(), 2U);

  const auto requests = server.requests();
  const auto completion = std::ranges::find_if(requests, [](const RecordedRequest& request) {
    return request.method == "POST" && request.target.contains("?uploadId=");
  });
  ASSERT_NE(completion, requests.end());
  const std::string_view completion_body{reinterpret_cast<const char*>(completion->body.data()),
                                         completion->body.size()};
  const std::size_t first = completion_body.find("<PartNumber>1</PartNumber>");
  const std::size_t second = completion_body.find("<PartNumber>2</PartNumber>");
  EXPECT_NE(first, std::string_view::npos);
  EXPECT_NE(second, std::string_view::npos);
  EXPECT_LT(first, second);
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, RejectsInvalidMultipartWorkerBound) {
  S3ObjectStoreConfig config{.endpoint = "https://s3.example",
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .multipart_maximum_concurrency = 0U};
  auto rejected = S3ObjectStore::create(std::move(config));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(S3ObjectStoreTest, MultipartPartFailureAbortsWithoutPublishingAnObject) {
  LocalS3Server server{LocalS3Behavior{.fail_multipart_part = 2U}};
  ASSERT_TRUE(server.valid()) << server.failure();
  constexpr std::size_t part_bytes = 5U * 1024U * 1024U;
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{5'000},
                             .multipart_threshold_bytes = part_bytes,
                             .multipart_part_bytes = part_bytes,
                             .maximum_response_bytes = part_bytes + 1U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes(part_bytes + 1U, std::byte{0x3C});
  auto uploaded = (*store)->put_if_absent("parts/multipart", bytes, ingest::sha256(bytes).value());
  ASSERT_FALSE(uploaded.has_value());
  EXPECT_EQ(uploaded.error().code(), common::StatusCode::kIoError);
  auto missing = (*store)->stat("parts/multipart");
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kNotFound);

  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 6U);
  EXPECT_EQ(requests[4].method, "DELETE");
  EXPECT_TRUE(requests[4].target.ends_with("?uploadId=fixture-upload%26id"));
  EXPECT_EQ(requests[5].method, "HEAD");
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, EmbeddedMultipartCompletionErrorAbortsWithoutPublishingAnObject) {
  LocalS3Server server{LocalS3Behavior{.embedded_multipart_completion_error = true}};
  ASSERT_TRUE(server.valid()) << server.failure();
  constexpr std::size_t part_bytes = 5U * 1024U * 1024U;
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{5'000},
                             .multipart_threshold_bytes = part_bytes,
                             .multipart_part_bytes = part_bytes,
                             .maximum_response_bytes = part_bytes + 1U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes(part_bytes + 1U, std::byte{0x6D});
  auto uploaded = (*store)->put_if_absent("parts/multipart", bytes, ingest::sha256(bytes).value());
  ASSERT_FALSE(uploaded.has_value());
  EXPECT_EQ(uploaded.error().code(), common::StatusCode::kCorruption);
  auto missing = (*store)->stat("parts/multipart");
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kNotFound);

  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 8U);
  EXPECT_EQ(requests[4].method, "POST");
  EXPECT_EQ(requests[5].method, "HEAD");
  EXPECT_EQ(requests[6].method, "DELETE");
  EXPECT_TRUE(requests[6].target.ends_with("?uploadId=fixture-upload%26id"));
  EXPECT_EQ(requests[7].method, "HEAD");
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

} // namespace
} // namespace chronos::tiering
