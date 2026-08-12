#include "chronos/tiering/object_store.hpp"

#include "chronos/ingest/sha256.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <curl/curl.h>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {
[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return common::Status{common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool contains_control(const std::string_view value) noexcept {
  return std::ranges::any_of(value, [](const char character) {
    const auto byte = static_cast<unsigned char>(character);
    return byte <= 0x1FU || byte == 0x7FU;
  });
}

[[nodiscard]] bool contains_space(const std::string_view value) noexcept {
  return value.contains(' ');
}

[[nodiscard]] char hexadecimal(const std::uint8_t value) noexcept {
  constexpr std::string_view digits{"0123456789abcdef"};
  return digits[value & 0x0FU];
}

[[nodiscard]] std::string digest_hex(const ingest::Sha256Digest& digest) {
  std::string output;
  output.resize(ingest::Sha256Digest::kSize * 2U);
  std::size_t offset{};
  for (const std::byte byte : digest.bytes()) {
    const auto value = std::to_integer<std::uint8_t>(byte);
    output[offset++] = hexadecimal(static_cast<std::uint8_t>(value >> 4U));
    output[offset++] = hexadecimal(value);
  }
  return output;
}

[[nodiscard]] std::string digest_base64(const ingest::Sha256Digest& digest) {
  constexpr std::string_view alphabet{
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"};
  const auto& bytes = digest.bytes();
  std::string output;
  output.reserve(44U);
  for (std::size_t index = 0U; index < bytes.size(); index += 3U) {
    const auto first = std::to_integer<std::uint32_t>(bytes[index]);
    const auto second =
        index + 1U < bytes.size() ? std::to_integer<std::uint32_t>(bytes[index + 1U]) : 0U;
    const auto third =
        index + 2U < bytes.size() ? std::to_integer<std::uint32_t>(bytes[index + 2U]) : 0U;
    const std::uint32_t packed = (first << 16U) | (second << 8U) | third;
    output.push_back(alphabet[(packed >> 18U) & 0x3FU]);
    output.push_back(alphabet[(packed >> 12U) & 0x3FU]);
    output.push_back(index + 1U < bytes.size() ? alphabet[(packed >> 6U) & 0x3FU] : '=');
    output.push_back(index + 2U < bytes.size() ? alphabet[packed & 0x3FU] : '=');
  }
  return output;
}

[[nodiscard]] common::Result<ingest::Sha256Digest>
parse_digest_hex(const std::string_view encoded) {
  if (encoded.size() != ingest::Sha256Digest::kSize * 2U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "S3 object checksum metadata is invalid"});
  }
  ingest::Sha256Digest::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    std::uint8_t value{};
    const char* begin = encoded.data() + static_cast<std::ptrdiff_t>(index * 2U);
    const auto parsed = std::from_chars(begin, begin + 2, value, 16);
    if (parsed.ec != std::errc{} || parsed.ptr != begin + 2) {
      return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                    "S3 object checksum metadata is invalid"});
    }
    bytes[index] = std::byte{value};
  }
  return ingest::Sha256Digest{bytes};
}

[[nodiscard]] bool unreserved(const unsigned char value) noexcept {
  return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
         (value >= '0' && value <= '9') || value == '-' || value == '_' || value == '.' ||
         value == '~';
}

[[nodiscard]] std::string encode_path(const std::string_view input, const bool preserve_slash) {
  std::string encoded;
  encoded.reserve(input.size());
  for (const char character : input) {
    const auto value = static_cast<unsigned char>(character);
    if (unreserved(value) || (preserve_slash && value == '/')) {
      encoded.push_back(static_cast<char>(value));
      continue;
    }
    encoded.push_back('%');
    encoded.push_back(hexadecimal(static_cast<std::uint8_t>(value >> 4U)));
    encoded.push_back(hexadecimal(value));
  }
  return encoded;
}

[[nodiscard]] bool valid_bucket(const std::string_view bucket) noexcept {
  return !bucket.empty() && bucket.size() <= 255U && !contains_control(bucket) &&
         std::ranges::all_of(bucket, [](const char character) {
           return unreserved(static_cast<unsigned char>(character));
         });
}

[[nodiscard]] CURLcode initialize_curl() noexcept {
  // A library cannot safely call the process-global cleanup while another component may still use
  // libcurl. C++ static initialization serializes this one process-lifetime initialization.
  static const CURLcode result = curl_global_init(CURL_GLOBAL_DEFAULT);
  return result;
}

struct CurlHandleDeleter {
  void operator()(CURL* handle) const noexcept {
    if (handle != nullptr)
      curl_easy_cleanup(handle);
  }
};
using CurlHandle = std::unique_ptr<CURL, CurlHandleDeleter>;

struct HeaderListDeleter {
  void operator()(curl_slist* headers) const noexcept {
    curl_slist_free_all(headers);
  }
};
using HeaderList = std::unique_ptr<curl_slist, HeaderListDeleter>;

struct ResponseCapture {
  std::vector<std::byte> body;
  std::size_t maximum_body_bytes{};
  std::optional<std::string> checksum_hex;
  std::optional<std::string> content_range;
  std::optional<std::string> entity_tag;
  bool checksum_conflict{};
  bool content_range_conflict{};
  bool entity_tag_conflict{};
  bool body_limit_exhausted{};
};

[[nodiscard]] std::string_view trim_header_value(std::string_view value) noexcept {
  while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
    value.remove_prefix(1U);
  while (!value.empty() && (value.back() == ' ' || value.back() == '\t' || value.back() == '\r' ||
                            value.back() == '\n')) {
    value.remove_suffix(1U);
  }
  return value;
}

[[nodiscard]] std::size_t capture_body(char* data, const std::size_t size, const std::size_t count,
                                       void* context) noexcept {
  auto& capture = *static_cast<ResponseCapture*>(context);
  if (count != 0U && size > std::numeric_limits<std::size_t>::max() / count) {
    capture.body_limit_exhausted = true;
    return 0U;
  }
  const std::size_t length = size * count;
  if (length > capture.maximum_body_bytes - capture.body.size()) {
    capture.body_limit_exhausted = true;
    return 0U;
  }
  try {
    const auto* bytes = reinterpret_cast<const std::byte*>(data);
    capture.body.insert(capture.body.end(), bytes, bytes + length);
  } catch (...) {
    capture.body_limit_exhausted = true;
    return 0U;
  }
  return length;
}

[[nodiscard]] std::size_t capture_header(char* data, const std::size_t size,
                                         const std::size_t count, void* context) noexcept {
  auto& capture = *static_cast<ResponseCapture*>(context);
  if (count != 0U && size > std::numeric_limits<std::size_t>::max() / count)
    return 0U;
  const std::size_t length = size * count;
  const std::string_view line{data, length};
  const auto matches_name = [&](const std::string_view name) {
    if (line.size() < name.size())
      return false;
    for (std::size_t index = 0U; index < name.size(); ++index) {
      const unsigned char value = static_cast<unsigned char>(line[index]);
      const char lowered = value >= 'A' && value <= 'Z' ? static_cast<char>(value + ('a' - 'A'))
                                                        : static_cast<char>(value);
      if (lowered != name[index])
        return false;
    }
    return true;
  };
  constexpr std::string_view checksum_name{"x-amz-meta-chronos-sha256:"};
  constexpr std::string_view content_range_name{"content-range:"};
  constexpr std::string_view entity_tag_name{"etag:"};
  try {
    if (matches_name(checksum_name)) {
      const std::string value{trim_header_value(line.substr(checksum_name.size()))};
      if (capture.checksum_hex.has_value())
        capture.checksum_conflict = true;
      else
        capture.checksum_hex = value;
    } else if (matches_name(content_range_name)) {
      const std::string value{trim_header_value(line.substr(content_range_name.size()))};
      if (capture.content_range.has_value())
        capture.content_range_conflict = true;
      else
        capture.content_range = value;
    } else if (matches_name(entity_tag_name)) {
      const std::string value{trim_header_value(line.substr(entity_tag_name.size()))};
      if (capture.entity_tag.has_value())
        capture.entity_tag_conflict = true;
      else
        capture.entity_tag = value;
    }
  } catch (...) {
    return 0U;
  }
  return length;
}

[[nodiscard]] common::Status curl_failure(const CURLcode code, const bool body_limit_exhausted) {
  if (body_limit_exhausted)
    return exhausted("S3 response exceeds configured memory bound");
  switch (code) {
  case CURLE_OPERATION_TIMEDOUT:
  case CURLE_COULDNT_CONNECT:
  case CURLE_COULDNT_RESOLVE_HOST:
  case CURLE_COULDNT_RESOLVE_PROXY:
  case CURLE_SEND_ERROR:
  case CURLE_RECV_ERROR:
  case CURLE_GOT_NOTHING:
  case CURLE_PARTIAL_FILE:
  case CURLE_SSL_CONNECT_ERROR:
  case CURLE_HTTP2_STREAM:
  case CURLE_QUIC_CONNECT_ERROR:
    return {common::StatusCode::kUnavailable, "S3 endpoint is unavailable"};
  case CURLE_PEER_FAILED_VERIFICATION:
  case CURLE_SSL_CERTPROBLEM:
  case CURLE_SSL_CACERT_BADFILE:
  case CURLE_SSL_ISSUER_ERROR:
    return {common::StatusCode::kUnauthenticated, "S3 TLS peer verification failed"};
  default:
    return {common::StatusCode::kIoError, "S3 HTTP transfer failed"};
  }
}

[[nodiscard]] common::Status http_failure(const long status) {
  if (status == 401L || status == 403L)
    return {common::StatusCode::kUnauthenticated, "S3 request was not authorized"};
  if (status == 404L)
    return {common::StatusCode::kNotFound, "S3 object does not exist"};
  if (status == 409L || status == 425L || status == 429L || (status >= 500L && status <= 599L))
    return {common::StatusCode::kUnavailable, "S3 request should be retried"};
  if (status == 416L)
    return invalid("S3 object range is outside immutable content");
  return {common::StatusCode::kIoError, "S3 endpoint returned an unsuccessful status"};
}

[[nodiscard]] bool retryable_http_status(const long status) noexcept {
  return status == 409L || status == 425L || status == 429L || (status >= 500L && status <= 599L);
}

[[nodiscard]] bool valid_credentials(const S3Credentials& credentials) noexcept {
  return !credentials.access_key_id.empty() && !credentials.secret_access_key.empty() &&
         credentials.access_key_id.size() <= 1024U &&
         credentials.secret_access_key.size() <= 4096U &&
         !credentials.access_key_id.contains(':') && !credentials.secret_access_key.contains(':') &&
         !contains_control(credentials.access_key_id) &&
         !contains_control(credentials.secret_access_key) &&
         !contains_space(credentials.access_key_id) &&
         !contains_space(credentials.secret_access_key) &&
         (!credentials.session_token.has_value() ||
          (!credentials.session_token->empty() && credentials.session_token->size() <= 8192U &&
           !contains_control(*credentials.session_token) &&
           !contains_space(*credentials.session_token)));
}

[[nodiscard]] common::Status append_header(HeaderList& headers, const std::string& header) {
  curl_slist* appended = curl_slist_append(headers.get(), header.c_str());
  if (appended == nullptr)
    return exhausted("S3 request header allocation failed");
  static_cast<void>(headers.release());
  headers.reset(appended);
  return common::Status::ok();
}

struct ParsedContentRange {
  std::size_t first{};
  std::size_t last{};
  std::size_t total{};
};

[[nodiscard]] common::Result<ParsedContentRange>
parse_content_range(const std::string_view encoded) {
  constexpr std::string_view prefix{"bytes "};
  if (!encoded.starts_with(prefix)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "S3 content range is invalid"});
  }
  const char* cursor = encoded.data() + static_cast<std::ptrdiff_t>(prefix.size());
  const char* end = encoded.data() + static_cast<std::ptrdiff_t>(encoded.size());
  ParsedContentRange range;
  auto parsed = std::from_chars(cursor, end, range.first);
  if (parsed.ec != std::errc{} || parsed.ptr == end || *parsed.ptr != '-')
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "S3 content range is invalid"});
  cursor = parsed.ptr + 1;
  parsed = std::from_chars(cursor, end, range.last);
  if (parsed.ec != std::errc{} || parsed.ptr == end || *parsed.ptr != '/')
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "S3 content range is invalid"});
  cursor = parsed.ptr + 1;
  parsed = std::from_chars(cursor, end, range.total);
  if (parsed.ec != std::errc{} || parsed.ptr != end || range.first > range.last ||
      range.last >= range.total) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "S3 content range is invalid"});
  }
  return range;
}
} // namespace

class MemoryObjectStore::Impl {
public:
  struct Object {
    std::vector<std::byte> bytes;
    ingest::Sha256Digest checksum;
  };
  mutable std::mutex mutex;
  std::map<std::string, Object, std::less<>> objects;
};

MemoryObjectStore::MemoryObjectStore() : impl_(std::make_unique<Impl>()) {}
MemoryObjectStore::~MemoryObjectStore() = default;

common::Result<ObjectMetadata>
MemoryObjectStore::put_if_absent(const std::string_view key, const common::ByteView bytes,
                                 const ingest::Sha256Digest& checksum) {
  if (key.empty())
    return common::make_unexpected(invalid("object key must be nonempty"));
  auto actual = ingest::sha256(bytes);
  if (!actual.has_value())
    return common::make_unexpected(actual.error());
  if (*actual != checksum) {
    return common::make_unexpected(invalid("object checksum does not match upload bytes"));
  }
  std::scoped_lock lock{impl_->mutex};
  const auto existing = impl_->objects.find(key);
  if (existing != impl_->objects.end()) {
    if (existing->second.checksum != checksum || existing->second.bytes.size() != bytes.size()) {
      return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                    "immutable object key has different content"});
    }
    return ObjectMetadata{std::string{key}, existing->second.bytes.size(), checksum};
  }
  impl_->objects.emplace(std::string{key}, Impl::Object{{bytes.begin(), bytes.end()}, checksum});
  return ObjectMetadata{std::string{key}, bytes.size(), checksum};
}

common::Result<ObjectMetadata> MemoryObjectStore::stat(const std::string_view key) const {
  std::scoped_lock lock{impl_->mutex};
  const auto found = impl_->objects.find(key);
  if (found == impl_->objects.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "object does not exist"});
  }
  return ObjectMetadata{found->first, found->second.bytes.size(), found->second.checksum};
}

common::Result<std::vector<std::byte>>
MemoryObjectStore::get_range(const std::string_view key, const std::size_t offset,
                             const std::size_t length) const {
  std::scoped_lock lock{impl_->mutex};
  const auto found = impl_->objects.find(key);
  if (found == impl_->objects.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "object does not exist"});
  }
  if (offset > found->second.bytes.size() || length > found->second.bytes.size() - offset) {
    return common::make_unexpected(invalid("object range is outside immutable content"));
  }
  return std::vector<std::byte>{found->second.bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                                found->second.bytes.begin() +
                                    static_cast<std::ptrdiff_t>(offset + length)};
}

std::size_t MemoryObjectStore::object_count() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->objects.size();
}

common::Result<ObjectDeletionReport>
MemoryObjectStore::remove_if_exact(const std::string_view key, const std::size_t expected_size,
                                   const ingest::Sha256Digest& expected_checksum) {
  if (key.empty())
    return common::make_unexpected(invalid("object key must be nonempty"));
  std::scoped_lock lock{impl_->mutex};
  const auto found = impl_->objects.find(key);
  if (found == impl_->objects.end())
    return ObjectDeletionReport{.already_absent = true};
  if (found->second.bytes.size() != expected_size || found->second.checksum != expected_checksum) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kAlreadyExists,
                       "object key does not contain the expected immutable content"});
  }
  impl_->objects.erase(found);
  return ObjectDeletionReport{.removed = true};
}

class S3ObjectStore::Impl {
public:
  enum class Method : std::uint8_t { kPut, kHead, kGetRange, kDelete };

  struct Request {
    Method method{};
    std::string_view key;
    common::ByteView upload;
    std::optional<ingest::Sha256Digest> checksum;
    std::size_t range_offset{};
    std::size_t range_length{};
    std::size_t maximum_body_bytes{};
    std::string_view match_validator;
  };

  struct Response {
    long status{};
    curl_off_t content_length{-1};
    ResponseCapture capture;
  };

  explicit Impl(S3ObjectStoreConfig configured) : config(std::move(configured)) {
    while (config.endpoint.size() > std::string_view{"https://"}.size() &&
           config.endpoint.back() == '/') {
      config.endpoint.pop_back();
    }
    signature = "aws:amz:" + config.region + ":s3";
  }

  [[nodiscard]] common::Result<S3Credentials>
  acquire_credentials(const S3CredentialRequest request) const {
    try {
      if (config.credential_provider != nullptr) {
        auto credentials = config.credential_provider->acquire(request);
        if (!credentials.has_value())
          return common::make_unexpected(credentials.error());
        if (!valid_credentials(*credentials)) {
          return common::make_unexpected(
              common::Status{common::StatusCode::kUnauthenticated,
                             "S3 credential provider returned invalid data"});
        }
        return credentials;
      }
      return S3Credentials{.access_key_id = config.access_key_id,
                           .secret_access_key = config.secret_access_key,
                           .session_token = config.session_token};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("S3 credential acquisition allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("S3 credential acquisition exceeded limits"));
    } catch (...) {
      return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                    "S3 credential provider raised an exception"});
    }
  }

  void wait_before_retry(const std::size_t completed_attempts) const {
    auto delay = config.initial_retry_backoff;
    for (std::size_t exponent = 1U; exponent < completed_attempts; ++exponent) {
      if (delay >= config.maximum_retry_backoff ||
          delay.count() > config.maximum_retry_backoff.count() / 2) {
        delay = config.maximum_retry_backoff;
        break;
      }
      delay *= 2;
    }
    if (delay > config.maximum_retry_backoff)
      delay = config.maximum_retry_backoff;
    if (delay.count() > 0)
      std::this_thread::sleep_for(delay);
  }

  [[nodiscard]] common::Result<Response> perform_once(const Request& request,
                                                      const S3Credentials& current) const {
    try {
      const std::string url = config.endpoint + "/" + encode_path(config.bucket, false) + "/" +
                              encode_path(request.key, true);
      const std::string credentials = current.access_key_id + ":" + current.secret_access_key;
      CurlHandle handle{curl_easy_init()};
      if (!handle)
        return common::make_unexpected(exhausted("S3 HTTP handle allocation failed"));

      Response response;
      response.capture.maximum_body_bytes = request.maximum_body_bytes;
      std::array<char, CURL_ERROR_SIZE> error_buffer{};
      auto set = [&](const CURLoption option, const auto value) -> common::Status {
        return curl_easy_setopt(handle.get(), option, value) == CURLE_OK
                   ? common::Status::ok()
                   : common::Status{common::StatusCode::kInternal,
                                    "S3 HTTP option configuration failed"};
      };
      common::Status configured = set(CURLOPT_URL, url.c_str());
      if (configured.is_ok())
        configured = set(CURLOPT_AWS_SIGV4, signature.c_str());
      if (configured.is_ok())
        configured = set(CURLOPT_USERPWD, credentials.c_str());
      if (configured.is_ok())
        configured =
            set(CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(config.connect_timeout.count()));
      if (configured.is_ok())
        configured = set(CURLOPT_TIMEOUT_MS, static_cast<long>(config.request_timeout.count()));
      if (configured.is_ok())
        configured = set(CURLOPT_NOSIGNAL, 1L);
      if (configured.is_ok())
        configured = set(CURLOPT_FOLLOWLOCATION, 0L);
      if (configured.is_ok())
        configured = set(CURLOPT_MAXREDIRS, 0L);
      if (configured.is_ok())
        configured = set(CURLOPT_PATH_AS_IS, 1L);
      if (configured.is_ok())
        configured = set(CURLOPT_TCP_KEEPALIVE, 1L);
      if (configured.is_ok())
        configured = set(CURLOPT_SSL_VERIFYPEER, config.endpoint.starts_with("https://") ? 1L : 0L);
      if (configured.is_ok())
        configured = set(CURLOPT_SSL_VERIFYHOST, config.endpoint.starts_with("https://") ? 2L : 0L);
      if (configured.is_ok())
        configured = set(CURLOPT_PROTOCOLS_STR, config.require_tls ? "https" : "http,https");
      if (configured.is_ok())
        configured = set(CURLOPT_HTTP_CONTENT_DECODING, 0L);
      if (configured.is_ok())
        configured = set(CURLOPT_WRITEFUNCTION, &capture_body);
      if (configured.is_ok())
        configured = set(CURLOPT_WRITEDATA, &response.capture);
      if (configured.is_ok())
        configured = set(CURLOPT_HEADERFUNCTION, &capture_header);
      if (configured.is_ok())
        configured = set(CURLOPT_HEADERDATA, &response.capture);
      if (configured.is_ok())
        configured = set(CURLOPT_ERRORBUFFER, error_buffer.data());
      if (configured.is_ok() && config.ca_bundle_path.has_value())
        configured = set(CURLOPT_CAINFO, config.ca_bundle_path->c_str());
      if (!configured.is_ok())
        return common::make_unexpected(configured);

      HeaderList headers;
      if (current.session_token.has_value()) {
        configured = append_header(headers, "x-amz-security-token: " + *current.session_token);
      }
      if (configured.is_ok() && request.method == Method::kPut) {
        const std::string hexadecimal_checksum = digest_hex(*request.checksum);
        configured = append_header(headers, "If-None-Match: *");
        if (configured.is_ok())
          configured = append_header(headers, "Expect:");
        if (configured.is_ok()) {
          configured = append_header(headers, "x-amz-content-sha256: " + hexadecimal_checksum);
        }
        if (configured.is_ok()) {
          configured =
              append_header(headers, "x-amz-checksum-sha256: " + digest_base64(*request.checksum));
        }
        if (configured.is_ok()) {
          configured = append_header(headers, "x-amz-meta-chronos-sha256: " + hexadecimal_checksum);
        }
      }
      std::string range_header;
      if (configured.is_ok() && request.method == Method::kGetRange) {
        range_header = "Range: bytes=" + std::to_string(request.range_offset) + "-" +
                       std::to_string(request.range_offset + request.range_length - 1U);
        configured = append_header(headers, range_header);
      }
      if (configured.is_ok() && request.method == Method::kDelete)
        configured = append_header(headers, "If-Match: " + std::string{request.match_validator});
      if (!configured.is_ok())
        return common::make_unexpected(configured);
      if (headers)
        configured = set(CURLOPT_HTTPHEADER, headers.get());
      if (configured.is_ok() && request.method == Method::kHead)
        configured = set(CURLOPT_NOBODY, 1L);
      if (configured.is_ok() && request.method == Method::kPut) {
        configured = set(CURLOPT_CUSTOMREQUEST, "PUT");
        if (configured.is_ok()) {
          const void* data =
              request.upload.empty() ? static_cast<const void*>("") : request.upload.data();
          configured = set(CURLOPT_POSTFIELDS, data);
        }
        if (configured.is_ok()) {
          configured =
              set(CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(request.upload.size()));
        }
      }
      if (configured.is_ok() && request.method == Method::kDelete)
        configured = set(CURLOPT_CUSTOMREQUEST, "DELETE");
      if (!configured.is_ok())
        return common::make_unexpected(configured);

      const CURLcode performed = curl_easy_perform(handle.get());
      if (performed != CURLE_OK) {
        return common::make_unexpected(
            curl_failure(performed, response.capture.body_limit_exhausted));
      }
      if (curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &response.status) != CURLE_OK ||
          curl_easy_getinfo(handle.get(), CURLINFO_CONTENT_LENGTH_DOWNLOAD_T,
                            &response.content_length) != CURLE_OK) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kIoError, "S3 response metadata is unavailable"});
      }
      if (response.capture.checksum_conflict || response.capture.content_range_conflict ||
          response.capture.entity_tag_conflict) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kCorruption, "S3 response metadata conflicts"});
      }
      return response;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("S3 request allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("S3 request exceeds container limits"));
    }
  }

  [[nodiscard]] common::Result<Response> perform(const Request& request) const {
    bool refresh_credentials{};
    for (std::size_t attempt = 1U; attempt <= config.maximum_attempts; ++attempt) {
      auto current = acquire_credentials(refresh_credentials ? S3CredentialRequest::kRefresh
                                                             : S3CredentialRequest::kCurrent);
      if (!current.has_value()) {
        if (current.error().code() != common::StatusCode::kUnavailable ||
            attempt == config.maximum_attempts) {
          return common::make_unexpected(current.error());
        }
        wait_before_retry(attempt);
        continue;
      }
      auto response = perform_once(request, *current);
      if (!response.has_value()) {
        if (response.error().code() != common::StatusCode::kUnavailable ||
            attempt == config.maximum_attempts) {
          return common::make_unexpected(response.error());
        }
        refresh_credentials = false;
        wait_before_retry(attempt);
        continue;
      }
      const bool authorization_rejected = response->status == 401L || response->status == 403L;
      if (attempt == config.maximum_attempts ||
          (!retryable_http_status(response->status) &&
           !(authorization_rejected && config.credential_provider != nullptr))) {
        return response;
      }
      refresh_credentials = authorization_rejected;
      wait_before_retry(attempt);
    }
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "S3 retry state is unreachable"});
  }

  S3ObjectStoreConfig config;
  std::string signature;
};

S3ObjectStore::S3ObjectStore(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
S3ObjectStore::~S3ObjectStore() = default;

common::Result<std::unique_ptr<S3ObjectStore>> S3ObjectStore::create(S3ObjectStoreConfig config) {
  const bool https = config.endpoint.starts_with("https://");
  const bool http = config.endpoint.starts_with("http://");
  const std::size_t scheme_length =
      https ? std::string_view{"https://"}.size() : std::string_view{"http://"}.size();
  const auto maximum_long = std::chrono::milliseconds{std::numeric_limits<long>::max()};
  const bool static_credentials = !config.access_key_id.empty() ||
                                  !config.secret_access_key.empty() ||
                                  config.session_token.has_value();
  if ((!https && !http) || (config.require_tls && !https) || config.endpoint.contains('?') ||
      config.endpoint.contains('#') || config.endpoint.contains('@') ||
      config.endpoint.size() <= scheme_length || config.endpoint[scheme_length] == '/' ||
      config.endpoint.size() > 4096U || contains_control(config.endpoint) ||
      contains_space(config.endpoint) || config.region.empty() || config.region.size() > 64U ||
      contains_control(config.region) || contains_space(config.region) ||
      config.region.contains(':') || !valid_bucket(config.bucket) ||
      (config.credential_provider == nullptr &&
       !valid_credentials({.access_key_id = config.access_key_id,
                           .secret_access_key = config.secret_access_key,
                           .session_token = config.session_token})) ||
      (config.credential_provider != nullptr && static_credentials) ||
      (config.ca_bundle_path.has_value() &&
       (config.ca_bundle_path->empty() || config.ca_bundle_path->size() > 4096U)) ||
      config.connect_timeout.count() <= 0 || config.connect_timeout > maximum_long ||
      config.request_timeout.count() <= 0 || config.request_timeout > maximum_long ||
      config.maximum_attempts == 0U || config.maximum_attempts > 32U ||
      config.initial_retry_backoff.count() < 0 || config.initial_retry_backoff > maximum_long ||
      config.maximum_retry_backoff.count() < 0 || config.maximum_retry_backoff > maximum_long ||
      config.initial_retry_backoff > config.maximum_retry_backoff ||
      config.maximum_response_bytes == 0U ||
      static_cast<std::uintmax_t>(config.maximum_response_bytes) >
          static_cast<std::uintmax_t>(std::numeric_limits<curl_off_t>::max())) {
    return common::make_unexpected(invalid("S3 object-store configuration is invalid"));
  }
  if (initialize_curl() != CURLE_OK) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "libcurl global initialization failed"});
  }
  try {
    return std::unique_ptr<S3ObjectStore>{
        new S3ObjectStore{std::make_unique<Impl>(std::move(config))}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("S3 object-store allocation failed"));
  }
}

common::Result<ObjectMetadata> S3ObjectStore::put_if_absent(const std::string_view key,
                                                            const common::ByteView bytes,
                                                            const ingest::Sha256Digest& checksum) {
  if (key.empty() || key.size() > 1024U || contains_control(key) ||
      bytes.size() > impl_->config.maximum_response_bytes) {
    return common::make_unexpected(invalid("S3 object key or upload size is invalid"));
  }
  auto actual = ingest::sha256(bytes);
  if (!actual.has_value())
    return common::make_unexpected(actual.error());
  if (*actual != checksum)
    return common::make_unexpected(invalid("object checksum does not match upload bytes"));
  auto response = impl_->perform({.method = Impl::Method::kPut,
                                  .key = key,
                                  .upload = bytes,
                                  .checksum = checksum,
                                  .maximum_body_bytes = 64U * 1024U});
  if (!response.has_value())
    return common::make_unexpected(response.error());
  if (response->status == 412L) {
    auto existing = stat(key);
    if (!existing.has_value()) {
      if (existing.error().code() == common::StatusCode::kNotFound) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kUnavailable,
                           "S3 conditional-write result changed before verification"});
      }
      return common::make_unexpected(existing.error());
    }
    if (existing->size != bytes.size() || existing->checksum != checksum) {
      return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                    "immutable S3 key has different content"});
    }
    return *existing;
  }
  if (response->status != 200L && response->status != 201L)
    return common::make_unexpected(http_failure(response->status));
  return ObjectMetadata{std::string{key}, bytes.size(), checksum};
}

common::Result<ObjectMetadata> S3ObjectStore::stat(const std::string_view key) const {
  if (key.empty() || key.size() > 1024U || contains_control(key))
    return common::make_unexpected(invalid("S3 object key is invalid"));
  auto response = impl_->perform(
      {.method = Impl::Method::kHead, .key = key, .maximum_body_bytes = 64U * 1024U});
  if (!response.has_value())
    return common::make_unexpected(response.error());
  if (response->status != 200L)
    return common::make_unexpected(http_failure(response->status));
  if (response->content_length < 0 ||
      static_cast<std::uintmax_t>(response->content_length) >
          std::numeric_limits<std::size_t>::max() ||
      !response->capture.checksum_hex.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "S3 object metadata is incomplete or unaddressable"});
  }
  auto checksum = parse_digest_hex(*response->capture.checksum_hex);
  if (!checksum.has_value())
    return common::make_unexpected(checksum.error());
  return ObjectMetadata{std::string{key}, static_cast<std::size_t>(response->content_length),
                        *checksum};
}

common::Result<std::vector<std::byte>> S3ObjectStore::get_range(const std::string_view key,
                                                                const std::size_t offset,
                                                                const std::size_t length) const {
  if (key.empty() || key.size() > 1024U || contains_control(key) ||
      length > impl_->config.maximum_response_bytes ||
      length > std::numeric_limits<std::size_t>::max() - offset) {
    return common::make_unexpected(invalid("S3 object key or range is invalid"));
  }
  if (length == 0U) {
    auto metadata = stat(key);
    if (!metadata.has_value())
      return common::make_unexpected(metadata.error());
    if (offset > metadata->size)
      return common::make_unexpected(invalid("S3 object range is outside immutable content"));
    return std::vector<std::byte>{};
  }
  auto response = impl_->perform({.method = Impl::Method::kGetRange,
                                  .key = key,
                                  .range_offset = offset,
                                  .range_length = length,
                                  .maximum_body_bytes = length});
  if (!response.has_value())
    return common::make_unexpected(response.error());
  if (response->status != 206L)
    return common::make_unexpected(http_failure(response->status));
  if (!response->capture.content_range.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "S3 content range is absent"});
  }
  auto content_range = parse_content_range(*response->capture.content_range);
  if (!content_range.has_value())
    return common::make_unexpected(content_range.error());
  if (content_range->first != offset || content_range->last != offset + length - 1U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "S3 content range differs from request"});
  }
  if (response->capture.body.size() != length) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "S3 range response length differs"});
  }
  return std::move(response->capture.body);
}

common::Result<ObjectDeletionReport>
S3ObjectStore::remove_if_exact(const std::string_view key, const std::size_t expected_size,
                               const ingest::Sha256Digest& expected_checksum) {
  if (key.empty() || key.size() > 1024U || contains_control(key))
    return common::make_unexpected(invalid("S3 object key is invalid"));
  auto head = impl_->perform(
      {.method = Impl::Method::kHead, .key = key, .maximum_body_bytes = 64U * 1024U});
  if (!head.has_value())
    return common::make_unexpected(head.error());
  if (head->status == 404L)
    return ObjectDeletionReport{.already_absent = true};
  if (head->status != 200L)
    return common::make_unexpected(http_failure(head->status));
  if (head->content_length < 0 ||
      static_cast<std::uintmax_t>(head->content_length) > std::numeric_limits<std::size_t>::max() ||
      !head->capture.checksum_hex.has_value() || !head->capture.entity_tag.has_value() ||
      head->capture.entity_tag->empty() || head->capture.entity_tag->size() > 1024U ||
      contains_control(*head->capture.entity_tag)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption,
                       "S3 object deletion metadata is incomplete or unaddressable"});
  }
  auto checksum = parse_digest_hex(*head->capture.checksum_hex);
  if (!checksum.has_value())
    return common::make_unexpected(checksum.error());
  if (static_cast<std::size_t>(head->content_length) != expected_size ||
      *checksum != expected_checksum) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kAlreadyExists,
                       "S3 key does not contain the expected immutable content"});
  }
  auto removed = impl_->perform({.method = Impl::Method::kDelete,
                                 .key = key,
                                 .maximum_body_bytes = 64U * 1024U,
                                 .match_validator = *head->capture.entity_tag});
  if (!removed.has_value())
    return common::make_unexpected(removed.error());
  if (removed->status == 404L)
    return ObjectDeletionReport{.already_absent = true};
  if (removed->status == 412L) {
    return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                  "S3 object changed before conditional deletion"});
  }
  if (removed->status != 200L && removed->status != 204L)
    return common::make_unexpected(http_failure(removed->status));
  return ObjectDeletionReport{.removed = true};
}

} // namespace chronos::tiering
