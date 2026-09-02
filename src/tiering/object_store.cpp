#include "chronos/tiering/object_store.hpp"

#include "chronos/ingest/sha256.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <curl/curl.h>
#include <exception>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {
constexpr std::size_t kSmallResponseLimit = std::size_t{64U} * 1024U;
constexpr std::size_t kCredentialResponseLimit = std::size_t{1024U} * 1024U;
constexpr std::size_t kMinimumMultipartPartBytes = std::size_t{5U} * 1024U * 1024U;

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return common::Status{common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] std::string_view byte_string_view(const common::ByteView bytes) noexcept {
  if (bytes.empty())
    return {};
  // Character types may inspect any object's representation; keep that necessary cast isolated.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

[[nodiscard]] common::ByteView string_byte_view(const std::string_view value) noexcept {
  return std::as_bytes(std::span{value.data(), value.size()});
}

class JoiningThreads final {
public:
  JoiningThreads() = default;
  JoiningThreads(const JoiningThreads&) = delete;
  JoiningThreads& operator=(const JoiningThreads&) = delete;

  ~JoiningThreads() noexcept {
    for (auto& thread : threads_) {
      if (!thread.joinable())
        continue;
      try {
        thread.join();
      } catch (...) {
        std::terminate();
      }
    }
  }

  [[nodiscard]] std::vector<std::thread>& threads() noexcept {
    return threads_;
  }

private:
  std::vector<std::thread> threads_;
};

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
  std::optional<std::string> server_side_encryption;
  std::optional<std::string> kms_key_id;
  std::optional<std::string> retry_after;
  bool checksum_conflict{};
  bool content_range_conflict{};
  bool entity_tag_conflict{};
  bool server_side_encryption_conflict{};
  bool kms_key_id_conflict{};
  bool retry_after_invalid{};
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

[[nodiscard]] std::optional<unsigned> month_number(const std::string_view month) noexcept {
  constexpr std::array<std::string_view, 12U> months{"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  const auto* const found = std::ranges::find(months, month);
  if (found == months.end())
    return std::nullopt;
  return static_cast<unsigned>(std::distance(months.begin(), found) + 1);
}

[[nodiscard]] std::optional<unsigned> weekday_number(const std::string_view weekday) noexcept {
  constexpr std::array<std::string_view, 7U> short_names{"Sun", "Mon", "Tue", "Wed",
                                                         "Thu", "Fri", "Sat"};
  constexpr std::array<std::string_view, 7U> long_names{
      "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
  const auto* found = std::ranges::find(short_names, weekday);
  if (found != short_names.end())
    return static_cast<unsigned>(std::distance(short_names.begin(), found));
  found = std::ranges::find(long_names, weekday);
  if (found == long_names.end())
    return std::nullopt;
  return static_cast<unsigned>(std::distance(long_names.begin(), found));
}

[[nodiscard]] bool parse_decimal_exact(const std::string_view encoded, unsigned& value) noexcept {
  if (encoded.empty())
    return false;
  const auto parsed = std::from_chars(encoded.data(), encoded.data() + encoded.size(), value);
  return parsed.ec == std::errc{} && parsed.ptr == encoded.data() + encoded.size();
}

struct HttpDateFields {
  unsigned weekday{};
  int year{};
  unsigned month{};
  unsigned day{};
  unsigned hour{};
  unsigned minute{};
  unsigned second{};
};

[[nodiscard]] bool valid_http_date(const HttpDateFields& fields) noexcept {
  using namespace std::chrono;
  const year_month_day date{year{fields.year}, month{fields.month}, day{fields.day}};
  return date.ok() && fields.hour < 24U && fields.minute < 60U && fields.second < 60U &&
         weekday{sys_days{date}}.c_encoding() == fields.weekday;
}

[[nodiscard]] std::optional<std::uint64_t> http_date_epoch_seconds(const std::string_view encoded) {
  HttpDateFields fields;
  unsigned parsed_year{};
  if (encoded.size() == 29U && encoded[3] == ',' && encoded[4] == ' ' && encoded[7] == ' ' &&
      encoded[11] == ' ' && encoded[16] == ' ' && encoded[19] == ':' && encoded[22] == ':' &&
      encoded.substr(25U) == " GMT") {
    auto weekday = weekday_number(encoded.substr(0U, 3U));
    auto month = month_number(encoded.substr(8U, 3U));
    if (!weekday.has_value() || !month.has_value() ||
        !parse_decimal_exact(encoded.substr(5U, 2U), fields.day) ||
        !parse_decimal_exact(encoded.substr(12U, 4U), parsed_year) ||
        !parse_decimal_exact(encoded.substr(17U, 2U), fields.hour) ||
        !parse_decimal_exact(encoded.substr(20U, 2U), fields.minute) ||
        !parse_decimal_exact(encoded.substr(23U, 2U), fields.second)) {
      return std::nullopt;
    }
    fields.weekday = *weekday;
    fields.month = *month;
    fields.year = static_cast<int>(parsed_year);
  } else if (encoded.size() >= 30U && encoded.size() <= 33U) {
    const std::size_t comma = encoded.find(',');
    if (comma < 6U || comma > 9U || encoded.size() != comma + 24U || encoded[comma + 1U] != ' ' ||
        encoded[comma + 4U] != '-' || encoded[comma + 8U] != '-' || encoded[comma + 11U] != ' ' ||
        encoded[comma + 14U] != ':' || encoded[comma + 17U] != ':' ||
        encoded.substr(comma + 20U) != " GMT") {
      return std::nullopt;
    }
    auto weekday = weekday_number(encoded.substr(0U, comma));
    auto month = month_number(encoded.substr(comma + 5U, 3U));
    if (!weekday.has_value() || !month.has_value() ||
        !parse_decimal_exact(encoded.substr(comma + 2U, 2U), fields.day) ||
        !parse_decimal_exact(encoded.substr(comma + 9U, 2U), parsed_year) ||
        !parse_decimal_exact(encoded.substr(comma + 12U, 2U), fields.hour) ||
        !parse_decimal_exact(encoded.substr(comma + 15U, 2U), fields.minute) ||
        !parse_decimal_exact(encoded.substr(comma + 18U, 2U), fields.second)) {
      return std::nullopt;
    }
    fields.weekday = *weekday;
    fields.month = *month;
    const int current_year = static_cast<int>(std::chrono::year_month_day{
        std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now())}
                                                  .year());
    fields.year = (current_year / 100) * 100 + static_cast<int>(parsed_year);
    if (fields.year > current_year + 50)
      fields.year -= 100;
  } else if (encoded.size() == 24U && encoded[3] == ' ' && encoded[7] == ' ' &&
             encoded[10] == ' ' && encoded[13] == ':' && encoded[16] == ':' && encoded[19] == ' ') {
    auto weekday = weekday_number(encoded.substr(0U, 3U));
    auto month = month_number(encoded.substr(4U, 3U));
    std::string_view day = encoded.substr(8U, 2U);
    if (day.front() == ' ')
      day.remove_prefix(1U);
    if (!weekday.has_value() || !month.has_value() || !parse_decimal_exact(day, fields.day) ||
        !parse_decimal_exact(encoded.substr(20U, 4U), parsed_year) ||
        !parse_decimal_exact(encoded.substr(11U, 2U), fields.hour) ||
        !parse_decimal_exact(encoded.substr(14U, 2U), fields.minute) ||
        !parse_decimal_exact(encoded.substr(17U, 2U), fields.second)) {
      return std::nullopt;
    }
    fields.weekday = *weekday;
    fields.month = *month;
    fields.year = static_cast<int>(parsed_year);
  } else {
    return std::nullopt;
  }
  if (!valid_http_date(fields))
    return std::nullopt;
  using namespace std::chrono;
  const sys_seconds instant = sys_days{year{fields.year} / month{fields.month} / day{fields.day}} +
                              hours{fields.hour} + minutes{fields.minute} + seconds{fields.second};
  const auto count = instant.time_since_epoch().count();
  return count < 0 ? std::nullopt : std::optional<std::uint64_t>{static_cast<std::uint64_t>(count)};
}

[[nodiscard]] std::optional<std::uint64_t>
retry_after_delay_seconds(const std::string_view encoded) {
  std::uint64_t delta{};
  const auto parsed = std::from_chars(encoded.data(), encoded.data() + encoded.size(), delta);
  if (!encoded.empty() && parsed.ec == std::errc{} && parsed.ptr == encoded.data() + encoded.size())
    return delta;
  auto target = http_date_epoch_seconds(encoded);
  if (!target.has_value())
    return std::nullopt;
  const auto current = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
  if (current < 0 || *target <= static_cast<std::uint64_t>(current))
    return 0U;
  return *target - static_cast<std::uint64_t>(current);
}

// libcurl's write callback ABI requires a mutable char pointer even though this callback only reads
// the received bytes.
// NOLINTNEXTLINE(readability-non-const-parameter)
[[nodiscard]] std::size_t capture_body(char* data, const std::size_t size, const std::size_t count,
                                       void* context) noexcept {
  auto& capture = *static_cast<ResponseCapture*>(context);
  if (count != 0U && size > std::numeric_limits<std::size_t>::max() / count) {
    capture.body_limit_exhausted = true;
    return 0U;
  }
  const std::size_t length = size * count;
  if (capture.body.size() > capture.maximum_body_bytes ||
      length > capture.maximum_body_bytes - capture.body.size()) {
    capture.body_limit_exhausted = true;
    return 0U;
  }
  try {
    const common::ByteView bytes = string_byte_view({data, length});
    capture.body.insert(capture.body.end(), bytes.begin(), bytes.end());
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
  constexpr std::string_view server_side_encryption_name{"x-amz-server-side-encryption:"};
  constexpr std::string_view kms_key_id_name{"x-amz-server-side-encryption-aws-kms-key-id:"};
  constexpr std::string_view retry_after_name{"retry-after:"};
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
    } else if (matches_name(kms_key_id_name)) {
      const std::string value{trim_header_value(line.substr(kms_key_id_name.size()))};
      if (capture.kms_key_id.has_value())
        capture.kms_key_id_conflict = true;
      else
        capture.kms_key_id = value;
    } else if (matches_name(server_side_encryption_name)) {
      const std::string value{trim_header_value(line.substr(server_side_encryption_name.size()))};
      if (capture.server_side_encryption.has_value())
        capture.server_side_encryption_conflict = true;
      else
        capture.server_side_encryption = value;
    } else if (matches_name(retry_after_name)) {
      const std::string_view value = trim_header_value(line.substr(retry_after_name.size()));
      if (capture.retry_after.has_value() || value.empty() || value.size() > 64U ||
          contains_control(value)) {
        capture.retry_after_invalid = true;
        capture.retry_after.reset();
      } else if (!capture.retry_after_invalid) {
        capture.retry_after = value;
      }
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

[[nodiscard]] common::Result<std::optional<std::string>>
copy_environment_value(const char* name, const std::size_t maximum_length) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0')
    return std::optional<std::string>{};
  std::size_t length{};
  while (length <= maximum_length && value[length] != '\0')
    ++length;
  if (length > maximum_length) {
    return common::make_unexpected(common::Status{common::StatusCode::kUnauthenticated,
                                                  "AWS environment credential exceeds its bound"});
  }
  return std::optional<std::string>{std::in_place, value, length};
}

[[nodiscard]] common::Result<std::string> extract_xml_element(const common::ByteView bytes,
                                                              const std::string_view element,
                                                              const std::size_t maximum_length) {
  try {
    const std::string_view xml = byte_string_view(bytes);
    const std::string opening = "<" + std::string{element} + ">";
    const std::string closing = "</" + std::string{element} + ">";
    const std::size_t opening_offset = xml.find(opening);
    if (opening_offset == std::string_view::npos ||
        xml.find(opening, opening_offset + opening.size()) != std::string_view::npos) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kCorruption, "S3 XML response element is absent or repeated"});
    }
    const std::size_t value_offset = opening_offset + opening.size();
    const std::size_t closing_offset = xml.find(closing, value_offset);
    if (closing_offset == std::string_view::npos ||
        xml.find(closing, closing_offset + closing.size()) != std::string_view::npos) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kCorruption, "S3 XML response element is incomplete"});
    }
    const std::string_view encoded = xml.substr(value_offset, closing_offset - value_offset);
    std::string decoded;
    decoded.reserve(encoded.size());
    for (std::size_t index = 0U; index < encoded.size();) {
      if (encoded[index] != '&') {
        decoded.push_back(encoded[index++]);
        continue;
      }
      const std::size_t semicolon = encoded.find(';', index + 1U);
      if (semicolon == std::string_view::npos) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kCorruption, "S3 XML response contains an invalid entity"});
      }
      const std::string_view entity = encoded.substr(index, semicolon - index + 1U);
      if (entity == "&amp;")
        decoded.push_back('&');
      else if (entity == "&lt;")
        decoded.push_back('<');
      else if (entity == "&gt;")
        decoded.push_back('>');
      else if (entity == "&quot;")
        decoded.push_back('"');
      else if (entity == "&apos;")
        decoded.push_back('\'');
      else
        return common::make_unexpected(common::Status{
            common::StatusCode::kCorruption, "S3 XML response contains an unsupported entity"});
      index = semicolon + 1U;
    }
    if (decoded.empty() || decoded.size() > maximum_length || contains_control(decoded)) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kCorruption, "S3 XML response element is invalid"});
    }
    return decoded;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("S3 XML response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("S3 XML response exceeds limits"));
  }
}

[[nodiscard]] bool is_complete_multipart_result(const common::ByteView bytes) noexcept {
  std::string_view xml = byte_string_view(bytes);
  const auto trim_leading = [&xml] {
    while (!xml.empty() && (xml.front() == ' ' || xml.front() == '\t' || xml.front() == '\r' ||
                            xml.front() == '\n')) {
      xml.remove_prefix(1U);
    }
  };
  trim_leading();
  if (xml.starts_with("<?xml")) {
    const std::size_t declaration_end = xml.find("?>");
    if (declaration_end == std::string_view::npos)
      return false;
    xml.remove_prefix(declaration_end + 2U);
    trim_leading();
  }
  constexpr std::string_view opening{"<CompleteMultipartUploadResult"};
  constexpr std::string_view closing{"</CompleteMultipartUploadResult>"};
  if (!xml.starts_with(opening) || xml.size() == opening.size() ||
      (xml[opening.size()] != '>' && xml[opening.size()] != ' ' && xml[opening.size()] != '\t' &&
       xml[opening.size()] != '\r' && xml[opening.size()] != '\n') ||
      xml.find("<Error") != std::string_view::npos) {
    return false;
  }
  const std::size_t closing_offset = xml.find(closing, opening.size());
  if (closing_offset == std::string_view::npos ||
      xml.find(closing, closing_offset + closing.size()) != std::string_view::npos) {
    return false;
  }
  xml.remove_prefix(closing_offset + closing.size());
  return std::ranges::all_of(xml, [](const char value) {
    return value == ' ' || value == '\t' || value == '\r' || value == '\n';
  });
}

[[nodiscard]] common::Result<std::string>
complete_multipart_xml(const std::span<const std::string> entity_tags) {
  try {
    std::string xml{"<CompleteMultipartUpload>"};
    for (std::size_t index = 0U; index < entity_tags.size(); ++index) {
      xml += "<Part><PartNumber>" + std::to_string(index + 1U) + "</PartNumber><ETag>";
      for (const char value : entity_tags[index]) {
        switch (value) {
        case '&':
          xml += "&amp;";
          break;
        case '<':
          xml += "&lt;";
          break;
        case '>':
          xml += "&gt;";
          break;
        default:
          xml.push_back(value);
          break;
        }
      }
      xml += "</ETag></Part>";
    }
    xml += "</CompleteMultipartUpload>";
    return xml;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("S3 multipart completion allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("S3 multipart completion exceeds limits"));
  }
}

[[nodiscard]] common::Status append_header(HeaderList& headers, const std::string& header) {
  curl_slist* appended = curl_slist_append(headers.get(), header.c_str());
  if (appended == nullptr)
    return exhausted("S3 request header allocation failed");
  [[maybe_unused]] curl_slist* const transferred = headers.release();
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

S3EnvironmentCredentialProvider::S3EnvironmentCredentialProvider(S3Credentials credentials) noexcept
    : credentials_(std::move(credentials)) {}

common::Result<std::shared_ptr<S3EnvironmentCredentialProvider>>
S3EnvironmentCredentialProvider::create() {
  try {
    auto access_key_id = copy_environment_value("AWS_ACCESS_KEY_ID", 1024U);
    auto secret_access_key = copy_environment_value("AWS_SECRET_ACCESS_KEY", 4096U);
    auto session_token = copy_environment_value("AWS_SESSION_TOKEN", 8192U);
    if (!access_key_id.has_value())
      return common::make_unexpected(access_key_id.error());
    if (!secret_access_key.has_value())
      return common::make_unexpected(secret_access_key.error());
    if (!session_token.has_value())
      return common::make_unexpected(session_token.error());
    S3Credentials credentials{.access_key_id = access_key_id->value_or(std::string{}),
                              .secret_access_key = secret_access_key->value_or(std::string{}),
                              .session_token = std::move(*session_token)};
    if (!valid_credentials(credentials)) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnauthenticated,
                         "AWS environment credentials are missing, incomplete, or invalid"});
    }
    return std::shared_ptr<S3EnvironmentCredentialProvider>{
        new S3EnvironmentCredentialProvider{std::move(credentials)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("AWS environment credential allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("AWS environment credentials exceed limits"));
  }
}

common::Result<S3Credentials>
S3EnvironmentCredentialProvider::acquire(const S3CredentialRequest request) {
  if (request == S3CredentialRequest::kRefresh) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnauthenticated,
        "AWS environment credentials cannot refresh; recreate the provider after rotation"});
  }
  if (request != S3CredentialRequest::kCurrent)
    return common::make_unexpected(invalid("S3 credential request is invalid"));
  try {
    return credentials_;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("AWS environment credential copy allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("AWS environment credential copy exceeds limits"));
  }
}

class S3CredentialProviderChain::Impl {
public:
  explicit Impl(std::vector<std::shared_ptr<S3CredentialProvider>> configured) noexcept
      : providers(std::move(configured)) {}

  std::mutex mutex;
  std::vector<std::shared_ptr<S3CredentialProvider>> providers;
  std::optional<std::size_t> selected;
};

S3CredentialProviderChain::S3CredentialProviderChain(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
S3CredentialProviderChain::~S3CredentialProviderChain() = default;

common::Result<std::shared_ptr<S3CredentialProviderChain>>
S3CredentialProviderChain::create(std::vector<std::shared_ptr<S3CredentialProvider>> providers) {
  if (providers.empty() || providers.size() > 32U ||
      std::ranges::any_of(providers, [](const auto& provider) { return provider == nullptr; })) {
    return common::make_unexpected(invalid("S3 credential provider chain is invalid"));
  }
  try {
    return std::shared_ptr<S3CredentialProviderChain>{
        new S3CredentialProviderChain{std::make_unique<Impl>(std::move(providers))}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("S3 credential provider chain allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("S3 credential provider chain exceeds limits"));
  }
}

common::Result<S3Credentials>
S3CredentialProviderChain::acquire(const S3CredentialRequest request) {
  if (request != S3CredentialRequest::kCurrent && request != S3CredentialRequest::kRefresh)
    return common::make_unexpected(invalid("S3 credential request is invalid"));
  std::scoped_lock lock{impl_->mutex};
  const auto& selected = impl_->selected;
  if (selected.has_value())
    return impl_->providers[*selected]->acquire(request);
  if (request == S3CredentialRequest::kRefresh) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnauthenticated,
                       "S3 credential provider chain cannot refresh before identity selection"});
  }
  for (std::size_t index = 0U; index < impl_->providers.size(); ++index) {
    auto credentials = impl_->providers[index]->acquire(S3CredentialRequest::kCurrent);
    if (credentials.has_value()) {
      impl_->selected = index;
      return credentials;
    }
    if (credentials.error().code() != common::StatusCode::kNotFound)
      return common::make_unexpected(credentials.error());
  }
  return common::make_unexpected(common::Status{common::StatusCode::kUnauthenticated,
                                                "S3 credential provider chain found no identity"});
}

struct JsonStringField {
  std::string_view name;
  std::size_t maximum_length{};
};

[[nodiscard]] common::Result<std::string> extract_json_string_field(const std::string_view json,
                                                                    const JsonStringField field) {
  try {
    const std::string needle = "\"" + std::string{field.name} + "\"";
    const std::size_t field_offset = json.find(needle);
    if (field_offset == std::string_view::npos ||
        json.find(needle, field_offset + needle.size()) != std::string_view::npos) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnauthenticated, "container credential response field is absent"});
    }
    std::size_t cursor = field_offset + needle.size();
    while (cursor < json.size() && (json[cursor] == ' ' || json[cursor] == '\t' ||
                                    json[cursor] == '\r' || json[cursor] == '\n'))
      ++cursor;
    if (cursor == json.size() || json[cursor] != ':') {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnauthenticated, "container credential response field is invalid"});
    }
    ++cursor;
    while (cursor < json.size() && (json[cursor] == ' ' || json[cursor] == '\t' ||
                                    json[cursor] == '\r' || json[cursor] == '\n'))
      ++cursor;
    if (cursor == json.size() || json[cursor] != '"') {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnauthenticated, "container credential response value is invalid"});
    }
    ++cursor;
    std::string value;
    value.reserve(std::min(field.maximum_length, json.size() - cursor));
    while (cursor < json.size() && json[cursor] != '"') {
      const unsigned char byte = static_cast<unsigned char>(json[cursor++]);
      if (byte < 0x20U || byte == '\\') {
        return common::make_unexpected(
            common::Status{common::StatusCode::kUnauthenticated,
                           "container credential response value uses unsupported escaping"});
      }
      if (value.size() == field.maximum_length) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kUnauthenticated,
                           "container credential response value is oversized"});
      }
      value.push_back(static_cast<char>(byte));
    }
    if (cursor == json.size() || value.empty()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnauthenticated,
                         "container credential response value is incomplete"});
    }
    return value;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("container credential response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("container credential response exceeds limits"));
  }
}

[[nodiscard]] std::optional<std::chrono::sys_seconds>
parse_utc_expiration(const std::string_view encoded) noexcept {
  if (encoded.size() != 20U || encoded[4] != '-' || encoded[7] != '-' || encoded[10] != 'T' ||
      encoded[13] != ':' || encoded[16] != ':' || encoded[19] != 'Z')
    return std::nullopt;
  unsigned year_value{};
  HttpDateFields fields;
  if (!parse_decimal_exact(encoded.substr(0U, 4U), year_value) ||
      !parse_decimal_exact(encoded.substr(5U, 2U), fields.month) ||
      !parse_decimal_exact(encoded.substr(8U, 2U), fields.day) ||
      !parse_decimal_exact(encoded.substr(11U, 2U), fields.hour) ||
      !parse_decimal_exact(encoded.substr(14U, 2U), fields.minute) ||
      !parse_decimal_exact(encoded.substr(17U, 2U), fields.second))
    return std::nullopt;
  fields.year = static_cast<int>(year_value);
  using namespace std::chrono;
  const year_month_day date{year{fields.year}, month{fields.month}, day{fields.day}};
  if (!date.ok() || fields.hour >= 24U || fields.minute >= 60U || fields.second >= 60U)
    return std::nullopt;
  return sys_days{date} + hours{fields.hour} + minutes{fields.minute} + seconds{fields.second};
}

class S3ContainerCredentialProvider::Impl {
public:
  explicit Impl(S3ContainerCredentialProviderConfig configured) : config(std::move(configured)) {}

  struct Cached {
    S3Credentials credentials;
    std::chrono::sys_seconds expiration;
  };

  [[nodiscard]] common::Result<Cached> fetch() const {
    try {
      CurlHandle handle{curl_easy_init()};
      if (!handle)
        return common::make_unexpected(
            exhausted("container credential HTTP handle allocation failed"));
      ResponseCapture capture;
      capture.maximum_body_bytes = config.maximum_response_bytes;
      auto set = [&](const CURLoption option, const auto value) -> bool {
        return curl_easy_setopt(handle.get(), option, value) == CURLE_OK;
      };
      const bool https = config.endpoint.starts_with("https://");
      if (!set(CURLOPT_URL, config.endpoint.c_str()) || !set(CURLOPT_PROXY, "") ||
          !set(CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(config.connect_timeout.count())) ||
          !set(CURLOPT_TIMEOUT_MS, static_cast<long>(config.request_timeout.count())) ||
          !set(CURLOPT_NOSIGNAL, 1L) || !set(CURLOPT_FOLLOWLOCATION, 0L) ||
          !set(CURLOPT_MAXREDIRS, 0L) || !set(CURLOPT_PROTOCOLS_STR, https ? "https" : "http") ||
          !set(CURLOPT_HTTP_CONTENT_DECODING, 0L) ||
          !set(CURLOPT_SSL_VERIFYPEER, https ? 1L : 0L) ||
          !set(CURLOPT_SSL_VERIFYHOST, https ? 2L : 0L) ||
          !set(CURLOPT_WRITEFUNCTION, &capture_body) || !set(CURLOPT_WRITEDATA, &capture) ||
          (config.ca_bundle_path.has_value() &&
           !set(CURLOPT_CAINFO, config.ca_bundle_path->c_str()))) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kInternal, "container credential HTTP configuration failed"});
      }
      HeaderList headers;
      if (config.authorization_token.has_value()) {
        const common::Status appended =
            append_header(headers, "Authorization: " + *config.authorization_token);
        if (!appended.is_ok())
          return common::make_unexpected(appended);
        if (!set(CURLOPT_HTTPHEADER, headers.get())) {
          return common::make_unexpected(common::Status{
              common::StatusCode::kInternal, "container credential HTTP configuration failed"});
        }
      }
      const CURLcode performed = curl_easy_perform(handle.get());
      if (performed != CURLE_OK)
        return common::make_unexpected(curl_failure(performed, capture.body_limit_exhausted));
      long status{};
      if (curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &status) != CURLE_OK)
        return common::make_unexpected(common::Status{
            common::StatusCode::kIoError, "container credential response status is unavailable"});
      if (status == 401L || status == 403L || status == 404L) {
        return common::make_unexpected(common::Status{common::StatusCode::kUnauthenticated,
                                                      "container credential request was rejected"});
      }
      if (status != 200L)
        return common::make_unexpected(common::Status{
            common::StatusCode::kUnavailable, "container credential endpoint is unavailable"});
      const std::string_view json = byte_string_view(capture.body);
      auto access =
          extract_json_string_field(json, {.name = "AccessKeyId", .maximum_length = 1024U});
      auto secret =
          extract_json_string_field(json, {.name = "SecretAccessKey", .maximum_length = 4096U});
      auto token = extract_json_string_field(json, {.name = "Token", .maximum_length = 8192U});
      auto expiration =
          extract_json_string_field(json, {.name = "Expiration", .maximum_length = 64U});
      if (!access.has_value())
        return common::make_unexpected(access.error());
      if (!secret.has_value())
        return common::make_unexpected(secret.error());
      if (!token.has_value())
        return common::make_unexpected(token.error());
      if (!expiration.has_value())
        return common::make_unexpected(expiration.error());
      auto expires_at = parse_utc_expiration(*expiration);
      S3Credentials credentials{.access_key_id = std::move(*access),
                                .secret_access_key = std::move(*secret),
                                .session_token = std::move(*token)};
      if (!expires_at.has_value() || !valid_credentials(credentials) ||
          *expires_at <=
              std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now())) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kUnauthenticated,
                           "container credential response is expired or invalid"});
      }
      return Cached{.credentials = std::move(credentials), .expiration = *expires_at};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          exhausted("container credential acquisition allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("container credential acquisition exceeds limits"));
    }
  }

  S3ContainerCredentialProviderConfig config;
  mutable std::mutex mutex;
  std::optional<Cached> cached;
};

S3ContainerCredentialProvider::S3ContainerCredentialProvider(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
S3ContainerCredentialProvider::~S3ContainerCredentialProvider() = default;

common::Result<std::shared_ptr<S3ContainerCredentialProvider>>
S3ContainerCredentialProvider::create(S3ContainerCredentialProviderConfig config) {
  const bool https = config.endpoint.starts_with("https://");
  const bool http = config.endpoint.starts_with("http://");
  const std::size_t scheme_length = https ? 8U : 7U;
  const auto maximum_long = std::chrono::milliseconds{std::numeric_limits<long>::max()};
  if ((!https && !http) || (config.require_tls && !https) ||
      config.endpoint.size() <= scheme_length || config.endpoint[scheme_length] == '/' ||
      config.endpoint.size() > 4096U || config.endpoint.contains('@') ||
      config.endpoint.contains('#') || contains_control(config.endpoint) ||
      contains_space(config.endpoint) ||
      (config.authorization_token.has_value() &&
       (config.authorization_token->empty() || config.authorization_token->size() > 8192U ||
        contains_control(*config.authorization_token))) ||
      (config.ca_bundle_path.has_value() &&
       (config.ca_bundle_path->empty() || config.ca_bundle_path->size() > 4096U)) ||
      config.connect_timeout.count() <= 0 || config.connect_timeout > maximum_long ||
      config.request_timeout.count() <= 0 || config.request_timeout > maximum_long ||
      config.refresh_before_expiration.count() < 0 ||
      config.refresh_before_expiration > std::chrono::hours{24 * 7} ||
      config.maximum_response_bytes == 0U ||
      config.maximum_response_bytes > kCredentialResponseLimit) {
    return common::make_unexpected(
        invalid("container credential provider configuration is invalid"));
  }
  if (initialize_curl() != CURLE_OK)
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "libcurl global initialization failed"});
  try {
    return std::shared_ptr<S3ContainerCredentialProvider>{
        new S3ContainerCredentialProvider{std::make_unique<Impl>(std::move(config))}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("container credential provider allocation failed"));
  }
}

common::Result<S3Credentials>
S3ContainerCredentialProvider::acquire(const S3CredentialRequest request) {
  if (request != S3CredentialRequest::kCurrent && request != S3CredentialRequest::kRefresh)
    return common::make_unexpected(invalid("S3 credential request is invalid"));
  try {
    std::scoped_lock lock{impl_->mutex};
    const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    const auto& cached_state = impl_->cached;
    if (request == S3CredentialRequest::kCurrent && cached_state.has_value()) {
      const auto& cached = *cached_state;
      if (now + impl_->config.refresh_before_expiration < cached.expiration)
        return cached.credentials;
    }
    auto fetched = impl_->fetch();
    if (!fetched.has_value())
      return common::make_unexpected(fetched.error());
    const auto& cached = impl_->cached.emplace(std::move(*fetched));
    return cached.credentials;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("container credential copy allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("container credential copy exceeds limits"));
  }
}

class S3InstanceCredentialProvider::Impl {
public:
  explicit Impl(S3InstanceCredentialProviderConfig configured) : config(std::move(configured)) {}

  struct Response {
    long status{};
    std::vector<std::byte> body;
  };

  struct Cached {
    S3Credentials credentials;
    std::chrono::sys_seconds expiration;
  };

  [[nodiscard]] common::Result<Response>
  request(const std::string_view path, const bool put,
          const std::optional<std::string_view> token = std::nullopt) const {
    try {
      const std::string url = config.endpoint + std::string{path};
      CurlHandle handle{curl_easy_init()};
      if (!handle)
        return common::make_unexpected(exhausted("IMDSv2 HTTP handle allocation failed"));
      ResponseCapture capture;
      capture.maximum_body_bytes = config.maximum_response_bytes;
      auto set = [&](const CURLoption option, const auto value) -> bool {
        return curl_easy_setopt(handle.get(), option, value) == CURLE_OK;
      };
      if (!set(CURLOPT_URL, url.c_str()) || !set(CURLOPT_PROXY, "") ||
          !set(CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(config.connect_timeout.count())) ||
          !set(CURLOPT_TIMEOUT_MS, static_cast<long>(config.request_timeout.count())) ||
          !set(CURLOPT_NOSIGNAL, 1L) || !set(CURLOPT_FOLLOWLOCATION, 0L) ||
          !set(CURLOPT_MAXREDIRS, 0L) || !set(CURLOPT_PROTOCOLS_STR, "http") ||
          !set(CURLOPT_HTTP_CONTENT_DECODING, 0L) || !set(CURLOPT_WRITEFUNCTION, &capture_body) ||
          !set(CURLOPT_WRITEDATA, &capture) || (put && !set(CURLOPT_CUSTOMREQUEST, "PUT"))) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kInternal, "IMDSv2 HTTP configuration failed"});
      }
      HeaderList headers;
      common::Status appended = common::Status::ok();
      if (put) {
        appended = append_header(headers, "X-aws-ec2-metadata-token-ttl-seconds: " +
                                              std::to_string(config.token_lifetime.count()));
      } else if (token.has_value()) {
        appended = append_header(headers, "X-aws-ec2-metadata-token: " + std::string{*token});
      }
      if (!appended.is_ok())
        return common::make_unexpected(appended);
      if (headers && !set(CURLOPT_HTTPHEADER, headers.get())) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kInternal, "IMDSv2 HTTP configuration failed"});
      }
      const CURLcode performed = curl_easy_perform(handle.get());
      if (performed != CURLE_OK)
        return common::make_unexpected(curl_failure(performed, capture.body_limit_exhausted));
      long status{};
      if (curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &status) != CURLE_OK) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kIoError, "IMDSv2 response status is unavailable"});
      }
      return Response{.status = status, .body = std::move(capture.body)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("IMDSv2 request allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("IMDSv2 request exceeds limits"));
    }
  }

  [[nodiscard]] common::Result<Cached> fetch() const {
    auto token_response = request("/latest/api/token", true);
    if (!token_response.has_value())
      return common::make_unexpected(token_response.error());
    if (token_response->status != 200L || token_response->body.empty() ||
        token_response->body.size() > 4096U) {
      return common::make_unexpected(common::Status{token_response->status == 404L
                                                        ? common::StatusCode::kNotFound
                                                        : common::StatusCode::kUnauthenticated,
                                                    "IMDSv2 token request failed"});
    }
    const std::string_view token = byte_string_view(token_response->body);
    if (contains_control(token)) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnauthenticated, "IMDSv2 token response is invalid"});
    }
    auto role_response = request("/latest/meta-data/iam/security-credentials/", false, token);
    if (!role_response.has_value())
      return common::make_unexpected(role_response.error());
    if (role_response->status == 404L)
      return common::make_unexpected(
          common::Status{common::StatusCode::kNotFound, "EC2 instance role is not configured"});
    if (role_response->status != 200L)
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnauthenticated, "IMDSv2 role request failed"});
    std::string_view role = byte_string_view(role_response->body);
    while (!role.empty() && (role.back() == '\r' || role.back() == '\n'))
      role.remove_suffix(1U);
    if (role.empty() || role.size() > 256U || contains_control(role) ||
        !std::ranges::all_of(role, [](const char character) {
          const auto value = static_cast<unsigned char>(character);
          return unreserved(value) || character == '+' || character == '=' || character == ',' ||
                 character == '@';
        })) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnauthenticated, "IMDSv2 role response is invalid"});
    }
    auto credentials_response = request(
        "/latest/meta-data/iam/security-credentials/" + encode_path(role, false), false, token);
    if (!credentials_response.has_value())
      return common::make_unexpected(credentials_response.error());
    if (credentials_response->status != 200L) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnauthenticated, "IMDSv2 credential request failed"});
    }
    const std::string_view json = byte_string_view(credentials_response->body);
    auto code = extract_json_string_field(json, {.name = "Code", .maximum_length = 64U});
    auto access = extract_json_string_field(json, {.name = "AccessKeyId", .maximum_length = 1024U});
    auto secret =
        extract_json_string_field(json, {.name = "SecretAccessKey", .maximum_length = 4096U});
    auto session = extract_json_string_field(json, {.name = "Token", .maximum_length = 8192U});
    auto expiration =
        extract_json_string_field(json, {.name = "Expiration", .maximum_length = 64U});
    if (!code.has_value() || *code != "Success" || !access.has_value() || !secret.has_value() ||
        !session.has_value() || !expiration.has_value()) {
      return common::make_unexpected(common::Status{common::StatusCode::kUnauthenticated,
                                                    "IMDSv2 credential response is invalid"});
    }
    auto expires_at = parse_utc_expiration(*expiration);
    S3Credentials credentials{.access_key_id = std::move(*access),
                              .secret_access_key = std::move(*secret),
                              .session_token = std::move(*session)};
    if (!expires_at.has_value() || !valid_credentials(credentials) ||
        *expires_at <= std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now())) {
      return common::make_unexpected(common::Status{common::StatusCode::kUnauthenticated,
                                                    "IMDSv2 credentials are expired or invalid"});
    }
    return Cached{.credentials = std::move(credentials), .expiration = *expires_at};
  }

  S3InstanceCredentialProviderConfig config;
  mutable std::mutex mutex;
  std::optional<Cached> cached;
};

S3InstanceCredentialProvider::S3InstanceCredentialProvider(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
S3InstanceCredentialProvider::~S3InstanceCredentialProvider() = default;

common::Result<std::shared_ptr<S3InstanceCredentialProvider>>
S3InstanceCredentialProvider::create(S3InstanceCredentialProviderConfig config) {
  const bool endpoint_valid = config.endpoint == "http://169.254.169.254" ||
                              config.endpoint == "http://[fd00:ec2::254]" ||
                              !config.require_link_local_endpoint;
  const std::size_t scheme_length = std::string_view{"http://"}.size();
  const auto maximum_long = std::chrono::milliseconds{std::numeric_limits<long>::max()};
  if (!config.endpoint.starts_with("http://") || !endpoint_valid ||
      config.endpoint.size() <= scheme_length || config.endpoint[scheme_length] == '/' ||
      config.endpoint.size() > 1024U || config.endpoint.contains('@') ||
      config.endpoint.contains('?') || config.endpoint.contains('#') ||
      config.endpoint.substr(scheme_length).contains('/') || contains_control(config.endpoint) ||
      contains_space(config.endpoint) || config.connect_timeout.count() <= 0 ||
      config.connect_timeout > maximum_long || config.request_timeout.count() <= 0 ||
      config.request_timeout > maximum_long || config.token_lifetime < std::chrono::seconds{1} ||
      config.token_lifetime > std::chrono::hours{6} ||
      config.refresh_before_expiration.count() < 0 ||
      config.refresh_before_expiration > std::chrono::hours{24 * 7} ||
      config.maximum_response_bytes == 0U ||
      config.maximum_response_bytes > kCredentialResponseLimit) {
    return common::make_unexpected(invalid("IMDSv2 credential provider configuration is invalid"));
  }
  if (initialize_curl() != CURLE_OK)
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "libcurl global initialization failed"});
  try {
    return std::shared_ptr<S3InstanceCredentialProvider>{
        new S3InstanceCredentialProvider{std::make_unique<Impl>(std::move(config))}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("IMDSv2 credential provider allocation failed"));
  }
}

common::Result<S3Credentials>
S3InstanceCredentialProvider::acquire(const S3CredentialRequest request) {
  if (request != S3CredentialRequest::kCurrent && request != S3CredentialRequest::kRefresh)
    return common::make_unexpected(invalid("S3 credential request is invalid"));
  try {
    std::scoped_lock lock{impl_->mutex};
    const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    const auto& cached_state = impl_->cached;
    if (request == S3CredentialRequest::kCurrent && cached_state.has_value()) {
      const auto& cached = *cached_state;
      if (now + impl_->config.refresh_before_expiration < cached.expiration)
        return cached.credentials;
    }
    auto fetched = impl_->fetch();
    if (!fetched.has_value())
      return common::make_unexpected(fetched.error());
    const auto& cached = impl_->cached.emplace(std::move(*fetched));
    return cached.credentials;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("IMDSv2 credential copy allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("IMDSv2 credential copy exceeds limits"));
  }
}

class S3ObjectStore::Impl {
public:
  enum class Method : std::uint8_t {
    kPut,
    kHead,
    kGetRange,
    kDelete,
    kCreateMultipart,
    kUploadPart,
    kCompleteMultipart,
    kAbortMultipart,
  };

  struct Request {
    Method method{};
    std::string_view key;
    common::ByteView upload;
    std::optional<ingest::Sha256Digest> checksum{std::nullopt};
    std::size_t range_offset{};
    std::size_t range_length{};
    std::size_t maximum_body_bytes{};
    std::string_view match_validator;
    std::string_view query;
    std::optional<ingest::Sha256Digest> object_checksum{std::nullopt};
  };

  struct Response {
    long status{};
    curl_off_t content_length{-1};
    ResponseCapture capture;
  };

  explicit Impl(S3ObjectStoreConfig configured)
      : config(std::move(configured)),
        jitter_sequence(config.retry_jitter_seed.value_or(default_jitter_seed(this))) {
    while (config.endpoint.size() > std::string_view{"https://"}.size() &&
           config.endpoint.back() == '/') {
      config.endpoint.pop_back();
    }
    signature = "aws:amz:" + config.region + ":s3";
  }

  [[nodiscard]] static std::uint64_t default_jitter_seed(const void* instance) noexcept {
    static std::atomic_uint64_t sequence{0x9E3779B97F4A7C15ULL};
    const auto steady =
        static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto wall =
        static_cast<std::uint64_t>(std::chrono::system_clock::now().time_since_epoch().count());
    const auto address = static_cast<std::uint64_t>(std::bit_cast<std::uintptr_t>(instance));
    return steady ^ (wall << 1U) ^ address ^ sequence.fetch_add(0x9E3779B97F4A7C15ULL);
  }

  [[nodiscard]] std::chrono::milliseconds
  random_jitter(const std::chrono::milliseconds maximum) const noexcept {
    if (maximum.count() <= 0)
      return std::chrono::milliseconds{0};
    std::uint64_t value = jitter_sequence.fetch_add(0x9E3779B97F4A7C15ULL);
    value = (value ^ (value >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    value = (value ^ (value >> 27U)) * 0x94D049BB133111EBULL;
    value ^= value >> 31U;
    const auto bound = static_cast<std::uint64_t>(maximum.count()) + 1U;
    return std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(value % bound)};
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

  void
  wait_before_retry(const std::size_t completed_attempts,
                    const std::optional<std::uint64_t> retry_after_seconds = std::nullopt) const {
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
    if (retry_after_seconds.has_value()) {
      const auto maximum_count = static_cast<std::uint64_t>(config.maximum_retry_backoff.count());
      const auto requested =
          *retry_after_seconds > maximum_count / 1'000U
              ? config.maximum_retry_backoff
              : std::chrono::milliseconds{
                    static_cast<std::chrono::milliseconds::rep>(*retry_after_seconds * 1'000U)};
      delay = std::min(config.maximum_retry_backoff, std::max(delay, requested));
    }
    const auto available = config.maximum_retry_backoff - delay;
    delay += random_jitter(std::min(config.maximum_retry_jitter, available));
    if (delay.count() > 0)
      std::this_thread::sleep_for(delay);
  }

  [[nodiscard]] common::Result<Response> perform_once(const Request& request,
                                                      const S3Credentials& current) const {
    try {
      const auto& request_checksum = request.checksum;
      const auto& object_checksum = request.object_checksum;
      const bool request_checksum_required = request.method == Method::kPut ||
                                             request.method == Method::kUploadPart ||
                                             request.method == Method::kCompleteMultipart;
      if ((request_checksum_required && !request_checksum.has_value()) ||
          (request.method == Method::kCreateMultipart && !object_checksum.has_value())) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kInternal, "S3 request is missing required checksum state"});
      }
      std::string url = config.endpoint + "/" + encode_path(config.bucket, false) + "/" +
                        encode_path(request.key, true);
      if (!request.query.empty())
        url += "?" + std::string{request.query};
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
            set(CURLOPT_PROXY, config.proxy_url.has_value() ? config.proxy_url->c_str() : "");
      if (configured.is_ok() && config.proxy_url.has_value())
        configured = set(CURLOPT_NOPROXY, "");
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
        const std::string hexadecimal_checksum = digest_hex(*request_checksum);
        configured = append_header(headers, "If-None-Match: *");
        if (configured.is_ok())
          configured = append_header(headers, "Expect:");
        if (configured.is_ok()) {
          configured = append_header(headers, "x-amz-content-sha256: " + hexadecimal_checksum);
        }
        if (configured.is_ok()) {
          configured =
              append_header(headers, "x-amz-checksum-sha256: " + digest_base64(*request_checksum));
        }
        if (configured.is_ok()) {
          configured = append_header(headers, "x-amz-meta-chronos-sha256: " + hexadecimal_checksum);
        }
      }
      if (configured.is_ok() && request.method == Method::kCreateMultipart) {
        configured =
            append_header(headers, "x-amz-meta-chronos-sha256: " + digest_hex(*object_checksum));
      }
      if (configured.is_ok() &&
          (request.method == Method::kPut || request.method == Method::kCreateMultipart) &&
          config.server_side_encryption.has_value()) {
        const auto& server_side_encryption = config.server_side_encryption;
        const bool kms = *server_side_encryption == S3ServerSideEncryption::kKms;
        configured = append_header(headers, std::string{"x-amz-server-side-encryption: "} +
                                                (kms ? "aws:kms" : "AES256"));
        if (configured.is_ok() && kms) {
          const auto& kms_key_id = config.kms_key_id;
          if (!kms_key_id.has_value()) {
            return common::make_unexpected(common::Status{
                common::StatusCode::kInternal, "S3 KMS request is missing a key identifier"});
          }
          configured =
              append_header(headers, "x-amz-server-side-encryption-aws-kms-key-id: " + *kms_key_id);
        }
      }
      if (configured.is_ok() &&
          (request.method == Method::kUploadPart || request.method == Method::kCompleteMultipart)) {
        const std::string hexadecimal_checksum = digest_hex(*request_checksum);
        configured = append_header(headers, "Expect:");
        if (configured.is_ok())
          configured = append_header(headers, "x-amz-content-sha256: " + hexadecimal_checksum);
        if (configured.is_ok() && request.method == Method::kCompleteMultipart) {
          configured = append_header(headers, "Content-Type: application/xml");
          if (configured.is_ok())
            configured = append_header(headers, "If-None-Match: *");
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
      if (configured.is_ok() &&
          (request.method == Method::kPut || request.method == Method::kUploadPart ||
           request.method == Method::kCreateMultipart ||
           request.method == Method::kCompleteMultipart)) {
        configured =
            set(CURLOPT_CUSTOMREQUEST,
                request.method == Method::kPut || request.method == Method::kUploadPart ? "PUT"
                                                                                        : "POST");
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
      if (configured.is_ok() &&
          (request.method == Method::kDelete || request.method == Method::kAbortMultipart))
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
          response.capture.entity_tag_conflict ||
          response.capture.server_side_encryption_conflict ||
          response.capture.kms_key_id_conflict) {
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
        if (request.method == Method::kCreateMultipart ||
            response.error().code() != common::StatusCode::kUnavailable ||
            attempt == config.maximum_attempts) {
          return common::make_unexpected(response.error());
        }
        refresh_credentials = false;
        wait_before_retry(attempt);
        continue;
      }
      const bool authorization_rejected = response->status == 401L || response->status == 403L;
      const bool replayable_status =
          retryable_http_status(response->status) && request.method != Method::kCreateMultipart &&
          !(request.method == Method::kCompleteMultipart && response->status == 409L);
      if (attempt == config.maximum_attempts ||
          (!replayable_status &&
           !(authorization_rejected && config.credential_provider != nullptr))) {
        return response;
      }
      refresh_credentials = authorization_rejected;
      std::optional<std::uint64_t> retry_delay;
      const auto& retry_after = response->capture.retry_after;
      if (replayable_status && !response->capture.retry_after_invalid && retry_after.has_value()) {
        retry_delay = retry_after_delay_seconds(*retry_after);
      }
      wait_before_retry(attempt, retry_delay);
    }
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "S3 retry state is unreachable"});
  }

  S3ObjectStoreConfig config;
  std::string signature;
  mutable std::atomic_uint64_t jitter_sequence;

  struct MultipartUploadIdentity {
    std::string_view key;
    std::string_view upload_id;
  };

  void abort_multipart_best_effort(const MultipartUploadIdentity identity) const noexcept {
    try {
      const std::string query = "uploadId=" + encode_path(identity.upload_id, false);
      [[maybe_unused]] auto aborted = perform({.method = Method::kAbortMultipart,
                                               .key = identity.key,
                                               .upload = {},
                                               .maximum_body_bytes = kSmallResponseLimit,
                                               .match_validator = {},
                                               .query = query});
    } catch (...) {
      return;
    }
  }
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
  const bool kms_encryption = config.server_side_encryption == S3ServerSideEncryption::kKms;
  const bool valid_encryption =
      !config.server_side_encryption.has_value() ||
      *config.server_side_encryption == S3ServerSideEncryption::kS3ManagedAes256 || kms_encryption;
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
      (config.proxy_url.has_value() &&
       ((!config.proxy_url->starts_with("http://") && !config.proxy_url->starts_with("https://")) ||
        config.proxy_url->ends_with("//") || config.proxy_url->size() > 4096U ||
        config.proxy_url->contains('@') || config.proxy_url->contains('?') ||
        config.proxy_url->contains('#') ||
        config.proxy_url->substr(config.proxy_url->find("//") + 2U).contains('/') ||
        contains_control(*config.proxy_url) || contains_space(*config.proxy_url))) ||
      config.connect_timeout.count() <= 0 || config.connect_timeout > maximum_long ||
      config.request_timeout.count() <= 0 || config.request_timeout > maximum_long ||
      config.maximum_attempts == 0U || config.maximum_attempts > 32U ||
      config.initial_retry_backoff.count() < 0 || config.initial_retry_backoff > maximum_long ||
      config.maximum_retry_backoff.count() < 0 || config.maximum_retry_backoff > maximum_long ||
      config.maximum_retry_jitter.count() < 0 || config.maximum_retry_jitter > maximum_long ||
      config.initial_retry_backoff > config.maximum_retry_backoff ||
      config.multipart_threshold_bytes == 0U ||
      config.multipart_part_bytes < kMinimumMultipartPartBytes ||
      static_cast<std::uintmax_t>(config.multipart_part_bytes) >
          static_cast<std::uintmax_t>(std::numeric_limits<curl_off_t>::max()) ||
      config.multipart_maximum_concurrency == 0U || config.multipart_maximum_concurrency > 64U ||
      !valid_encryption || (kms_encryption != config.kms_key_id.has_value()) ||
      (config.kms_key_id.has_value() &&
       (config.kms_key_id->empty() || config.kms_key_id->size() > 2048U ||
        contains_control(*config.kms_key_id) || contains_space(*config.kms_key_id))) ||
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
  const auto verify_existing = [&]() -> common::Result<ObjectMetadata> {
    auto existing = stat(key);
    if (!existing.has_value())
      return common::make_unexpected(existing.error());
    if (existing->size != bytes.size() || existing->checksum != checksum) {
      return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                    "immutable S3 key has different content"});
    }
    return existing;
  };
  if (bytes.size() >= impl_->config.multipart_threshold_bytes) {
    try {
      const std::size_t part_count =
          1U + ((bytes.size() - 1U) / impl_->config.multipart_part_bytes);
      if (part_count > 10'000U) {
        return common::make_unexpected(
            exhausted("S3 multipart upload exceeds the provider part-count limit"));
      }
      auto before = verify_existing();
      if (before.has_value() || before.error().code() != common::StatusCode::kNotFound)
        return before;

      auto created = impl_->perform({.method = Impl::Method::kCreateMultipart,
                                     .key = key,
                                     .upload = {},
                                     .maximum_body_bytes = kSmallResponseLimit,
                                     .match_validator = {},
                                     .query = "uploads",
                                     .object_checksum = checksum});
      if (!created.has_value())
        return common::make_unexpected(created.error());
      if (created->status != 200L)
        return common::make_unexpected(http_failure(created->status));
      const common::ByteView creation_body{created->capture.body};
      const std::string_view creation_xml = byte_string_view(creation_body);
      if (!creation_xml.contains("<InitiateMultipartUploadResult")) {
        return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                      "S3 multipart creation response is invalid"});
      }
      auto upload_id = extract_xml_element(creation_body, "UploadId", 2048U);
      if (!upload_id.has_value())
        return common::make_unexpected(upload_id.error());
      struct MultipartAbortGuard {
        const Impl* impl{};
        std::string_view key;
        std::string_view upload_id;
        bool active{true};

        ~MultipartAbortGuard() {
          if (active)
            impl->abort_multipart_best_effort({.key = key, .upload_id = upload_id});
        }

        void release() noexcept {
          active = false;
        }
      } abort_guard{impl_.get(), key, *upload_id};
      const std::string upload_query = "uploadId=" + encode_path(*upload_id, false);

      std::vector<std::string> entity_tags;
      std::vector<std::optional<common::Status>> part_failures;
      try {
        entity_tags.resize(part_count);
        part_failures.resize(part_count);
      } catch (const std::bad_alloc&) {
        return common::make_unexpected(exhausted("S3 multipart worker-state allocation failed"));
      } catch (const std::length_error&) {
        return common::make_unexpected(exhausted("S3 multipart worker-state count exceeds limits"));
      }
      std::atomic_size_t next_part{};
      std::atomic_bool part_failed{};
      const auto upload_worker = [&] {
        while (!part_failed.load()) {
          const std::size_t part_index = next_part.fetch_add(1U);
          if (part_index >= part_count)
            return;
          try {
            const std::size_t offset = part_index * impl_->config.multipart_part_bytes;
            const std::size_t length =
                std::min(impl_->config.multipart_part_bytes, bytes.size() - offset);
            const common::ByteView part = bytes.subspan(offset, length);
            auto part_checksum = ingest::sha256(part);
            if (!part_checksum.has_value()) {
              part_failures[part_index] = part_checksum.error();
              part_failed.store(true);
              return;
            }
            const std::string query =
                "partNumber=" + std::to_string(part_index + 1U) + "&" + upload_query;
            auto uploaded = impl_->perform({.method = Impl::Method::kUploadPart,
                                            .key = key,
                                            .upload = part,
                                            .checksum = *part_checksum,
                                            .maximum_body_bytes = kSmallResponseLimit,
                                            .match_validator = {},
                                            .query = query});
            if (!uploaded.has_value() || uploaded->status != 200L ||
                !uploaded->capture.entity_tag.has_value() ||
                uploaded->capture.entity_tag->empty() ||
                uploaded->capture.entity_tag->size() > 1024U ||
                contains_control(*uploaded->capture.entity_tag)) {
              part_failures[part_index] =
                  !uploaded.has_value()
                      ? uploaded.error()
                      : (uploaded->status != 200L
                             ? http_failure(uploaded->status)
                             : common::Status{common::StatusCode::kCorruption,
                                              "S3 multipart part ETag is absent or invalid"});
              part_failed.store(true);
              return;
            }
            entity_tags[part_index] = std::move(*uploaded->capture.entity_tag);
          } catch (const std::bad_alloc&) {
            part_failures[part_index] = exhausted("S3 multipart worker allocation failed");
            part_failed.store(true);
            return;
          } catch (const std::length_error&) {
            part_failures[part_index] = exhausted("S3 multipart worker exceeded limits");
            part_failed.store(true);
            return;
          } catch (...) {
            part_failures[part_index] = common::Status{common::StatusCode::kInternal,
                                                       "S3 multipart worker raised an exception"};
            part_failed.store(true);
            return;
          }
        }
      };
      try {
        JoiningThreads worker_group;
        auto& workers = worker_group.threads();
        const std::size_t worker_count =
            std::min(part_count, impl_->config.multipart_maximum_concurrency);
        workers.reserve(worker_count);
        for (std::size_t worker = 0U; worker < worker_count; ++worker)
          workers.emplace_back(upload_worker);
      } catch (const std::system_error&) {
        return common::make_unexpected(exhausted("S3 multipart worker creation failed"));
      }
      if (part_failed.load()) {
        for (auto& failure : part_failures) {
          if (failure.has_value())
            return common::make_unexpected(std::move(*failure));
        }
        return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                      "S3 multipart failure state is incomplete"});
      }

      auto completion_xml = complete_multipart_xml(entity_tags);
      if (!completion_xml.has_value())
        return common::make_unexpected(completion_xml.error());
      const common::ByteView completion_body = string_byte_view(*completion_xml);
      auto completion_checksum = ingest::sha256(completion_body);
      if (!completion_checksum.has_value())
        return common::make_unexpected(completion_checksum.error());
      auto completed = impl_->perform({.method = Impl::Method::kCompleteMultipart,
                                       .key = key,
                                       .upload = completion_body,
                                       .checksum = *completion_checksum,
                                       .maximum_body_bytes = kSmallResponseLimit,
                                       .match_validator = {},
                                       .query = upload_query});
      if (completed.has_value() && completed->status == 200L) {
        const common::ByteView response_body{completed->capture.body};
        if (is_complete_multipart_result(response_body)) {
          auto verified = verify_existing();
          if (verified.has_value()) {
            abort_guard.release();
            return verified;
          }
          if (verified.error().code() == common::StatusCode::kNotFound) {
            return common::make_unexpected(
                common::Status{common::StatusCode::kUnavailable,
                               "S3 multipart completion is not visible during exact verification"});
          }
          return common::make_unexpected(verified.error());
        }
      }

      auto verified = verify_existing();
      if (verified.has_value()) {
        abort_guard.release();
        return verified;
      }
      if (!completed.has_value())
        return common::make_unexpected(completed.error());
      if (completed->status == 412L) {
        if (verified.error().code() == common::StatusCode::kNotFound) {
          return common::make_unexpected(
              common::Status{common::StatusCode::kUnavailable,
                             "S3 conditional multipart result changed before verification"});
        }
        return common::make_unexpected(verified.error());
      }
      if (completed->status != 200L)
        return common::make_unexpected(http_failure(completed->status));
      return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                    "S3 multipart completion response is invalid"});
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("S3 multipart upload allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("S3 multipart upload exceeded limits"));
    }
  }
  auto response = impl_->perform({.method = Impl::Method::kPut,
                                  .key = key,
                                  .upload = bytes,
                                  .checksum = checksum,
                                  .maximum_body_bytes = kSmallResponseLimit,
                                  .match_validator = {},
                                  .query = {}});
  if (!response.has_value())
    return common::make_unexpected(response.error());
  if (response->status == 412L) {
    auto existing = verify_existing();
    if (!existing.has_value()) {
      if (existing.error().code() == common::StatusCode::kNotFound) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kUnavailable,
                           "S3 conditional-write result changed before verification"});
      }
      return common::make_unexpected(existing.error());
    }
    return *existing;
  }
  if (response->status != 200L && response->status != 201L)
    return common::make_unexpected(http_failure(response->status));
  if (impl_->config.server_side_encryption.has_value()) {
    auto verified = verify_existing();
    if (!verified.has_value() && verified.error().code() == common::StatusCode::kNotFound) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnavailable,
                         "S3 encrypted object is not visible during exact verification"});
    }
    return verified;
  }
  return ObjectMetadata{std::string{key}, bytes.size(), checksum};
}

common::Result<ObjectMetadata> S3ObjectStore::stat(const std::string_view key) const {
  if (key.empty() || key.size() > 1024U || contains_control(key))
    return common::make_unexpected(invalid("S3 object key is invalid"));
  auto response = impl_->perform({.method = Impl::Method::kHead,
                                  .key = key,
                                  .upload = {},
                                  .maximum_body_bytes = kSmallResponseLimit,
                                  .match_validator = {},
                                  .query = {}});
  if (!response.has_value())
    return common::make_unexpected(response.error());
  if (response->status != 200L)
    return common::make_unexpected(http_failure(response->status));
  const auto& configured_encryption = impl_->config.server_side_encryption;
  if (configured_encryption.has_value()) {
    const bool kms = *configured_encryption == S3ServerSideEncryption::kKms;
    const std::string_view expected = kms ? "aws:kms" : "AES256";
    const auto& received_encryption = response->capture.server_side_encryption;
    if (!received_encryption.has_value()) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kCorruption, "S3 object server-side encryption metadata is invalid"});
    }
    if (*received_encryption != expected ||
        (kms && response->capture.kms_key_id != impl_->config.kms_key_id) ||
        (!kms && response->capture.kms_key_id.has_value())) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kCorruption, "S3 object server-side encryption metadata is invalid"});
    }
  }
  const auto& checksum_hex = response->capture.checksum_hex;
  if (response->content_length < 0 ||
      static_cast<std::uintmax_t>(response->content_length) >
          std::numeric_limits<std::size_t>::max() ||
      !checksum_hex.has_value()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "S3 object metadata is incomplete or unaddressable"});
  }
  auto checksum = parse_digest_hex(*checksum_hex);
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
                                  .upload = {},
                                  .range_offset = offset,
                                  .range_length = length,
                                  .maximum_body_bytes = length,
                                  .match_validator = {},
                                  .query = {}});
  if (!response.has_value())
    return common::make_unexpected(response.error());
  if (response->status != 206L)
    return common::make_unexpected(http_failure(response->status));
  const auto& encoded_content_range = response->capture.content_range;
  if (!encoded_content_range.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "S3 content range is absent"});
  }
  auto content_range = parse_content_range(*encoded_content_range);
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
  auto head = impl_->perform({.method = Impl::Method::kHead,
                              .key = key,
                              .upload = {},
                              .maximum_body_bytes = kSmallResponseLimit,
                              .match_validator = {},
                              .query = {}});
  if (!head.has_value())
    return common::make_unexpected(head.error());
  if (head->status == 404L)
    return ObjectDeletionReport{.already_absent = true};
  if (head->status != 200L)
    return common::make_unexpected(http_failure(head->status));
  const auto& checksum_hex = head->capture.checksum_hex;
  const auto& entity_tag_state = head->capture.entity_tag;
  if (head->content_length < 0 ||
      static_cast<std::uintmax_t>(head->content_length) > std::numeric_limits<std::size_t>::max() ||
      !checksum_hex.has_value() || !entity_tag_state.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption,
                       "S3 object deletion metadata is incomplete or unaddressable"});
  }
  const auto& entity_tag = *entity_tag_state;
  if (entity_tag.empty() || entity_tag.size() > 1024U || contains_control(entity_tag)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption,
                       "S3 object deletion metadata is incomplete or unaddressable"});
  }
  auto checksum = parse_digest_hex(*checksum_hex);
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
                                 .upload = {},
                                 .maximum_body_bytes = kSmallResponseLimit,
                                 .match_validator = entity_tag,
                                 .query = {}});
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
