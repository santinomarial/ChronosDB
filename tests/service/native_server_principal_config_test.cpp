#include "chronos/service/native_server_principal_config.hpp"

#include "gtest/gtest.h"
#include <cstddef>
#include <string>
#include <string_view>

namespace chronos::service {
namespace {

constexpr std::string_view kFingerprint1 =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
constexpr std::string_view kFingerprint2 =
    "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

[[nodiscard]] std::string line(const std::string_view principal,
                               const std::string_view fingerprint) {
  return std::string{principal} + "=" + std::string{fingerprint};
}

[[nodiscard]] std::string config(const std::string_view body) {
  return std::string{kNativeServerPrincipalConfigV1Magic} + "\n" + std::string{body};
}

TEST(NativeServerPrincipalConfigTest, ParsesCanonicalOrderedCertificateAuthority) {
  const auto parsed = parse_native_server_principal_config(
      config(line("7", kFingerprint1) + "\n" + line("42", kFingerprint2) + "\n"));
  ASSERT_TRUE(parsed.has_value()) << parsed.error().to_string();
  ASSERT_EQ(parsed->size(), 2U);
  EXPECT_EQ((*parsed)[0].principal_id, 7U);
  EXPECT_EQ((*parsed)[0].certificate_sha256.front(), 0x01U);
  EXPECT_EQ((*parsed)[0].certificate_sha256.back(), 0xefU);
  EXPECT_EQ((*parsed)[1].principal_id, 42U);
  EXPECT_EQ((*parsed)[1].certificate_sha256.front(), 0x11U);

  const auto without_final_lf =
      parse_native_server_principal_config(config(line("7", kFingerprint1)));
  ASSERT_TRUE(without_final_lf.has_value()) << without_final_lf.error().to_string();
  EXPECT_EQ(without_final_lf->size(), 1U);
}

TEST(NativeServerPrincipalConfigTest, RejectsMalformedAndAmbiguousAuthority) {
  const auto reject = [](const std::string& text) {
    const auto parsed = parse_native_server_principal_config(text);
    EXPECT_FALSE(parsed.has_value()) << text;
    if (!parsed.has_value()) {
      EXPECT_EQ(parsed.error().code(), common::StatusCode::kInvalidArgument);
    }
  };

  reject("");
  reject("CHRONOSDB_NATIVE_SERVER_PRINCIPALS_V0\n" + line("1", kFingerprint1));
  reject(std::string{kNativeServerPrincipalConfigV1Magic});
  reject(config(""));
  reject(config("\n"));
  reject(config(line("0", kFingerprint1)));
  reject(config(line("01", kFingerprint1)));
  reject(config(line("-1", kFingerprint1)));
  reject(config(line("1 ", kFingerprint1)));
  reject(config(line("1", kFingerprint1.substr(1U))));
  reject(config(line("1", "A123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")));
  reject(config(line("1", kFingerprint1) + "\r\n"));
  reject(config(line("1", kFingerprint1) + "\n\n"));
  reject(config(line("2", kFingerprint1) + "\n" + line("1", kFingerprint2)));
  reject(config(line("1", kFingerprint1) + "\n" + line("1", kFingerprint2)));
  reject(config(line("1", kFingerprint1) + "\n" + line("2", kFingerprint1)));
  reject(config("1==" + std::string{kFingerprint1}));
  reject(config("1=" + std::string{kFingerprint1} + "=extra"));
  reject(config("#comment"));
}

TEST(NativeServerPrincipalConfigTest, EnforcesByteCountAndPrincipalLimits) {
  const std::string one = config(line("1", kFingerprint1));
  auto parsed = parse_native_server_principal_config(
      one, {.maximum_bytes = one.size() - 1U, .maximum_principals = 1U});
  ASSERT_FALSE(parsed.has_value());
  EXPECT_EQ(parsed.error().code(), common::StatusCode::kResourceExhausted);

  parsed = parse_native_server_principal_config(
      config(line("1", kFingerprint1) + "\n" + line("2", kFingerprint2)),
      {.maximum_bytes = 1024U, .maximum_principals = 1U});
  ASSERT_FALSE(parsed.has_value());
  EXPECT_EQ(parsed.error().code(), common::StatusCode::kResourceExhausted);

  for (const NativeServerPrincipalConfigLimits limits :
       {NativeServerPrincipalConfigLimits{.maximum_bytes = 0U, .maximum_principals = 1U},
        NativeServerPrincipalConfigLimits{.maximum_bytes = 1U, .maximum_principals = 0U},
        NativeServerPrincipalConfigLimits{.maximum_bytes = 1U, .maximum_principals = 65'537U}}) {
    parsed = parse_native_server_principal_config(one, limits);
    ASSERT_FALSE(parsed.has_value());
    EXPECT_EQ(parsed.error().code(), common::StatusCode::kInvalidArgument);
  }
}

} // namespace
} // namespace chronos::service
