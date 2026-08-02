#include "chronos/common/version.hpp"

#include <gtest/gtest.h>
#include <string>
#include <string_view>

namespace chronos::common {
namespace {

TEST(VersionTest, ExposesSemanticVersion) {
  EXPECT_EQ(semantic_version(), "0.1.0");
  EXPECT_EQ(version_info().semantic_version, semantic_version());
}

TEST(VersionTest, ExposesNonemptyBuildMetadata) {
  const VersionInfo info = version_info();
  EXPECT_FALSE(info.build_type.empty());
  EXPECT_FALSE(info.compiler.empty());
  EXPECT_FALSE(info.target_architecture.empty());
  EXPECT_FALSE(info.operating_system.empty());
  if (info.git_metadata_available) {
    EXPECT_FALSE(info.git_commit.empty());
    EXPECT_NE(info.git_commit, "unknown");
  }
}

TEST(VersionTest, ProducesHumanReadableOutput) {
  const std::string text = version_text();
  EXPECT_NE(text.find("ChronosDB 0.1.0"), std::string::npos);
  EXPECT_NE(text.find("git commit:"), std::string::npos);
  EXPECT_NE(text.find("build type:"), std::string::npos);
  EXPECT_NE(text.find("compiler:"), std::string::npos);
  EXPECT_NE(text.find("target architecture:"), std::string::npos);
  EXPECT_NE(text.find("operating system:"), std::string::npos);
}

TEST(VersionTest, EscapesJsonAndUsesStableFieldNames) {
  constexpr VersionInfo info{
      .semantic_version = "1\"2\\3\n",
      .git_commit = "abc\tdef",
      .git_metadata_available = true,
      .git_dirty = true,
      .build_type = "Debug\rBuild",
      .compiler = "compiler\x01",
      .target_architecture = "arm64",
      .operating_system = "TestOS",
  };

  EXPECT_EQ(
      version_json(info),
      R"({"version":"1\"2\\3\n","git_commit":"abc\tdef","git_dirty":true,"build_type":"Debug\rBuild","compiler":"compiler\u0001","target_architecture":"arm64","operating_system":"TestOS"})");
}

TEST(VersionTest, RepresentsUnavailableGitMetadata) {
  constexpr VersionInfo info{
      .semantic_version = "0.1.0",
      .git_commit = "unknown",
      .git_metadata_available = false,
      .git_dirty = false,
      .build_type = "Debug",
      .compiler = "Test Compiler",
      .target_architecture = "test-arch",
      .operating_system = "TestOS",
  };

  const std::string json = version_json(info);
  EXPECT_NE(json.find(R"("git_commit":null)"), std::string::npos);
  EXPECT_NE(json.find(R"("git_dirty":null)"), std::string::npos);
  EXPECT_NE(version_text(info).find("git commit: unavailable"), std::string::npos);
}

TEST(VersionTest, PreservesValidUtf8AndReplacesInvalidBytes) {
  constexpr VersionInfo info{
      .semantic_version = "caf\xc3\xa9",
      .git_commit = "bad\xff",
      .git_metadata_available = true,
      .git_dirty = false,
      .build_type = "Debug",
      .compiler = "Test Compiler",
      .target_architecture = "test-arch",
      .operating_system = "TestOS",
  };

  const std::string json = version_json(info);
  EXPECT_NE(json.find("caf\xc3\xa9"), std::string::npos);
  EXPECT_NE(json.find(R"("git_commit":"bad\ufffd")"), std::string::npos);
}

TEST(VersionTest, ValidatesUtf8BoundarySequences) {
  const std::string valid{"\xc2\x80|\xe0\xa0\x80|\xed\x9f\xbf|\xee\x80\x80|"
                          "\xf0\x90\x80\x80|\xf4\x8f\xbf\xbf"};
  const VersionInfo valid_info{
      .semantic_version = "0.1.0",
      .git_commit = "0123456789ab",
      .git_metadata_available = true,
      .git_dirty = false,
      .build_type = "Test",
      .compiler = "Test Compiler",
      .target_architecture = "test-architecture",
      .operating_system = valid,
  };
  EXPECT_NE(version_json(valid_info).find(valid), std::string::npos);

  const std::string invalid{"\xc0\xaf|\xed\xa0\x80|\xf4\x90\x80\x80|\xe2\x82"};
  const VersionInfo invalid_info{
      .semantic_version = "0.1.0",
      .git_commit = "0123456789ab",
      .git_metadata_available = true,
      .git_dirty = false,
      .build_type = "Test",
      .compiler = "Test Compiler",
      .target_architecture = "test-architecture",
      .operating_system = invalid,
  };
  EXPECT_NE(
      version_json(invalid_info)
          .find(
              R"("operating_system":"\ufffd\ufffd|\ufffd\ufffd\ufffd|\ufffd\ufffd\ufffd\ufffd|\ufffd\ufffd")"),
      std::string::npos);
}

TEST(VersionTest, StableInputsProduceDeterministicOutput) {
  const VersionInfo info = version_info();
  EXPECT_EQ(version_text(info), version_text(info));
  EXPECT_EQ(version_json(info), version_json(info));
}

} // namespace
} // namespace chronos::common
