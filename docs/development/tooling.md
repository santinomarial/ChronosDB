# Development Tooling

The local workflows mirror CI and keep compiler policy attached to ChronosDB targets. Third-party
targets fetched for tests or benchmarks do not inherit ChronosDB warnings, sanitizers, or tidy.

## Formatting

`.clang-format` defines the C++ style. Formatting is pinned to clang-format major version 18 because
different LLVM majors can produce different output from the same style file. The script verifies
the selected executable's major version and refuses clang-format 17, 19, 22, or another unpinned
major. Apply or verify it with:

```sh
scripts/format.sh
scripts/format.sh --check
```

The script searches for `clang-format-18` and the standard Homebrew `llvm@18` paths before an
unversioned executable. Set `CLANG_FORMAT` when the executable has another location; it must still
report version 18.x. CI installs `clang-format-18` explicitly and runs the same check command.

## Lint and static analysis

`.clang-tidy` enables focused analyzer, bug-prone, performance, portability, safe-modernization, and
objective readability checks. It deliberately excludes broad subjective style rules. Run it as part
of compilation:

```sh
scripts/lint.sh debug
```

This reconfigures the selected build with `CHRONOS_ENABLE_CLANG_TIDY=ON`, so subsequent builds of
that directory continue to run tidy. Set `CLANG_TIDY` to select a nonstandard executable. LLVM
clang-tidy 17+ is the supported compatibility baseline.

The normal full local sequence is:

```sh
scripts/check.sh
```

It checks formatting, configures, builds, tests, and then compiles with clang-tidy. Failures are not
suppressed. Sanitizers remain separate commands because running all sanitizer configurations on
every edit is expensive and TSan cannot share a runtime with ASan.

## Dependency updates

C++ test and benchmark dependency declarations live in `cmake/ChronosDependencies.cmake` and use
immutable commit IDs. External GitHub Actions in `.github/workflows/` are also pinned to full commit
IDs; `scripts/check-workflow-actions.sh` enforces that rule. An update must be intentional: review
upstream release notes and provenance, update the pin and its nearby release comment, configure from
an empty build tree, and run normal, sanitizer, formatting, and static-analysis checks. A new
production dependency additionally requires the review/ADR evidence described in
[ADR-0011](../adr/0011-dependency-and-build-versus-buy-policy.md). Do not commit populated
FetchContent directories.

## CI matrix

GitHub Actions builds and tests Debug configurations on Linux with GCC/libstdc++, Linux with
Clang/libc++, and macOS with AppleClang/libc++. Ubuntu 24.04's default Clang/libstdc++ pairing does
not expose C++23 `std::expected`, so every Linux Clang job installs and selects libc++ explicitly.
Every normal compiler-matrix job treats warnings in ChronosDB targets as errors. Separate Linux
Clang/libc++ jobs run ASan+UBSan and TSan. Independent jobs verify clang-format, immutable workflow
action pins, and clang-tidy. Test logs are uploaded when a test job fails. Benchmarks do not gate
shared CI because trustworthy performance comparisons require a controlled environment.
