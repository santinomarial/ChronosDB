# ADR 0205: Explicit S3 proxy policy

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB security, object-storage, runtime, and operations maintainers
- **Extends:** [ADR 0182](0182-libcurl-sigv4-s3-object-store.md)

## Context

libcurl normally consults process proxy variables when `CURLOPT_PROXY` is unset. A database process
can therefore route signed S3 traffic through an ambient proxy without ChronosDB configuration or
review. Although HTTPS CONNECT preserves endpoint TLS verification, the proxy still observes target
metadata and controls availability; plaintext S3 deployments additionally expose credentials and
object bytes. Proxy userinfo in a URL also creates another secret-bearing configuration surface.

## Decision

Every S3 request sets `CURLOPT_PROXY`. The default value is the empty string, which explicitly
disables libcurl proxy-environment lookup. An operator may opt in with one `proxy_url` in
`S3ObjectStoreConfig`; it applies uniformly to every request and sets an empty `CURLOPT_NOPROXY` so
ambient bypass variables cannot silently override the explicit choice.

An explicit proxy must be a bounded nonempty HTTP(S) scheme plus authority. Userinfo, paths,
queries, fragments, control characters, and spaces are rejected. Proxy credentials are not
supported. Redirects remain disabled. For HTTPS S3 endpoints, libcurl tunnels and still performs
peer and hostname verification against the configured S3 endpoint using the existing CA policy.
Plain HTTP remains an explicit insecure deployment mode and exposes signed traffic to the proxy.

## Consequences and validation

Deployments no longer inherit `http_proxy`, `https_proxy`, `all_proxy`, or `no_proxy` policy. Proxy
use is reviewable in the same owned configuration as endpoint and CA settings. Authenticated proxy,
per-host bypass, PAC, SOCKS, and separate proxy CA/client-certificate policy remain unsupported.

The focused test points ambient `http_proxy` at an unreachable local port and proves a signed
request still reaches the intended loopback S3 fixture. It separately rejects a credential-bearing
proxy URL and verifies the returned error does not contain the password. Existing TLS-default,
signing, retry, multipart, and exact-response tests remain in the gate.

Invariants 2, 3, 8, 10, 14, and 18 apply.

## Alternatives considered

- **Inherit process variables:** rejected because signed database traffic would follow implicit
  mutable global policy.
- **Disable all proxy use permanently:** rejected because some controlled deployments require an
  egress proxy.
- **Allow proxy URL userinfo:** rejected because it expands secret storage/redaction and proxy
  authentication policy without a current requirement.
- **Trust `no_proxy`:** rejected because it is another ambient override rather than owned config.

## Migration and rollback

Existing configurations stop inheriting environment proxies. Operators that intentionally relied on
that behavior must copy the reviewed HTTP(S) proxy authority into `proxy_url`. No durable, network,
object, or credential format changes.

## References

- [CURLOPT_PROXY](https://curl.se/libcurl/c/CURLOPT_PROXY.html)
- [CURLOPT_NOPROXY](https://curl.se/libcurl/c/CURLOPT_NOPROXY.html)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
