# ADR 0212: IMDSv2-only S3 instance credentials

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB security, runtime, object-storage, and operations maintainers
- **Extends:** [ADR 0196](0196-bounded-s3-retry-and-credential-refresh.md), [ADR 0210](0210-pinned-ordered-s3-credential-chain.md)

## Context

EC2 instance roles expose rotating temporary credentials through the Instance Metadata Service.
IMDSv2 requires a session token obtained by PUT and sent on subsequent role and credential GETs;
IMDSv1 omits that SSRF defense. ChronosDB needed an instance provider without silently enabling
metadata access in every process or permitting arbitrary authorities to masquerade as IMDS.

## Decision

`S3InstanceCredentialProvider::create` is explicit and defaults to the IPv4 IMDS authority
`http://169.254.169.254`. Production policy accepts only that exact authority or the documented IPv6
authority `http://[fd00:ec2::254]`. An explicit `require_link_local_endpoint=false` exists for
isolated loopback tests and reviewed metadata proxies. Authorities are bounded HTTP-only values
without userinfo, paths, queries, fragments, controls, or spaces.

Every refresh performs the IMDSv2 sequence:

1. PUT `/latest/api/token` with a configured token lifetime from one second through six hours;
2. GET `/latest/meta-data/iam/security-credentials/` with that token; and
3. GET the percent-encoded discovered role beneath the same path with the same token.

There is no IMDSv1 fallback. Every request uses a fresh libcurl handle, disables ambient proxies and
content decoding, follows no redirects, accepts HTTP only, and has finite connect/request and body
bounds. Token and role bodies are bounded; tokens reject controls; role names permit only the IAM
role-name character subset before encoding. Credential JSON requires one `Code: Success`, access
key, secret, token, and strict unexpired UTC expiration using the same bounded parser as container
credentials. Values and response bodies never appear in errors.

One mutex serializes token/role/credential fetch and cache publication. `kCurrent` reuses credentials
only outside the configured expiration refresh window; `kRefresh` always performs a new complete
IMDSv2 session. Failed refresh never returns the rejected cache. A 404 token or role response maps to
`NOT_FOUND` so an explicit ordered chain may advance only when IMDS or an instance role is absent;
other failures do not permit fallback.

## Consequences and validation

Instance metadata access is available but never automatic. An embedding runtime must choose this
provider and its position in a pinned chain. IMDS calls are serialized and cached as AWS recommends,
reducing metadata throttling. Containers should still prefer workload identity and restrict IMDS
access at the network/instance configuration boundary.

Focused loopback tests prove token PUT headers, mandatory token use on both GETs, bounded role-path
construction, credential parsing, cache reuse, forced full-session refresh, rejection of non-link-
local defaults, secret-bearing authorities, and token TTL above six hours. The external consumer
gate references the public factory. Live EC2 IPv4/IPv6, hop-limit/container networking, throttling,
clock-step, and metadata-disable qualification remain deferred.

Invariants 2, 3, 8, 10, 14, 16, and 18 apply.

## Alternatives considered

- **Fall back to IMDSv1:** rejected because it weakens the explicit SSRF-resistant session boundary.
- **Accept any configured URL by default:** rejected because metadata credential fetch is a
  high-value SSRF primitive.
- **Read AWS metadata environment settings automatically:** rejected because network authority and
  credential precedence must be explicit.
- **Cache the role forever but refresh credentials only:** rejected because role association can
  change and a complete bounded refresh is infrequent.
- **Query metadata per S3 attempt:** rejected because it creates avoidable throttling and latency.

## Migration and rollback

Existing credential modes are unchanged. EC2 deployments may explicitly construct this provider or
place it after higher-priority providers in `S3CredentialProviderChain`. No durable, object,
credential, or network format changes.

## References

- [Use IMDSv2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html)
- [Retrieve EC2 role credentials](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-metadata-security-credentials.html)
- [Pinned ordered S3 credential chain](0210-pinned-ordered-s3-credential-chain.md)
- [S3 object-store learning guide](../learning/s3-object-store.md)
- [Architecture invariants](../architecture/invariants.md)
