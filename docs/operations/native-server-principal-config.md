# Native Server Principal Configuration

Packaged native mutual TLS uses one external allowlist that maps verified client leaf certificates
to stable nonzero protocol principals. Example:

```text
CHRONOSDB_NATIVE_SERVER_PRINCIPALS_V1
7=30aa529b935af809084e419d00f39bce2bf5641da93d7bd9ad71e67bc21de368
42=40aa529b935af809084e419d00f39bce2bf5641da93d7bd9ad71e67bc21de369
```

Each entry is a positive canonical decimal principal ID, `=`, and the lowercase hexadecimal
SHA-256 fingerprint of one client leaf certificate. IDs must be strictly increasing and
fingerprints unique. The file permits no whitespace, comments, blank lines, CR line endings,
duplicate fields, or extra fields. The final LF is optional. Input is limited to 1 MiB and 4,096
principals.

This is coarse native-protocol admission, not authorization to a Raft node, group, leader,
placement epoch, table, or operation. The verified certificate selects only the configured stable
principal. Source IP is deliberately not authority. Raft peer configuration and native client
route configuration are separate trust maps and must not be inferred from this file.

Configure the complete server bundle atomically:

```text
--native-client-principals /etc/chronosdb/native-principals.conf \
--native-tls-cert /etc/chronosdb/native-server.pem \
--native-tls-key /etc/chronosdb/native-server-key.pem \
--native-tls-ca /etc/chronosdb/native-client-ca.pem
```

All four options are required together and require the epoll backend. With the bundle, `--listen`
accepts one canonical nonzero IPv4 address and `chronosd` reports `native_transport=tls`. Without
the bundle, the native listener remains plaintext and may bind only IPv4 loopback. The io_uring
backend never downgrades TLS to plaintext.

The daemon opens final paths without following symlinks and requires bounded nonempty regular
files. The principal file, certificate, and trust store must not be writable by group or other;
the private key must be inaccessible to group and other. It reads each credential completely from
that qualified descriptor and builds the immutable OpenSSL context from those exact PEM bytes; it
does not resolve the configured names again.

To rotate native admission, stage replacements for all four configured names with the same
permission rules, then send `SIGHUP` to `chronosd`. The daemon rereads and validates the complete
bundle before publishing it. Success emits `native_security_reloaded` with the new process-local
generation. Failure emits `native_security_reload_failed` and retains the prior generation. A
successful reload closes connections still inside mutual-TLS handshake so one context cannot be
authorized by another principal generation; already authenticated connections retain their old
certificate decision and principal until disconnect or idle timeout. Revoke those sessions through
a separate connection policy or restart when immediate eviction is required. File watching and
cross-file deployment transactions are not inferred, so do not signal until every name is staged.

TLS verifies the client chain against the configured trust store and requires a client
certificate. Only then does the immutable allowlist compare the verified leaf fingerprint. An
unknown, missing, or unverified certificate is closed before native protocol dispatch. Keep the
same principal ID for the same logical client across certificate rotations when audit identity
continuity matters.
