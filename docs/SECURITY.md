# Security

## Trust model

The organization that owns the hardware trusts Eidonix not to misuse it; that
is covered by the agreement between them, not by the connector. So the
connector does not decide *which* Eidonix organizations or users may use a
service: Eidonix Setup does (an administrator binds services to
organizations). The connector records the organization and user Eidonix
reports for every request, in its own access log, so the owner can compare
it with Eidonix's usage records; it does not authorise on them.

What the connector guarantees is that it is secure within its own
configuration:

- **It exposes exactly what the file says.** Each service accepts exactly the
  requests its `allowed-endpoints` lists and nothing else. Paths are
  canonicalised strictly first (no encodings, dot segments, empty segments or
  queries other than on GET) and matched exactly, or below a final `/*` the
  owner wrote. Request and response headers are allowlisted, redirects are
  never followed, sizes and idle time are bounded, and when a service lists
  models, requests naming any other model are refused before anything is
  sent. A `/*` entry allows everything below it: list narrower paths when the
  server offers more than Eidonix should reach.
- **It reaches exactly the machines the file names.** Every outbound
  connection goes through a dialer pinned to the upstream, the optional
  proxy, and each service's address, at the IPs those names resolved to at
  start. DNS changes cannot widen that; there is no implicit localhost.
  Upstream calls never use a proxy.
- **It talks only to the real Eidonix**, as far as a signature can show
  (below).
- **Credentials stay on the box.** Service keys, storage credentials and the
  storage encryption key are used by the connector and never sent to
  Eidonix. Eidonix cannot change the configuration, run commands or choose a
  destination; a stream names only a configured service.

It does not protect a service from what an allowed request can make it do:
model and conversion servers may fetch URLs found in request bodies (image
URLs, document links, converter directives). Restrict those servers' own
egress and disable remote fetching where possible.

## Configuration file

The file holds the token, service keys and storage secrets, so the file
itself is the secret to protect. `run` and `check` refuse a file that is not
owned by root (or, for development, by the non-root user running it), that
other users can access, or that its group can write, and print the exact fix.
Secrets are never logged; `check --redacted` prints the file with each secret
replaced by `•••• (set)`. Back up the file securely, especially an
`encryption-key`.

## Upstream identity (review item M5)

v0.1 sent the connection token in the WebSocket upgrade before the server had
proved anything. Since v0.2 the order is:

1. The connector's upgrade request carries only a fresh 32-byte nonce and its
   instance id: no credential.
2. The server answers with a signature over
   `eidonix-connector.v3|nonce|instance|server time` and the id of its key.
3. The connector checks the key is one of its `public-keys`, the time is
   within five minutes, and the signature. On any failure it closes the
   connection; the token is never sent.
4. Only then does it send the token on the multiplexed control stream.

So an impostor without a configured key, including one holding a TLS
certificate the host trusts, never learns the token.

**Remaining limitation.** The signature is not bound to the TLS channel. A
TLS-inspecting proxy that the host trusts (its CA is in the system store or
`ca-certificate`) can relay the challenge to the real Eidonix, pass back the
valid signature, and then read the token and every request, or inject
requests the configuration allows. Pinning keys does not stop a trusted
man-in-the-middle; treat any TLS inspection infrastructure the host trusts as
part of the security boundary. Closing this fully would need an
application-layer key exchange bound to the signature, which v0.3 does not
implement.

After authentication the connector names its `site`; Eidonix refuses a
session whose site is not the connector's name in Setup, so a token pasted
into the wrong machine's file is caught rather than serving the wrong
services.

Key rotation: Eidonix publishes the new key in Setup; list both old and new
in `public-keys`, reload, let Eidonix switch, then remove the old key. Never
accept a key learned only from the connection it is meant to verify.

## Storage

Eidonix can ask a storage service only to put, get, head, list, delete or
copy files inside the configured bucket (the whole bucket belongs to
Eidonix; a `read-only` service refuses put, delete and copy). It cannot supply an
endpoint, credentials or an encryption key, reach the reserved
`.eidonix-connector/` folder, administer buckets or mint presigned links.
Keys are validated before storage is contacted; uploads commit only after
byte counts, SHA-256 and a clean end of stream verify.

These limits do not give per-user file authorization: Eidonix must authorize
each user and file itself. Whoever controls the Eidonix side can read every
file in the bucket as plaintext through `get`, and overwrite or delete files
unless the service is read-only.

**What `encryption` does and does not protect:**

- `sse-c`: the storage encrypts each file with a key that is kept on this
  machine and sent to the storage service with each request (over verified
  HTTPS; `sse-c` refuses plain HTTP). It protects files at rest against
  someone who has the disks, a backup, or storage credentials without the
  key. It does **not** hide content from Eidonix (the connector decrypts on
  `get`), from a compromised connector host, or from a malicious live
  storage server, which sees the key in each request.
- `sse-s3`: the storage encrypts with its own keys. Anyone with the storage
  credentials reads plaintext.
- `none`: files are stored as they arrive. This is the owner's decision, for
  storage the organization already secures on its premises; `check` states
  it plainly.

Content protected from every party except the user needs client-side
encryption before upload (Cletus vault folders); such files pass through the
connector and storage as ciphertext whatever the `encryption` setting.

## Access log

Every request is logged as one JSON line: service, method, path, status,
sizes, duration, model, and the organization, user key and request id
Eidonix reported. Bodies, prompts, outputs, file names and secrets are never
logged; storage keys appear only as keyed hashes. The metadata is still
sensitive: restrict and rotate the log.

## Reporting vulnerabilities

Use [GitHub private reporting](https://github.com/eidonix-ai/eidonix-connector/security/advisories/new)
if the repository has it enabled; otherwise ask a maintainer for a private
contact without posting details, tokens or configurations publicly.
Contributions must use synthetic data and documentation addresses
(`example.com`, `10.0.0.x`, `203.0.113.x`); CI scans for secrets.
