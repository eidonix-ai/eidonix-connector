# Installation

Targets: Ubuntu 22.04/24.04/26.04, Debian 12/13, Fedora 41+, Rocky/Alma 9/10; Linux amd64/arm64, systemd >=249 with cgroup v2 and working systemd cgroup BPF IP filtering. Alpine Linux (OpenRC) is also packaged; see [Alpine (OpenRC)](#alpine-openrc) for how its egress restriction differs. These are intended targets; consult `ci/evidence` for actual verification. Do not deploy where IP filtering is unavailable.

Verify the release manifest using a release public key received through an independently trusted channel, then verify checksums. No release key or signed artifacts are supplied with this source tree.

```sh
minisign -Vm SHA256SUMS -P "$TRUSTED_RELEASE_PUBLIC_KEY"
sha256sum -c SHA256SUMS
sudo apt install ./eidonix-connector_*.deb   # Debian/Ubuntu, select one matching architecture
# Fedora/RHEL family: sudo dnf install ./eidonix-connector_*.rpm
# Alpine: apk add --allow-untrusted ./eidonix-connector_*.apk (see below)
```

Package installation creates the service user and log directory; it never enables or starts the service. Copy `/usr/share/doc/eidonix-connector/config.example.toml` to `/etc/eidonix-connector/config.toml` (the only configuration file and format; see [configuration](CONFIG.md)). Add one `[[routes]]` entry per local service, with `[routes.models]` aliases where the gateway's model names differ from the upstream ids. Set upstreams, obtain the connector token from the gateway operator, and install the independently verified server Ed25519 public key. The release-signing key and server identity key serve different purposes.

```sh
sudo install -m 0640 -o root -g eidonix-connector /secure/token /etc/eidonix-connector/token
sudo chown root:eidonix-connector /etc/eidonix-connector/config.toml
sudo chmod 0640 /etc/eidonix-connector/config.toml
sudo -u eidonix-connector eidonix-connector check-config
sudo mkdir -p /etc/systemd/system/eidonix-connector.service.d
sudo --preserve-env=HTTPS_PROXY,NO_PROXY,https_proxy,no_proxy sh -c 'eidonix-connector print-systemd-network > /etc/systemd/system/eidonix-connector.service.d/network.conf'
sudoedit /etc/systemd/system/eidonix-connector.service.d/network.conf
sudo systemctl daemon-reload
sudo systemctl enable --now eidonix-connector
sudo journalctl -u eidonix-connector -f
```

DNS review is mandatory before enabling the unit. The generator permits localhost and prints remote `/etc/resolv.conf` nameserver IPs as commented `# IPAddressAllow=` lines. Review and uncomment the resolvers this host actually uses, or configure a local DNS stub. Debian and RHEL-family installations commonly use remote resolvers; leaving their lines commented prevents startup DNS and periodic refresh. Do not allow an entire LAN to solve a DNS failure. Regenerate and review the drop-in after resolver or endpoint address changes. systemd filters IPs; application dialers additionally restrict upstream/cloud destination ports.

Use the same proxy settings for generation, validation and the service. `sudo` normally discards them, so preserve only the relevant variables as shown above; for validation use `sudo --preserve-env=HTTPS_PROXY,NO_PROXY,https_proxy,no_proxy -u eidonix-connector eidonix-connector check-config`. Configure the service's proxy separately in a local unit drop-in, for example:

```ini
[Service]
Environment="HTTPS_PROXY=http://proxy.example.com:3128"
Environment="NO_PROXY=localhost,127.0.0.1"
```

`sudo` environment preservation does not configure systemd. A local root-owned `EnvironmentFile` is another option when proxy credentials are needed. Protect that file and never publish it. TLS inspection requires an explicitly trusted CA; the handshake signature remains checked, but it does not prevent a trusted proxy from relaying the server's signed handshake or observing/injecting traffic (see [security](SECURITY.md)).

Development builds before TOML read `config.yaml`; that file is no longer read. Rewrite it as `config.toml` (keys keep their names) and run `check-config`.

After editing config, run `check-config`, regenerate the network drop-in if destinations changed, then `systemctl daemon-reload` and `systemctl reload eidonix-connector`. SIGHUP validates replacement configuration before replacing the active runtime; rejected changes leave the active configuration running. A successful reload reconnects and ends active streams. Logrotate uses SIGUSR1 to reopen the audit file without reloading configuration, reconnecting or interrupting active streams. A local `systemctl kill --kill-who=main --signal=USR1 eidonix-connector` performs the same reopen; failure to open the replacement keeps the previous file open and is logged. Retain logs according to local policy; they contain request metadata but no bodies.

For SELinux enforcing RHEL-family hosts, the intended deployment uses the default `unconfined_service_t`; no custom policy or booleans are shipped. VM evidence is required to confirm behavior on a target host.

## Alpine (OpenRC)

The `.apk` package installs the same binary, configuration example and logrotate policy, plus an OpenRC service (`/etc/init.d/eidonix-connector`, run under `supervise-daemon` as the `eidonix-connector` user). It suits small hosts such as an Alpine LXC container.

The package is not signed with an apk key; its integrity comes from the verified `SHA256SUMS` above, so install it with `--allow-untrusted` only after `sha256sum -c` passes.

```sh
apk add logrotate                      # optional; rotation of the audit log
apk add --allow-untrusted ./eidonix-connector_*_amd64.apk
cp /usr/share/doc/eidonix-connector/config.example.toml /etc/eidonix-connector/config.toml
install -m 0640 -o root -g eidonix-connector /secure/token /etc/eidonix-connector/token
chown root:eidonix-connector /etc/eidonix-connector/config.toml
chmod 0640 /etc/eidonix-connector/config.toml
su -s /bin/sh eidonix-connector -c 'eidonix-connector check-config'
rc-update add eidonix-connector default
rc-service eidonix-connector start
tail -f /var/log/messages               # service output goes to syslog (tag eidonix-connector)
```

The token file must be a regular file owned by root with mode 0600 or 0640; anything else (for example 0644) is refused, including on reload.

Day to day:

- `rc-service eidonix-connector reload` validates and applies a changed configuration (SIGHUP; rejected changes leave the running configuration in place; a successful reload reconnects and ends active streams).
- `rc-service eidonix-connector reopen` reopens the audit log (SIGUSR1); the packaged logrotate policy calls it after rotation.
- `rc-service eidonix-connector status`, `stop`, `restart` behave as usual; `supervise-daemon` restarts the connector if it exits.

**Egress.** OpenRC has no equivalent of the systemd unit's sandbox or cgroup IP filter, and `print-systemd-network` does not apply. The connector's own dial allowlist (upstream and cloud destinations from the configuration) still applies. Restrict the host's egress outside the connector: on an LXC container, the hypervisor firewall for the container (for example the Proxmox VE firewall with an outbound default of DROP), otherwise nftables on the host. Permit DNS to the resolvers in use, the gateway's IPs on 443 (or the proxy), and each upstream IP:port; reject everything else. Review the rules whenever an endpoint or resolver address changes.

## Container

```sh
docker buildx build --platform linux/amd64,linux/arm64 -t eidonix-connector:local .
docker run --read-only --cap-drop ALL --security-opt no-new-privileges \
  --network connector-egress \
  -v /etc/eidonix-connector:/etc/eidonix-connector:ro \
  -v /var/log/eidonix-connector:/var/log/eidonix-connector \
  eidonix-connector:local
```

Create a dedicated Docker bridge and apply host firewall rules to its subnet in the `DOCKER-USER` chain (or the corresponding nftables forwarding chain): permit established replies and only resolved cloud/proxy IPs and upstream IP:ports, then reject all other egress. Configure and test DNS separately if hostname refresh is used. Docker's network by itself does not enforce an egress allowlist. Use host-specific rules; incorrect ordering can bypass restrictions. Ensure mounted config/secrets are readable by UID 65532 and the audit directory writable by it, without making secrets world-readable. No listening port is published by default.

## Building a release

`VERSION=0.1.0 make package` requires nfpm 2.41.3. `make sbom` requires syft 1.20.0. `MINISIGN_SECRET_KEY=/secure/release.key make sign` requires minisign; keep the private key outside this repository. Rebuild twice with the same Go version, dependency lockfile and version, and compare binary hashes. Run the distro matrix before declaring support. `IMAGE_DIGEST=registry/name@sha256:... COSIGN_KEY=/secure/image.key scripts/sign-image.sh` signs a published immutable image with operator-provided cosign tooling. This repository does not claim image signatures.


## Provisioning object storage

Provision the bucket outside the connector with operator credentials. Choose a dedicated bucket when possible, then create a restricted service credential. Store its two-line access-key/secret file and any SSE-C key file root-owned, mode 0600 or 0640, readable only by the service group. Use operator-selected paths from [configuration](CONFIG.md). The connector never creates buckets or changes ACLs, policies, encryption defaults, lifecycle or versioning.

This IAM-style template grants exactly six S3 actions. Replace the neutral bucket and prefix with your local choices, and review backend policy semantics:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::documentstore-example/documents/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::documentstore-example",
      "Condition": {"StringLikeIfExists": {"s3:prefix": "documents/*"}}
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucketMultipartUploads",
      "Resource": "arn:aws:s3:::documentstore-example"
    }
  ]
}
```

`HeadBucket` uses `ListBucket`; the `IfExists` condition allows requests without a prefix context and constrains list requests when that context is present. This follows [AWS condition semantics](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html) and [HeadBucket permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html). Confirm equivalent behavior on your backend. Multipart-list permission may expose in-progress key names throughout the bucket when the backend cannot scope it by prefix, so use a dedicated bucket. No `ListBuckets`, policy, ACL, lifecycle, versioning or presign administration grant is included. SSE-KMS may additionally need a separately reviewed KMS policy.

Run read-only checks before enabling the route:

```sh
sudo --preserve-env=HTTPS_PROXY,NO_PROXY,https_proxy,no_proxy -u eidonix-connector eidonix-connector check-config
# Explicitly permits a small temporary upload/read/delete probe:
sudo --preserve-env=HTTPS_PROXY,NO_PROXY,https_proxy,no_proxy -u eidonix-connector eidonix-connector check-config --write-probe
```

Default checks inspect the bucket, list one object under the prefix, check an existing nonempty object's encryption when available, check multipart-list permission, and test whether account-wide bucket listing succeeds. A successful account-wide list produces an over-privilege warning; it is not a required permission. These checks do not write objects. The write probe uses a random path beneath the reserved local `.eidonix-connector/probe/` area, verifies a round trip and deletion, and attempts cleanup on failure. For SSE-C it also checks that reading without the key fails. Investigate any reported leftover object or failed abort. Neither the gateway, normal service startup nor installation scripts can request the write probe.

After reviewing existing bucket lifecycle rules, use your administrative tools to add an incomplete-multipart cleanup rule under the chosen prefix. Merge it with existing rules; the connector never installs or reads this lifecycle configuration:

```json
{
  "Rules": [{
    "ID": "connector-incomplete-uploads",
    "Status": "Enabled",
    "Filter": {"Prefix": "documents/"},
    "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}
  }]
}
```

The local sweep runs at startup and every six hours. It aborts only matching uploads older than at least 24 hours, skipping this process's active IDs. All instances must honor the six-hour upload lifetime and synchronized clocks. Missing multipart-list permission causes a warning; operator lifecycle cleanup then remains essential. Lowering upload lifetime is permitted; lowering cleanup age below 24 hours is rejected.

AWS currently blocks SSE-C writes by default for new buckets and some existing accounts. If you select SSE-C, an operator must deliberately allow it in the bucket's encryption configuration using separate administrative credentials; the connector cannot do so. See [AWS SSE-C guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html).

## Isolating document conversion

For Gotenberg, disable `downloadFrom` and isolate the converter's egress. A locally allowed LibreOffice conversion body can contain remote links; the connector does not inspect every embedded document resource. Use a reviewed image digest and a dedicated internal network with no published converter port:

```yaml
services:
  converter:
    image: <reviewed-gotenberg-image-digest>
    command: [gotenberg, --api-disable-download-from]
    networks: [conversion]
  connector:
    image: <reviewed-connector-image-digest>
    networks: [conversion, cloud-egress]
networks:
  conversion:
    internal: true
  cloud-egress: {}
```

Apply the connector egress restrictions described above to the cloud network. Attach only the connector and converter to the conversion network; do not attach the converter to other networks. Configure the local converter origin explicitly. The disable flag is documented in [Gotenberg configuration](https://gotenberg.dev/docs/configuration). Verify it against the deployed version; the example is a deployment pattern, not evidence of a completed converter test.
