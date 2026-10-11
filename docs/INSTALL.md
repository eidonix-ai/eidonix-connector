# Installation

Targets: Ubuntu 22.04/24.04/26.04, Debian 12/13, Fedora 41+, Rocky/Alma 9/10; Linux amd64/arm64, systemd >=249 with cgroup v2 and working systemd cgroup BPF IP filtering. These are intended targets; consult `ci/evidence` for actual verification. Do not deploy where IP filtering is unavailable.

Verify the release manifest using a release public key received through an independently trusted channel, then verify checksums. No release key or signed artifacts are supplied with this source tree.

```sh
minisign -Vm SHA256SUMS -P "$TRUSTED_RELEASE_PUBLIC_KEY"
sha256sum -c SHA256SUMS
sudo apt install ./cletus-connector_*.deb   # Debian/Ubuntu, select one matching architecture
# Fedora/RHEL family: sudo dnf install ./cletus-connector_*.rpm
```

Package installation creates the service user and log directory; it never enables or starts the service. Copy `/usr/share/doc/cletus-connector/config.example.yaml` to `/etc/cletus-connector/config.yaml`. Set upstreams, obtain the connector token from the gateway operator, and install the independently verified server Ed25519 public key. The release-signing key and server identity key serve different purposes.

```sh
sudo install -m 0640 -o root -g cletus-connector /secure/token /etc/cletus-connector/token
sudo chown root:cletus-connector /etc/cletus-connector/config.yaml
sudo chmod 0640 /etc/cletus-connector/config.yaml
sudo -u cletus-connector cletus-connector check-config
sudo mkdir -p /etc/systemd/system/cletus-connector.service.d
sudo sh -c 'cletus-connector print-systemd-network > /etc/systemd/system/cletus-connector.service.d/network.conf'
sudo systemctl daemon-reload
sudo systemctl enable --now cletus-connector
sudo journalctl -u cletus-connector -f
```

Set the same `HTTPS_PROXY`/`NO_PROXY` environment when generating the drop-in and running the service. A corporate inspection CA may be configured locally; application key verification remains mandatory. DNS changes never justify broadening egress to an entire LAN: regenerate the drop-in, reload systemd, and restart after reviewing the resolved IPs. systemd restricts IPs; the process additionally restricts destination ports. The generated unit permits localhost for a local DNS stub (for example systemd-resolved on 127.0.0.53). A remote DNS resolver is blocked unless explicitly reviewed and added to the host policy; configure a local stub before using hostname refresh. The connector does not silently authorize arbitrary DNS servers.

After editing config, run `check-config`, regenerate the network drop-in if destinations changed, then `systemctl daemon-reload` and `systemctl reload cletus-connector`. SIGHUP validates replacement configuration before replacing the active runtime; rejected changes leave the active configuration running. A successful reload reconnects and ends active streams. Logrotate uses SIGHUP to reopen the audit file through runtime replacement. Retain logs according to local policy; they contain request metadata but no bodies.

For SELinux enforcing RHEL-family hosts, the intended deployment uses the default `unconfined_service_t`; no custom policy or booleans are shipped. VM evidence is required to confirm behavior on a target host.

## Container

```sh
docker buildx build --platform linux/amd64,linux/arm64 -t cletus-connector:local .
docker run --read-only --cap-drop ALL --security-opt no-new-privileges \
  --network connector-egress \
  -v /etc/cletus-connector:/etc/cletus-connector:ro \
  -v /var/log/cletus-connector:/var/log/cletus-connector \
  cletus-connector:local
```

Create a dedicated Docker bridge and apply host firewall rules to its subnet in the `DOCKER-USER` chain (or the corresponding nftables forwarding chain): permit established replies and only resolved cloud/proxy IPs and upstream IP:ports, then reject all other egress. Configure and test DNS separately if hostname refresh is used. Docker's network by itself does not enforce an egress allowlist. Use host-specific rules; incorrect ordering can bypass restrictions. Ensure mounted config/secrets are readable by UID 65532 and the audit directory writable by it, without making secrets world-readable. No listening port is published by default.

## Building a release

`VERSION=0.1.0 make package` requires nfpm 2.41.3. `make sbom` requires syft 1.20.0. `MINISIGN_SECRET_KEY=/secure/release.key make sign` requires minisign; keep the private key outside this repository. Rebuild twice with the same Go version, dependency lockfile and version, and compare binary hashes. Run the distro matrix before declaring support. `IMAGE_DIGEST=registry/name@sha256:... COSIGN_KEY=/secure/image.key scripts/sign-image.sh` signs a published immutable image with operator-provided cosign tooling. This repository does not claim image signatures.
