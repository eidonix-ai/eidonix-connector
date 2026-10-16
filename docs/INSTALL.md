# Installation

Packages for Linux amd64 and arm64: `.apk` (Alpine, OpenRC), `.deb`
(Debian/Ubuntu, systemd) and `.rpm` (Fedora/RHEL family, systemd). Each
installs the binary, the service account `eidonix-connector`, the service
definition, a logrotate policy and the blank configuration
`/etc/eidonix-connector/connector.yaml` (`0640 root:eidonix-connector`). The
service is never enabled or started by installation.

Verify release artefacts before installing: check the signed `SHA256SUMS`
with a release key obtained through an independently trusted channel, then
`sha256sum -c SHA256SUMS`. No release key or signed artefact is part of this
repository.

## 1. Install

Alpine (for example a small LXC container):

```sh
apk add --allow-untrusted ./eidonix-connector_0.2.0_amd64.apk   # after sha256sum -c
```

The `.apk` is not signed with an apk key; its integrity comes from the
verified checksums, which is why `--allow-untrusted` is needed.

Debian / Ubuntu:

```sh
sudo apt install ./eidonix-connector_0.2.0_amd64.deb
```

Fedora / RHEL / Rocky / Alma:

```sh
sudo dnf install ./eidonix-connector_0.2.0_amd64.rpm
```

## 2. Fill in the configuration

Edit `/etc/eidonix-connector/connector.yaml` (see [configuration](CONFIG.md)):

1. Paste the `upstream:` block Eidonix Setup shows when it issues the
   connector's token (url, token, public keys).
2. Add the services this machine shares, by type.
3. Check it, as the service account:

```sh
sudoedit /etc/eidonix-connector/connector.yaml
sudo -u eidonix-connector eidonix-connector check     # Alpine: su -s /bin/sh eidonix-connector -c 'eidonix-connector check'
```

`check` refuses a file other users can read and prints the exact
`chown`/`chmod` to fix it. Keep the file `0640 root:eidonix-connector`: it
holds the token and any service keys. To show the configuration to someone
else, use `eidonix-connector check --redacted`.

For a storage service with `encryption: sse-c`, create the key with
`eidonix-connector generate-key`, paste it as `encryption_key`, and keep a
copy somewhere safe: files cannot be read without it.

## 3. Start

systemd (Debian, Ubuntu, RHEL family):

```sh
sudo systemctl enable --now eidonix-connector
sudo journalctl -u eidonix-connector -f
```

OpenRC (Alpine):

```sh
rc-update add eidonix-connector default
rc-service eidonix-connector start
rc-service eidonix-connector checkconfig     # explain the configuration
```

The connector runs unprivileged as `eidonix-connector` and refuses to run as
root. It needs outbound HTTPS to the upstream (directly or through
`upstream.proxy_url`) and plain connections to the machines its services
name; it listens on no port. Where it may connect is enforced inside the
process from the configuration file, the same on every distribution; you do
not need a separate firewall rule for the connector (compute nodes can stay
open to your LAN). If you also restrict the host's egress, allow the upstream
host on 443 and each service's host:port.

The systemd unit adds an empty capability set, a read-only filesystem except
`/var/log/eidonix-connector`, and a system-call filter. For SELinux enforcing
hosts the unit runs as the default `unconfined_service_t`.

## 4. Changes and logs

After editing the file: `eidonix-connector check`, then
`systemctl reload eidonix-connector` (or `rc-service eidonix-connector reload`).
A reload that fails validation is logged and the running configuration stays;
a successful one reconnects and ends active streams.

Every request is logged to `/var/log/eidonix-connector/access.log` (one JSON
line; never request or response contents). logrotate rotates it daily, keeps
14 days, and signals the connector (SIGUSR1) to reopen the file without
interrupting streams.

## Docker

The image (`Dockerfile`, distroless, runs as UID/GID 65532) expects the file
at `/etc/eidonix-connector/connector.yaml` and an access log path it can
write. The permission gate applies inside the container too: mount a file
owned by UID 65532 with mode `0600`, and point `access_log` at a writable
volume.

```sh
docker build -t eidonix-connector:0.2.0 .
install -m 0600 -o 65532 -g 65532 connector.yaml /srv/eidonix-connector/connector.yaml
docker run -d --name eidonix-connector --restart unless-stopped \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -v /srv/eidonix-connector/connector.yaml:/etc/eidonix-connector/connector.yaml:ro \
  -v /srv/eidonix-connector/log:/var/log/eidonix-connector \
  eidonix-connector:0.2.0
```

(`/srv/eidonix-connector/log` must be writable by UID 65532.)

## Upgrading from v0.1

v0.1 was packaged as `cletus-connector` with `/etc/cletus-connector/config.toml`
plus separate token and secret files. v0.2 reads only `connector.yaml`. Install
the new package next to the old one, then convert:

```sh
sudo eidonix-connector convert /etc/cletus-connector/config.toml > /tmp/connector.yaml
sudo install -m 0640 -o root -g eidonix-connector /tmp/connector.yaml /etc/eidonix-connector/connector.yaml
sudo shred -u /tmp/connector.yaml
sudo -u eidonix-connector eidonix-connector check
```

`convert` reads the old file and the files it references (token, header
secrets, storage credentials and keys, CA files), writes the new file to
standard output and prints a review list to standard error: everything it
could not carry (a flavor's extra paths, headers other than a bearer key,
settings now fixed by type) and every service that became `models: any`.
Read the list, adjust the file, check it, then stop and disable the old
service and enable the new one. The old package can stay installed for
connectors not yet upgraded.
