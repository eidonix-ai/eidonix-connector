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
apk add --allow-untrusted ./eidonix-connector_0.4.0_amd64.apk   # after sha256sum -c
```

The `.apk` is not signed with an apk key; its integrity comes from the
verified checksums, which is why `--allow-untrusted` is needed.

Debian / Ubuntu:

```sh
sudo apt install ./eidonix-connector_0.4.0_amd64.deb
```

Fedora / RHEL / Rocky / Alma:

```sh
sudo dnf install ./eidonix-connector_0.4.0_amd64.rpm
```

## 2. Fill in the configuration

Edit `/etc/eidonix-connector/connector.yaml` (see [configuration](CONFIG.md)):

1. Set `site` to the connector's name in Eidonix Setup, and paste the
   `upstream:` block Setup shows when it issues the connector's token (url,
   token, public keys).
2. Add the services this machine shares, by name: each one's address and the
   requests it allows.
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
`eidonix-connector generate-key`, paste it as `encryption-key`, and keep a
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
`upstream.proxy`) and plain connections to the machines its services
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
A reload that fails validation is logged and the running configuration stays.
A successful one does not drop traffic: the connector opens a new session with
the new file, and once that is up it drains the old one (tells the hub to send
it nothing new, lets the requests already running finish, then closes it), for
at most the longest `idle-timeout` in the old file (never more than 15
minutes). If the new session cannot be established within a minute, the old
one keeps serving and the log says why. Stopping the service drains the same
way for up to `--drain-timeout` (default 10 minutes; the packaged units allow
11 minutes before they kill the process); a second SIGTERM stops at once.
Restarting after an upgrade therefore waits for running requests to finish.

`eidonix-connector status` shows whether the connector is connected, to which
hub and since when, the last connection error, each service's health,
requests in flight and the requests and errors of the last hour. It reads
`/run/eidonix-connector/status.json`, which the running connector rewrites
every 15 seconds (`0640`, in a `0750` directory owned by the service
account), so run it with `sudo`. `--json` prints the file itself.

Every request is logged to `/var/log/eidonix-connector/access.log` (one JSON
line; never request or response contents). logrotate rotates it daily, keeps
14 days, and signals the connector (SIGUSR1) to reopen the file without
interrupting streams.

## Docker

The image (`Dockerfile`, distroless, runs as UID/GID 65532) expects the file
at `/etc/eidonix-connector/connector.yaml` and an access log path it can
write. The permission gate applies inside the container too: mount a file
owned by UID 65532 with mode `0600`, and point `access-log` at a writable
volume.

```sh
docker build -t eidonix-connector:0.4.0 .
install -m 0600 -o 65532 -g 65532 connector.yaml /srv/eidonix-connector/connector.yaml
docker run -d --name eidonix-connector --restart unless-stopped \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -v /srv/eidonix-connector/connector.yaml:/etc/eidonix-connector/connector.yaml:ro \
  -v /srv/eidonix-connector/log:/var/log/eidonix-connector \
  eidonix-connector:0.4.0
```

(`/srv/eidonix-connector/log` must be writable by UID 65532.) Add
`--stop-timeout 660` so a stop can drain, and, for `eidonix-connector status`
(`docker exec eidonix-connector eidonix-connector status`), a writable status
directory: `--tmpfs /run/eidonix-connector:uid=65532,mode=0750`.

## Upgrading from v0.3

v0.4 reads v0.3 files unchanged and speaks the same protocol (v3), so it works
with v0.3 gateways: one that does not know the `draining` message ignores it
(the old session simply keeps receiving requests until it closes), and
`application` and `for` in the access log stay empty unless the hub sends
them. Install the package and restart the service; from then on reloads and
stops drain instead of cutting running requests. Docker users add
`--stop-timeout 660` (see above).

## Upgrading from v0.2

v0.3 lists services by name instead of grouping them by type, and the
connector no longer knows how Eidonix uses a service: each service lists the
requests it allows. A v0.2 file is refused with the command to run. Convert
it, naming the connector as Eidonix Setup does:

```sh
sudo eidonix-connector convert --site <name in Setup> /etc/eidonix-connector/connector.yaml > /tmp/connector.yaml
sudo -u eidonix-connector eidonix-connector check --config /tmp/connector.yaml   # or as root
sudo install -m 0640 -o root -g eidonix-connector /tmp/connector.yaml /etc/eidonix-connector/connector.yaml
sudo shred -u /tmp/connector.yaml
```

`convert` writes the new file to standard output and a review list to
standard error. It keeps service names (rename them in the file if Eidonix
will address them by new names), turns each type into the requests Eidonix
used of it (`chat` → `POST /v1/*, GET /v1/models`, `embeddings` with
`api_path: /` → `POST /embeddings`, …), turns model aliases around
(`<name on the server>: {alias: <name Eidonix uses>}`), writes out the
limits the old type implied, and sets `health: /health` as v0.2 probed.
`models: any` becomes no `models` (bodies are not inspected), and only the
first of several `urls` is kept. Eidonix's service paths change where
`api_path` was set (for example `/v1/embeddings` → `/embeddings`).

A storage service loses its folder: v0.3 uses the whole bucket. The review
list prints the exact command; with the connector stopped:

```sh
sudo eidonix-connector storage move-to-root <service> --from cletus/ --dry-run
sudo eidonix-connector storage move-to-root <service> --from cletus/
```

It copies every object to the top of the bucket on the storage server (with
the `sse-c` key from the file), verifies every copy, then removes the
originals; it refuses, changing nothing, if any destination already exists.

## Upgrading from v0.1

v0.1 was packaged as `cletus-connector` with `/etc/cletus-connector/config.toml`
plus separate token and secret files. `convert` reads that file too, and the
files it references (token, header secrets, storage credentials and keys, CA
files): install the new package next to the old one, then run the same
`convert --site …` on `/etc/cletus-connector/config.toml`. Its review list
also names everything v0.1 had that could not be carried (a flavor's extra
paths, headers other than a bearer key, settings now fixed internally). Stop
and disable the old service and enable the new one once `check` passes.
