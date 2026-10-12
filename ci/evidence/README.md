# Verification evidence

This directory records development checks, not a signed release or full distro certification.

Current storage checks reported complete:

- The full `go test -race ./...` suite passes after connector-side storage integration.
- `ci/storage-minio.sh` passes against a real TLS MinIO backend: six operations, 0-byte/73-byte/5 MiB + 37-byte objects, ranges, empty SSE-C objects, rejected reads without the SSE-C key, copy/list/delete, explicit probe cleanup, failed small/multipart uploads without partial objects or remaining uploads, preservation of existing objects, prefix confinement, pagination and read/rate budgets.
- These checks cover the standalone connector. They do not establish production gateway/application integration or Ceph/LocalStack compatibility. Current race, vet, vulnerability, Linux amd64/arm64 build and TLS MinIO results are recorded in `storage-checks.txt`. Container-image and package rebuilds remain pending. The vulnerability scan reports no reachable or imported-package vulnerabilities; its one module-only advisory concerns unused OpenPGP code.

Historical packaging checks (before current storage changes):

- Go 1.27.2 Linux amd64/arm64 builds; two successive builds have identical binary bytes (`build.txt`).
- Four unsigned deb/rpm packages using nfpm 2.41.3; CycloneDX SBOMs using syft 1.20.0 are in `dist/` (ignored build artifacts).
- Package installation smoke tests on arm64 Ubuntu 24.04, Debian 12 and Debian 13 disposable containers: user/group, directory permissions, no enable symlink, binary version/hash, root refusal and systemd unit syntax (`package-*-arm64.log`). These containers do not run systemd as PID1; they do not prove the service sandbox or kernel egress filter.
- An arm64 distroless image builds and runs with UID/GID 65532, read-only filesystem, no network and all capabilities dropped (`container-build.txt`, `container-smoke.txt`). The Dockerfile pins both base images by digest and uses temporary in-memory compilation storage. This is a version-command smoke test, not a runtime egress certification.
- Local test/race, vet, fuzz and vulnerability results are recorded in the corresponding logs. See their actual output; workflow definitions alone are not evidence of execution.

**NOT RUN:** the full distro VM matrix (including amd64), real systemd runtime/kernel BPF egress/capability/listener checks, SELinux runtime verification, amd64 container build, image signatures, release signatures, production service compatibility, a 45-minute generation and ten-minute live voice. No release private key was provided; packages are unsigned. Incus/Vagrant/minisign are unavailable here.

Docker smoke inputs were streamed into disposable containers because the daemon could not access the local staging bind mount. No host services or daemon settings were changed. Source inputs in each container were made read-only by permissions. Cached images:

| Image | Architecture | Registry digest |
|---|---|---|
| ubuntu:24.04 | arm64 | `sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3` |
| debian:bookworm-slim | arm64 | `sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171` |
| debian:trixie-slim | arm64 | `sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132` |

For VM certification, run `VERSION=<version> ARCH=amd64 scripts/distro-matrix.sh`, then repeat on an arm64 Incus host with `ARCH=arm64`. Logs include timestamp and distro image, package installation, disabled/stopped service checks, sandbox analysis, a real default-deny kernel egress probe, development gateway runtime, CapEff/NoNewPrivs/no listening sockets, and SIGHUP. Image aliases should be resolved to recorded fingerprints. The script fails if prerequisites or IP filtering are unavailable. Additionally verify logrotate and allowed egress/blocked nonallowlisted LAN egress under the shipped unit; record kernel/systemd/BPF versions. Preserve logs and mark missing checks NOT RUN.

The final package rechecks after the audit-log default fix ran sequentially in disposable containers with a tmpfs-backed chroot (`/work`, `rw,exec,dev`, 1536 MiB). This avoided the Docker VM's full disk without deleting unrelated artifacts. The `dev` flag permits only the four standard device nodes copied into the temporary chroot (`null`, `zero`, `random`, `urandom`); no extra device or privileged-container access was granted. `ci/package-smoke-memory.sh` records the equivalent chroot setup. Final package logs include the installed binary hash, matching `build.txt`.

The initial build and package evidence predates the module move to `github.com/eidonix-ai/cletus-connector`. `publication-checks.txt` records validation of the public module path. Local artifacts in `dist/` retain their original provenance and are not published with the source.

The historical build, package and image logs above must not be interpreted as verification of the current storage implementation. Preserve their provenance when adding new runs.

The Docker MinIO runner accepts `MINIO_TEST_IMAGE`, which must be a reviewed immutable image digest. The local run used the cached default digest in `ci/storage-minio.sh`. Anonymous registry manifest checks returned authorization errors during this work. Set the repository variable `MINIO_TEST_IMAGE` to an accessible reviewed mirror digest to enable the optional storage CI job; it is skipped until configured. The ordinary race suite still runs all recording-backend security tests without Docker.

The public working-tree snapshot was manually checked for customer and infrastructure details and scanned with Gitleaks 8.24.2, including recursive decoding: no leaks found. This is a working-tree scan, not approval to publish historical commits. The initial unpublished commit predates starter-profile sanitization; prepare reviewed, sanitized initial history before the first public push.
