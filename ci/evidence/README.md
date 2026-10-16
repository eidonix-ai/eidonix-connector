# Verification evidence

This directory holds recorded verification runs for a release. Only recorded
runs count as evidence; a workflow definition is not evidence that it ran.

The v0.1 logs were removed with the v0.2 rename: they
certified a source revision, a package name and a configuration format that no
longer exist. They remain in the repository history.

Recorded for v0.2 so far: `package-smoke-0.2.0-arm64.txt` (Debian 12 and
Alpine package installs in disposable containers: service account, config
ownership 0640 root:eidonix-connector, root refusal, the blank template
refused by check, the permission gate's fix-it message). Not run: RPM and
amd64 installs, real systemd/OpenRC runtime.

Before a release, record here: the
`go test -race ./...`, `go vet ./...`, fuzz and `govulncheck` output; the
reproducible-build check (two builds, identical bytes); the package list from
`make`; package installation on Alpine (OpenRC), Debian/Ubuntu and RHEL-family
hosts (`scripts/distro-matrix.sh`, `ci/verify-installed.sh`); and the TLS MinIO
storage suite (`ci/storage-minio.sh`). Mark anything not run as NOT RUN.
