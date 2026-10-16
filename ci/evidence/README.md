# Verification evidence

This directory holds recorded verification runs for a release. Only recorded
runs count as evidence; a workflow definition is not evidence that it ran.

The v0.1 (cletus-connector) logs were removed with the v0.2 rename: they
certified a source revision, a package name and a configuration format that no
longer exist. They remain in the repository history.

No v0.2 evidence has been recorded yet. Before a release, record here: the
`go test -race ./...`, `go vet ./...`, fuzz and `govulncheck` output; the
reproducible-build check (two builds, identical bytes); the package list from
`make`; package installation on Alpine (OpenRC), Debian/Ubuntu and RHEL-family
hosts (`scripts/distro-matrix.sh`, `ci/verify-installed.sh`); and the TLS MinIO
storage suite (`ci/storage-minio.sh`). Mark anything not run as NOT RUN.
