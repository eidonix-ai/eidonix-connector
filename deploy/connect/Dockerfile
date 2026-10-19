# eidonix-connect, the hub: a static binary on distroless, running as 65532.
# Build from the repository root:
#   docker build -f deploy/connect/Dockerfile -t eidonix-connect .
FROM golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETARCH
ARG VERSION=0.4.0
RUN --mount=type=tmpfs,target=/tmp GOCACHE=/tmp/go-cache CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false -ldflags "-s -w -buildid= -X main.version=${VERSION}" -o /connect ./cmd/eidonix-connect \
 && mkdir -p /out/var/log/eidonix-connect
FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
COPY --from=build /connect /usr/bin/eidonix-connect
# The access-log volume starts out owned by the hub's user.
COPY --from=build --chown=65532:65532 /out/var/log/eidonix-connect /var/log/eidonix-connect
USER 65532:65532
ENTRYPOINT ["/usr/bin/eidonix-connect"]
CMD ["serve", "--config", "/etc/eidonix-connect/connect.yaml"]
