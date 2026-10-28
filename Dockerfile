FROM golang:1.27.3@sha256:898838c455eecb68ed7b0b6f0635630ebdd657e732f07a23d0ab475007a52c36 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETARCH
ARG VERSION=0.5.2
RUN --mount=type=tmpfs,target=/tmp GOCACHE=/tmp/go-cache CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false -ldflags "-s -w -buildid= -X main.version=${VERSION}" -o /connector ./cmd/eidonix-connector
FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
COPY --from=build /connector /usr/bin/eidonix-connector
USER 65532:65532
ENTRYPOINT ["/usr/bin/eidonix-connector"]
CMD ["run", "--config", "/etc/eidonix-connector/connector.yaml"]
