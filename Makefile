VERSION ?= 0.4.0
export VERSION

.PHONY: all test build package sbom sign clean
# make: static Linux binaries (amd64, arm64) and .deb, .rpm and .apk packages in dist/.
all: package
test:
	go vet ./...
	go test -race ./...
build:
	./scripts/build.sh
package: build
	./scripts/package.sh
sbom:
	./scripts/sbom.sh
sign:
	./scripts/sign.sh
clean:
	rm -rf dist
