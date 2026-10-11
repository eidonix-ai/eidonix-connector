.PHONY: test build package sbom sign
test:
	go test -race ./...
build:
	./scripts/build.sh
package: build
	./scripts/package.sh
sbom:
	./scripts/sbom.sh
sign:
	./scripts/sign.sh
