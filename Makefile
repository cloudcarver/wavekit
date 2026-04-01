SHELL := /bin/zsh
PROJECT_DIR=$(shell pwd)

.PHONY: dev

###################################################
### Dev 
###################################################

dev:
	docker compose up --build

reload:
	docker compose restart dev

db:
	psql "postgresql://postgres:postgres@localhost:5432/postgres?sslmode=disable"

test:
	TEST_DIR=$(PROJECT_DIR)/e2e HOLD="$(HOLD)" ./scripts/run-local-test.sh "$(K)" 

ut:
	@COLOR=ALWAYS go test -race -covermode=atomic -coverprofile=coverage.out -tags ut ./... 
	@go tool cover -html coverage.out -o coverage.html
	@go tool cover -func coverage.out | fgrep total | awk '{print "Coverage:", $$3}'

REPO=cloudcarver/wavekit

build-docker:
	GOOS=linux GOARCH=amd64 go build -o $(PROJECT_DIR)/bin/wavekit-server-amd64 cmd/main.go
	CXX=aarch64-linux-gnu-g++ CC=aarch64-linux-gnu-gcc CGO_ENABLED=1 GOOS=linux GOARCH=arm64 go build -o $(PROJECT_DIR)/bin/wavekit-server-arm64 cmd/main.go
	docker buildx build --load --platform linux/amd64,linux/arm64 -f dev/Dockerfile.pgbundle -t $(REPO):$(shell cat VERSION)-pgbundle .
	docker buildx build --load --platform linux/amd64,linux/arm64 -f dev/Dockerfile -t $(REPO):$(shell cat VERSION) .

push-docker: build-docker
	docker push $(REPO):$(shell cat VERSION)-pgbundle
	docker push $(REPO):$(shell cat VERSION)
