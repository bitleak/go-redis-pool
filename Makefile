# Many Go tools take file globs or directories as arguments instead of packages.
PKG_FILES ?= *.go

all: build

.PHONY: all

build: lint
	go build .

test: setup-redis
	go test
	@cd scripts/redis && docker-compose down && cd ../..

coverage: setup-redis
	go test -v -covermode=count -coverprofile=coverage.out
	$(HOME)/gopath/bin/goveralls -coverprofile=coverage.out -service=travis-ci -repotoken $(COVERAGE_TOKEN)
	@cd scripts/redis && docker-compose down && cd ../..

lint:
	@rm -rf lint.log
	@printf $(CCCOLOR)"Checking format...\n"$(ENDCOLOR)
	@gofmt -d -s $(PKG_FILES) 2>&1 | tee lint.log
	@printf $(CCCOLOR)"Checking vet...\n"$(ENDCOLOR)
	@go vet $(PKG_FILES) 2>&1 | tee -a lint.log;
	@[ ! -s lint.log ]

setup-redis: nop
	cd scripts/redis && docker-compose up -d && cd ../..

teardown-redis: nop
	cd scripts/redis && docker-compose down && cd ../..

nop:

