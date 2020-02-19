
all: setup-redis test

.PHONY: all

test: setup-redis
	go test
	@cd scripts/redis && docker-compose down && cd ../..

setup-redis: nop
	cd scripts/redis && docker-compose up -d && cd ../..

teardown-redis: nop
	cd scripts/redis && docker-compose down && cd ../..

nop:

