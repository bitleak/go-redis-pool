
all: setup-redis test

.PHONY: all

test: setup-redis
	go test

setup-redis: nop
	cd scripts/redis && docker-compose up -d && cd ../..

nop:

