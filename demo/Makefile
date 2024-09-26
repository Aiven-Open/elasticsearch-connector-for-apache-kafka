all: build

build:
	docker compose build

up:
	docker compose up -d

test:
	curl -sSL 'http://localhost:8083/connector-plugins' \
		| jq '.[] | select (.class | startswith("io.aiven"))'
