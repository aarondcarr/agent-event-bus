.PHONY: test race e2e up down demo

test:
	go test ./...

race:
	go test -race -count=1 ./...

up:
	docker compose up -d --build --wait ingest processor jaeger otel-collector

demo:
	docker compose --profile demo run --rm simagent

e2e:
	./scripts/e2e.sh

down:
	docker compose down -v
