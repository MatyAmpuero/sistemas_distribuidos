.PHONY: build up down logs client restart

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f server

client:
	docker compose run --rm client

restart:
	docker compose restart server
