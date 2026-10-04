.PHONY: up down restart logs build ps clean cli

up:
	./start.sh

down:
	./stop.sh

restart:
	docker compose restart

build:
	docker compose build

logs:
	docker compose logs -f

ps:
	docker compose ps

cli:
	docker compose run --rm airflow-cli bash

clean:
	docker compose down -v --remove-orphans
