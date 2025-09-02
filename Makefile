.PHONY: up down logs ps
up: ; cd docker && docker compose up -d
down: ; cd docker && docker compose down -v
logs: ; cd docker && docker compose logs -f --tail=200
ps: ; cd docker && docker compose ps