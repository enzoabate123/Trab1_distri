N ?= 2

up:
	@echo "Subindo Kafka, 3 Sensores e $(N) Consumidores..."
	docker compose up -d --build --scale consumer=$(N)

down:
	@echo "Derrubando tudo e apagando volumes..."
	docker compose down -v

logs:
	docker compose logs -f

scale:
	@echo "Escalando consumidores para $(N)..."
	docker compose up -d --scale consumer=$(N)

fail-broker:
	@echo "Derrubando o kafka-2..."
	docker compose stop kafka-2

fail-consumer:
	@echo "Derrubando UM consumidor aleatório..."
	docker stop $$(docker compose ps -q consumer | head -n 1)

recover:
	@echo "Recuperando containers parados..."
	docker compose start
