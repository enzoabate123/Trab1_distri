N ?= 2
ESPERA ?= 45
INTERVALO_CARGA ?= 0.1
ETAPA ?= manual
EVIDENCIAS ?= logs/relatorio-$(shell date +%Y%m%d-%H%M%S)
export EVIDENCIAS

-include .env
KAFKA_TOPIC ?= dados-sensores
GROUP_ID ?= processadores

.PHONY: up down logs scale fail-broker fail-consumer recover status topic group offsets alerts evidence load normal report help

up:
	@echo "Subindo Kafka, 3 Sensores e $(N) Consumidores..."
	docker compose up -d --build --scale consumer=$(N)

down:
	@echo "Derrubando containers e preservando o banco de alertas..."
	docker compose down

logs:
	docker compose logs -f

scale:
	@echo "Escalando consumidores para $(N)..."
	docker compose up -d --scale consumer=$(N)

fail-broker:
	@echo "Derrubando o kafka-2..."
	docker compose stop kafka-2

fail-consumer:
	@echo "Derrubando o primeiro consumidor listado..."
	docker stop $$(docker compose ps -q consumer | head -n 1)

recover:
	@echo "Recuperando containers parados..."
	docker compose start

status:
	docker compose ps -a

topic:
	docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic "$(KAFKA_TOPIC)"

group:
	docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group "$(GROUP_ID)" --members --verbose

offsets:
	docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group "$(GROUP_ID)"

alerts:
	docker compose run --rm --no-deps -T consumer python -c 'import os, sqlite3; c = sqlite3.connect("file:" + os.environ["DB_PATH"] + "?mode=ro", uri=True); print("Total de alertas:", c.execute("SELECT COUNT(*) FROM alertas").fetchone()[0]); print("Ultimos 10: id, sensor, temperatura, vibracao, motivo, criado_em"); [print(row) for row in c.execute("SELECT * FROM alertas ORDER BY id DESC LIMIT 10")]; c.close()'

# Duas amostras permitem comparar o progresso dos offsets e dos alertas.
evidence:
	@mkdir -p "$(EVIDENCIAS)/$(ETAPA)"
	make --no-print-directory status topic group offsets alerts > "$(EVIDENCIAS)/$(ETAPA)/antes.txt" 2>&1
	sleep 10
	make --no-print-directory offsets alerts > "$(EVIDENCIAS)/$(ETAPA)/depois.txt" 2>&1
	docker compose logs --no-color --timestamps --since 3m > "$(EVIDENCIAS)/$(ETAPA)/containers.log" 2>&1
	@echo "Evidencias salvas em $(EVIDENCIAS)/$(ETAPA)"

# Override temporario: aumenta a carga sem editar o .env.
load:
	@set -eu; override=$$(mktemp); trap 'rm -f "$$override"' EXIT; \
	printf 'services:\n  sensor-1:\n    environment:\n      INTERVALO_ENVIO: "$(INTERVALO_CARGA)"\n  sensor-2:\n    environment:\n      INTERVALO_ENVIO: "$(INTERVALO_CARGA)"\n  sensor-3:\n    environment:\n      INTERVALO_ENVIO: "$(INTERVALO_CARGA)"\n' > "$$override"; \
	docker compose -f docker-compose.yml -f "$$override" up -d --no-deps sensor-1 sensor-2 sensor-3

normal:
	docker compose up -d --no-deps sensor-1 sensor-2 sensor-3

# Executar com o cluster disponivel; interrompe se um comando falhar.
# As evidencias devem ser interpretadas: nao e uma prova automatica de zero perdas.
report:
	$(MAKE) up N=2
	sleep $(ESPERA)
	$(MAKE) evidence ETAPA=01-normal
	$(MAKE) fail-consumer
	sleep $(ESPERA)
	$(MAKE) evidence ETAPA=02-falha-consumidor
	$(MAKE) recover
	sleep $(ESPERA)
	$(MAKE) evidence ETAPA=03-recuperacao-consumidor
	$(MAKE) fail-broker
	sleep $(ESPERA)
	$(MAKE) evidence ETAPA=04-falha-broker
	$(MAKE) recover
	sleep $(ESPERA)
	$(MAKE) evidence ETAPA=05-recuperacao-broker
	$(MAKE) load
	sleep $(ESPERA)
	$(MAKE) evidence ETAPA=06-carga-dois-consumidores
	$(MAKE) scale N=4
	$(MAKE) load
	sleep $(ESPERA)
	$(MAKE) evidence ETAPA=07-carga-quatro-consumidores
	$(MAKE) scale N=2
	$(MAKE) load
	sleep $(ESPERA)
	$(MAKE) evidence ETAPA=08-reducao-consumidores
	$(MAKE) normal
	sleep $(ESPERA)
	$(MAKE) evidence ETAPA=09-final
	@echo "Coleta concluida em $(EVIDENCIAS). Compare antes/depois e os logs de cada etapa."

help:
	@echo "make report                  Executa os cenarios e salva evidencias (cerca de 9 minutos + inicializacao)."
	@echo "make report ESPERA=90        Usa mais tempo para inicializacao e rebalanco."
	@echo "make status topic group offsets alerts   Consulta o estado atual."
	@echo "make evidence ETAPA=manual   Salva duas amostras e logs sem provocar falhas."
	@echo "make load / make normal      Aumenta carga / restaura intervalo do .env."
	@echo "Se interromper report: make recover, make scale N=2 e make normal."
	@echo "make down preserva o volume SQLite; nao use down -v antes de guardar os resultados."
