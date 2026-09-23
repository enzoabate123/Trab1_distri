# 🏭 Monitoramento de Sensores — Kafka + Docker

Trabalho de Distribuição e Concorrência (2026/1).

Balanceamento de carga, elasticidade e failover com Kafka em Docker Compose, rodando em AWS EC2.

## Quickstart

```bash
cp .env.example .env
make up
make logs
```

## Cenários de falha

```bash
make fail-broker      # para um broker Kafka
make fail-consumer    # para um consumidor
make scale N=4        # escala consumidores
make recover          # restaura tudo
```
