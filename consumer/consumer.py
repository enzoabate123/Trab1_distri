"""Consumidor Kafka responsável por processar dados dos sensores."""

import json
import os
import sqlite3

from kafka import KafkaConsumer


KAFKA_BROKERS = os.environ["KAFKA_BROKERS"].split(",")
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
GROUP_ID = os.environ["GROUP_ID"]
TEMP_LIMITE = float(os.environ["TEMP_LIMITE"])
VIBR_LIMITE = float(os.environ["VIBR_LIMITE"])
DB_PATH = os.environ["DB_PATH"]


def criar_banco():
    """Cria a tabela de alertas caso ainda não exista."""

    conexao = sqlite3.connect(DB_PATH)

    conexao.execute(
        """
        CREATE TABLE IF NOT EXISTS alertas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor TEXT,
            temperatura REAL,
            vibracao REAL,
            motivo TEXT,
            criado_em DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conexao.commit()
    conexao.close()


def salvar_alerta(dados, motivo):
    """Salva uma anomalia detectada no banco SQLite."""

    conexao = sqlite3.connect(DB_PATH, timeout=30)

    sensor = dados.get("sensor_id", dados.get("sensor", "desconhecido"))

    conexao.execute(
        """
        INSERT INTO alertas (
            sensor,
            temperatura,
            vibracao,
            motivo
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            sensor,
            dados.get("temperatura"),
            dados.get("vibracao"),
            motivo,
        ),
    )

    conexao.commit()
    conexao.close()


def main():
    """Consome mensagens Kafka e identifica anomalias."""

    criar_banco()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        value_deserializer=lambda mensagem: json.loads(
            mensagem.decode("utf-8")
        ),
    )

    print(f"Consumidor conectado ao tópico {KAFKA_TOPIC}")

    for mensagem in consumer:
        dados = mensagem.value

        print(
            f"partição={mensagem.partition} "
            f"offset={mensagem.offset} "
            f"dados={dados}"
        )

        motivos = []

        if dados.get("temperatura", 0) > TEMP_LIMITE:
            motivos.append("temperatura")

        if dados.get("vibracao", 0) > VIBR_LIMITE:
            motivos.append("vibracao")

        if motivos:
            motivo = ", ".join(motivos)

            print(f"ANOMALIA: {motivo}")

            salvar_alerta(dados, motivo)


if __name__ == "__main__":
    main()