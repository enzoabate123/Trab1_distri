import os
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def get_env_float(name: str, default: float) -> float:
    """Obtém variável de ambiente como float, com fallback."""
    return float(os.getenv(name, default))

def main():
    """Loop principal que simula o sensor."""
    sensor_id = os.getenv("SENSOR_ID", "sensor-desconhecido")
    topic = os.getenv("KAFKA_TOPIC", "dados-sensores")
    interval = get_env_float("INTERVALO_ENVIO", 2.0)
    
    # Range de simulação
    temp_min = get_env_float("TEMP_MIN", 20.0)
    temp_max = get_env_float("TEMP_MAX", 95.0)
    vibr_min = get_env_float("VIBR_MIN", 0.1)
    vibr_max = get_env_float("VIBR_MAX", 10.0)

    # Brokers (espera uma lista separada por vírgula)
    brokers = os.getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092").split(",")

    print(f"[{sensor_id}] Conectando ao Kafka: {brokers}...")
    
    # Retry simples de conexão
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None
            )
        except Exception as e:
            print(f"[{sensor_id}] Falha ao conectar: {e}. Retentando em 5s...")
            time.sleep(5)

    print(f"[{sensor_id}] Conectado! Enviando dados para '{topic}' a cada {interval}s.")

    while True:
        payload = {
            "timestamp": datetime.now().isoformat(),
            "sensor_id": sensor_id,
            "temperatura": round(random.uniform(temp_min, temp_max), 2),
            "vibracao": round(random.uniform(vibr_min, vibr_max), 2)
        }
        
        # Envia usando sensor_id como chave para manter ordem por sensor numa partição
        producer.send(topic, value=payload)
        
        producer.flush()
        
        print(f"[{sensor_id}] Enviado: {payload}")
        time.sleep(interval)

if __name__ == "__main__":
    main()
