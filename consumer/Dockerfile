FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python-ng

COPY consumer.py .

CMD ["python", "-u", "consumer.py"]