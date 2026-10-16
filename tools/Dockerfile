# Контейнер со скриптами: клиент Trino, pyarrow (читает футеры файлов прямо из MinIO),
# matplotlib для графиков и requests для REST API Superset
FROM python:3.12-slim
RUN pip install --no-cache-dir --default-timeout=180 --retries 10 \
      "trino>=0.330,<1" \
      "pyarrow>=17,<22" \
      "matplotlib>=3.8,<4" \
      "requests>=2.31,<3"
ENV PYTHONUNBUFFERED=1
WORKDIR /work
