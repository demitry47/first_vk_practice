# Образ Superset 4.1.2, в который доустановлен SQLAlchemy-драйвер для Trino (trino://...)
FROM apache/superset:4.1.2
USER root
RUN pip install --no-cache-dir "trino[sqlalchemy]>=0.330,<1"
USER superset
