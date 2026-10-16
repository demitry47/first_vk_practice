#!/usr/bin/env bash
# Прогон целиком: стек -> ELT -> Iceberg (часть 2) -> бенчмарк -> Superset -> сводка.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results
RUN="docker compose run --rm -T toolbox python"

docker compose up -d
docker compose build toolbox
docker compose ps -a --format "table {{.Name}}\t{{.Image}}\t{{.Status}}" | tee results/docker_ps.txt

$RUN tools/pipeline.py       2>&1 | tee results/log_pipeline.txt
$RUN tools/iceberg_part2.py  2>&1 | tee results/log_iceberg.txt
./bench.sh                    2>&1 | tee results/log_bench.txt

docker compose --profile bi up -d --build superset
$RUN tools/superset_bi.py    2>&1 | tee results/log_superset.txt
$RUN tools/summary.py        2>&1 | tee results/log_summary.txt
echo "Готово. Сводка: results/summary.md, Superset: http://localhost:8088 (admin / admin)"
