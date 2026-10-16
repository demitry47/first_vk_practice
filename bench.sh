#!/usr/bin/env bash
# Бенчмарк: каждый вариант Bronze пишется отдельным запуском скрипта.
# После записи ждем, пока бакет перестанет уменьшаться, и (только в Docker Desktop) делаем fstrim,
# чтобы удаленные файлы не раздували виртуальный диск и не тормозили следующую запись.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results
BENCH="docker compose run --rm -T toolbox python tools/benchmark.py"

free_disk() {  # вне замеров: отдать освобожденные блоки виртуального диска Docker Desktop
  if docker info --format '{{.OperatingSystem}}' 2>/dev/null | grep -qi "docker desktop"; then
    docker run --rm --privileged --pid=host alpine:3.20 nsenter -t 1 -m -- fstrim -av || true
  fi
}

for v in PARQUET:NONE PARQUET:SNAPPY PARQUET:GZIP PARQUET:ZSTD ORC:NONE ORC:SNAPPY ORC:GZIP ORC:ZSTD; do
  echo "### запись $v"
  $BENCH --writes-only --skip-existing --variants "$v"
  free_disk
done
echo "### генератор, чтение всех вариантов, раскладка Silver"
$BENCH --reads-only
