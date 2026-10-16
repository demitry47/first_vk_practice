"""Часть 2 (п. 6.2): Apache Superset поверх Lakehouse.

Подключение "Trino / Iceberg" контейнер Superset создает сам при старте. Скрипт через REST API:
  1. выполняет в SQL Lab запрос к Bronze (iceberg.bronze.lineitem) и сохраняет ответ
     в results/superset_bronze_query.json - это подтверждение, что BI читает сырой слой;
  2. сохраняет этот запрос в SQL Lab;
  3. заводит датасеты bronze.orders, gold.q18_answer, gold.bulk_orders_by_year,
     gold.order_qty_histogram;
  4. строит графики и собирает из них дашборд.

    docker compose --profile bi up -d superset
    docker compose run --rm -T toolbox python tools/superset_bi.py
"""
from __future__ import annotations

import json
import os
import time

import requests

from common import QTY_LIMIT, info, write_json

URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
DB_NAME = "Trino / Iceberg"
DASHBOARD = "TPC-H Q18: оптовые заказы"
SAVED_QUERY = "Bronze: заказы по суммарному количеству товара"
http = requests.Session()


def api(method: str, path: str, **kw):
    r = http.request(method, f"{URL}/api/v1/{path}", timeout=600, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path}: HTTP {r.status_code} {r.text[:400]}")
    return r.json() if r.content else {}


def sign_in() -> None:
    for attempt in range(60):
        try:
            if requests.get(f"{URL}/health", timeout=5).ok:
                break
        except requests.RequestException:
            pass
        info(f"Superset еще стартует ({attempt + 1})")
        time.sleep(5)
    token = api("POST", "security/login", json={"username": "admin", "password": "admin",
                                                 "provider": "db", "refresh": True})["access_token"]
    http.headers["Authorization"] = f"Bearer {token}"
    try:
        http.headers["X-CSRFToken"] = api("GET", "security/csrf_token/")["result"]
        http.headers["Referer"] = URL
    except RuntimeError:
        pass  # в superset_config.py CSRF выключен


def find_id(resource: str, field: str, value: str):
    q = f"(filters:!((col:{field},opr:eq,value:'{value}')),page_size:100)"
    found = api("GET", f"{resource}/", params={"q": q})["result"]
    return found[0]["id"] if found else None


def get_dataset(db: int, schema: str, table: str) -> int:
    q = f"(filters:!((col:table_name,opr:eq,value:'{table}')),page_size:100)"
    for ds in api("GET", "dataset/", params={"q": q})["result"]:
        if ds.get("schema") == schema:
            return ds["id"]
    return api("POST", "dataset/", json={"database": db, "schema": schema, "table_name": table})["id"]


def sql_metric(expr: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


def put_chart(name: str, viz: str, ds: int, params: dict, dash: int) -> int:
    existing = find_id("chart", "slice_name", name)
    params = {"viz_type": viz, "datasource": f"{ds}__table", **params}
    body = {"slice_name": name, "viz_type": viz, "datasource_id": ds, "datasource_type": "table",
            "params": json.dumps(params, ensure_ascii=False), "dashboards": [dash]}
    if existing:
        api("PUT", f"chart/{existing}", json=body)
        return existing
    return api("POST", "chart/", json=body)["id"]


def main() -> None:
    sign_in()
    db = find_id("database", "database_name", DB_NAME)
    if not db:
        raise RuntimeError(f"подключения «{DB_NAME}» нет, смотри docker logs q18-superset")
    info(f"Superset видит Trino, database id = {db}")

    bronze_sql = (f"SELECT size_class, count(*) AS orders FROM (\n"
                  f"  SELECT orderkey,\n"
                  f"         CASE WHEN sum(quantity) > {QTY_LIMIT} THEN 'оптовый (> {QTY_LIMIT})'\n"
                  f"              WHEN sum(quantity) > 200 THEN 'большой (201–{QTY_LIMIT})'\n"
                  f"              ELSE 'обычный (до 200)' END AS size_class\n"
                  f"  FROM iceberg.bronze.lineitem\n"
                  f"  GROUP BY orderkey) t\n"
                  f"GROUP BY size_class ORDER BY orders DESC")
    info("SQL Lab: запрос по Bronze")
    t0 = time.time()
    res = api("POST", "sqllab/execute/", json={"database_id": db, "sql": bronze_sql, "schema": "bronze",
                                                "runAsync": False, "json": True})
    write_json("superset_bronze_query.json",
               {"sql": bronze_sql, "seconds": round(time.time() - t0, 2),
                "columns": [c.get("column_name", c.get("name")) for c in res.get("columns", [])],
                "data": res.get("data")})
    if not find_id("saved_query", "label", SAVED_QUERY):
        api("POST", "saved_query/", json={"db_id": db, "schema": "bronze", "sql": bronze_sql, "label": SAVED_QUERY})

    dash = find_id("dashboard", "dashboard_title", DASHBOARD) or \
        api("POST", "dashboard/", json={"dashboard_title": DASHBOARD, "published": True})["id"]

    ds_orders = get_dataset(db, "bronze", "orders")
    ds_answer = get_dataset(db, "gold", "q18_answer")
    ds_year = get_dataset(db, "gold", "bulk_orders_by_year")
    ds_hist = get_dataset(db, "gold", "order_qty_histogram")

    charts = {
        "Bronze: статусы заказов по приоритетам": put_chart(
            "Bronze: статусы заказов по приоритетам", "echarts_timeseries_bar", ds_orders,
            {"x_axis": "orderpriority", "metrics": [sql_metric("COUNT(*)", "заказов")], "groupby": ["orderstatus"],
             "row_limit": 1000, "stack": "Stack", "show_legend": True}, dash),
        "Оптовых заказов": put_chart(
            "Оптовых заказов", "big_number_total", ds_year,
            {"metric": sql_metric("SUM(bulk_orders)", "оптовых заказов"),
             "subheader": f"суммарное количество > {QTY_LIMIT}"}, dash),
        "Оптовые заказы по годам (по сегментам)": put_chart(
            "Оптовые заказы по годам (по сегментам)", "echarts_timeseries_bar", ds_year,
            {"x_axis": "order_year", "metrics": [sql_metric("SUM(bulk_orders)", "оптовых заказов")],
             "groupby": ["mktsegment"], "row_limit": 1000, "stack": "Stack", "show_legend": True}, dash),
        "Гистограмма заказов по количеству товара": put_chart(
            "Гистограмма заказов по количеству товара", "echarts_timeseries_bar", ds_hist,
            {"x_axis": "qty_bucket", "metrics": [sql_metric("SUM(orders)", "заказов")], "row_limit": 100,
             "logAxis": True, "show_legend": False}, dash),
        "Ответ Q18: топ-100": put_chart(
            "Ответ Q18: топ-100", "table", ds_answer,
            {"query_mode": "raw", "all_columns": ["rank_no", "cust_name", "custkey", "orderkey", "orderdate",
                                                   "totalprice", "total_quantity", "mktsegment", "nation"],
             "order_by_cols": [json.dumps(["rank_no", True])], "row_limit": 100}, dash),
    }
    summary = {"superset": URL, "database_id": db, "dashboard_id": dash,
               "dashboard_url": f"{URL}/superset/dashboard/{dash}/",
               "datasets": {"bronze.orders": ds_orders, "gold.q18_answer": ds_answer,
                            "gold.bulk_orders_by_year": ds_year, "gold.order_qty_histogram": ds_hist},
               "charts": charts}
    write_json("superset_setup.json", summary)
    info(f"дашборд: {summary['dashboard_url']} (admin / admin)")


if __name__ == "__main__":
    main()
