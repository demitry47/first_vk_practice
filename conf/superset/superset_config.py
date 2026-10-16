# Настройки Superset для учебного стенда, который крутится только на localhost
SECRET_KEY = "tpch-q18-superset-local-only-secret"
# CSRF и Talisman выключены: датасеты, графики и дашборд создаются скриптом через REST API,
# а сам стенд доступен только с этой машины
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300
ROW_LIMIT = 50000
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}
