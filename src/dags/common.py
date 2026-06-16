"""Общие утилиты для DAG'ов."""
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import vertica_python
import logging

logger = logging.getLogger(__name__)

# Константы из Airflow Variables
STAGING_SCHEMA = Variable.get('staging_schema', default_var='VT260312E5C416__STAGING')
DWH_SCHEMA = Variable.get('dwh_schema', default_var='VT260312E5C416__DWH')
S3_BUCKET = Variable.get('s3_bucket', default_var='final-project')
BATCH_SIZE = int(Variable.get('batch_size', default_var='500'))


def get_vertica_conn():
    """Подключение к Vertica через Airflow Connection."""
    conn = BaseHook.get_connection('vertica_dwh')
    return vertica_python.connect(
        host=conn.host,
        port=conn.port,
        database=conn.schema,
        user=conn.login,
        password=conn.password
    )


def log_alert(message):
    """Логирование алерта (заглушка для будущего мониторинга)."""
    logger.warning(f"[ALERT] {message}")