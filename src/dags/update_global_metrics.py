"""
DAG для ежедневного инкрементального обновления витрины global_metrics.
Использует Airflow Connections вместо .env.
"""
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import vertica_python
from common import get_vertica_conn, STAGING_SCHEMA, DWH_SCHEMA, log_alert

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'uud-eparh',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 10, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def update_global_metrics(**context):
    """Инкрементальное обновление витрины за вчерашний день."""
    execution_date = context['execution_date']
    yesterday = execution_date.strftime('%Y-%m-%d')
    logger.info(f"Обновление витрины за {yesterday}")
    
    conn = get_vertica_conn()
    cursor = conn.cursor()
    
    # Удаляем старые данные за эту дату
    cursor.execute(f"DELETE FROM {DWH_SCHEMA}.global_metrics WHERE date_update = %s", (yesterday,))
    
    # Вставляем новые данные
    cursor.execute(f"""
        INSERT INTO {DWH_SCHEMA}.global_metrics
        SELECT
            t.date_update,
            t.currency_code AS currency_from,
            SUM(t.amount * COALESCE(c.currency_with_div, 1.0)) AS amount_total,
            COUNT(t.operation_id) AS cnt_transactions,
            COUNT(t.operation_id) / NULLIF(COUNT(DISTINCT t.account_number_from), 0) AS avg_transactions_per_account,
            COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
        FROM {STAGING_SCHEMA}.transactions t
        LEFT JOIN {STAGING_SCHEMA}.currencies c 
            ON t.currency_code = c.currency_code 
            AND c.currency_code_with = 420
            AND t.date_update = c.date_update::DATE
        WHERE t.account_number_from > 0
          AND t.account_number_to > 0
          AND t.status = 'done'
          AND t.date_update = %s
        GROUP BY t.date_update, t.currency_code
    """, (yesterday,))
    
    conn.commit()
    
    cursor.execute(f"SELECT COUNT(*) FROM {DWH_SCHEMA}.global_metrics WHERE date_update = %s", (yesterday,))
    count = cursor.fetchone()[0]
    logger.info(f"Витрина обновлена за {yesterday}: {count} записей")
    
    cursor.close()
    conn.close()


with DAG(
    'update_global_metrics',
    default_args=default_args,
    description='Ежедневное обновление витрины global_metrics',
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['dwh', 'vertica', 'metrics'],
) as dag:

    update_task = PythonOperator(
        task_id='update_global_metrics',
        python_callable=update_global_metrics,
    )