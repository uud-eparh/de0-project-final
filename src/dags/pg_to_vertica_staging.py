import json, logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from common import get_vertica_conn, STAGING_SCHEMA, BATCH_SIZE, log_alert
from airflow.hooks.base import BaseHook

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'uud-eparh',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 10, 31),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def get_pg_conn():
    conn = BaseHook.get_connection('postgres_raw')
    return psycopg2.connect(
        host=conn.host, port=conn.port, dbname=conn.schema,
        user=conn.login, password=conn.password
    )


def transfer_data(**context):
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    logger.info(f"Перенос данных за {date_str}")

    try:
        pg = get_pg_conn()
        pg_cur = pg.cursor()
        vertica = get_vertica_conn()
        v_cur = vertica.cursor()
    except Exception as e:
        log_alert(f"Ошибка подключения: {e}")
        raise

    # Очистка старых данных
    v_cur.execute(f"DELETE FROM {STAGING_SCHEMA}.transactions WHERE date_update = %s", (date_str,))
    v_cur.execute(f"DELETE FROM {STAGING_SCHEMA}.currencies WHERE date_update::DATE = %s", (date_str,))

    # Транзакции
    pg_cur.execute("SELECT raw_json FROM raw.transactions WHERE raw_json::json->>'object_type' = 'TRANSACTION'")
    tx_data = []
    for row in pg_cur.fetchall():
        try:
            msg = json.loads(row[0])
            p = msg.get('payload', msg)
            if p.get('transaction_dt', '')[:10] == date_str:
                tx_data.append((p.get('operation_id'), p.get('account_number_from'),
                    p.get('account_number_to'), p.get('currency_code'), p.get('country'),
                    p.get('status'), p.get('transaction_type'), p.get('amount'), p.get('transaction_dt')))
        except Exception as e:
            logger.warning(f"Ошибка парсинга транзакции: {e}")

    if tx_data:
        for i in range(0, len(tx_data), BATCH_SIZE):
            batch = tx_data[i:i + BATCH_SIZE]
            v_cur.executemany(f"""INSERT INTO {STAGING_SCHEMA}.transactions 
                (operation_id, account_number_from, account_number_to, currency_code,
                 country, status, transaction_type, amount, transaction_dt)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", batch)
        vertica.commit()
        logger.info(f"Загружено {len(tx_data)} транзакций за {date_str}")

    # Курсы
    pg_cur.execute("SELECT raw_json FROM raw.currencies WHERE raw_json::json->>'object_type' = 'CURRENCY'")
    cur_data = []
    for row in pg_cur.fetchall():
        try:
            msg = json.loads(row[0])
            p = msg.get('payload', msg)
            if p.get('date_update', '')[:10] == date_str:
                cur_data.append((p.get('date_update'), p.get('currency_code'),
                    p.get('currency_code_with'), p.get('currency_with_div')))
        except Exception as e:
            logger.warning(f"Ошибка парсинга курса: {e}")

    if cur_data:
        for i in range(0, len(cur_data), BATCH_SIZE):
            batch = cur_data[i:i + BATCH_SIZE]
            v_cur.executemany(f"""INSERT INTO {STAGING_SCHEMA}.currencies 
                (date_update, currency_code, currency_code_with, currency_with_div)
                VALUES (%s,%s,%s,%s)""", batch)
        vertica.commit()
        logger.info(f"Загружено {len(cur_data)} курсов за {date_str}")

    pg_cur.close(); pg.close()
    v_cur.close(); vertica.close()


with DAG(
    'pg_to_vertica_staging',
    default_args=default_args,
    description='Перенос из PostgreSQL raw в Vertica STAGING',
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['pg', 'vertica', 'staging'],
) as dag:
    PythonOperator(task_id='transfer_data', python_callable=transfer_data)