"""
DAG для загрузки данных из S3 в Vertica STAGING.
Скачивает transactions и currencies из бакета final-project.
Использует Airflow Connections.
"""
import csv
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import vertica_python
from datetime import datetime, timedelta
from common import get_vertica_conn, STAGING_SCHEMA, BATCH_SIZE, log_alert, S3_BUCKET

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'uud-eparh',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_currencies(**context):
    """Загружает справочник валют из S3 в STAGING.currencies."""
    s3_hook = S3Hook(aws_conn_id='s3_final_project')
    logger.info("Скачивание currencies_history.csv из S3...")
    
    obj = s3_hook.get_key(key='currencies_history.csv', bucket_name=S3_BUCKET)
    csv_data = obj.get()['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(csv_data)
    
    conn = get_vertica_conn()
    cursor = conn.cursor()
    
    count = 0
    for row in reader:
        cursor.execute(f"""
            SELECT COUNT(*) FROM {STAGING_SCHEMA}.currencies 
            WHERE date_update = %s AND currency_code = %s AND currency_code_with = %s
        """, (row['date_update'], int(row['currency_code']), int(row['currency_code_with'])))
        
        if cursor.fetchone()[0] == 0:
            cursor.execute(f"""
                INSERT INTO {STAGING_SCHEMA}.currencies (date_update, currency_code, currency_code_with, currency_with_div)
                VALUES (%s, %s, %s, %s)
            """, (
                row['date_update'],
                int(row['currency_code']),
                int(row['currency_code_with']),
                float(row['currency_with_div'])
            ))
            count += 1
            if count % 1000 == 0:
                conn.commit()
                logger.info(f"Загружено {count} новых записей currencies")
    
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"currencies загружены. Новых: {count} записей")


def load_transactions(**context):
    """Загружает транзакции из S3 в STAGING.transactions батчами."""
    s3_hook = S3Hook(aws_conn_id='s3_final_project')
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    logger.info(f"Загрузка транзакций за {date_str}")
    
    conn = get_vertica_conn()
    cursor = conn.cursor()
    
    # Удаляем старые данные за эту дату
    cursor.execute(f"DELETE FROM {STAGING_SCHEMA}.transactions WHERE date_update = %s", (date_str,))
    conn.commit()
    logger.info(f"Очищены старые данные за {date_str}")
    
    total_count = 0
    batch_rows = []
    BATCH_SIZE = 500
    
    for batch_num in range(1, 11):
        key = f'transactions_batch_{batch_num}.csv'
        try:
            obj = s3_hook.get_key(key=key, bucket_name=S3_BUCKET)
            csv_data = obj.get()['Body'].read().decode('utf-8').splitlines()
            reader = csv.DictReader(csv_data)
            
            count = 0
            for row in reader:
                if row['transaction_dt'].startswith(date_str):
                    batch_rows.append((
                        row['operation_id'],
                        int(row['account_number_from']),
                        int(row['account_number_to']),
                        int(row['currency_code']),
                        row['country'],
                        row['status'],
                        row['transaction_type'],
                        int(row['amount']),
                        row['transaction_dt']
                    ))
                    
                    if len(batch_rows) >= BATCH_SIZE:
                        cursor.executemany(f"""
                            INSERT INTO {STAGING_SCHEMA}.transactions 
                            (operation_id, account_number_from, account_number_to, currency_code, 
                             country, status, transaction_type, amount, transaction_dt)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, batch_rows)
                        conn.commit()
                        count += len(batch_rows)
                        batch_rows = []
            
            # Остатки
            if batch_rows:
                cursor.executemany(f"""
                    INSERT INTO {STAGING_SCHEMA}.transactions 
                    (operation_id, account_number_from, account_number_to, currency_code, 
                     country, status, transaction_type, amount, transaction_dt)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch_rows)
                conn.commit()
                count += len(batch_rows)
                batch_rows = []
            
            total_count += count
            logger.info(f"Батч {batch_num}: загружено {count} транзакций за {date_str} (итого: {total_count})")
            
        except Exception as e:
            logger.warning(f"Батч {batch_num} пропущен: {e}")
    
    cursor.close()
    conn.close()
    logger.info(f"Транзакции загружены. Всего: {total_count} записей за {date_str}")


with DAG(
    's3_to_vertica_staging',
    default_args=default_args,
    description='Загрузка данных из S3 в Vertica STAGING',
    schedule_interval='@daily',
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    catchup=True,
    max_active_runs=1,
    tags=['s3', 'vertica', 'staging'],
) as dag:

    load_currencies_task = PythonOperator(
        task_id='load_currencies',
        python_callable=load_currencies,
    )

    load_transactions_task = PythonOperator(
        task_id='load_transactions',
        python_callable=load_transactions,
    )

    load_currencies_task >> load_transactions_task