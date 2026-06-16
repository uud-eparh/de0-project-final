"""
Spark Streaming: загрузка данных из Kafka в Vertica STAGING.
Обрабатывает сообщения object_type=TRANSACTION и object_type=CURRENCY.
Данные приходят в формате JSON с вложенным payload.
"""
import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Загружаем переменные окружения
load_dotenv()

# Логирование на русском
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Запуск Spark Streaming: Kafka -> Vertica STAGING")

# Параметры из .env
KAFKA_BROKERS = f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}"
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_USER = os.getenv('KAFKA_USER')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')

VERTICA_HOST = os.getenv('VERTICA_HOST')
VERTICA_PORT = os.getenv('VERTICA_PORT')
VERTICA_DB = os.getenv('VERTICA_DB')
VERTICA_USER = os.getenv('VERTICA_USER')
VERTICA_PASSWORD = os.getenv('VERTICA_PASSWORD')
STAGING_SCHEMA = os.getenv('STAGING_SCHEMA')

CA_CERT_PATH = os.getenv('CA_CERT_PATH', '/data/CA.pem')

# --- Spark-сессия ---
spark = SparkSession.builder \
    .appName("KafkaToVertica_STAGING") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", 1) \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark-сессия создана")

# --- Параметры Kafka ---
kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BROKERS,
    "subscribe": KAFKA_TOPIC,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";',
    "kafka.ssl.ca.location": CA_CERT_PATH,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

# --- Схема JSON (данные внутри payload) ---
message_schema = StructType([
    StructField("object_type", StringType()),
    StructField("payload", StructType([
        # Поля транзакции
        StructField("operation_id", StringType()),
        StructField("account_number_from", IntegerType()),
        StructField("account_number_to", IntegerType()),
        StructField("currency_code", IntegerType()),
        StructField("country", StringType()),
        StructField("status", StringType()),
        StructField("transaction_type", StringType()),
        StructField("amount", IntegerType()),
        StructField("transaction_dt", StringType()),
        # Поля курса
        StructField("date_update", StringType()),
        StructField("currency_code_with", IntegerType()),
        StructField("currency_with_div", DoubleType()),
    ]))
])

# --- Читаем поток из Kafka ---
logger.info("Подключение к Kafka...")
raw_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Парсим JSON и извлекаем поля из payload
parsed = raw_stream.select(
    from_json(col("value").cast("string"), message_schema).alias("msg")
).select(
    col("msg.object_type").alias("object_type"),
    col("msg.payload.operation_id").alias("operation_id"),
    col("msg.payload.account_number_from").alias("account_number_from"),
    col("msg.payload.account_number_to").alias("account_number_to"),
    col("msg.payload.currency_code").alias("currency_code"),
    col("msg.payload.country").alias("country"),
    col("msg.payload.status").alias("status"),
    col("msg.payload.transaction_type").alias("transaction_type"),
    col("msg.payload.amount").alias("amount"),
    col("msg.payload.transaction_dt").alias("transaction_dt"),
    col("msg.payload.date_update").alias("date_update"),
    col("msg.payload.currency_code_with").alias("currency_code_with"),
    col("msg.payload.currency_with_div").alias("currency_with_div")
)

# --- Функция для записи батча в Vertica ---
def write_to_vertica(df, epoch_id):
    """Записывает батч данных через vertica-python executemany."""
    import vertica_python
    
    conn_info = {
        'host': VERTICA_HOST,
        'port': VERTICA_PORT,
        'database': VERTICA_DB,
        'user': VERTICA_USER,
        'password': VERTICA_PASSWORD
    }
    
    rows = df.collect()
    logger.info(f"Получено {len(rows)} записей из Kafka")
    
    if len(rows) == 0:
        return
    
    transactions_data = []
    currencies_data = []
    
    for row in rows:
        row_dict = row.asDict()
        obj_type = row_dict.get('object_type')
        
        if obj_type == 'TRANSACTION':
            if row_dict.get('operation_id') is not None:
                transactions_data.append((
                    row_dict.get('operation_id'),
                    row_dict.get('account_number_from'),
                    row_dict.get('account_number_to'),
                    row_dict.get('currency_code'),
                    row_dict.get('country'),
                    row_dict.get('status'),
                    row_dict.get('transaction_type'),
                    row_dict.get('amount'),
                    row_dict.get('transaction_dt')
                ))
        elif obj_type == 'CURRENCY':
            if row_dict.get('currency_code') is not None and row_dict.get('currency_code_with') is not None:
                currencies_data.append((
                    row_dict.get('date_update'),
                    row_dict.get('currency_code'),
                    row_dict.get('currency_code_with'),
                    row_dict.get('currency_with_div')
                ))
    
    conn = vertica_python.connect(**conn_info)
    cursor = conn.cursor()
    
    if transactions_data:
        logger.info(f"Вставка {len(transactions_data)} транзакций...")
        try:
            cursor.executemany(f"""
                INSERT INTO {STAGING_SCHEMA}.transactions 
                (operation_id, account_number_from, account_number_to, currency_code, 
                 country, status, transaction_type, amount, transaction_dt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, transactions_data)
            conn.commit()
            logger.info(f"Транзакции загружены: {len(transactions_data)} шт.")
        except Exception as e:
            logger.error(f"Ошибка вставки транзакций: {e}")
            conn.rollback()
    
    if currencies_data:
        logger.info(f"Вставка {len(currencies_data)} курсов...")
        try:
            cursor.executemany(f"""
                INSERT INTO {STAGING_SCHEMA}.currencies 
                (date_update, currency_code, currency_code_with, currency_with_div)
                VALUES (%s, %s, %s, %s)
            """, currencies_data)
            conn.commit()
            logger.info(f"Курсы загружены: {len(currencies_data)} шт.")
        except Exception as e:
            logger.error(f"Ошибка вставки курсов: {e}")
            conn.rollback()
    
    cursor.close()
    conn.close()

# --- Запуск стрима ---
logger.info("Запуск стриминга...")
query = parsed.writeStream \
    .foreachBatch(write_to_vertica) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

logger.info("Стриминг запущен. Ожидание данных из Kafka...")
query.awaitTermination()