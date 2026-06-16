"""
Spark Streaming: загрузка сырых данных из Kafka в PostgreSQL raw-слой.
Сохраняет JSON как есть, без обработки.
"""
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
logger.info("Запуск Spark Streaming: Kafka -> PostgreSQL (raw)")

# --- Параметры ---
KAFKA_HOST = os.getenv('KAFKA_HOST', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9091')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'uud-eparh_transaction-service-input')
KAFKA_USER = os.getenv('KAFKA_USER', 'de-student')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'ltcneltyn')
CA_CERT_PATH = os.getenv('CA_CERT_PATH', '/data/CA.pem')

PG_HOST = os.getenv('PG_HOST', 'host.docker.internal')
PG_PORT = os.getenv('PG_PORT', '15432')
PG_DB = os.getenv('PG_DB', 'postgres')
PG_USER = os.getenv('PG_USER', 'jovyan')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'jovyan')

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# --- Spark-сессия ---
spark = SparkSession.builder \
    .appName("KafkaToPostgresRaw") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark-сессия создана")

# --- Схема для object_type ---
type_schema = StructType([
    StructField("object_type", StringType(), True)
])

# --- Kafka options ---
kafka_options = {
    "kafka.bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}",
    "subscribe": KAFKA_TOPIC,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";',
    "kafka.ssl.ca.location": CA_CERT_PATH,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

# --- Читаем поток ---
logger.info("Подключение к Kafka...")
raw_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Извлекаем object_type
parsed = raw_stream.select(
    col("value").cast("string").alias("raw_json"),
    from_json(col("value").cast("string"), type_schema).getField("object_type").alias("object_type"),
    current_timestamp().alias("loaded_at")
)


def write_to_postgres(df, epoch_id):
    try:
        """Записывает батч в PostgreSQL raw-слой."""
        logger.info(f"[Батч {epoch_id}] Получены данные...")

        tx_df = df.filter(col("object_type") == "TRANSACTION") \
            .select("raw_json", "object_type", "loaded_at")
        cur_df = df.filter(col("object_type") == "CURRENCY") \
            .select("raw_json", "object_type", "loaded_at")

        tx_count = tx_df.count()
        cur_count = cur_df.count()
        logger.info(f"[Батч {epoch_id}] Транзакций: {tx_count}, Курсов: {cur_count}")

        if tx_count > 0:
            tx_df.write \
                .jdbc(url=PG_URL, table="raw.transactions", mode="append", properties=PG_PROPERTIES)
            logger.info(f"[Батч {epoch_id}] Транзакции записаны в raw: {tx_count} шт.")

        if cur_count > 0:
            cur_df.write \
                .jdbc(url=PG_URL, table="raw.currencies", mode="append", properties=PG_PROPERTIES)
            logger.info(f"[Батч {epoch_id}] Курсы записаны в raw: {cur_count} шт.")

    except Exception as e:
        logger.error(f"[ALERT] Ошибка в батче {epoch_id}: {e}")
        raise


# --- Запуск стрима ---
logger.info("Запуск стриминга в PostgreSQL raw...")
query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

logger.info("Стриминг запущен. Ожидание данных...")
query.awaitTermination()