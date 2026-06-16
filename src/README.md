# DWH Pipeline для аналитики финансовых транзакций

## 📌 Описание проекта

Проект представляет собой **DWH (Data Warehouse)** для финтех-стартапа, предлагающего международные банковские услуги. Пайплайн обрабатывает транзакционную активность пользователей и курсы валют, объединяя данные из разных источников для единой аналитики.

### 🎯 Цели проекта

- Построение трёхуровневого DWH (RAW → STAGING → DWH)
- Потоковая обработка транзакций из Kafka через Spark Streaming
- Пакетная загрузка курсов валют из S3
- Формирование витрины `global_metrics` с ежедневной динамикой оборотов
- Обеспечение идемпотентности и инкрементальной загрузки

---

## 🔗 Ссылка на итоговую витрину

``` text
Схема: VT260312E5C416__DWH
Таблица: global_metrics
Хост: vertica.data-engineer.education-services.ru
Порт: 5433
БД: dwh
```
---

## 🏗️ Архитектура пайплайна

                    ┌───────────────┐     ┌──────────────┐     ┌───────────────┐     ┌──────────────┐
    Kafka  ──────▶ │Spark Streaming│────▶│ PostgreSQL   │───▶│   Airflow     │────▶│   Vertica    │
    (топик)         │  (PySpark)    │     │   RAW-слой   │     │   (ETL DAG)   │     │ STAGING/DWH  │
                    └───────────────┘     └──────────────┘     └───────────────┘     └──────────────┘
                                                                                           │
     S3  ─────────▶ Airflow DAG ──────▶ Vertica STAGING (currencies, transactions) ◀──────┘

### Потоки данных

| Источник | Инструмент | Приёмник | Назначение | Файл |
|----------|-----------|----------|------------|------|
| **Kafka** | Spark Streaming | PostgreSQL RAW | Сырые JSON-сообщения (основной поток) | `kafka_to_pg_raw.py` |
| **Kafka** | Spark Streaming | Vertica STAGING | Прямая загрузка очищенных данных (альтернативный) | `kafka_to_staging.py` |
| **PostgreSQL RAW** | Airflow DAG | Vertica STAGING | Парсинг JSON → очищенные данные | `pg_to_vertica_staging.py` |
| **S3** | Airflow DAG | Vertica STAGING | Пакетная загрузка CSV (альтернативный) | `s3_to_staging_dag.py` |
| **Vertica STAGING** | Airflow DAG | Vertica DWH | Формирование витрины global_metrics | `update_global_metrics.py` |

### Основной и альтернативные потоки

| Поток | Тип | Описание |
|-------|-----|----------|
| **Kafka → PG → Vertica** | Основной | Потоковая обработка через Spark Streaming + ETL через Airflow |
| **Kafka → Vertica** | Альтернативный | Прямая загрузка Spark Streaming в Vertica (без промежуточного слоя) |
| **S3 → Vertica** | Альтернативный | Пакетная загрузка CSV-файлов через Airflow для быстрого построения витрин |

---

## 📦 Компоненты системы

### Сервисы

| Сервис | Назначение | Вход | Выход |
|--------|------------|------|-------|
| **Spark Streaming** | Чтение сырых данных из Kafka, запись в PostgreSQL RAW | `uud-eparh_transaction-service-input` | `raw.transactions`, `raw.currencies` |
| **S3 DAG** | Загрузка CSV из S3 в Vertica STAGING | `final-project` bucket | `STAGING.transactions`, `STAGING.currencies` |
| **PG-to-Vertica DAG** | Парсинг JSON из RAW и загрузка в STAGING | `raw.transactions`, `raw.currencies` | `STAGING.transactions`, `STAGING.currencies` |
| **Global Metrics DAG** | Формирование витрины с агрегатами | `STAGING.transactions`, `STAGING.currencies` | `DWH.global_metrics` |

### Базы данных и хранилища

| Компонент | Назначение | Технология | Подключение |
|-----------|------------|-------------|-------------|
| PostgreSQL RAW | Хранение сырых JSON из Kafka | PostgreSQL (схема `raw`) | `localhost:15432` |
| Vertica STAGING | Очищенные данные с типами | Vertica (схема `VT260312E5C416__STAGING`) | `vertica.data-engineer.education-services.ru:5433` |
| **Vertica DWH** | **Витрина global_metrics** | **Vertica (схема `VT260312E5C416__DWH`)** | **`vertica.data-engineer.education-services.ru:5433`** |
| S3 | Пакетные CSV-файлы | Yandex Object Storage | `storage.yandexcloud.net/final-project` |
| Kafka | Потоковая передача транзакций и курсов | Yandex Managed Kafka | `rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091` |

---

## 📊 Модель данных

### Слой RAW (PostgreSQL)

| Таблица | Назначение | Поля |
|---------|------------|------|
| `raw.transactions` | Сырые JSON-сообщения типа TRANSACTION | `id`, `raw_json`, `object_type`, `loaded_at` |
| `raw.currencies` | Сырые JSON-сообщения типа CURRENCY | `id`, `raw_json`, `object_type`, `loaded_at` |

### Слой STAGING (Vertica)

| Таблица | Поля |
|---------|-------|
| `transactions` | `operation_id`, `account_number_from`, `account_number_to`, `currency_code`, `country`, `status`, `transaction_type`, `amount`, `transaction_dt`, `date_update` |
| `currencies` | `date_update`, `currency_code`, `currency_code_with`, `currency_with_div` |

### Слой DWH (Vertica)

| Витрина | Поля | Назначение |
|---------|------|------------|
| `global_metrics` | `date_update`, `currency_from`, `amount_total`, `cnt_transactions`, `avg_transactions_per_account`, `cnt_accounts_make_transactions` | Ежедневная динамика оборотов в USD |

---

## 🔧 Переменные окружения (.env)
``` text
VERTICA_HOST=vertica.data-engineer.education-services.ru
VERTICA_PORT=5433
VERTICA_DB=dwh
VERTICA_USER=vt260312e5c416
VERTICA_PASSWORD=...
STAGING_SCHEMA=VT260312E5C416__STAGING
DWH_SCHEMA=VT260312E5C416__DWH
KAFKA_HOST=rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net
KAFKA_PORT=9091
KAFKA_USER=de-student
KAFKA_PASSWORD=...
KAFKA_TOPIC=uud-eparh_transaction-service-input
PG_HOST=host.docker.internal
PG_PORT=15432
PG_DB=postgres
PG_USER=jovyan
PG_PASSWORD=...
S3_BUCKET=final-project
S3_ENDPOINT=https://storage.yandexcloud.net
S3_ACCESS_KEY=...
S3_SECRET_KEY=...
```
---

## 🚀 Запуск

### 1. Инфраструктура
``` shell
docker run -d -p 8998:8998 -p 8280:8280 -p 15432:5432 --name=de-final-prj-local cr.yandex/crp1r8pht0n0gl25aug1/de-final-prj:latest
```
### 2. Установка зависимостей в контейнере
``` shell
docker exec -it de-final-prj-local pip install pyspark==3.2.3 python-dotenv psycopg2-binary vertica-python boto3

docker exec -it de-final-prj-local wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.3/spark-sql-kafka-0-10_2.12-3.2.3.jar

docker exec -it de-final-prj-local wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar

docker exec -it de-final-prj-local wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar

docker cp CA.pem de-final-prj-local:/data/CA.pem
```
### 3. Создание таблиц

Выполнить SQL-скрипты в DBeaver:
``` shell
- `src/sql/create_pg_raw.sql` → PostgreSQL
- `src/sql/create_staging_tables.sql` → Vertica
- `src/sql/create_dwh_tables.sql` → Vertica
```
### 4. Airflow Connections и Variables

**Connections (Admin → Connections):**

| Conn Id | Type | Параметры |
|---------|------|-----------|
| `vertica_dwh` | Vertica | host: vertica.data-engineer.education-services.ru, port: 5433, schema: dwh |
| `postgres_raw` | Postgres | host: host.docker.internal, port: 15432, schema: postgres |
| `s3_final_project` | Amazon Web Services | AWS Key + Extra: {"endpoint_url": "https://storage.yandexcloud.net"} |

**Variables (Admin → Variables):**

| Key | Value |
|-----|-------|
| `staging_schema` | `VT260312E5C416__STAGING` |
| `dwh_schema` | `VT260312E5C416__DWH` |
| `s3_bucket` | `final-project` |
| `batch_size` | `500` |

### 5. Проверка Kafka
``` shell
docker run -it --network=host -v "C:\Users\User\.redis\CA.pem:/data/CA.pem" edenhill/kcat:1.7.1 -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 -t uud-eparh_transaction-service-input -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username=de-student -X sasl.password="ltcneltyn" -X ssl.ca.location=/data/CA.pem -L
```
### 6. Запуск Spark Streaming (Kafka → PostgreSQL)
``` shell
docker cp src/py/kafka_to_pg_raw.py de-final-prj-local:/root/
docker exec -it -e KAFKA_HOST=... -e PG_HOST=host.docker.internal de-final-prj-local spark-submit /root/kafka_to_pg_raw.py
```
### 7. Запуск Airflow DAG'ов

docker cp src/dags/* de-final-prj-local:/lessons/dags/

| DAG | Назначение | Режим |
|-----|------------|-------|
| `s3_to_vertica_staging` | S3 → STAGING (альтернативный) | @daily, catchup (октябрь 2022) |
| `pg_to_vertica_staging` | PG RAW → Vertica STAGING (основной) | @daily, catchup (октябрь 2022) |
| `update_global_metrics` | STAGING → DWH витрина | @daily, catchup (октябрь 2022) |

---

## 📁 Структура репозитория
``` text
de0-project-final/
├── src/
│   ├── README.md                       # Документация проекта
│   ├── dags/
│   │   ├── common.py                   # Общие утилиты
│   │   ├── pg_to_vertica_staging.py    # PG RAW → Vertica STAGING
│   │   ├── s3_to_staging_dag.py        # S3 → Vertica STAGING
│   │   └── update_global_metrics.py    # STAGING → DWH витрина
│   ├── py/
│   │   ├── kafka_to_pg_raw.py          # Spark: Kafka → PG RAW
│   │   ├── kafka_to_staging.py         # Spark: Kafka → Vertica
│   │   └── requirements.txt
│   └── sql/
│       ├── create_pg_raw.sql           # Таблицы PostgreSQL RAW
│       ├── create_staging_tables.sql   # Таблицы Vertica STAGING
│       └── create_dwh_tables.sql       # Витрина Vertica DWH
├── .env
└── .gitignore
```
---

## 🛠️ Технологии

| Категория | Технологии |
|-----------|------------|
| **Orchestration** | Apache Airflow 2.x |
| **Streaming** | Apache Spark 3.2.3 (PySpark), Kafka 3.2.3 |
| **Storage** | PostgreSQL (RAW), Vertica (STAGING/DWH), S3 |
| **Инфраструктура** | Docker, Docker Compose |
| **Языки** | Python 3.9, SQL |
| **Формат данных** | JSON (Kafka), CSV (S3) |
| **Мониторинг** | Логирование (logging), алерты (log_alert) |

---

## ✅ Статус проекта

- [x] Построение RAW-слоя (PostgreSQL)
- [x] Построение STAGING-слоя (Vertica)
- [x] Построение DWH-витрины (Vertica)
- [x] Spark Streaming: Kafka → PostgreSQL
- [x] Airflow DAG: S3 → Vertica STAGING
- [x] Airflow DAG: PostgreSQL → Vertica STAGING
- [x] Airflow DAG: Vertica STAGING → DWH
- [x] Инкрементальная загрузка за октябрь 2022
- [x] Очистка тестовых аккаунтов (account < 0)
- [x] Логирование
- [x] Airflow Connections и Variables
- [x] Общий модуль common.py