-- Raw-слой: сырые данные как из Kafka (JSON)
CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.transactions;
CREATE TABLE raw.transactions (
    id          SERIAL PRIMARY KEY,
    raw_json    TEXT,
    object_type VARCHAR(20),
    loaded_at   TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS raw.currencies;
CREATE TABLE raw.currencies (
    id          SERIAL PRIMARY KEY,
    raw_json    TEXT,
    object_type VARCHAR(20),
    loaded_at   TIMESTAMP DEFAULT NOW()
);