-- Создание таблиц STAGING-слоя в Vertica
-- Схема: VT260312E5C416__STAGING

-- Таблица transactions
DROP TABLE IF EXISTS VT260312E5C416__STAGING.transactions;

CREATE TABLE VT260312E5C416__STAGING.transactions (
    operation_id       VARCHAR(36),
    account_number_from INTEGER,
    account_number_to   INTEGER,
    currency_code       INTEGER,
    country             VARCHAR(50),
    status              VARCHAR(20),
    transaction_type    VARCHAR(30),
    amount              INTEGER,
    transaction_dt      TIMESTAMP,
    date_update         DATE DEFAULT (transaction_dt)::DATE
)
ORDER BY date_update
SEGMENTED BY HASH(operation_id, date_update) ALL NODES;

-- Проекция для transactions
DROP PROJECTION IF EXISTS VT260312E5C416__STAGING.transactions_proj;

CREATE PROJECTION VT260312E5C416__STAGING.transactions_proj
(
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt,
    date_update
)
AS
    SELECT 
        operation_id,
        account_number_from,
        account_number_to,
        currency_code,
        country,
        status,
        transaction_type,
        amount,
        transaction_dt,
        date_update
    FROM VT260312E5C416__STAGING.transactions
ORDER BY date_update, operation_id
SEGMENTED BY HASH(operation_id, date_update) ALL NODES;

-- Таблица currencies
DROP TABLE IF EXISTS VT260312E5C416__STAGING.currencies;

CREATE TABLE VT260312E5C416__STAGING.currencies (
    date_update          TIMESTAMP,
    currency_code        INTEGER,
    currency_code_with   INTEGER,
    currency_with_div    NUMERIC(18,6)
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_code, currency_code_with) ALL NODES;

-- Проекция для currencies
DROP PROJECTION IF EXISTS VT260312E5C416__STAGING.currencies_proj;

CREATE PROJECTION VT260312E5C416__STAGING.currencies_proj
(
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
)
AS
    SELECT 
        date_update,
        currency_code,
        currency_code_with,
        currency_with_div
    FROM VT260312E5C416__STAGING.currencies
ORDER BY date_update, currency_code
SEGMENTED BY HASH(date_update, currency_code, currency_code_with) ALL NODES;