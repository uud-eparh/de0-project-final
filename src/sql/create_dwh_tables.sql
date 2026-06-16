-- Создание витрины в DWH-слое
-- Схема: VT260312E5C416__DWH

-- Витрина global_metrics
DROP TABLE IF EXISTS VT260312E5C416__DWH.global_metrics;

CREATE TABLE VT260312E5C416__DWH.global_metrics (
    date_update                  DATE,
    currency_from                INTEGER,
    amount_total                 NUMERIC(18,2),
    cnt_transactions             INTEGER,
    avg_transactions_per_account NUMERIC(18,4),
    cnt_accounts_make_transactions INTEGER
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;

-- Проекция для global_metrics
DROP PROJECTION IF EXISTS VT260312E5C416__DWH.global_metrics_proj;

CREATE PROJECTION VT260312E5C416__DWH.global_metrics_proj
(
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
AS
    SELECT
        date_update,
        currency_from,
        amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions
    FROM VT260312E5C416__DWH.global_metrics
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;