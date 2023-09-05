DROP TABLE IF EXISTS STV230529__STAGING.transactions

CREATE TABLE STV230529__STAGING.transactions(
	operation_id varchar(60),
	account_number_from int,
	account_number_to int,
	currency_code int,
	country varchar(30),
	status varchar(30),
	transaction_type varchar(30),
	amount int,
	transaction_dt timestamp,
	unique(operation_id, transaction_dt)
)
order by transaction_dt
SEGMENTED BY HASH((operation_id, transaction_dt)) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);

CREATE PROJECTION STV230529__STAGING.transactions_proj as
SELECT
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
FROM STV230529__STAGING.transactions
ORDER BY transaction_dt
SEGMENTED BY HASH((operation_id, transaction_dt)) all nodes  

DROP TABLE IF EXISTS STV230529__STAGING.currencies

CREATE TABLE STV230529__STAGING.currencies(
	date_update timestamp,
	currency_code int,
	currency_code_with int,
	currency_with_div numeric(5, 3),
	unique(date_update, currency_code)
	)
order by date_update
SEGMENTED BY currency_code all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

CREATE PROJECTION STV230529__STAGING.currencies_proj as
SELECT
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
FROM STV230529__STAGING.currencies
ORDER BY date_update
SEGMENTED BY HASH((date_update, currency_code)) all nodes  

DROP TABLE IF EXISTS STV230529__DWH.global_metrics

CREATE TABLE STV230529__DWH.global_metrics(
	date_update timestamp,
	currency_from int,
	amount_total float,
	cnt_transactions int,
	avg_transactions_per_account float,
	cnt_accounts_make_transactions int,
	unique(date_update, currency_from)
)
order by date_update
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);
