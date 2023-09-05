INSERT INTO STV230529__DWH.global_metrics
                WITH curr AS (
                SELECT 
                    *, 
                    date(date_update) 
                FROM 
                    STV230529__STAGING.currencies c 
                WHERE 
                    currency_code_with = 420 
                    AND date(date_update) = '{self._date}'
                ), 
                trans_amount_count AS (
                SELECT 
                    t.currency_code, 
                    sum(amount) amount_curr, 
                    count(1) cnt_transactions, 
                    date(transaction_dt) date,
                    count(DISTINCT account_number_from) cnt_accounts_make_transactions
                FROM 
                    STV230529__STAGING.transactions t
                WHERE 
                    date(transaction_dt) = '{self._date}'
                    AND status in ('done', 'chargeback') 
                    AND account_number_from > -1 
                GROUP BY 
                    t.currency_code, 
                    date(transaction_dt)
                ), 
                trans_avg_count AS (
                SELECT 
                    currency_code, 
                    avg(count) avg_transactions_per_account
                FROM 
                    (
                    SELECT 
                        account_number_from, 
                        currency_code, 
                        count(1) 
                    FROM 
                        STV230529__STAGING.transactions t 
                    WHERE 
                        date(transaction_dt) = '{self._date}'
                        AND account_number_from > -1 
                        AND status in ('done', 'chargeback')
                    GROUP BY 
                        account_number_from, 
                        currency_code
                    ) gb 
                GROUP BY 
                    currency_code
                ) 
                SELECT 
                tam.date date_update, 
                tam.currency_code, 
                CASE 
                    WHEN currency_code_with IS NULL 
                        THEN amount_curr 
                    ELSE amount_curr / c.currency_with_div 
                END amount_total, 
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions
                FROM 
                trans_amount_count tam
                LEFT JOIN curr c ON tam.currency_code = c.currency_code
                LEFT JOIN trans_avg_count tav ON tam.currency_code = tav.currency_code