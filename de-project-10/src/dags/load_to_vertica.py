from vert_connect import VertConnect

INSERT_TRANSACTION_QUERY = """
                INSERT INTO STV230529__STAGING.transactions (
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    country,
                    status,
                    transaction_type,
                    amount,
                    transaction_dt
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """

INSERT_CURRENCY_QUERY = """
                   INSERT INTO STV230529__STAGING.currencies (
                    date_update,
                    currency_code,
                    currency_code_with,
                    currency_with_div
                    )
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """


class LoadToVertica:

    def __init__(self, db: VertConnect, values) -> None:
        self._db = db
        self._values = values

    def trans_to_vertica(self): 
        with self._db.connection() as conn:
            print('start_trans_to_vertica')
            with conn.cursor() as cur:
                cur.executemany(INSERT_TRANSACTION_QUERY, self._values, use_prepared_statements=True) 
                print('end_trans_to_vertica')

    def curr_to_vertica(self): 
        with self._db.connection() as conn:
            print('start_curr_to_vertica')
            with conn.cursor() as cur:
                cur.executemany(INSERT_CURRENCY_QUERY, self._values, use_prepared_statements=True)
                print('end_curr_to_vertica')


