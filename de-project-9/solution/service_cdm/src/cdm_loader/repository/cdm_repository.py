import uuid
from datetime import datetime

from lib.pg import PgConnect

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_user_product_counters(self, user_id, product_id, product_name) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                        INSERT INTO cdm.user_product_counters(user_id, product_id, product_name, order_cnt)
                        VALUES (md5('{user_id}')::uuid, md5('{product_id}')::uuid, '{product_name}', 1)
                        ON CONFLICT DO NOTHING
                    """
                )

    def insert_user_category_counters(self, user_id, category_name) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                f"""
                    INSERT INTO cdm.user_category_counters(user_id, category_id, category_name, order_cnt)
                    VALUES (md5('{user_id}')::uuid, md5('{category_name}')::uuid, '{category_name}', 1)
                    ON CONFLICT DO NOTHING
                """
                )



        
