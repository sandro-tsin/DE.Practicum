from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from vert_connect import VertConnect
from load_to_vertica import LoadToVertica
from stg_to_dwh import StgToDwh

vert = BaseHook.get_connection('vert_conn')

host=str(vert.host)
user=str(vert.login)
password=str(vert.password)
port = str(vert.port) 

start_date=datetime(2022, 10, 1)
end_date=datetime(2022, 10, 31)
date_diff = (end_date - start_date).days

vert_connect_instance = VertConnect(host, user, password, port)

def get_date_string(dt):
    return dt.strftime('%Y-%m-%d')

def trans_pg_to_vertica():
    print('test')
    print(date_diff)
    pg_hook = PostgresHook(postgres_conn_id="pg_conn")

    for i in range(date_diff):
        dt = start_date+timedelta(i)
        dt_str = get_date_string(dt)
        print(dt_str)
        

        sql_query_tranc = f"""
                        SELECT *
                        FROM public.transactions t
                        WHERE date(transaction_dt) = '{dt_str}'
                        """
        records_tranc = pg_hook.get_records(sql_query_tranc)
        print(len(records_tranc))
        if len(records_tranc) > 0:
            l = LoadToVertica(vert_connect_instance, records_tranc)
            l.trans_to_vertica()
        else:
            print(f'no data for {dt_str}')


def curr_to_vertica():
    pg_hook = PostgresHook(postgres_conn_id="pg_conn")

    for i in range(date_diff):
        dt = start_date+timedelta(i)
        dt_str = get_date_string(dt)
        print(dt_str)

        sql_query_tranc = f"""
                        SELECT * 
                        FROM public.currencies c 
                        WHERE date(date_update) = '{dt_str}'
                        """
        records_curr = pg_hook.get_records(sql_query_tranc)

        if len(records_curr) > 0:
            l = LoadToVertica(vert_connect_instance, records_curr)
            l.curr_to_vertica()
        else:
            print(f'no data for {dt_str}')

def stg_to_dds():
    for i in range(date_diff):
        dt = start_date+timedelta(i)
        dt_str = get_date_string(dt)

        s = StgToDwh(vert_connect_instance, dt_str)
        s.stg_to_dwh()

@dag(
    schedule_interval=None, #'@daily', 
    start_date=datetime(2022, 10, 1),
    # end_date=datetime(2022, 10, 5), 
    catchup=False,
    tags = ['daily_routine']
)

def pg_to_vertica():
    
    transactions = PythonOperator(
        task_id = 'trans_pg_to_vertica',
        python_callable = trans_pg_to_vertica
    )

    currencies = PythonOperator(
        task_id = 'curr_to_vertica',
        python_callable = curr_to_vertica
    )

    mart = PythonOperator(
        task_id = 'stg_to_dds',
        python_callable = stg_to_dds
    )

    [transactions, currencies] >> mart

pg_to_vertica = pg_to_vertica()