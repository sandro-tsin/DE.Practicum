from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os


os.environ['AWS_ACCESS_KEY_ID'] = 'YCAJEWXOyY8Bmyk2eJL-hlt2K'
os.environ['AWS_ACCESS_KEY_ID'] = 'YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA'

import boto3
from datetime import datetime
import vertica_python

def load_file(key:str, dir:str): 

    vert = BaseHook.get_connection('vertica')

    conn = vertica_python.connect(
        host=str(vert.host), 
        user=str(vert.login), 
        password=str(vert.password), 
        port = str(vert.port)
    )

    with conn.cursor() as cur: 
        cur.execute(
            f"""
            COPY STV230529__STAGING.{key}
            FROM LOCAL '{dir}'
            DELIMITER ','
            """
        )

        conn.commit()
        conn.close()
        return(f'{key} uploaded')
         

def fetch_file(bucket:str, key:str, ds):

    dest_dir = f'/data/{key}_{ds}.csv'

    print(os.environ.get("AWS_ACCESS_KEY_ID") , os.environ.get("AWS_SECRET_ACCESS_KEY"))
    
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    try: 
        s3_client.download_file(
            Bucket=bucket,
            Key=f'{key}.csv',
            Filename=dest_dir
            )
        load_file(key, dest_dir)
    except Exception as e:
        print(f'Error while downloading file from s3: {e}')


keys = ['users', 'groups', 'dialogs', 'group_log']

@dag (
    schedule_interval=None, 
    start_date=datetime(2021, 12, 1), 
    catchup=False
)

def load_dag():

    start = DummyOperator(task_id="srart")

    load_dialogs = PythonOperator(
        task_id = 'load_dialogs',
        python_callable = fetch_file,
        
        op_kwargs = {
            'bucket': 'sprint6',
            'key': keys[2] ,
            'ds': '{{ ds_nodash }}'
        }
    )
    
    load_groups = PythonOperator(
        task_id = 'load_groups',
        python_callable = fetch_file,
        op_kwargs = {
            'bucket': 'sprint6',
            'key': keys[1],
            'ds': '{{ ds_nodash }}'
        }
    )

    load_users = PythonOperator(
        task_id = 'load_users',
        python_callable = fetch_file,
        op_kwargs = {
            'bucket': 'sprint6',
            'key': keys[0],
            'ds': '{{ ds_nodash }}'
        }
    )

    load_group_log = PythonOperator(
        task_id = 'load_group_log',
        python_callable = fetch_file,
        op_kwargs = {
            'bucket': 'sprint6',
            'key': keys[3],
            'ds': '{{ ds_nodash }}'
        }

    )

    end = DummyOperator(task_id="end")

    start >> [load_dialogs, load_groups, load_users, load_group_log] >> end


load_dag = load_dag()
