from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                'owner': 'airflow',
                'start_date':datetime(2020, 1, 1),
                }

dag_spark = DAG(
                        dag_id = "users_marts",
                        default_args=default_args,
                        schedule_interval=None,
                        )

spark_submit_users_geo = SparkSubmitOperator(
                        task_id='users_geo_mart_task',
                        dag=dag_spark,
                        application ='users_geo_mart.py',
                        conn_id= 'yarn_spark',
                        application_args = ["2", "2022-05-21", "/user/master/data/geo/events", "/user/atsinam/data/marts/users_geo_mart"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


spark_submit_zones_mart = SparkSubmitOperator(
                        task_id='zones_mart_task',
                        dag=dag_spark,
                        application ='zones_mart.py',
                        conn_id= 'yarn_spark',
                        application_args = ["2", "2022-05-21", "/user/master/data/geo/events", "/user/atsinam/data/marts/zones_mart"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

spark_submit_friend_sub = SparkSubmitOperator(
                        task_id='friends_sug_task',
                        dag=dag_spark,
                        application ='friends_sug.py',
                        conn_id= 'yarn_spark',
                        application_args = ["2", "2022-05-21", "/user/master/data/geo/events", "/user/atsinam/data/marts/friends_sug"],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


spark_submit_users_geo >> spark_submit_zones_mart >> spark_submit_friend_sub