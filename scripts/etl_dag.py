import sys
import os
from airflow.models import DAG
from datetime import timedelta
import boto3
from configparser import ConfigParser
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

config = ConfigParser()
config.read("/home/ubuntu/workspace/configs/config.ini")

BUCKET_NAME = config['INFO']['BUCKET_NAME']
TGT_DIR = config['INFO']['DATA_DIR']
SCRIPTS_DIR = config['INFO']['SCRIPTS_DIR']


def download_files_from_s3():

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)
    objects = bucket.objects.filter(Prefix='pending/')

    for obj in objects:
        path, filename = os.path.split(obj.key)
        if filename:
            bucket.download_file(obj.key, TGT_DIR + filename)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

etl_dag = DAG('etl_user_place_profiles', default_args=default_args,
              schedule_interval='0 */2 * * *', start_date=days_ago(1))

download = PythonOperator(
    task_id='download_files',
    dag=etl_dag,
    python_callable=download_files_from_s3
)

etl_place_details = SparkSubmitOperator(
    task_id='etl_place_details',
    dag=etl_dag,
    application=SCRIPTS_DIR+'etl_place_details.py',
    conn_id='spark_local'
)

etl_user_details = SparkSubmitOperator(
    task_id='etl_user_details',
    dag=etl_dag,
    application=SCRIPTS_DIR+'etl_user_details.py',
    conn_id='spark_local'
)

etl_place_details.set_upstream(download)
etl_user_details.set_upstream(download)
