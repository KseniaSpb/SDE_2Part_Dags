from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from beeline.airflow.hashicorp_vault.VaultOperator import VaultOperator
from airflow.operators.dummy_operator import DummyOperator
import pendulum

local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 14, 1, 0, tzinfo=local_tz),
    'email': ['########'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'school_de_########_el',
    default_args=default_args,
    description='EL for tables from postgres',
    schedule_interval=None,
    on_success_callback=VaultOperator.cleanup_xcom,
    on_failure_callback=VaultOperator.cleanup_xcom,
    tags=['school_de_########'],
)

dag.doc_md = """
#### Проект:
#### Описание DAG:
EL таблиц
#### Входные данные:
postgres - bookings.seats
postgres - bookings.flights_v
#### Выходные данные:
hive - school_de.seats_########
hive - school_de.flights_v_########
"""

start_DAG = DummyOperator(
    task_id='start',
    dag=dag)

set_secrets = VaultOperator(
    task_id='set_secrets',
    provide_context=True,
    secret_path="airflow-secrets",
    secret_name="########",
    dag=dag
)

submit_spark_job = SparkSubmitOperator(
    application="########",
    name="school_de_########_el",
    conf={'spark.yarn.queue': '########', 'spark.submit.deployMode': 'cluster', 'spark.driver.memory': '4g',
          'spark.executor.memory': '8g', 'spark.executor.cores': '1', 'spark.dynamicAllocation.enabled': 'true',
          'spark.shuffle.service.enabled': 'true', 'spark.dynamicAllocation.maxExecutors': '10'
          },
    task_id="submit_########_el",
    conn_id="########",
    java_class="AppEl",
    queue='########',
    application_args=[
        "{{task_instance.xcom_pull(key='########-username', task_ids=['set_secrets'])[0]}}",
        "{{task_instance.xcom_pull(key='########-password', task_ids=['set_secrets'])[0]}}"
    ],
    dag=dag
)
start_DAG >> set_secrets >> submit_spark_job