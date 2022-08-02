from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from ######## import VaultOperator
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
    'school_de_########_calc',
    default_args=default_args,
    description='Calc results',
    schedule_interval=None,
    tags=['school_de_########'],
)

dag.doc_md = """
#### Проект:
#### Описание DAG:
Расчёт задач
#### Входные данные:
hive - school_de.bookings_ticket_flights
hive - school_de.bookings_flights
hive - school_de.bookings_flights_v
hive - school_de.bookings_airports
hive - school_de.bookings_routes
hive - school_de.bookings_aircrafts
#### Выходные данные:
hive - school_de.results_########
"""

start_DAG = DummyOperator(
    task_id='start',
    dag=dag)

submit_spark_job = SparkSubmitOperator(
    application="########",
    name="school_de_########_calc",
    conf={'spark.yarn.queue': '########', 'spark.submit.deployMode': 'cluster', 'spark.driver.memory': '4g',
          'spark.executor.memory': '8g', 'spark.executor.cores': '1', 'spark.dynamicAllocation.enabled': 'true',
          'spark.shuffle.service.enabled': 'true', 'spark.dynamicAllocation.maxExecutors': '10'
          },
    task_id="submit_########_calc",
    conn_id="########",
    java_class="AppCalc",
    queue='########',
    dag=dag
)
start_DAG >> submit_spark_job
