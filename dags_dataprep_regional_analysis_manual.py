import json
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
from airflow.models import Variable

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': airflow.utils.dates.days_ago(0),
  'email': ['airflow@example.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(seconds=15)
}
headers = {
  "Content-Type": "application/json",
  "Authorization": Variable.get("trifacta_bearer")
}

DAG_NAME = 'dataprep_regional_analysis_manual'
recipe_id = Variable.get("recipe_id")
region = Variable.get("region")

def check_flow_run_complete(response):
  return response.json()['status'] == 'Complete'

def run_flow_and_wait_for_completion():
  run_flow_task = SimpleHttpOperator(
    task_id='run_flow',
    endpoint='/v4/jobGroups',
    data=json.dumps({"wrangledDataset": {"id": int(recipe_id)},"runParameters": {"overrides": {"data": [{"key": "region","value": str(region) }]}}}),
    headers=headers,
    xcom_push=True,
    dag=dag,
  )

  wait_for_flow_run_to_complete = HttpSensor(
    task_id='wait_for_flow_run_to_complete',
    endpoint='/v4/jobGroups/{{ json.loads(ti.xcom_pull(task_ids="run_flow"))["id"] }}?embed=jobs.errorMessage',
    headers=headers,
    response_check=check_flow_run_complete,
    poke_interval=10,
    dag=dag,
  )

  run_flow_task.set_downstream(wait_for_flow_run_to_complete)

  return wait_for_flow_run_to_complete

with DAG(DAG_NAME, default_args=default_args, catchup=False, schedule_interval=None, user_defined_macros={ 'json': json }) as dag:
  task_sequence = run_flow_and_wait_for_completion()