"""
### Example Data Pipeline triggering Dataprep job, writing result to BigQuery and then running aggregation SQL job
### based on https://cloud.google.com/blog/products/data-analytics/how-to-orchestrate-cloud-dataprep-jobs-using-cloud-composer
"""

import airflow
from airflow.operators import SimpleHttpOperator, HttpSensor
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import json

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),datetime.min.time())

headers = {
  "Content-Type": "application/json",
  "Authorization": Variable.get("trifacta_bearer")
}


default_args = {
    'owner': 'Alex Osterloh',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['aosterloh@google.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

def check_dataprep_run_complete(response):
  return response.json()['status'] == 'Complete'

with airflow.DAG(
        'dataprep_bq_automation',
        default_args=default_args,
        # Not scheduled, trigger only
        schedule_interval=None,
        user_defined_macros={
          'json': json
        }
) as dag:

# recipe id = 1339729
# example URL https://clouddataprep.com/flows/250119?recipe=1339729&tab=recipe&projectId=thatistoomuchdata
  run_dataprep_task = SimpleHttpOperator(
    task_id='run_dataprep_job',
    endpoint='/v4/jobGroups',
    data=json.dumps({"wrangledDataset": {"id": 1339729},"runParameters": {"overrides": {"data": [{"key": "country","value": "Germany"}]}}}),
    headers=headers,
    xcom_push=True,
    dag=dag,
  )

  wait_for_dataprep_job_to_complete = HttpSensor(
    task_id='wait_for_dataprep_job_to_complete',
    endpoint='/v4/jobGroups/{{ json.loads(ti.xcom_pull(task_ids="run_dataprep_job"))["id"] }}?embed=jobs.errorMessage',
    headers=headers,
    response_check=check_dataprep_run_complete,
    poke_interval=10,
    dag=dag,
    )

bigquery_run_sql = BigQueryOperator(
    task_id='bq_run_sql',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bql='''
    #standardsql
    SELECT
      BPQ150A,
      AVG(bPXPLS) AS avg_BPXPLS,
      AVG(bpxsy2) AS avg_BPXSY2
    FROM
      `thatistoomuchdata.dataprep_output.BPX_E_XPT_cleaned`
    WHERE
      BPXSY2 > 0
    GROUP BY
      BPQ150A
    ''',
    destination_dataset_table='thatistoomuchdata.dataprep_output.daily_aggregate${{ yesterday_ds_nodash }}',
    dag=dag)



run_dataprep_task >> wait_for_dataprep_job_to_complete >> bigquery_run_sql
