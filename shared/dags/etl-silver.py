import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'Helder',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 2,
}

dag = DAG(
    dag_id='etl-silver',
    default_args=args,
    schedule_interval=None
)

A = BashOperator(
    task_id='ler-script-R',
    bash_command="Rscript /usr/local/spark/app/scripts/etl-silver.R",
    dag=dag)

A