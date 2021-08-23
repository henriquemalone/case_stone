from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 8, 21),
}

dag = DAG(
    dag_id='dag_stone',
    schedule_interval='0 7 * * *',
    default_args=args,
)

hello_my_task = BashOperator(
    task_id='comando',
    bash_command='python3 /usr/local/airflow/dags/comando.py',
    dag=dag,
)