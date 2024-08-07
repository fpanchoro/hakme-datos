from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils import helperHakmeDaily
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

DAG_NAME = "hakme-pipeline-daily-v2"
DEFAULT_ARGS = {
  "owner": "airflow",
}

with DAG(
        dag_id=DAG_NAME,
        default_args=DEFAULT_ARGS,
        start_date=datetime(2024, 8, 8, 5, 25, 0),
        schedule_interval=timedelta(days=1),
        catchup=False,
) as dag:

    inicializar_cola_analisis_diario_v2 = PythonOperator(
        task_id="inicializar_cola_analisis_diario_v2",
        python_callable=helperHakmeDaily.inicializar_cola,
        op_kwargs={"agentName": "analisisDiariov2", "status": "pendiente"},
    )

    procesar_cola_analisis_diario_v2 = PythonOperator(
        task_id="procesar_cola_analisis_diario_v2",
        python_callable=helperHakmeDaily.procesar_cola,
        op_kwargs={"agentName": "analisisDiariov2", "status": "pendiente"},
    )

    inicializar_cola_analisis_diario_v2 >> procesar_cola_analisis_diario_v2