from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils import helperHakmeDaily
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

DAG_NAME = "hakme-clean"
DEFAULT_ARGS = {
  "owner": "airflow",
}

with DAG(
        dag_id=DAG_NAME,
        default_args=DEFAULT_ARGS,
        start_date=datetime(2024, 8, 8, 5, 10, 0),
        schedule_interval=timedelta(days=1),
        catchup=False,
) as dag:

    inicializar_cola_cancelados_analisis_diario = PythonOperator(
        task_id="inicializar_cola_cancelados_analisis_diario",
        python_callable=helperHakmeDaily.inicializar_cola,
        op_kwargs={"agentName": "analisisDiario", "status": "cancelado"},
    )

    procesar_cola_cancelados_analisis_diario = PythonOperator(
        task_id="procesar_cola_cancelados_analisis_diario",
        python_callable=helperHakmeDaily.procesar_cola,
        op_kwargs={"agentName": "analisisDiario", "status": "cancelado"},
    )

    inicializar_cola_cancelados_resumen_semanal = PythonOperator(
        task_id="inicializar_cola_cancelados_resumen_semanal",
        python_callable=helperHakmeDaily.inicializar_cola,
        op_kwargs={"agentName": "resumenSemanal", "status": "cancelado"},
    )

    procesar_cola_cancelados_resumen_semanal = PythonOperator(
        task_id="procesar_cola_cancelados_resumen_semanal",
        python_callable=helperHakmeDaily.procesar_cola,
        op_kwargs={"agentName": "resumenSemanal", "status": "cancelado"},
    )

    inicializar_cola_cancelados_analisis_diario_v2 = PythonOperator(
        task_id="inicializar_cola_cancelados_analisis_diario_v2",
        python_callable=helperHakmeDaily.inicializar_cola,
        op_kwargs={"agentName": "analisisDiariov2", "status": "cancelado"},
    )

    procesar_cola_cancelados_analisis_diario_v2 = PythonOperator(
        task_id="procesar_cola_cancelados_analisis_diario_v2",
        python_callable=helperHakmeDaily.procesar_cola,
        op_kwargs={"agentName": "analisisDiariov2", "status": "cancelado"},
    )

    inicializar_cola_cancelados_analisis_diario >> procesar_cola_cancelados_analisis_diario >> inicializar_cola_cancelados_resumen_semanal >> procesar_cola_cancelados_resumen_semanal >> inicializar_cola_cancelados_analisis_diario_v2 >> procesar_cola_cancelados_analisis_diario_v2