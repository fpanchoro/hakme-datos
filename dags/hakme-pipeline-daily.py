from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from utils import helperHakmeDaily
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

DAG_NAME = "hakme-pipeline-daily"
DEFAULT_ARGS = {
  "owner": "airflow",
}


with DAG(
        dag_id=DAG_NAME,
        default_args=DEFAULT_ARGS,
        start_date=datetime(2024, 6, 30, 5, 30, 0),
        schedule_interval=timedelta(days=1),
        catchup=False,
) as dag:

    process_airflow_runtime_parameters = PythonOperator(
        task_id="process_airflow_runtime_parameters",
        python_callable=helperHakmeDaily.process_airflow_runtime_parameters
    )

    inicializar_cola_analisis_diario = PythonOperator(
        task_id="inicializar_cola_analisis_diario",
        python_callable=helperHakmeDaily.inicializar_cola,
        op_kwargs={"query": "SELECT p.* from public.agentes_inteligentes_hakme p where p.type = 'System' and name = 'analisisDiario'"},
    )

    procesar_cola_analisis_diario = PythonOperator(
        task_id="procesar_cola_analisis_diario",
        python_callable=helperHakmeDaily.procesar_cola,
        op_kwargs={"query": "SELECT p.* from public.agentes_inteligentes_hakme p where p.type = 'System' and name = 'analisisDiario'"},
    )

    inicializar_cola_resumen_semanal = PythonOperator(
        task_id="inicializar_cola_resumen_semanal",
        python_callable=helperHakmeDaily.inicializar_cola,
        op_kwargs={"query": "SELECT p.* from public.agentes_inteligentes_hakme p where p.type = 'System' and name = 'resumenSemanal'"},
    )

    procesar_cola_resumen_semanal = PythonOperator(
        task_id="procesar_cola_resumen_semanal",
        python_callable=helperHakmeDaily.procesar_cola,
        op_kwargs={"query": "SELECT p.* from public.agentes_inteligentes_hakme p where p.type = 'System' and name = 'resumenSemanal'"},
    )

    process_airflow_runtime_parameters >> inicializar_cola_analisis_diario >> procesar_cola_analisis_diario >> inicializar_cola_resumen_semanal >> procesar_cola_resumen_semanal