def config_db():
    from airflow.models import Variable
    configura = {
        'dbname': Variable.get("DB_NAME"),
        'user': Variable.get("DB_USER"),
        'password': Variable.get("DB_PASSWORD"),
        'host': Variable.get("DB_HOST"),
        'port': Variable.get("DB_PORT")
    }
    return configura
    

def inicializar_cola(**kwargs):
  from sql import *
  import pandas as pd
  import logging

  df_agentes = sql_consultar(f"select p.* from public.agentes_inteligentes_hakme p where p.type = 'System' and p.status = 'Activo' and name ='{str(kwargs["agentName"])}'")
  logging.info(f"[inicializar_cola] Empezando agente: {str(agente['name'])}")

  if len(df_agentes.index) == 1:
    agente = df_agentes.iloc[0]

    df_data = sql_consultar(agente["parametros"].get("sql"))
    for index, data in df_data.iterrows():
      sql_insertar_procesamiento(data.to_dict(), str(agente["agent_id"]), agente["parametros"].get('output_table'))
    logging.info(f"[inicializar_cola] Terminado agente: {str(agente['name'])}")
  else:
    logging.info(f"No se encontró el agente {str(kwargs["agentName"])} o hay más de los esperados")

def procesar_cola(**kwargs):
  from sql import *
  from openai import *
  import pandas as pd
  import logging
  import os

  df_agentes = sql_consultar(f"select p.* from public.agentes_inteligentes_hakme p where p.type = 'System' and p.status = 'Activo' and name ='{str(kwargs["agentName"])}'")

  if len(df_agentes.index) == 1:
    agente = df_agentes.iloc[0]
    df_cola = sql_consultar("select pa.* from public.procesamiento_analisis pa where pa.agent_id = " + str(agente["agent_id"]) + " and pa.estado = 'pendiente' order by id_procesamiento_analisis asc")

    DynamicModel = None
    DynamicModel = create_dynamic_model(agente["name"], agente["parametros"].get("class"))
    os.environ["OPENAI_API_KEY"] = agente["parametros"].get("agente_id")

    for index, cola in df_cola.iterrows():
      sql_actualizar_estado_procesamiento(cola["id_procesamiento_analisis"], "en procesamiento")
      r = openai_procesar(agente["parametros"], cola["raw"], DynamicModel)
      sql_insertar(dict(r), cola["tabla_output"])
      sql_actualizar_estado_procesamiento(cola["id_procesamiento_analisis"], "procesado")

  else:
    logging.info(f"No se encontró el agente {str(kwargs["agentName"])} o hay más de los esperados")