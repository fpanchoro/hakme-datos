from utils import sql_fn
from utils import openai_fn

def inicializar_cola(**kwargs):
  import pandas as pd
  import logging

  logging.info(f"[inicializar_cola] Empezando agente: {str(kwargs['agentName'])}")
  df_agentes = sql_fn.sql_consultar("select p.* from public.agentes_inteligentes_hakme p where p.type = 'System' and p.status = 'Activo' and name ='" + str(kwargs["agentName"]) + "'")

  if len(df_agentes.index) == 1:
    agente = df_agentes.iloc[0]

    df_data = sql_fn.sql_consultar(agente["parametros"].get("sql"))
    for index, data in df_data.iterrows():
      sql_fn.sql_insertar_procesamiento(data.to_dict(), str(agente["agent_id"]), agente["parametros"].get('output_table'))
    logging.info(f"[inicializar_cola] Terminado agente: {str(kwargs['agentName'])}")
  else:
    logging.info("No se encontró el agente " + str(kwargs["agentName"]) + " o hay más de los esperados")

def procesar_cola(**kwargs):
  import pandas as pd
  import logging
  import os

  logging.info(f"[procesar_cola] Empezando agente: {str(kwargs['agentName'])}")
  df_agentes = sql_fn.sql_consultar("select p.* from public.agentes_inteligentes_hakme p where p.type = 'System' and p.status = 'Activo' and name ='" + str(kwargs["agentName"]) + "'")

  if len(df_agentes.index) == 1:
    agente = df_agentes.iloc[0]
    df_cola = sql_fn.sql_consultar("select pa.* from public.procesamiento_analisis pa where pa.agent_id = " + str(agente["agent_id"]) + " and pa.estado = '" + str(kwargs["status"]) + "' order by id_procesamiento_analisis asc")

    DynamicModel = None
    DynamicModel = openai_fn.create_dynamic_model(agente["name"], agente["parametros"].get("class"))
    os.environ["OPENAI_API_KEY"] = agente["parametros"].get("agente_id")

    for index, cola in df_cola.iterrows():
        sql_fn.sql_actualizar_estado_procesamiento(cola["id_procesamiento_analisis"], "en procesamiento")
        r = openai_fn.openai_procesar(agente["parametros"], cola["raw"], DynamicModel)
        if r is not None:
          sql_fn.sql_insertar(dict(r), cola["tabla_output"])
          sql_actualizar_estado_procesamiento(cola["id_procesamiento_analisis"], "procesado")
        else:
          sql_actualizar_estado_procesamiento(cola["id_procesamiento_analisis"], "cancelado")
    logging.info(f"[procesar_cola] Terminado agente: {str(kwargs['agentName'])}")
  else:
    logging.info("No se encontró el agente " + str(kwargs["agentName"]) + " o hay más de los esperados")