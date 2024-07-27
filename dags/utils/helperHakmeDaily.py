def process_airflow_runtime_parameters(**kwargs) -> None:
    from airflow.models import Variable
    from uuid import uuid4

    training_id = str(uuid4())

    """Process airflow runtime parameters."""
    ti = kwargs["ti"]
    bucket=''
    path_base=''
    training_dataset_file=''

    DAG_PARAMS = {
        "MODE": "twitter"
        
    }
    runtime_conf = kwargs["dag_run"].conf

    if runtime_conf:
        for key in runtime_conf.keys():
            if key in DAG_PARAMS.keys():
                DAG_PARAMS[key] = runtime_conf[key]

    for key in DAG_PARAMS.keys():
        ti.xcom_push(key=key, value=DAG_PARAMS[key])

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
    
def sql_consultar(sql_consulta):
    import psycopg2
    from psycopg2 import OperationalError
    import pandas as pd
    configura = config_db()
    conexion = psycopg2.connect(**configura)
    cursor = conexion.cursor()
    try:
        df_res = pd.read_sql_query(sql_consulta, conexion)
    except Exception as e:
        print(f"Error al consultar datos: {e}")
        df_res = pd.DataFrame()  # Devolver un DataFrame vacío en caso de error
    finally:
        conexion.close()
        return df_res

def sql_actualizar_estado_procesamiento(id, estado):
    import psycopg2
    from psycopg2 import OperationalError
    import pandas as pd
    configura = config_db()
    conexion = psycopg2.connect(**configura)
    cursor = conexion.cursor()
    try:
      consulta_sql = "UPDATE public.procesamiento_analisis SET estado=%s WHERE id_procesamiento_analisis = %s"
      cursor.execute(consulta_sql, (estado, id))
      conexion.commit()
    except OperationalError as e:
      print(f"Error al insertar el procesamiento_analisis: {e}")
    finally:
      cursor.close()
      conexion.close()

def sql_insertar_procesamiento(raw, agent_id, output):
    import json
    import psycopg2
    from psycopg2 import OperationalError
    import pandas as pd
    configura = config_db()
    conexion = psycopg2.connect(**configura)
    cursor = conexion.cursor()
    try:
      consulta_sql = """
        INSERT INTO procesamiento_analisis
          (id_paciente, agent_id, raw, tabla_output)
        VALUES
          (%s, %s, %s::jsonb, %s)
      """
      cursor.execute(consulta_sql, (raw["id_paciente"], agent_id, json.dumps(raw), output))
      conexion.commit()
    except OperationalError as e:
      print(f"Error al insertar el procesamiento_analisis: {e}")
    finally:
      cursor.close()
      conexion.close()

def sql_insertar(data, output):
    import psycopg2
    from psycopg2 import OperationalError
    import pandas as pd
    configura = config_db()
    conexion = psycopg2.connect(**configura)
    cursor = conexion.cursor()
    try:
        query = "INSERT INTO " + output + " ({}) VALUES ({})"
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))

        full_query = query.format(columns, placeholders)
        values = tuple(data.values())
        cursor.execute(full_query, values)
        conexion.commit()
    except OperationalError as e:
        print(f"Error al insertar el analisis_diario: {e}")
    finally:
        cursor.close()
        conexion.close()

def openai_procesar(parametros_del_agente, data, nombre_del_agente):
    import os
    import openai
    from langchain_community.embeddings import OpenAIEmbeddings
    from langchain.text_splitter import CharacterTextSplitter
    from langchain.vectorstores import PGVector
    from langchain_community.document_loaders import TextLoader
    from langchain.docstore.document import Document

    from langchain.chains.openai_functions import (
      create_openai_fn_chain,
      create_structured_output_chain,
    )
    from langchain_openai import ChatOpenAI
    from langchain.prompts import ChatPromptTemplate
    from langchain.pydantic_v1 import BaseModel, Field

    class resumen_semanal_data(BaseModel):
      id_paciente: int = Field(..., description="ID del paciente")
      semana: str = Field(..., description="fecha")
      semana_tratamiento: int = Field(..., description="Número de semana de tratamiento")
      resumen_recomendaciones: str = Field(..., description="Resumen de recomendaciones para el tratamiento")
      promedio_optimismo: float = Field(..., description="Promedio de Optimismo")
      promedio_pesimismo: float = Field(..., description="Promedio de Pesimismo")
      promedio_intencion_de_mejora: float = Field(..., description="Promedio de intención de mejora")
      ids_analisis_diario: list = Field(..., description="Lista de ids de mensajes utilizados")

    class analisis_diario_data(BaseModel):
        id_paciente: int = Field(..., description="ID del paciente")
        dia: str = Field(..., description="fecha")
        dia_tratamiento: int = Field(..., description="Número de día de tratamiento")
        optimismo: int = Field(..., description="Indice de optimismo")
        pesimismo: int = Field(..., description="Indice de pesimismo")
        intencionDeMejora: int = Field(..., description="Indice de intención de mejora")
        validos: list = Field(..., description="Lista de identificadores de mensajes válidos")
        ignorados: list = Field(..., description="Lista de identificadores de mensajes ignorados")
        recomendaciones: str = Field(..., description="Recomendaciones de mejora")

    os.environ["OPENAI_API_KEY"] = parametros_del_agente.get("agente_id")

    # If we pass in a model explicitly, we need to make sure it supports the OpenAI function-calling API.
    llm = ChatOpenAI(model=parametros_del_agente.get("model_name"), temperature=0)

    # remplazo dinámico de variables
    input_text = parametros_del_agente.get("human1")
    for key in data.keys():
      input_text = input_text.replace(f"<{key}>", str(data.get(key)))

    prompt = ChatPromptTemplate.from_messages(
          [
              ("system", parametros_del_agente.get("system")),
              ("human", input_text),
              ("human", parametros_del_agente.get("human2")),
          ]
      )

    chain = create_structured_output_chain(analisis_diario_data if nombre_del_agente == "analisisDiario" else resumen_semanal_data, llm, prompt)
    result=chain.run(data['input'])

    return result

def get_consulta_sql(nombre):
    consulta_sql = ""
    if nombre == "analisisDiario":
      consulta_sql = '''
          WITH primera_anotacion AS (
              SELECT
                  id_sesion,
                  min(fechahora) as primer_mensaje
              FROM public.anotaciones a
              GROUP BY id_sesion
          ), ultima_actualizacion as (
              SELECT
                id_paciente,
                max(dia) as ultimo_dia_actualizado
              FROM public.analisis_diario
              GROUP BY id_paciente
          ), anotacion as (
              SELECT
                  id_paciente,
                  f.primer_mensaje,
                  a.*
              FROM public.anotaciones a
              JOIN public.sesiones s
                  USING (id_sesion)
              JOIN public.pacientes p
                  USING (id_paciente)
              JOIN primera_anotacion f
                  USING (id_sesion)
              left join ultima_actualizacion u
                  using (id_paciente)
              WHERE coalesce(trim(pregunta), '') <> ''
                and coalesce(trim(emocion_principal), '') <> ''
                and escala > 0
                and date_trunc('day', (a.fechahora at time zone 'COT')) > date_trunc('day', (coalesce(u.ultimo_dia_actualizado, '2000-01-01') at time zone 'COT'))
          )
          SELECT
              id_paciente,
              array_agg(id_anotacion order by id_anotacion) as ids_anotaciones,
              substr(date_trunc('day', (fechahora at time zone 'COT'))::text, 0, 11) as dia,
              EXTRACT(days from (fechahora at time zone 'COT') - (primer_mensaje at time zone 'COT'))::int +1 as dia_tratamiento,
              array_to_string(array_agg(concat('Mensaje ', id_anotacion, ' (', emocion_principal,', ', escala , ')', ': ' , pregunta) order by id_anotacion), chr(10)) AS input
          FROM anotacion
          where date_trunc('day', (fechahora at time zone 'COT')) < date_trunc('day', (now() at time zone 'COT'))
            and id_anotacion not in (
              SELECT
                jsonb_array_elements(pa.raw -> 'ids_anotaciones')::int4 as id_anotacion
              from public.procesamiento_analisis pa
              WHERE pa.tabla_output = 'analisis_diario'
            )
          GROUP BY id_paciente, dia, dia_tratamiento
          ORDER BY id_paciente, dia_tratamiento asc
        '''
    elif nombre == "resumenSemanal":
        consulta_sql = '''
          SELECT
            id_paciente,
            substr(date_trunc('week', ad.dia at time zone 'COT')::text, 0, 11) AS semana,
            floor(ad.dia_tratamiento::real / 7)::int4 +1 AS semana_tratamiento,
            array_agg(ad.id_analisis_diario ORDER BY ad.id_analisis_diario ASC) AS ids_analisis_diario,
            round(avg(ad.optimismo), 2)::numeric(5,2) AS promedio_optimismo,
            round(avg(ad.pesimismo), 2)::numeric(5,2) AS promedio_pesimismo,
            round(avg(ad.intenciondemejora), 2)::numeric(5,2) AS promedio_intencion_de_mejora,
            array_to_string(array_agg(concat('- ', ad.recomendaciones) ORDER BY ad.id_analisis_diario ASC), concat(chr(10), chr(10))) AS input
          FROM public.analisis_diario ad
          WHERE coalesce(trim(ad.recomendaciones), '') <> ''
            AND date_trunc('week', ad.dia at time zone 'COT') < date_trunc('week', (now() at time zone 'COT'))
            AND ad.id_analisis_diario NOT IN (
              SELECT unnest(rs.ids_analisis_diario) FROM public.resumen_semanal rs
            )
            and id_analisis_diario not in (
                SELECT
                    jsonb_array_elements(pa.raw -> 'ids_analisis_diario')::int4 as id_analisis_diario
                FROM public.procesamiento_analisis pa
                where pa.tabla_output = 'resumen_semanal'
            )
          GROUP BY semana, id_paciente, semana_tratamiento
          ORDER BY id_paciente, semana ASC
        '''
    
    return consulta_sql

def inicializar_cola(**kwargs):
  import pandas as pd
  import logging

  df_agentes = sql_consultar(kwargs["query"])
  logging.info(f"[inicializar_cola] Hay {len(df_agentes.index)} agentes listos")

  for index, agente in df_agentes.iterrows():
      df_encolar = sql_consultar(get_consulta_sql(agente["name"]))
      logging.info(f"[inicializar_cola] Empezando agente: {str(agente['name'])}")
      for index, encolar in df_encolar.iterrows():
          sql_insertar_procesamiento(encolar.to_dict(), agente["agent_id"], agente["parametros"].get('output_table'))
      logging.info(f"[inicializar_cola] Terminado agente: {str(agente['name'])}")
      df_encolar = pd.DataFrame()

def procesar_cola(**kwargs):
  import pandas as pd
  import logging

  df_agentes = sql_consultar(kwargs["query"])
  logging.info(f"[procesar_cola] Hay {len(df_agentes.index)} agentes listos")

  for index, agente in df_agentes.iterrows():
    df_cola = sql_consultar("SELECT pa.* from public.procesamiento_analisis pa where pa.agent_id = " + str(agente["agent_id"]) + " and pa.estado = 'pendiente'")
    logging.info(f"[procesar_cola] Empezando agente: {str(agente['name'])}")

    for index, cola in df_cola.iterrows():
      sql_actualizar_estado_procesamiento(cola["id_procesamiento_analisis"], "en procesamiento")
      r = openai_procesar(agente["parametros"], cola["raw"], agente["name"])
      sql_insertar(dict(r), cola["tabla_output"])
      sql_actualizar_estado_procesamiento(cola["id_procesamiento_analisis"], "procesado")
    
    logging.info(f"[procesar_cola] Terminado agente: {str(agente['name'])}")
