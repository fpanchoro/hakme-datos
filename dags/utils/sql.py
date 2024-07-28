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