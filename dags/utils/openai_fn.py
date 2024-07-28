def num_tokens_from_string(string: str) -> int:
  import tiktoken
  """Returns the number of tokens in a text string."""

  encoding = tiktoken.get_encoding("cl100k_base")
  num_tokens = len(encoding.encode(string))
  return num_tokens

def create_dynamic_model(class_name: str, fields_json: dict):
  import sqlite3
  from langchain.pydantic_v1 import create_model, Field
  fields = {}
  for field_name, field_info in fields_json.items():
      field_type = eval(field_info['type'])
      description = field_info.get('description', '')
      fields[field_name] = (field_type, Field(..., description=description))

  return create_model(class_name, **fields)

def openai_procesar(parametros_del_agente, data, modelo_data):
  import openai
  from langchain.chains.openai_functions import (
    create_openai_fn_chain,
    create_structured_output_chain,
  )
  from langchain_openai import ChatOpenAI
  from langchain.prompts import ChatPromptTemplate


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

  try:
    chain = create_structured_output_chain(modelo_data, llm, prompt)
    result = chain.run(data['input'])

  except OperationalError as e:
    print(f"Error al procesar: {e}")

  return result