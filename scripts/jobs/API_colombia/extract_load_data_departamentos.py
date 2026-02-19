import os
import polars as pl
from typing import TYPE_CHECKING
from sqlalchemy import create_engine
# Modulos personalizados
from common.api_connection import fetch_data_from_api
from common.load_data_to_pg import load_data_to_pg
from transformation.json.struct_or_list_to_json import json_string

# Verficar tipos de funciones
if TYPE_CHECKING:
    from polars import DataFrame 

def extract_departamentos_data() -> "pl.DataFrame":
    """Función para extraer los datos de los departamentos desde la API de Colombia."""
    # URL para obtener los datos de los departamentos
    URL_API_COLOMBIA = 'https://api-colombia.com/api/v1/Department'

    # Obtener datos desde la API
    return fetch_data_from_api(URL_API_COLOMBIA)


def transform_departamentos_data(df: "pl.DataFrame") -> "pl.DataFrame":
    # Aplicar transformación para convertir columnas de tipo Struct o List a JSON string
    df = json_string(df)
    
    return df


def load_departamentos_data(df: "pl.DataFrame") -> None:
    # Obtener URI de conexión desde variable de entorno
    conn_uri = os.getenv('AIRFLOW_CONN_WAREHOUSE_POSTGRES')
    
    # Cargar datos a PostgreSQL utilizando la función genérica load_data_to_pg
    load_data_to_pg(
        df,
        'raw.api_colombia_departamentos',
        connection_uri=conn_uri,
        if_table_exists='replace'
    )
    
if __name__ == "__main__":
    # Obtener datos desde la API-Colombia
    df_departamentos = extract_departamentos_data()
    # Transformar columnas de tipo Struct o List a JSON string
    df_departamentos = transform_departamentos_data(df_departamentos)
    # Cargar datos a PostgreSQL
    load_departamentos_data(df_departamentos)
