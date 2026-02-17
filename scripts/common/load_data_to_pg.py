import os
import polars as pl
from typing import Any, TYPE_CHECKING
from sqlalchemy import create_engine

if TYPE_CHECKING:
    from polars import DataFrame

def load_data_to_pg(
        df: "pl.DataFrame", 
        table_name: str,
        **kwargs: Any
    ) -> None:
    """
    Carga un DataFrame de Polars a una tabla en PostgreSQL utilizando SQLAlchemy. 
    El usuario puede especificar si desea reemplazar o agregar a la tabla existente a través de kwargs, 
    así como otros argumentos compatibles con pl.DataFrame.write_database.
    
    :param df: DataFrame de Polars que se desea cargar en PostgreSQL.
    :type df: `pl.DataFrame`
    :param table_name: Nombre de la tabla en PostgreSQL donde se cargará el DataFrame.
    :type table_name: `str`
    :param kwargs: Argumentos adicionales para controlar el comportamiento de la carga (por ejemplo, if_table_exists).
    :type kwargs: `keyword arguments`
    :return: `None`
    """
    # Obtener URI de conexión desde variable de entorno
    conn_uri = os.getenv('AIRFLOW_CONN_WAREHOUSE_POSTGRES')

    # Crear motor de conexión
    engine = create_engine(conn_uri)

    try:
        print(f"Loading data into PostgreSQL table '{table_name}'...")

        # Se deja que el usuario especifique si desea reemplazar o agregar a la tabla existente a través de kwargs
        # Adicional a esto, se pueden pasar otros argumentos compatibles con pl.DataFrame.write_database a través de kwargs
        df.write_database(
            table_name=table_name,
            connection=engine,
            engine='sqlalchemy',
            **kwargs
        )

        print(f"Data loaded successfully into PostgreSQL table '{table_name}'.")

    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")
        raise e
