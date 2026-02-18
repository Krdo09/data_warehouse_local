import logging
import os
import polars as pl
from sqlalchemy import create_engine
from typing import Any, TYPE_CHECKING

# Validación de tipospara parametros de funciones
if TYPE_CHECKING:
    from polars import DataFrame

# Configurración de logging
logger = logging.getLogger(__name__)
# Activar visualizacion de logs a nivel INFO
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_data_to_pg(
        df: "pl.DataFrame", 
        table_name: str,
        connection_uri: str,
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
    # Crear motor de conexión
    logging.info(f"Creating database engine with URI: '{connection_uri}'")
    engine = create_engine(connection_uri)

    # Establecer conexión y cargar datos a postgreSQL utilizando el método write_database de Polars, que es compatible con SQLAlchemy
    with engine.begin() as connection:
        logging.info(f"Loading data, rows {df.height} into PostgreSQL table '{table_name}'...")
        df.write_database(
            table_name=table_name,
            connection=connection,
            engine='sqlalchemy',
            **kwargs
        )

        logging.info(f"Data loaded successfully into table '{table_name}'")
