import json
import logging
import polars as pl
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from polars import DataFrame

# Configurración de logging
logger = logging.getLogger(__name__)
# Activar visualizacion de logs a nivel INFO
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def json_string(df: "pl.DataFrame") -> "pl.DataFrame":
    """
    Dado un DataFrame de Polars, esta función identifica las columnas que son de tipo Struct o List y aplica
    json.dumps a cada celda de esas columnas para convertirlas en cadenas JSON. Esto es útil para preparar 
    los datos antes de cargarlos en una base de datos que no soporta tipos complejos.
    
    :param df: DataFrame de Polars que se desea procesar.
    :type df: `pl.DataFrame`
    :return: DataFrame de Polars con las columnas de tipo Struct o List convertidas a cadenas JSON.
    :rtype: `pl.DataFrame`
    """
    logging.info("Starting JSON colums processing for Struct and List columns.")

    # Consultar estructura del DataFrame
    schema = df.schema
    # Seleccionamos las columnas que son de tipo Struct o List
    cols_to_json = [
        col_name for col_name, dtype in schema.items()
        if isinstance(dtype, (pl.Struct, pl.List))
    ]

    if cols_to_json:
        # Aplicamos json.dumps a cada celda de esas columnas
        cols_str = "".join([f"\n                                    - {col}" for col in cols_to_json])
        logging.info(f"Columns to process: {cols_str}")
        df = df.with_columns([
            pl.col(col).map_elements(
                # ! WARNIG json.dumps no soportar operaciones vectorizadas, por lo que se aplicará a cada elemento 
                # ! individualmente a través de map_elements, lo que puede ser lento para grandes DataFrames.
                lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None, 
                return_dtype=pl.String
            ).alias(col) 
            for col in cols_to_json
        ])

        logging.info("JSON  columns processing completed.")
        return df
    
    else:
        logging.info("No columns to process.")
