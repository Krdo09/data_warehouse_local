import os
from typing import Dict, Any

def get_pg_connection_details() -> Dict[str, Any]:
    """
    Función para obtener los detalles de conexión a PostgreSQL desde variables de entorno.
    
    :return: Diccionario con los detalles de conexión a PostgreSQL.
    :rtype: `Dict[str, Any]`
    """
    return {
        'host': os.getenv('PG_HOST', 'localhost'),
        'port': os.getenv('WAREHOUSE_HOST_PORT', '5432'),
        'database': os.getenv('WAREHOUSE_DB', 'my_database'),
        'user': os.getenv('WAREHOUSE_USER', 'postgres'),
        'password': os.getenv('WAREHOUSE_PASSWORD', 'postgres')
    }