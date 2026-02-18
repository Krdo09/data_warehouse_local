import logging
import requests
import polars as pl
from tenacity import (
    retry,
    retry_if_exception_type, 
    stop_after_attempt,
    wait_exponential
)
from typing import Dict, Any

# Configurración de logging
logger = logging.getLogger(__name__)
# Activar visualizacion de logs a nivel INFO
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Definir la configuración de reintentos para la función de conexión a la API
# Se intentará hasta 3 veces, con un tiempo de espera exponencial entre intentos (1s, 2s, 4s, etc.),
# Solo se reintentará si ocurre una excepción de tipo RequestException (errores de red, tiempo de espera, etc.)
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def fetch_data_from_api(
        url: str, 
        params: Dict[str, Any] = None,
        timeout: int = 10
    ) -> "pl.DataFrame":
    """
    Obtiene datos desde una API y retorna la información obtenida como un DataFrame de Polars.
    La función incluye un mecanismo de reintentos para manejar posibles fallos en la conexión a la API, como errores de red o tiempos de espera.
    
    :param url: URL de la API desde la cual se desea obtener los datos
    :type url: `str`
    :param params: Parámetros de la petición a la API, por ejemplo, filtros o claves de autenticación
    :type params: `Dict[str, Any]`
    :param timeout: Tiempo máximo de espera para la petición HTTP/S en segundos (default: 10). El usuario puede variar dicho tiempo dependiendo las necesidades de extracción y la estabilidad de la API. 
    Un timeout más corto puede ayudar a detectar rápidamente problemas de conexión, mientras que un timeout más largo puede ser útil para APIs que suelen responder lentamente.
    :type timeout: `int` *default: 10*
    :return: DataFrame de Polars con los datos obtenidos de la API
    :rtype: `pl.DataFrame`
    """
    logger.info(f'Fetching data from API, URL: "{url}"')

    try:
        # Realizar llamado a la API con un timeout para evitar bloqueos prolongados
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()  # Lanza una excepción si la respuesta HTTP no es exitosa (código de estado 4xx o 5xx)

        # Obtener datos en formato json
        data = response.json()
        logging.info("Data fetched successfully")

        # Se valida si los datos son una lista de diccionarios o un diccionario
        if isinstance(data, dict):
            data = [data]  # Convertir a lista de un solo elemento para crear el DataFrame

        # Convertir la respuesta JSON a un DataFrame de Polars
        df = pl.DataFrame(data)
        logging.info(f"DataFrame created with {df.shape[0]} rows and {df.shape[1]} columns.")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        raise  # Re-lanzar la excepción para que sea manejada por el decorador de reintentos (tenacity)
