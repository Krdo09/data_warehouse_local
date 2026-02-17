import polars as pl
import requests
from typing import Dict, Any

def fetch_data_from_api(
        url: str, 
        params: Dict[str, Any] = None
    ) -> "pl.DataFrame":
    """
    Docstring for fetch_data_from_api
    
    :param url: Description
    :type url: str
    :param params: Description
    :type params: Dict[str, Any]
    :return: Description
    :rtype: DataFrame
    """
    print(f'Fetching data from API, URL: "{url}"')

    # Enviar petición GET a la API
    response = requests.get(url, params=params)
    # Verificar si la respuesta fue exitosa
    if response.status_code == 200:
        print("Data fetched successfully.")
        # Convertir la respuesta JSON a un DataFrame de Polars
        data = response.json()
        df = pl.DataFrame(data)
        
        print(f"DataFrame created with {df.shape[0]} rows and {df.shape[1]} columns.")
        return df
    
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        response.raise_for_status()
