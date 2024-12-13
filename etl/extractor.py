import os

import requests
import pandas as pd
from dotenv import load_dotenv

from utils.logger_manager import LoggerManager
from utils.save_csv_debug import SaveFileDebug

load_dotenv()

BASE_URL = os.getenv('BASE_URL')
API_KEY = os.getenv('API_KEY')
output_path = os.getenv('LOG_FOLDER_DATA')

class Extractor:
    def __init__(self, script_name: str = __file__):

        self.logger = LoggerManager(script_name=script_name).get_logger()

    def fetch_data(self, endpoint, params=None):

        url = f"{BASE_URL}{endpoint}"
        params = params or {}
        params['api_key'] = API_KEY

        try:
            self.logger.info(f"Realizando solicitud a la URL: {url} con parámetros: {params}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            self.logger.info(f"Solicitud exitosa a la URL: {url}")
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error al realizar solicitud a la URL: {url} - {e}", exc_info=True)
            return None

    def extract_movies(self):

        try:
            self.logger.info("Iniciando extracción de películas populares.")
            movies = []

            for page in range(1, 11):  # 200 películas, 20 por página
                self.logger.info(f"Extrayendo películas de la página {page}.")
                data = self.fetch_data("/movie/popular", params={'page': page})

                if data:
                    page_results = data.get('results', [])
                    movies.extend(page_results)
                    self.logger.info(f"Página {page} procesada con {len(page_results)} películas.")
                else:
                    self.logger.warning(f"No se obtuvieron datos para la página {page}.")

            self.logger.info(f"Extracción completada. Total de películas obtenidas: {len(movies)}.")
            save = SaveFileDebug(path=output_path, filename="movies_api.csv")
            save.save(pd.DataFrame(movies))
            return pd.DataFrame(movies)
        except Exception as e:
            self.logger.error(f"Error durante la extracción de películas populares: {e}", exc_info=True)
            return pd.DataFrame()

    def extract_movie_details(self, movie_ids):

        try:
            self.logger.info("Iniciando extracción de detalles de películas.")
            details = []

            for idx, movie_id in enumerate(movie_ids, start=1):
                self.logger.info(f"Extrayendo detalles para la película con ID: {movie_id} ({idx}/{len(movie_ids)}).")
                data = self.fetch_data(f"/movie/{movie_id}")

                if data:
                    details.append(data)
                    self.logger.info(f"Detalles obtenidos para la película con ID: {movie_id}.")
                else:
                    self.logger.warning(f"No se obtuvieron detalles para la película con ID: {movie_id}.")

            self.logger.info(f"Extracción de detalles completada. Total de películas procesadas: {len(details)}.")
            save = SaveFileDebug(path=output_path, filename="details_api.csv")
            save.save(pd.DataFrame(details))
            return pd.DataFrame(details)
        except Exception as e:
            self.logger.error(f"Error durante la extracción de detalles de películas: {e}", exc_info=True)
            return pd.DataFrame()
