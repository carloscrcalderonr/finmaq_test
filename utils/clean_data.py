import os

import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils.logger_manager import LoggerManager
from utils.save_csv_debug import SaveFileDebug

load_dotenv()

output_path = os.getenv('LOG_FOLDER_DATA')


class CleanerValidator:
    def __init__(self, script_name: str = __file__):
        self.logger = LoggerManager(script_name=script_name).get_logger()

    def clean_and_validate(self, movies: pd.DataFrame, details: pd.DataFrame) -> pd.DataFrame:
        try:
            # Selección de columnas
            self.logger.info("Seleccionando columnas necesarias de los DataFrames.")
            movies = movies[['id', 'title', 'release_date', 'vote_average', 'vote_count', 'popularity']].copy()
            details = details[['id', 'genres', 'runtime', 'budget', 'revenue']].copy()
            self.logger.info("Columnas seleccionadas correctamente.")
        except KeyError as e:
            self.logger.error(f"Error al seleccionar columnas: {e}")
            raise

        try:
            # Limpieza
            self.logger.info("Iniciando la limpieza de datos.")

            # Eliminar duplicados en movies
            duplicated_movies = movies[movies.duplicated(subset=['id'])]
            if not duplicated_movies.empty:
                saver = SaveFileDebug(path=output_path, filename="discarded_duplicated_movies.csv")
                saver.save(duplicated_movies)
                self.logger.info(
                    f"Registros duplicados eliminados en 'movies': {len(duplicated_movies)} guardados en 'output/discarded_duplicated_movies.csv'.")
            movies = movies.drop_duplicates(subset=['id'])

            # Eliminar duplicados en details
            duplicated_details = details[details.duplicated(subset=['id'])]
            if not duplicated_details.empty:
                saver = SaveFileDebug(path=output_path, filename="discarded_duplicated_details.csv")
                saver.save(duplicated_details)
                self.logger.info(
                    f"Registros duplicados eliminados en 'details': {len(duplicated_details)} guardados en 'output/discarded_duplicated_details.csv'.")
            details = details.drop_duplicates(subset=['id'])

            # Fechas inválidas
            movies['release_date'] = pd.to_datetime(movies['release_date'], errors='coerce')
            discarded_invalid_dates = movies[movies['release_date'].isna()]
            if not discarded_invalid_dates.empty:
                saver = SaveFileDebug(path=output_path, filename="discarded_invalid_dates.csv")
                saver.save(discarded_invalid_dates)
                self.logger.info(
                    f"Registros descartados por fechas inválidas: {len(discarded_invalid_dates)} guardados en 'output/discarded_invalid_dates.csv'.")

            movies = movies[movies['release_date'].notna()]

            # Fechas futuras
            discarded_future_dates = movies[movies['release_date'] > datetime.now()]
            if not discarded_future_dates.empty:
                saver = SaveFileDebug(path=output_path, filename="discarded_future_dates.csv")
                saver.save(discarded_future_dates)
                self.logger.info(
                    f"Registros descartados por fechas futuras: {len(discarded_future_dates)} guardados en 'output/discarded_future_dates.csv'.")

            movies = movies[movies['release_date'] <= datetime.now()]

            # Presupuesto vacío
            discarded_missing_budget = details[details['budget'].isna()]
            if not discarded_missing_budget.empty:
                saver = SaveFileDebug(path=output_path, filename="discarded_missing_budget.csv")
                saver.save(discarded_missing_budget)
                self.logger.info(
                    f"Registros descartados por presupuesto faltante: {len(discarded_missing_budget)} guardados en 'output/discarded_missing_budget.csv'.")

            details['budget'] = details['budget'].fillna(0).astype(int)
            details['revenue'] = details['revenue'].fillna(0).astype(int)
            details['genres'] = details['genres'].apply(
                lambda x: ', '.join([g['name'] for g in x]) if x else 'Unknown'
            )
            self.logger.info("Limpieza de datos completada.")
        except Exception as e:
            self.logger.error(f"Error durante la limpieza de datos: {e}")
            raise

        try:
            # Normalización
            self.logger.info("Normalizando el campo 'title'.")
            movies['title'] = movies['title'].str.title()
            self.logger.info("Normalización completada.")
        except Exception as e:
            self.logger.error(f"Error durante la normalización: {e}")
            raise

        try:
            # Validación
            self.logger.info("Validando datos con criterios específicos.")

            # Votos insuficientes
            discarded_low_votes = movies[movies['vote_count'] < 50]
            if not discarded_low_votes.empty:
                saver = SaveFileDebug(path=output_path, filename="discarded_low_votes.csv")
                saver.save(discarded_low_votes)
                self.logger.info(
                    f"Registros descartados por votos insuficientes: {len(discarded_low_votes)} guardados en 'output/discarded_low_votes.csv'.")

            movies = movies[movies['vote_count'] >= 50]
            self.logger.info("Validación completada.")
        except Exception as e:
            self.logger.error(f"Error durante la validación de datos: {e}")
            raise

        try:
            # Combinación
            self.logger.info("Combinando los DataFrames de 'movies' y 'details'.")
            combined_df = pd.merge(movies, details, on='id')
            self.logger.info("Combinación completada correctamente.")
        except Exception as e:
            self.logger.error(f"Error al combinar los DataFrames: {e}")
            raise

        self.logger.info("Proceso de limpieza y validación completado.")
        return combined_df
