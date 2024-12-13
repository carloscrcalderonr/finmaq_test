import os
import pandas as pd
from dotenv import load_dotenv
from utils.logger_manager import LoggerManager
from utils.save_csv_debug import SaveFileDebug

load_dotenv()
output_path = os.getenv('LOG_FOLDER_DATA')


class Transformer:
    def __init__(self, script_name: str = __file__):
        self.logger = LoggerManager(script_name=script_name).get_logger()

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Iniciando la transformación de datos.")

        try:
            # Renombrar columnas
            self.logger.info("Renombrando columnas del DataFrame.")
            df.rename(columns={
                'id': 'movie_id',
                'vote_average': 'rating',
                'popularity': 'popularity_score',
                'runtime': 'duration_minutes',
                'budget': 'budget_usd',
                'revenue': 'revenue_usd'
            }, inplace=True)
            self.logger.info("Columnas renombradas correctamente.")
        except Exception as e:
            self.logger.error(f"Error al renombrar columnas: {e}")
            raise

        try:
            # Calcular margen de beneficio
            self.logger.info("Calculando margen de beneficio para las películas.")
            df['profit_margin'] = df.apply(
                lambda x: None if x['budget_usd'] == 0 else (x['revenue_usd'] - x['budget_usd']) / x['budget_usd'],
                axis=1
            )
            self.logger.info("Margen de beneficio calculado correctamente.")
        except Exception as e:
            self.logger.error(f"Error al calcular el margen de beneficio: {e}")
            raise

        try:
            # Categorizar calificaciones
            self.logger.info("Categorizando calificaciones en 'rating_category'.")
            df['rating_category'] = pd.cut(
                df['rating'],
                bins=[-float('inf'), 0.4, 6.0, 8.0, float('inf')],
                labels=['Malo', 'Promedio', 'Bueno', 'Excelente']
            )
            self.logger.info("Calificaciones categorizadas correctamente.")
        except Exception as e:
            self.logger.error(f"Error al categorizar calificaciones: {e}")
            raise

        try:
            # Filtrar y guardar películas con budget_usd == 0
            self.logger.info("Filtrando películas con budget_usd igual a 0.")
            movies_with_zero_budget = df[df['budget_usd'] == 0]
            if not movies_with_zero_budget.empty:
                save_zero_budget = SaveFileDebug(path=output_path, filename="movies_zero_budget.csv")
                save_zero_budget.save(movies_with_zero_budget)
                self.logger.info(f"Películas con presupuesto 0 guardadas en 'movies_zero_budget.csv'.")

            # Filtrar y guardar películas con revenue_usd == 0
            self.logger.info("Filtrando películas con revenue_usd igual a 0.")
            movies_with_zero_revenue = df[df['revenue_usd'] == 0]
            if not movies_with_zero_revenue.empty:
                save_zero_revenue = SaveFileDebug(path=output_path, filename="movies_zero_revenue.csv")
                save_zero_revenue.save(movies_with_zero_revenue)
                self.logger.info(f"Películas con revenue 0 guardadas en 'movies_zero_revenue.csv'.")

        except Exception as e:
            self.logger.error(f"Error al filtrar o guardar películas con presupuesto o revenue 0: {e}")
            raise

        self.logger.info("Transformación de datos completada.")
        save = SaveFileDebug(path=output_path, filename="movies_transformed.csv")
        save.save(df)

        return df
