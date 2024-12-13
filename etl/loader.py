from database.database_manager import DatabaseManager
from utils.logger_manager import LoggerManager
import pandas as pd
from psycopg2.extras import execute_batch

class Loader:
    def __init__(self, script_name: str = __file__):
        self.logger = LoggerManager(script_name=script_name).get_logger()

    def load_to_postgres(self, df: pd.DataFrame):

        db_manager = DatabaseManager()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS movies (
            id SERIAL PRIMARY KEY,
            movie_id INT UNIQUE, 
            title VARCHAR,
            release_date DATE,
            rating DECIMAL(3, 1),
            vote_count INT,
            popularity_score DECIMAL,
            genres TEXT,
            duration_minutes INT,
            budget_usd BIGINT,
            revenue_usd BIGINT,
            profit_margin DECIMAL,
            rating_category VARCHAR
        );
        """

        insert_query = """
        INSERT INTO movies (
            movie_id, title, release_date, rating, vote_count, popularity_score, genres, duration_minutes, budget_usd, revenue_usd, profit_margin, rating_category
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (movie_id) 
        DO UPDATE SET
            title = EXCLUDED.title,
            release_date = EXCLUDED.release_date,
            rating = EXCLUDED.rating,
            vote_count = EXCLUDED.vote_count,
            popularity_score = EXCLUDED.popularity_score,
            genres = EXCLUDED.genres,
            duration_minutes = EXCLUDED.duration_minutes,
            budget_usd = EXCLUDED.budget_usd,
            revenue_usd = EXCLUDED.revenue_usd,
            profit_margin = EXCLUDED.profit_margin,
            rating_category = EXCLUDED.rating_category;
        """

        try:
            # Crear la tabla si no existe
            self.logger.info("Verificando si la tabla 'movies' existe o debe ser creada.")
            db_manager.execute_query(create_table_query)
            self.logger.info("Tabla 'movies' creada o ya existente.")

            # Insertar datos en lote para mejorar el rendimiento
            self.logger.info(f"Iniciando la inserción de {len(df)} registros en la tabla 'movies'.")
            data = [
                (
                    row['movie_id'], row['title'], row['release_date'], row['rating'],
                    row['vote_count'], row['popularity_score'], row['genres'],
                    row['duration_minutes'], row['budget_usd'], row['revenue_usd'],
                    row['profit_margin'], row['rating_category']
                ) for _, row in df.iterrows()
            ]

            with db_manager.cursor() as cursor:
                execute_batch(cursor, insert_query, data, page_size=100)

            self.logger.info("Inserción o actualización de datos completada exitosamente.")
        except Exception as e:
            self.logger.error(f"Error durante la carga de datos: {e}", exc_info=True)
            raise
        finally:
            db_manager.close()
