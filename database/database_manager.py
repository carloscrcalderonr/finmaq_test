import os
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv

from utils.logger_manager import LoggerManager

load_dotenv()


class DatabaseManager:
    def __init__(self,  script_name: str = __file__):

        self.db_config = {
            'dbname': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432))
        }
        self.connection = None

        self.logger = LoggerManager(script_name=script_name).get_logger()

    def connect(self):

        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.logger.info("Conexión a la base de datos exitosa.")
        except psycopg2.Error as e:
            self.logger.error(f"Error al conectar con la base de datos: {e}", exc_info=True)
            raise

    def close(self):

        if self.connection:
            self.connection.close()
            self.logger.info("Conexión a la base de datos cerrada.")

    @contextmanager
    def cursor(self):

        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Error durante la operación: {e}", exc_info=True)
            raise
        finally:
            cursor.close()

    def execute_query(self, query: str, params: tuple = None):

        try:
            self.logger.info(f"Ejecutando consulta: {query}")
            with self.cursor() as cursor:
                cursor.execute(query, params)
            self.logger.info("Consulta ejecutada exitosamente.")
            affected_rows = cursor.rowcount
            self.logger.info(f"Consulta ejecutada exitosamente. Filas afectadas: {affected_rows}")
        except Exception as e:
            self.logger.error(f"Error al ejecutar la consulta: {e}", exc_info=True)
            raise
