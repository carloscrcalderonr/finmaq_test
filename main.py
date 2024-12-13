from etl.extractor import Extractor
from etl.loader import Loader
from etl.transformer import Transformer
from utils.clean_data import CleanerValidator
from utils.logger_manager import LoggerManager


def main():
    logger = LoggerManager(script_name=__file__).get_logger()

    try:
        logger.info("Iniciando el proceso ETL.")
        print("Iniciando el proceso ETL.")

        try:
            extractor = Extractor()

            logger.info("Extrayendo datos de películas populares.")
            print("Extrayendo datos de películas populares.")
            movies = extractor.extract_movies()

            logger.info(f"Películas populares extraídas: {len(movies)} registros.")
            print(f"Películas populares extraídas: {len(movies)} registros.")

            logger.info("Extrayendo detalles de las películas.")
            print("Extrayendo detalles de las películas.")
            movie_details = extractor.extract_movie_details(movies['id'])

            logger.info(f"Detalles de películas extraídos: {len(movie_details)} registros.")
            print(f"Detalles de películas extraídos: {len(movie_details)} registros.")
        except Exception as e:
            logger.error(f"Error durante la extracción de datos: {e}", exc_info=True)
            print(f"Error durante la extracción de datos: {e}")
            raise

        # Limpieza y validación
        try:
            logger.info("Limpiando y validando los datos.")
            print("Limpiando y validando los datos.")

            cleaner_validator = CleanerValidator()
            data = cleaner_validator.clean_and_validate(movies, movie_details)

            logger.info(f"Datos limpiados y validados: {len(data)} registros.")
            print(f"Datos limpiados y validados: {len(data)} registros.")
        except Exception as e:
            logger.error(f"Error durante la limpieza y validación de los datos: {e}", exc_info=True)
            print(f"Error durante la limpieza y validación de los datos: {e}")
            raise

        # Transformación
        try:
            logger.info("Transformando los datos.")
            print("Transformando los datos.")

            transformer = Transformer()
            transformed_data = transformer.transform_data(data)

            logger.info(f"Datos transformados con éxito. Total de registros: {len(transformed_data)}.")
            print(f"Datos transformados con éxito. Total de registros: {len(transformed_data)}.")
        except Exception as e:
            logger.error(f"Error durante la transformación de los datos: {e}", exc_info=True)
            print(f"Error durante la transformación de los datos: {e}")
            raise

        # Carga
        try:
            logger.info("Cargando datos transformados en la base de datos.")
            print("Cargando datos transformados en la base de datos.")

            loader = Loader()
            loader.load_to_postgres(transformed_data)

            logger.info("Datos cargados en la base de datos con éxito.")
            print("Datos cargados en la base de datos con éxito.")
        except Exception as e:
            logger.error(f"Error durante la carga de datos en la base de datos: {e}", exc_info=True)
            print(f"Error durante la carga de datos en la base de datos: {e}")
            raise

        logger.info("Proceso ETL completado exitosamente.")
        print("Proceso ETL completado exitosamente.")

    except Exception as e:
        logger.critical(f"Error crítico en el proceso ETL: {e}", exc_info=True)
        print(f"Error crítico en el proceso ETL: {e}")


if __name__ == "__main__":
    main()
