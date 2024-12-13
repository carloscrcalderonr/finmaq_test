# Proyecto ETL: Extracción, Transformación y Carga de Datos de Películas

Este proyecto implementa un proceso ETL (Extracción, Transformación y Carga) para obtener información sobre películas populares y detalles adicionales utilizando la API de The Movie Database (TMDb), procesar y limpiar los datos, y finalmente almacenarlos en una base de datos PostgreSQL.

---

## Estructura del Proyecto

El proyecto está organizado en los siguientes módulos y archivos:

### Archivos principales

- **main.py**: Controla la ejecución principal del proceso ETL.
- **.env**: Contiene las configuraciones del entorno, como las credenciales de la base de datos y la clave de la API.

### Submódulos del ETL

1. **extractor.py**:
   - Responsable de extraer información desde la API de TMDb.
   - Obtiene una lista de películas populares y detalles específicos de cada película.

2. **transformer.py**:
   - Transforma los datos, renombra columnas y calcula nuevas métricas como el margen de beneficio y la categoría de calificaciones.

3. **loader.py**:
   - Carga los datos transformados en una base de datos PostgreSQL.
   - Utiliza inserciones por lotes para optimizar el rendimiento.

4. **clean_data.py**:
   - Limpia y valida los datos eliminando duplicados, registros inválidos y datos inconsistentes.
   - Combina información de múltiples DataFrames.

### Utilidades

- **database_manager.py**: Maneja la conexión y las operaciones con la base de datos PostgreSQL.
- **logger_manager.py**: Configura el sistema de logs para registrar eventos del proceso.
- **save_csv_debug.py**: Permite guardar archivos CSV para depuración en las etapas del proceso.

---

## Requisitos

### Software

- Python 3.8 o superior.
- PostgreSQL.
- Bibliotecas Python: `pandas`, `requests`, `psycopg2`, `dotenv`.

### Configuración del entorno

Crear un archivo `.env` con el siguiente contenido:

```
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=1234567890
DB_HOST=localhost
DB_PORT=5432

BASE_URL=https://api.themoviedb.org/3
API_KEY=TU_API_KEY

LOG_FOLDER_DATA=output_data
```

### Base de datos

Asegúrese de tener una base de datos PostgreSQL configurada. El proyecto creará la tabla necesaria si no existe.

---

## Ejecución

1. Instalar las dependencias necesarias:
   ```bash
   pip install -r requirements.txt
   ```

2. Ejecutar el archivo principal:
   ```bash
   python main.py
   ```

3. Verificar los resultados en los logs generados en el directorio `logs/` y los datos cargados en PostgreSQL.

---

## Proceso ETL

1. **Extracción**:
   - Se obtienen hasta 200 películas populares de TMDb, junto con sus detalles.

2. **Limpieza y Validación**:
   - Se eliminan duplicados y registros con fechas inválidas o futuras.
   - Se normalizan campos como el título y se descartan películas con pocos votos.

3. **Transformación**:
   - Se calcula el margen de beneficio y se categorizan las calificaciones de las películas.
   - Se guardan registros con presupuestos y ganancias iguales a 0 para su depuración.

4. **Carga**:
   - Los datos transformados se almacenan en una tabla llamada `movies` en PostgreSQL, actualizando los registros existentes según el ID de la película.

---

## Ejemplo de Tabla `movies`

| movie_id | title          | release_date | rating | vote_count | popularity_score | genres     | duration_minutes | budget_usd | revenue_usd | profit_margin | rating_category |
|----------|----------------|--------------|--------|------------|------------------|------------|------------------|------------|------------|---------------|-----------------|
| 12345    | Movie Example  | 2024-01-01   | 7.5    | 1200       | 150.3            | Action     | 120              | 50000000   | 100000000  | 1.0           | Bueno           |

---

## Manejo de Logs

- Los logs se generan en un directorio `logs/` separado por subdirectorios para cada módulo.
- El formato del log incluye información detallada sobre el evento, incluyendo el archivo y la línea de código.

---

## Depuración

Archivos CSV generados durante la ejecución:

- `movies_api.csv`: Datos crudos extraídos de la API.
- `details_api.csv`: Detalles de las películas.
- `discarded_invalid_dates.csv`: Registros con fechas inválidas.
- `movies_zero_budget.csv`: Películas con presupuesto igual a 0.
- `movies_transformed.csv`: Datos transformados listos para cargar.

---

