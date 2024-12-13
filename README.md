# Proyecto ETL: Extracci�n, Transformaci�n y Carga de Datos de Pel�culas

Este proyecto implementa un proceso ETL (Extracci�n, Transformaci�n y Carga) para obtener informaci�n sobre pel�culas populares y detalles adicionales utilizando la API de The Movie Database (TMDb), procesar y limpiar los datos, y finalmente almacenarlos en una base de datos PostgreSQL.

---

## Estructura del Proyecto

El proyecto est� organizado en los siguientes m�dulos y archivos:

### Archivos principales

- **main.py**: Controla la ejecuci�n principal del proceso ETL.
- **.env**: Contiene las configuraciones del entorno, como las credenciales de la base de datos y la clave de la API.

### Subm�dulos del ETL

1. **extractor.py**:
   - Responsable de extraer informaci�n desde la API de TMDb.
   - Obtiene una lista de pel�culas populares y detalles espec�ficos de cada pel�cula.

2. **transformer.py**:
   - Transforma los datos, renombra columnas y calcula nuevas m�tricas como el margen de beneficio y la categor�a de calificaciones.

3. **loader.py**:
   - Carga los datos transformados en una base de datos PostgreSQL.
   - Utiliza inserciones por lotes para optimizar el rendimiento.

4. **clean_data.py**:
   - Limpia y valida los datos eliminando duplicados, registros inv�lidos y datos inconsistentes.
   - Combina informaci�n de m�ltiples DataFrames.

### Utilidades

- **database_manager.py**: Maneja la conexi�n y las operaciones con la base de datos PostgreSQL.
- **logger_manager.py**: Configura el sistema de logs para registrar eventos del proceso.
- **save_csv_debug.py**: Permite guardar archivos CSV para depuraci�n en las etapas del proceso.

---

## Requisitos

### Software

- Python 3.8 o superior.
- PostgreSQL.
- Bibliotecas Python: `pandas`, `requests`, `psycopg2`, `dotenv`.

### Configuraci�n del entorno

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

Aseg�rese de tener una base de datos PostgreSQL configurada. El proyecto crear� la tabla necesaria si no existe.

---

## Ejecuci�n

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

1. **Extracci�n**:
   - Se obtienen hasta 200 pel�culas populares de TMDb, junto con sus detalles.

2. **Limpieza y Validaci�n**:
   - Se eliminan duplicados y registros con fechas inv�lidas o futuras.
   - Se normalizan campos como el t�tulo y se descartan pel�culas con pocos votos.

3. **Transformaci�n**:
   - Se calcula el margen de beneficio y se categorizan las calificaciones de las pel�culas.
   - Se guardan registros con presupuestos y ganancias iguales a 0 para su depuraci�n.

4. **Carga**:
   - Los datos transformados se almacenan en una tabla llamada `movies` en PostgreSQL, actualizando los registros existentes seg�n el ID de la pel�cula.

---

## Ejemplo de Tabla `movies`

| movie_id | title          | release_date | rating | vote_count | popularity_score | genres     | duration_minutes | budget_usd | revenue_usd | profit_margin | rating_category |
|----------|----------------|--------------|--------|------------|------------------|------------|------------------|------------|------------|---------------|-----------------|
| 12345    | Movie Example  | 2024-01-01   | 7.5    | 1200       | 150.3            | Action     | 120              | 50000000   | 100000000  | 1.0           | Bueno           |

---

## Manejo de Logs

- Los logs se generan en un directorio `logs/` separado por subdirectorios para cada m�dulo.
- El formato del log incluye informaci�n detallada sobre el evento, incluyendo el archivo y la l�nea de c�digo.

---

## Depuraci�n

Archivos CSV generados durante la ejecuci�n:

- `movies_api.csv`: Datos crudos extra�dos de la API.
- `details_api.csv`: Detalles de las pel�culas.
- `discarded_invalid_dates.csv`: Registros con fechas inv�lidas.
- `movies_zero_budget.csv`: Pel�culas con presupuesto igual a 0.
- `movies_transformed.csv`: Datos transformados listos para cargar.

---

