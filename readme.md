# Ejercicios en Airflow

## Clase 7 

### Consignas

1. En Hive, crear la siguiente tabla (externa) en la base de datos tripdata: airport_trips
2. En Hive, mostrar el esquema de airport_trips
3. Crear un archivo .bash que permita descargar los archivos mencionados abajo e
ingestarlos en HDFS: Yellow_tripdata_2021-01.parquet, Yellow_tripdata_2021-02.parquet)

Se hizo en [archivos-clase7/ingest.sh](archivos-clase7/ingest.sh).

4. Crear un archivo .py que permita, mediante Spark, crear un data frame uniendo los
viajes del mes 01 y mes 02 del año 2021 y luego Insertar en la tabla airport_trips los
viajes que tuvieron como inicio o destino aeropuertos, que hayan pagado con dinero.

Se hizo en [archivos-clase7/transformation.py](archivos-clase7/transformation.py).

5. Realizar un proceso automático en Airflow que orqueste los archivos creados en los
puntos 3 y 4. Correrlo y mostrar una captura de pantalla (del DAG y del resultado en la base de datos)

Se hizo en [archivos-clase7/ingest-airport-trips.py](archivos-clase7/ingest-airport-trips.py).

## Clase 8 

### Consignas 

1. Crear la siguientes tablas externas en la base de datos f1 en hive:

a. driver_results (driver_forename, driver_surname, driver_nationality, points)

b. constructor_results (constructorRef, cons_name, cons_nationality, url, points)

2. En Hive, mostrar el esquema de driver_results y constructor_results
3. Crear un archivo .bash que permita descargar los archivos mencionados abajo e
ingestarlos en HDFS: results.csv, drivers.csv, constructors.csv, races.csv

Se hizo en [archivos-clase8/ingest.sh](archivos-clase8/ingest.sh).

4. Generar un archivo .py que permita, mediante Spark:

a. insertar en la tabla driver_results los corredores con mayor cantidad de puntos
en la historia.

b. insertar en la tabla constructor_result quienes obtuvieron más puntos en el
Spanish Grand Prix en el año 1991

Se hizo en [archivos-clase8/f1-transformation.py](archivos-clase8/f1-transformation.py).

5. Realizar un proceso automático en Airflow que orqueste los archivos creados en los
puntos 3 y 4. Correrlo y mostrar una captura de pantalla (del DAG y del resultado en la base de datos)

Se hizo en [archivos-clase8/f1-etl.py](archivos-clase8/f1-etl.py).

## Resultados 

El archivo donde se muestra el procedimiento incluído el codigo es [AIRFLOW.pdf](AIRFLOW.pdf). 