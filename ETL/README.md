# ETL.

Para el proceso de Extraccion, Transformacion y Carga de datos se opto por utilizar Google Functions para ejecutrar los scripts que realizan las diversas tareas de estos procesos.

## Extraccion de Datos.

Dentro del proceso de extraccion de datos se realizaron dos tipos de tareas, la extraccion de datos en bloque y de carga incremental.

### Extraccion de Datos en Bloque.

Se utilizan estas tareas para extraer varios registrosde datos existentes en una sola ejecución.

- extract_air_quality.py **(API)**
- extract_bulk_active_medallion_vehicles.py **(API)**
- exrtact_bulk_trip_record_data.py **(Web Scraping)**
- extract_greenhouse_gas_emissions.py **(API)**


### Extraccion de Datos Carga Incremental.

Se utilizan para tareas que se ejecutan periodicamente, para extraer nuevos datos.

- extract_incremental_load_active_medallion_vehicles.py  **(API)**
- extract_incremental_load_trip_record_data.py **(Web Scraping)**
- extract_incremental_load_weather.py **(API)**

## Transformacion & Carga de datos.

## Carga Incremental.

Como orquestador de las diferentes tareas se utiliza Google Composer, el cual es un servicio administrado que implementa el Airflow en Google Cloud.

Los flujos de trabajos se encuentran divididos en DAGs que se ejecutan en distinto tiempo.

- DAG_incremental_load_active_medallion.py **(Mensual)**
- DAG_incremental_load_air_quality.py **(Anual)**
- DAG_incremental_load_green_house_emissions.py **(Anual)**
- DAG_incremental_load_trip_taxi_data.py **(Mensual)**
- DAG_incremental_load_weather.py **(Mensual)**