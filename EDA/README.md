# Análisis Exploratorio de los Datos (EDA).

Esta carpeta contiene los EDA's de todas las fuentes de datos utilizadas para este proyecto. En total se analizaron 7 fuentes de datos, listadas a continuación:

Datasets relacionados con información de los viajes:
* [Taxis Amarillos](EDA%20yellow_tripdata_09_2024.ipynb).
* [Taxis Verdes](EDA%20green_tripdata_09_2024.ipynb).
* [Taxis For-Hire](EDA%20For-Hire_tripdata_09_2024.ipynb).
* [Taxis High-Volume-For-Hire](EDA%20HVFHV%2009_2024.ipynb).

Datasets relacionados con información medioambiental:
* [Calidad de Aire](EDA-Calidad_de_Air_Quality.ipynb).
* [Emisión de Gases](EDA-Calidad_de_Gas_Emissions.ipynb).
* [Condiciones Climáticas](EDA%20raw_dataset_weather_2022_2024.ipynb).

## Análiis Preliminar y Calidad de los Datos.

Durante el 1er Sprint de este proyecto se realizó el análisis de calidad de los datos, con la finalidad de determinar qué porcentaje de los datos crudos tienen información relevante, correcta y completa para los objetivos del presente proyecto, encontrándose que la gran mayoría de ellos (más del 99%) cumplían con los requerimentos de validez para ser usados. En este [notebook](Docs/Análisis%20Preliminar.md) podrán leer una descripción más detallada de los datasets y las fuentes de donde provienen.

## Tipos de Datos y Proceso ETL.

Durante el 2do Sprint de este proyecto se realizó el proceso de Extracción, Transformación y Carga (ETL), el cuál requirió un análisis de los datos que mostrara las columnas y los tipos de datos presentes en los datasets crudos, para posteriormente hacer sugerencias al grupo encargado del área de ingeniería sobre cuáles columnas conservar y que modificaciones eran necesarias en función de crear una data unificada en un DataWarehouse que disponibilizara la información para ser utilizada por el resto del equipo en el desarrollo de los productos ofrecidos en el presente proyecto. En este [notebook](Docs/Resumen%20Tipos%20de%20dato%20y%20sugerencias%20ETL.md) podrán leer un resumen más detallado de este análisis.


## Correlaciones y Tendencias.

Durante el 3er Sprint de este proyecto se desarrolló el producto de Machine Learning (ML), el cuál requirió un análisis de los datos que mostrara el comportamiento de la demanda de los viajes en la ciudad de Nueva York y su relación con las diferentes variables (columnas), que permitiera al grupo encargado del área de ML seleccionar el mejor modelo y las variables mas importantes e influyentes, para realizar el entrenamiento correspondiente para la creación del modelo de predicción propuesto en el presente proyecto.. En este [notebook](Docs/Resumen%20Correlaciones%20y%20tendencias.md) podrán leer un resumen más detallado de este análisis.

🏠[Inicio](/README.md)