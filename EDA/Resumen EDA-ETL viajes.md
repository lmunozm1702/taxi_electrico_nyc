# EDA previo al ETL

## Como antesala del proceso ETL, del análisis de la data y en conformidad con los objetivos y producto ML del presente proyecto, se sugiere lo siguiente:

### Las principales columnas necesarias con información crucial son:

| Amarillos          | Verdes |  For-hire | High-volume |  Tipo de dato  | Detalle
| ------             |    -----  | -----  |    -----  | -----  |  ---- |   
| --             |    --  | --  | request_datetime  | datetime64[us] | Considerar el tipo de dato a la hora de usar esta columna.
| --             |    --  | -- | on_scene_datetime  | datetime64[us] | Considerar el tipo de dato a la hora de usar esta columna.
| tpep_pickup_datetime  |lpep_pickup_datetime | pickup_datetime | pickup_datetime | datetime64[us] | Se deben normalizar los nombres de las columnas antes de concatenar. Considerar el tipo de dato a la hora de usar esta columna.
| tpep_dropoff_datetime | lpep_dropoff_datetime | dropOff_datetime | dropoff_datetime |datetime64[us]| Se deben normalizar los nombres de las columnas antes de concatenar. Considerar el tipo de dato a la hora de usar esta columna.
| trip_distance    |trip_distance      | -- | trip_miles | float64 | Se deben normalizar los nombres de las columnas antes de concatenar  
| --            |    --  | --  |trip_time     |        int64   | Considerar el tipo de dato a la hora de usar esta columna (NO es `datetime`, es `int`)          
| PULocationID   | PULocationID        |PUlocationID (float64) | PULocationID   |  int32 | Se deben cambiar el tipo de dato en "For-Hire" de `float` a `int32`, para que todos sean el mismo tipo
| DOLocationID| DOLocationID         |  DOlocationID (float64) | DOLocationID | int32 | Se deben cambiar el tipo de dato en "For-Hire" de `float` a `int32`, para que todos sean el mismo tipo



### Las siguientes son columnas opcionales que se pueden conservar para algun tipo de extra o plus (todos relacionados con tarifas, recargos e impuestos).

| Columna          | Dataset |     tipo de dato   |   
| ------             |    -----  | ----- 
| fare_amount | * amarillos <br> * verdes      | float64 |
| total_amount|* amarillos <br> * verdes       | float64|
| extra| * amarillos <br> * verdes  | float64       |
| mta_tax| * amarillos <br> * verdes  | float64       |
| tip_amount  | * amarillos <br> * verdes  | float64       |
| tolls_amount | * amarillos <br> * verdes  | float64       |
| improvement_surcharge| * amarillos <br> * verdes  | float64  |            
| congestion_surcharge| * amarillos <br> * verdes <br> * High-volume | float64   |    
| Airport_fee           | * amarillos <br> * High-volume |float64|
| ehail_fee         | * verdes|      float64|
| base_passenger_fare | * High-volume | float64       |
| tolls | * High-volume |  float64       |
| bcf | * High-volume | float64       |
| sales_tax| * High-volume |float64                  |
| tips| * High-volume |float64       |
| driver_pay| * High-volume |float64|

### Las siguientes son columnas que, por el momento, no aportan información relevante relacionada con el proyecto


| Columna          | Dataset |     Tipo de dato      
| ------             |    -----  | ----- 
| VendorID   | * amarillos <br> * verdes  |            int32      
|  passenger_count | * amarillos <br> * verdes  |       float64       
|  RatecodeID   | * amarillos <br> * verdes  |          float64       
| store_and_fwd_flag| * amarillos <br> * verdes  |     object          
| payment_type  | * amarillos <br> * verdes  |         int64                  
| trip_type     | * verdes|           float64 
| dispatching_base_num | * For-hire |   object              
| SR_Flag   | * For-hire | float64       
| Affiliated_base_number | * For-hire |  object 
| hvfhs_license_num  | * High-Volume |   object        
| dispatching_base_num  | * High-Volume |object        
|  originating_base_num  | * High-Volume |object        
| shared_request_flag   | * High-Volume |object        
| shared_match_flag     | * High-Volume |object        
| access_a_ride_flag    | * High-Volume |object        
| wav_request_flag      | * High-Volume |object        
| wav_match_flag        | * High-Volume |object

## Para el filtrado de datos, se sugiere seguir los siguientes criterios:

* Eliminar las filas donde `pick_up_datetime` y `pick_up_location` sean nulos.
* Calcular tiempo de viaje (`drop_off_datetime` - `pick_up_datetime`).
* Eliminar tiempos y distancias 0 y negativas.
* Con tiempos y distancias positivas crear una nueva columna calculando velocidad promedio.
* Mantener filas cuyas velocidades sean razonables (**Valor a establecer**, en este EDA se usara 65 mph como valor maximo ya que es la maxima velocidad permitida en NYC).

## NOTA: para el dataset de `For-Hire` no hay informacion de la distancia, por lo que se debe tomar una decision sobre como filtrar este dataset, si se aplicará algun criterio y cual, o descartar el dataset entero.

## Luego de normalizar los nombres de las columnas mas importantes, y cambiar el tipo de dato donde es requerido como se menciona en la tabla anterior en la columna de `Detalle`, se pueden concatenar los 4 dataset para generar uno solo con toda la información relevante.

### A continuación se muestra el resultado de aplicar los criterios sugeridos sobre la muestra de sept-2024 (no se incluye `For-Hire` por lo expuesto anteriormente sobre el dataset)

![Pie Charts](/assets/img/Pie_charts_viajes_EDA_ETL.png)