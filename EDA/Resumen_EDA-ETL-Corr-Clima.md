🏠[Inicio](/README.md)
# EDA Clima 2022-2024

## Para el producto ML se decidió añadir información sobre diversos elementos del clima como variables para predecir la demanda de taxis.

Del análisis exploratorio se tiene que el archivo `.csv` contiene 2 tablas:

1. La primera contiene información sobre las ubicaciones solicitadas, en nuestro caso un punto de longitud y latitud correspondiente a cada uno de los 5 borough de NYC.
2. La segunda contiene informacion sobre las variables del clima requeridos. En el [notebook](EDA-ETL-Corr-Clima_2022_2024.ipynb) está el codigo sobre como obtener ambas tablas separadas.

Para el momento de realización de este EDA, **la data está disponible hasta las 19 horas del 31/12/2024**. Si se consulta algún valor posterior a esa hora, la tabla registrará las 2 primeras columnas (`location_id` y `time`), pero el resto de los valores serán nulos, lo cual hay que tomar en cuenta al momento de hacer la carga de estos datos.

### A continuación un breve resumen de los tipos de datos en la tabla con información sobre las condiciones climáticas:

| Columna        |  Tipo de dato  | Detalle
| ------             |    -----  | -----  
| location_id             |    int64  | --  |
| time               |    object  | Se debe cambiar a tipo `datetime` para<br>poder trabajar con la data de viajes  |
| temperature_2m (°C)             |    float64  | --  |
| relative_humidity_2m (%)             |    float64  | --  |
| dew_point_2m (°C)             |    float64| --  |
| dew_point_2m (°C)             |    float64| --  |
| rain (mm)             |    float64  | --  |
| snowfall (cm)             |    float64  | --  |
| weather_code (wmo code)             |    float64  | --  |
| pressure_msl (hPa)             |    float64| --  |
| cloud_cover (%)             |    float64| --  |
| wind_speed_10m (km/h)             |    float64  | --  |
| wind_direction_10m (°)             |    float64 | --  |
| wind_gusts_10m (km/h)             |    float64  | --  |

### Todos los valores cuantitativos son del tipo `float64`, lo cual es esperado y correcto, por lo que no requiere transformacion. De igual forma con el `location_id` que esto tipo `int64`.

### Para la columna `time`, que es tipo `object`, es necesario cambiarlo a `datetime`, ya que este valor sera necesario para hacer el `join` con el dataset the viajes. El código sugerido para realizar dicha transformación se encuentra en el siguiente [notebook](EDA-ETL-Corr-Clima_2022_2024.ipynb).

🏠[Inicio](/README.md)