from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from google.cloud import storage

# Crear una instancia del cliente BigQuery usando el archivo de credenciales JSON
credentials_path = 'C:/Users/NoxiePC/Desktop/henry/driven-atrium-445021-m2-a773215c2f46.json'
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Crear el cliente BigQuery con las credenciales
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Definir la consulta SQL para acceder a la vista
query_viajes_por_ubicacion_y_tiempo = """SELECT * FROM `driven-atrium-445021-m2.project_data.viajes_por_ubicacion_y_tiempo`"""
query_weather_borough_details = """SELECT * FROM `driven-atrium-445021-m2.project_data.weather_borough_details`"""
query_coordinates = """SELECT * FROM driven-atrium-445021-m2.project_data.coordinates"""

# Ejecutar la consulta y cargar los resultados en un DataFrame de pandas
df_coordinates = client.query(query_coordinates).to_dataframe()
df_clima = client.query(query_weather_borough_details).to_dataframe()
df_viajes = client.query(query_viajes_por_ubicacion_y_tiempo).to_dataframe()

clima = df_clima
viajes = df_viajes

#Crear dataset auxiliar con las fechas y asignar lugar para las locaciones
fechas = clima[['year', 'month', 'day_of_month', 'day_of_week', 'hour_of_day']]
fechas = fechas.loc[fechas.index.repeat(261)].reset_index(drop=True)

# Agregar la columna 'location_id' a fechas
unique_location_ids = df_viajes['location_id'].unique()
repeated_location_ids = list(unique_location_ids) * (len(fechas) // len(unique_location_ids)) + list(unique_location_ids)[:(len(fechas) % len(unique_location_ids))]
fechas['location_id'] = repeated_location_ids

# Agregar el borough y datos del clima
fechas = pd.merge(fechas, df_coordinates[['location_id', 'borough']], on='location_id', how='left')
resultado = pd.merge(fechas, clima, on=['borough', 'year', 'month', 'day_of_month', 'hour_of_day', 'day_of_week'])


# Unir con la columna de viajes y rellenar valores nulos del merge
clima_con_viajes = pd.merge(resultado, viajes[['location_id', 'year', 'month', 'day_of_month', 'day_of_week', 'hour_of_day', 'cantidad_de_viajes']],
                            on=['location_id', 'year', 'month', 'day_of_month', 'day_of_week', 'hour_of_day'], 
                            how='left')

clima_con_viajes['cantidad_de_viajes'] = clima_con_viajes['cantidad_de_viajes'].fillna(0)



# Variables predictoras (X) y variable objetivo (y)
predictors = [
    'location_id', 'day_of_month','hour_of_day', 'day_of_week',
    'relative_humidity', 'apparent_temperature','temperature', 'weather_code',
    'cloud_cover','wind_speed', 'wind_gusts'
]
X = clima_con_viajes[predictors]
y = clima_con_viajes['cantidad_de_viajes']

# Dividir los datos en conjunto de entrenamiento y prueba (80% entrenamiento, 20% prueba)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Crear el modelo XGBoost
model = xgb.XGBRegressor(
    n_estimators=100,
    max_depth=20,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)

# Entrenar el modelo con los datos de entrenamiento
model.fit(X_train, y_train)
model.save_model('modelo_entrenado.json')

