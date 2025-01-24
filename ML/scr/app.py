import functions_framework
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from google.cloud import storage
import itertools
import pickle
from google.cloud import storage
from numpy import random
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


@functions_framework.http
def hello_http(request):


    client = bigquery.Client(project = 'driven-atrium-445021-m2')

    # Definir la consulta SQL para acceder a la vista
    query_viajes_por_ubicacion_y_tiempoPU = """SELECT * FROM `driven-atrium-445021-m2.project_data.viajes_por_ubicacion_y_tiempo`;"""
    query_viajes_por_ubicacion_y_tiempoDO = """SELECT * FROM `driven-atrium-445021-m2.project_data.oferta_por_ubicacion` ;"""
    query_weather_borough_details = """SELECT * FROM `driven-atrium-445021-m2.project_data.weather_borough_details`;"""
    query_coordinates = """SELECT location_id, borough FROM driven-atrium-445021-m2.project_data.coordinates"""

    # Ejecutar la consulta y cargar los resultados en un DataFrame de pandas
    df_coordinates = client.query(query_coordinates).to_dataframe()
    df_clima = client.query(query_weather_borough_details).to_dataframe()
    df_viajesPU = client.query(query_viajes_por_ubicacion_y_tiempoPU).to_dataframe()
    df_viajesDO = client.query(query_viajes_por_ubicacion_y_tiempoDO).to_dataframe()

    clima = df_clima
    viajesPU = df_viajesPU
    viajesDO = df_viajesDO

    # DEJAMOS LISTA EL DATASET DE COORDENADAS  
    coordinates = df_coordinates[~df_coordinates['location_id'].isin([264, 265, 1])]
    boroughs = ["Queens", "Brooklyn", "Manhattan", "Bronx", "Staten Island"]
    coordinates = coordinates[coordinates['borough'].isin(boroughs)]
    coordinates = coordinates.rename(columns={'location_id':'locationID'})



    # CREAMOS EL DATASET AUXILIAR 
    locaciones = coordinates['locationID'].unique()

    clima = clima.rename(columns={'day_of_month':'day', 'hour_of_day':'hour'})
    df = pd.DataFrame()
    df['datetime'] = pd.to_datetime(clima[['year', 'month', 'day', 'hour']])
    min_date = df['datetime'].min()
    max_date = df['datetime'].max()

    date_range = pd.date_range(start=min_date, end=max_date, freq='h')
    df_aux = pd.DataFrame(date_range, columns=['datetime'])
    clima = clima.rename(columns={'day':'day_of_month', 'hour':'hour_of_day'})



    # PONER TODAS LAS TABLAS EN EL MISMO RANGO DE FECHAS 
    viajesDO = viajesDO.rename(columns={'day_of_month':'day', 'hour_of_day':'hour'})
    viajesPU = viajesPU.rename(columns={'day_of_month':'day', 'hour_of_day':'hour'})

    viajesDO['datetime'] = pd.to_datetime(viajesDO[['year', 'month', 'day', 'hour']])
    viajesPU['datetime'] = pd.to_datetime(viajesPU[['year', 'month', 'day', 'hour']])

    viajesDO = viajesDO[viajesDO['datetime'].isin(date_range)]
    viajesPU = viajesPU[viajesPU['datetime'].isin(date_range)]

    viajesDO = viajesDO.drop(columns=['datetime'])
    viajesPU = viajesPU.drop(columns=['datetime'])

    viajesDO = viajesDO.rename(columns={'day':'day_of_month', 'hour':'hour_of_day'})
    viajesPU = viajesPU.rename(columns={'day':'day_of_month', 'hour':'hour_of_day'})



    df_aux = list(itertools.product(df_aux['datetime'], locaciones))
    df_aux = pd.DataFrame(df_aux, columns=['datetime', 'locationID'])


    df_aux['year'] = df_aux['datetime'].dt.year
    df_aux['month'] = df_aux['datetime'].dt.month
    df_aux['day_of_month'] = df_aux['datetime'].dt.day
    df_aux['hour_of_day'] = df_aux['datetime'].dt.hour
    df_aux['day_of_week'] = df_aux['datetime'].dt.dayofweek
    df_aux = df_aux.drop(columns=['datetime'])


    day_of_week_mapping = {1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 0: 6}
    viajesDO['day_of_week'] = viajesDO['day_of_week'].replace(day_of_week_mapping)


    resultado = pd.DataFrame()

    resultado = df_aux.merge(viajesDO, on=['locationID', 'year', 'month', 'day_of_month', 'hour_of_day', 'day_of_week'], how='left')
    resultado['oferta'] = resultado['oferta'].fillna(0)
    resultado = resultado.merge(viajesPU, on=['locationID', 'year', 'month', 'day_of_month', 'hour_of_day', 'day_of_week'], how='left')
    resultado['solicitudes'] = resultado['solicitudes'].fillna(0)
    resultado['total_fare_amount'] = resultado['total_fare_amount'].fillna(0)


    trip_data = resultado
    #  Realizar el merge para agregar la columna 'borough' al dataset 'trip_data' 
    trip_data = trip_data.merge(coordinates[['locationID', 'borough']], on='locationID', how='left')
    trip_data = trip_data.merge(clima, on=['year', 'month', 'day_of_month', 'hour_of_day', 'day_of_week', 'borough'], how='left')


    # Crear una máscara booleana aleatoria para seleccionar el 50% de los datos
    mask = random.rand(len(trip_data)) < 0.80
    trip_data_reduced = trip_data[mask]



    # Variables predictoras (X) y variable objetivo (y)
    predictors = [
        'locationID', 'day_of_month','hour_of_day', 'day_of_week',
        'relative_humidity', 'apparent_temperature','temperature', 'weather_code',
        'cloud_cover','wind_speed', 'wind_gusts'
    ]

    X = trip_data_reduced[predictors]
    y1 = trip_data_reduced['solicitudes']
    y2 = trip_data_reduced['oferta']
    y3 = trip_data_reduced['total_fare_amount']

    # Dividir los datos en conjunto de entrenamiento y prueba (80% entrenamiento, 20% prueba)
    X_train, X_test, y1_train, y1_test = train_test_split(X, y1, test_size=0.2, random_state=42)
    X_train, X_test, y2_train, y2_test = train_test_split(X, y2, test_size=0.2, random_state=42)
    X_train, X_test, y3_train, y3_test = train_test_split(X, y3, test_size=0.2, random_state=42)

    # Crear el modelo XGBoost
    model_1 = xgb.XGBRegressor(
        n_estimators=80,
        max_depth=15,
        learning_rate=0.2,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )
    model_2 = xgb.XGBRegressor(
        n_estimators=80,
        max_depth=15,
        learning_rate=0.2,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )
    model_3 = xgb.XGBRegressor(
        n_estimators=80,
        max_depth=15,
        learning_rate=0.2,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )

    # Entrenar el modelo con los datos de entrenamiento
    model_1.fit(X_train, y1_train)
    model_2.fit(X_train, y2_train)
    model_3.fit(X_train, y3_train)

    bucket_name = 'modelo_entrenado'
    model_1_filename = 'XGB_model_1.pkl'
    model_2_filename = 'XGB_model_2.pkl'
    model_3_filename = 'XGB_model_3.pkl'

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    def save_model_to_bucket(model, model_filename):

        model_data = pickle.dumps(model)
        blob = bucket.blob(model_filename)  # Crear un blob (objeto) en el bucket con el nombre del archivo    
        blob.upload_from_string(model_data)  # Subir los datos del modelo al bucket

    save_model_to_bucket(model_1, model_1_filename)
    save_model_to_bucket(model_2, model_2_filename)
    save_model_to_bucket(model_3, model_3_filename)

    # Predecir con los datos de prueba
    y1_pred = model_1.predict(X_test)

    mse_1 = mean_squared_error(y1_test, y1_pred)
    mae_1 = mean_absolute_error(y1_test, y1_pred)
    r2_1 = r2_score(y1_test, y1_pred)
    print("Modelo 1:")
    print(f"Mean Squared Error (MSE): {mse_1}")
    print(f"Mean Absolute Error (MAE): {mae_1}")
    print(f"R² Score: {r2_1}")

    y2_pred = model_2.predict(X_test)

    mse_2 = mean_squared_error(y2_test, y2_pred)
    mae_2 = mean_absolute_error(y2_test, y2_pred)
    r2_2 = r2_score(y2_test, y2_pred)
    print("\nModelo 2:")
    print(f"Mean Squared Error (MSE): {mse_2}")
    print(f"Mean Absolute Error (MAE): {mae_2}")
    print(f"R² Score: {r2_2}")

    y3_pred = model_3.predict(X_test)

    mse_3 = mean_squared_error(y3_test, y3_pred)
    mae_3 = mean_absolute_error(y3_test, y3_pred)
    r2_3 = r2_score(y3_test, y3_pred)
    print("\nModelo 3:")
    print(f"Mean Squared Error (MSE): {mse_3}")
    print(f"Mean Absolute Error (MAE): {mae_3}")
    print(f"R² Score: {r2_3}")

    print("Modelos guardados exitosamente en el bucket.")

    return 'exito'
