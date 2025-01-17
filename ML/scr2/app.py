import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from datetime import datetime
import io
import base64
import requests
import matplotlib
from google.oauth2 import service_account
matplotlib.use('Agg')  
from google.cloud import storage
from google.oauth2 import service_account
import xgboost as xgb

def load_model():
    credentials_path = '/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json'
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = storage.Client(credentials=credentials)

    bucket_name = 'modelo_entrenado'
    source_blob_name = ''

    # Cargar el modelo desde el bucket
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    model_json_string = blob.download_as_text()
    
    # Crear el modelo XGBoost y cargar los datos del JSON
    model = xgb.Booster()
    model.load_model(model_json_string)
    
    return model

def call_api(date):
    # Coordenadas y boroughs
    latitudes = [40.6815, 40.6501, 40.7834, 40.8499, 40.5623]
    longitudes = [-73.8365, -73.9496, -73.9663, -73.8664, -74.1399]
    boroughs = ["Queens", "Brooklyn", "Manhattan", "Bronx", "Staten Island"]

    # API de OpenWeatherMap
    url = "https://api.openweathermap.org/data/3.0/onecall"
    api_key = "9ad3421deebe0566ec148c1d78cb1257" 
    dataframes = []

    for lat, lon, borough in zip(latitudes, longitudes, boroughs):
        params = {
            "lat": lat,
            "lon": lon,
            "exclude": "current,minutely,daily,alerts",  # Datos a excluir
            "appid": api_key
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            hourly_df = pd.DataFrame(data['hourly'])
            hourly_df['borough'] = borough
            hourly_df['lat'] = lat
            hourly_df['lon'] = lon
            dataframes.append(hourly_df)
        else:
            print(f"Error en la solicitud para {borough}: {response.status_code}")
            print(response.text)

    df_weather = pd.concat(dataframes, ignore_index=True)
    df_weather['dt'] = pd.to_datetime(df_weather['dt'], unit='s')

    clima_desanidado = df_weather['weather'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else {}).apply(pd.Series)
    clima = pd.concat([df_weather, clima_desanidado], axis=1)

    clima = clima.drop(columns=['weather', 'pop'])
    clima = clima.drop(columns=['lat', 'lon'])

    code_conversion = {
        200: 95, 201: 95, 202: 95, 210: 95, 211: 95, 212: 95, 221: 95, 230: 95, 231: 95, 232: 95,
        300: 50, 301: 51, 302: 53, 310: 50, 311: 51, 312: 53, 313: 55, 314: 55, 321: 55,
        500: 60, 501: 61, 502: 62, 503: 63, 504: 64, 511: 66, 520: 67, 521: 68, 522: 69, 531: 69,
        600: 70, 601: 71, 602: 72, 611: 73, 612: 74, 613: 75, 615: 76, 616: 77, 620: 78, 621: 79, 622: 80,
        701: 81, 711: 82, 721: 83, 731: 84, 741: 85, 751: 86, 761: 87, 762: 88, 771: 89, 781: 90,
        800: 0, 801: 1, 802: 2, 803: 3, 804: 4
    }

    clima['weather_code'] = clima['id'].map(code_conversion)
    clima = clima.rename(columns={
        'temp': 'temperature_2m', 
        'feels_like': 'apparent_temperature',
        'pressure': 'pressure_msl',
        'humidity': 'relative_humidity_2m',
        'dew_point': 'dew_point_2m',
        'clouds': 'cloud_cover',
        'wind_speed': 'wind_speed_10m',
        'wind_deg': 'wind_direction_10m',
        'wind_gust': 'wind_gusts_10m',
        'dt': 'date'
    })

    date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    clima_filtrado = clima[clima['date'] == date]

    if clima_filtrado.empty:
        print("No se encontraron datos para la fecha seleccionada.")
    else:
        print(f"Filtrado datos del clima: {len(clima_filtrado)} filas.")

        # importar dataset de locaciones
        locaciones = pd.read_csv('../LOCAL/recursos/dataset.csv')
        data = clima_filtrado.merge(locaciones, on='borough', how='left')
        data = data.drop_duplicates()
        data = data.dropna(subset=['LocationID'])
    
    return data

# Función para predecir los datos usando el modelo de ML
def predict_data(dataframe, model):
    predictors = [
        'LocationID', 'Day', 'Hour', 'Day_of_week',
        'temperature_2m', 'relative_humidity_2m', 
        'apparent_temperature', 'weather_code',
        'cloud_cover', 'wind_speed_10m', 'wind_gusts_10m'
    ]

    predictors = [
    'location_id', 'day_of_month','hour_of_day', 'day_of_week',
    'relative_humidity', 'apparent_temperature','temperature', 'weather_code',
    'cloud_cover','wind_speed', 'wind_gusts'
]
    input_data = dataframe[predictors]
    input_data = input_data.rename(columns={
        'LocationID':'location_id',
        'Day': 'day_of_month',
        'Hour': 'hour_of_day',
        'Day_of_week': 'day_of_week',
        'temperature_2m': 'temperature',
        'relative_humidity_2m' : 'relative_humidity',
        'apparent_temperature': 'apparent_temperature',
        'wind_speed_10m': 'wind_speed',
        'wind_gusts_10m':'wind_gusts'
    })
    predictions = model.predict(input_data)
    dataframe['predicted_trips'] = predictions
    dataframe['predicted_trips'] = dataframe['predicted_trips'].round().astype(int)

    return dataframe

import matplotlib.colors as mcolors

def generate_heatmap(dataframe, shp_file, borough):
    taxi_zones = gpd.read_file(shp_file)
    dataframe = dataframe.rename(columns={"predicted_trips": "solicitudes"})
    merged_data = taxi_zones.merge(dataframe, on="LocationID", how="left")
    merged_data["solicitudes"] = merged_data["solicitudes"].fillna(0)

    # Añadir la columna 'borough' después de la fusión
    merged_data['borough'] = taxi_zones['borough']

    # Filtrar por borough seleccionado
    if borough:
        merged_data = merged_data[merged_data["borough"] == borough]

    fig, ax = plt.subplots(1, 1, figsize=(12, 12))
    merged_data.plot(
        column="solicitudes",
        cmap="OrRd",
        legend=True,
        edgecolor="black",
        linewidth=0.5,
        ax=ax,
        norm=mcolors.LogNorm()
    )
    ax.set_title("Mapa de calor de solicitudes de viajes en NYC", fontsize=16)
    ax.set_xlabel("Longitud", fontsize=12)
    ax.set_ylabel("Latitud", fontsize=12)

    # Añadir anotaciones a cada locación con el valor del número de viajes
    for idx, row in merged_data.iterrows():
        ax.annotate(text=row['solicitudes'], xy=(row.geometry.centroid.x, row.geometry.centroid.y),
                    ha='center', fontsize=8, color='blue')

    # Guardar la imagen en memoria
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", bbox_inches="tight")
    buffer.seek(0)
    plt.close(fig)

    return base64.b64encode(buffer.getvalue()).decode('utf-8')


# Inicializar la aplicación Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = dbc.Container(fluid=True, children=[
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H1("Predicción de Solicitudes de Viajes en NYC", className="text-center my-4")),
                dbc.CardBody([
                    html.H4("Utiliza esta aplicación para predecir las solicitudes de viajes en la ciudad de Nueva York."),
                    html.P("Seleccione una fecha y hora para obtener predicciones precisas basadas en datos históricos y algoritmos avanzados de machine learning.")
                ])
            ], className="mb-4", style={'margin': '0 auto', 'width': '80%'})
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Selecciona una fecha y hora", className="text-center"),
                dbc.CardBody([
                    dcc.DatePickerSingle(
                        id='date-picker',
                        placeholder='Selecciona una fecha',
                        display_format='YYYY-MM-DD',
                        style={'width': '100%', 'marginBottom': '10px'}
                    ),
                    dcc.Dropdown(
                        id='hour-dropdown',
                        options=[{'label': f'{i:02d}:00', 'value': i} for i in range(0, 24)],
                        placeholder='Hora',
                        style={'width': '40%', 'marginTop': '10px', 'marginLeft': 'auto', 'marginRight': 'auto'},
                        className='mx-auto'
                    ),
                    dcc.Dropdown(
                        id='borough-dropdown',
                        options=[{'label': borough, 'value': borough} for borough in ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']],
                        placeholder='Selecciona un borough',
                        style={'width': '100%', 'marginTop': '10px'}
                    )
                ], style={'textAlign': 'center'})
            ], className="mb-4", style={'margin': '0 auto', 'width': '80%'})
        ], width=6, style={'paddingRight': '0px'}),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Resumen de Predicción", className="text-center"),
                dbc.CardBody([
                    html.P("Aquí se mostrará el resultado de la predicción y cualquier otra información relevante."),
                    html.Div(id='output-container', className="text-center text-success mt-2"),
                ], style={'textAlign': 'center'})
            ], className="mb-4", style={'margin': '0 auto', 'width': '80%'})
        ], width=6, style={'paddingLeft': '0px'})
    ]),
    dbc.Row([
        dbc.Col(
            dbc.Button(
                [html.I(className="bi bi-search"), " Predecir"],
                id='predict-button', 
                color="primary", 
                className="mt-3 w-25 rounded-pill shadow-sm",
                style={'margin': '0 auto'}
            ),
            width=12,
            className="text-center mb-4"
        )
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody(html.Img(id='heatmap', className='img-fluid'))
            ], style={'margin': '0 auto', 'width': '40%'})
        ], width=12)
    ]),
])

@app.callback(
    [Output('output-container', 'children'),
     Output('heatmap', 'src')],
    [Input('predict-button', 'n_clicks')],
    [Input('date-picker', 'date'),
     Input('hour-dropdown', 'value'),
     Input('borough-dropdown', 'value')]
)


def process_and_generate(n_clicks, selected_date, selected_hour, selected_borough):
    if selected_date and selected_hour is not None:
        selected_date = datetime.strptime(selected_date, '%Y-%m-%d')
        current_time = datetime.now()

        # Verificar si la fecha está dentro de las próximas 48 horas
        if (selected_date - current_time).total_seconds() < 48 * 3600:
            dataframe = call_api(selected_date.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            dataframe = pd.read_csv('../LOCAL/recursos/dataset.csv')  # Leer datos locales si es fuera de las 48 horas

        dataframe['date'] = pd.to_datetime(dataframe['date'])
        dataframe['date'] = selected_date
        dataframe['Day'] = dataframe['date'].dt.day
        dataframe['Month'] = dataframe['date'].dt.month
        dataframe['Year'] = dataframe['date'].dt.year
        dataframe['Hour'] = dataframe['date'].dt.hour
        dataframe['Day_of_week'] = dataframe['date'].dt.weekday

        model = load_model()
        predicted_data = predict_data(dataframe, model)

        heatmap_image = generate_heatmap(predicted_data, '../LOCAL/recursos/taxi_zones.shp', selected_borough)
        image_src = f"data:image/png;base64,{heatmap_image}"

        return f'Predicciones generadas para: {selected_date.strftime("%Y-%m-%d %H:%M")}', image_src

    return 'Por favor selecciona todos los campos.', None

server = app.server
if __name__ == "__main__":
    app.run_server()
    
"""if __name__ == "__main__":
    app.run_server(debug=True)

"""