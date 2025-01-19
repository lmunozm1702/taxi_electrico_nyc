from math import radians, sin, cos, sqrt, atan2
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely import wkt
from shapely.geometry import MultiPolygon
import io
from PIL import Image
import joblib
import tabulate

import dash
from dash import Input, Output, State, html, dcc
import dash_bootstrap_components as dbc
from datetime import date, datetime

import base64
from io import BytesIO
import os

def load():
    # Construir las rutas absolutas a los archivos
    model_1_path = os.path.join(os.getcwd(), 'app', 'models', 'xgboost_model_1.pkl')
    model_2_path = os.path.join(os.getcwd(), 'app', 'models', 'xgboost_model_2.pkl')
    model_3_path = os.path.join(os.getcwd(), 'app', 'models', 'xgboost_model_3.pkl')
    coordinates_path = os.path.join(os.getcwd(), 'app', 'data', 'coordinates.csv')

    # Cargar los modelos y los datos
    model_1 = joblib.load(model_1_path)
    model_2 = joblib.load(model_2_path)
    model_3 = joblib.load(model_3_path)
    coordinates = pd.read_csv(coordinates_path)

    return model_1, model_2, model_3, coordinates

def get_clima(date):
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
        'temp': 'temperature', 
        'feels_like': 'apparent_temperature',
        'pressure': 'pressure_msl',
        'humidity': 'relative_humidity',
        'dew_point': 'dew_point_2m',
        'clouds': 'cloud_cover',
        'wind_speed': 'wind_speed',
        'wind_deg': 'wind_direction',
        'wind_gust': 'wind_gusts',
        'dt': 'date'
    })

    date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    clima_filtrado = clima[clima['date'] == date]

    # Filtrar el DataFrame para que contenga solo las columnas especificadas
    columnas_deseadas = ['date', 'temperature', 'apparent_temperature', 'relative_humidity',
                        'cloud_cover', 'wind_speed', 'wind_direction',
                        'wind_gusts', 'borough', 'weather_code']

    # Suponiendo que la columna 'date' todavía existe en el DataFrame df_clima
    clima_filtrado = clima_filtrado[columnas_deseadas]
    clima_filtrado['year'] = clima_filtrado['date'].dt.year
    clima_filtrado['month'] = clima_filtrado['date'].dt.month
    clima_filtrado['day_of_month'] = clima_filtrado['date'].dt.day
    clima_filtrado['hour_of_day'] = clima_filtrado['date'].dt.hour
    clima_filtrado['day_of_week'] = clima_filtrado['date'].dt.dayofweek
    clima_filtrado = clima_filtrado.drop(columns=['date'])

    if clima_filtrado.empty: print("No se encontraron datos para la fecha seleccionada.")
    else: print(f"Filtrado datos del clima: {len(clima_filtrado)} filas.")   
    return clima_filtrado

#test = get_clima('2025-01-19 10:00:00')

def get_df(date, coordinates):

    clima = get_clima(date)
    df = coordinates
    date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    df['date'] = date
    # Descomponer la columna date en Year, Month, Day, Hour y Day_of_week
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day_of_month'] = df['date'].dt.day
    df['hour_of_day'] = df['date'].dt.hour
    df['day_of_week'] = df['date'].dt.dayofweek
    df = df.drop(columns=['date'])

    clima = clima.rename(columns={'Borough': 'borough'})

    df_unido = pd.merge(df, clima, on=['borough', 'year', 'month', 'day_of_month', 'hour_of_day', 'day_of_week'], how='inner')

    return df_unido

#test = get_df('2025-01-19 10:00:00', coordinates)

def get_distance(lat1, lon1, lat2, lon2):
    
    R = 6371.0  # Radio de la Tierra en kilómetros
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    # Fórmula de Haversine
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    distance = R * c # Distancia en kilómetros
    return distance

def get_prediction(date, R, locationID, model_1, model_2, model_3, coordinates):
    df = get_df(date, coordinates)
    
    # Obtener los datos para las predicciones
    predictors = ['locationID', 'day_of_month', 'hour_of_day', 'day_of_week',
                  'relative_humidity', 'apparent_temperature', 'temperature', 'weather_code',
                  'cloud_cover', 'wind_speed', 'wind_gusts']
    X = df[predictors]

    # Obtener la prediccion de solicitud, oferta y precio
    solicitud = model_1.predict(X)
    oferta = model_2.predict(X)
    precio = model_3.predict(X)

    # Convertir predicciones negativas a cero
    solicitud = np.maximum(solicitud, 0)
    oferta = np.maximum(oferta, 0)
    precio = np.maximum(precio, 0)

    k = (solicitud + 1) * (precio + 1) / (oferta + 1)
    df['k'] = k

    val = df[df['locationID'] == locationID].iloc[0]
    lat = val['lat']
    lon = val['lon']

    loc_cercanos = []
    for i in range(len(df)):
        r = get_distance(df.iloc[i]['lat'], df.iloc[i]['lon'], lat, lon)
        if r < R:
            loc_cercanos.append(df.iloc[i])

    # Crear DataFrame de loc_cercanos y mantener solo las columnas locationID y k
    loc_cercanos_df = pd.DataFrame(loc_cercanos)[['locationID', 'k']]
    loc_cercanos_df = loc_cercanos_df.sort_values(by='k')
    
    return df, loc_cercanos_df

def get_map(df):
    # Convertir la columna 'geometry' de objetos geométricos a WKT
    df['geometry'] = df['geometry'].apply(lambda geom: geom.wkt if isinstance(geom, MultiPolygon) else geom)

    # Convertir la columna 'geometry' de WKT a objetos geométricos
    df['geometry'] = df['geometry'].apply(wkt.loads)

    # Convertir el DataFrame a un GeoDataFrame
    geo_df = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")

    # Crear la columna logarítmica
    geo_df['log_k'] = np.log(geo_df['k'] + 1)  # Añadir 1 para evitar log(0)

    # Crear el mapa de calor con bordes negros
    fig, ax = plt.subplots(1, 1, figsize=(18, 12))
    geo_df.plot(column='log_k', ax=ax, legend=True, cmap='OrRd', edgecolor='black', legend_kwds={'label': "Logaritmo de Intensidad de K"})

    # Colocar el valor de 'locationID' en el centro de cada locación
    for idx, row in geo_df.iterrows():
        plt.annotate(text=row['locationID'], xy=(row.geometry.centroid.x, row.geometry.centroid.y), 
                     ha='center', va='center', fontsize=6, color='black', weight='bold')

    # Configurar el gráfico
    ax.set_title("Mapa de Calor - Variable 'k' (Escala Logarítmica)", fontsize=16)
    ax.set_xlabel("Longitud")
    ax.set_ylabel("Latitud")

    # Convertir el gráfico en una imagen optimizada para la web
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
    buf.seek(0)
    img = Image.open(buf)

    return img

# ----------------------------------  app web --------------------------------- #


# Importa tus modelos y funciones
# from your_module import get_prediction, model_1, model_2, model_3, get_df

# App initialization
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout
def create_layout():
    return dbc.Container([
        # Título principal con mayor estilo
        dbc.Row([
            dbc.Col(html.H1("Predicción de Indicadores", className="text-center mb-5 font-weight-bold"), width=12)
        ]),
        
        # Contenedor con dos columnas: Inputs a la izquierda y resultados a la derecha
        dbc.Row([
            # Inputs a la izquierda con un diseño más limpio y espaciado
            dbc.Col([
                html.Label("Seleccione una fecha:", className="font-weight-bold"),
                dcc.DatePickerSingle(
                    id="date-picker",
                    min_date_allowed=date(2020, 1, 1),
                    max_date_allowed=date(2030, 12, 31),
                    initial_visible_month=date.today(),
                    date=date.today(),
                    className="form-control"
                ),
                html.Label("Seleccione una hora:", className="mt-4 font-weight-bold"),
                dcc.Input(
                    id="time-picker",
                    type="text",
                    placeholder="HH:MM:SS",
                    value="12:00:00",
                    className="form-control"
                ),
                html.Label("Ingrese la distancia R (km):", className="mt-4 font-weight-bold"),
                dbc.Input(id="input-r", type="number", placeholder="Distancia en kilómetros", min=0, className="form-control"),
                html.Label("Ingrese el Location ID:", className="mt-4 font-weight-bold"),
                dbc.Input(id="input-location", type="number", placeholder="ID de la ubicación", min=0, className="form-control"),
                dbc.Button("Calcular", id="submit-button", color="primary", className="mt-4 w-100")
            ], width=4, className="p-4", style={"background-color": "#f8f9fa", "border-radius": "8px", "box-shadow": "0 4px 8px rgba(0,0,0,0.1)"}),
            
            # Resultados a la derecha con un diseño limpio y centrado
            dbc.Col([
                html.H4("Resultados", className="font-weight-bold mb-3"),
                dcc.Loading(
                    id="loading-results",
                    type="circle",
                    children=html.Div(id="output-results")
                ),
            ], width=8, className="p-4", style={"background-color": "#ffffff", "border-radius": "8px", "box-shadow": "0 4px 8px rgba(0,0,0,0.1)"})
        ], justify="center")
    ], fluid=True, style={"padding": "2rem"})
app.layout = create_layout()

# Callbacks
@app.callback(
    Output("output-results", "children"),
    Input("submit-button", "n_clicks"),
    State("date-picker", "date"),
    State("time-picker", "value"),
    State("input-r", "value"),
    State("input-location", "value")
)



def update_results(n_clicks, date, time, r, location_id):
    if not date or not time or r is None or location_id is None:
        return dbc.Alert("Por favor, complete todos los campos.", color="warning")

    try:
        model_1, model_2, model_3, coordinates = load()

        # Combina la fecha y la hora seleccionadas (date y time ya son cadenas)
        selected_datetime_str = f"{date} {time}"

        # Llama a la función de predicción (asegúrate de tener modelos y datos cargados)
        df, loc_cercanos_df = get_prediction(selected_datetime_str, r, location_id, model_1, model_2, model_3, coordinates)

        # Genera la imagen del mapa usando la función get_map
        img = get_map(df)

        # Convierte la imagen a formato base64 para incrustarla en HTML
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")

        # Devuelve los resultados: imagen del mapa y el DataFrame de ubicaciones cercanas
        return html.Div([
            # Título y presentación de las ubicaciones cercanas
            html.Div([
                html.H5("Ubicaciones Cercanas", className="font-weight-bold mb-3"),
                dbc.Card(
                    dbc.CardBody([
                        dcc.Markdown(loc_cercanos_df.to_markdown(index=False)),
                    ]),
                    className="mb-4",
                    style={"box-shadow": "0 4px 8px rgba(0,0,0,0.1)", "border-radius": "8px", "background-color": "#f8f9fa"}
                ),
            ], className="mb-4"),
            
            # Título y presentación del mapa generado
            html.Div([
                html.H5("Mapa Generado", className="font-weight-bold mb-3"),
                dbc.Card(
                    dbc.CardBody([
                        html.Img(src=f"data:image/png;base64,{img_str}", style={"width": "100%", "height": "auto", "border-radius": "8px"})
                    ]),
                    className="mb-4",
                    style={"box-shadow": "0 4px 8px rgba(0,0,0,0.1)", "border-radius": "8px", "background-color": "#ffffff"}
                ),
            ])
        ])
    except Exception as e:
        return dbc.Alert(f"Error al procesar los datos: {str(e)}", color="danger")

if __name__ == "__main__":
    app.run_server(debug=True)
