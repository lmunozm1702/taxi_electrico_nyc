from math import radians, sin, cos, sqrt, atan2
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely import wkt
from shapely.geometry import MultiPolygon, Point
import io
from PIL import Image
import tabulate

import dash
from dash import Input, Output, State, html, dcc
import dash_bootstrap_components as dbc
from datetime import date, datetime, timedelta

import base64
from io import BytesIO

from google.oauth2 import service_account, id_token
from google.auth.transport.requests import AuthorizedSession
import google.auth.transport.requests


from google.auth.transport.requests import Request
import os
import json


import logging

credentials, project = google.auth.load_credentials_from_file(
    'C:/Users/NoxiePC/Desktop/henry/driven-atrium-445021-m2-a773215c2f46.json'
)
credentials = credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])

# URL de la Google Cloud Function (reemplázala con la URL de tu función)
url = 'https://us-central1-driven-atrium-445021-m2.cloudfunctions.net/obtener_predicciones'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'C:/Users/NoxiePC/Desktop/henry/driven-atrium-445021-m2-a773215c2f46.json'
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json'

def get_prediction(selected_datetime_str, r, location_id):
    data = {
        'date': selected_datetime_str,
        'R': r,
        'locationID': location_id
    }

    credentials.refresh(Request())
    headers = {
        'Authorization': 'Bearer ' + credentials.token
    }

    response = requests.post(url, json=data, headers=headers)

    if response.status_code == 200:
        print("Datos enviados correctamente")
        response_json = response.json()
        # Cargar los DataFrames desde los JSON recibidos
        df1 = pd.read_json(response_json['df1'], orient='split')
        df2 = pd.read_json(response_json['df2'], orient='split')
        return df1, df2
    else:
        print(f"Error al enviar datos: {response.status_code}")
        return None, None

# ------------------------------------ otro mapa --------------------------------#



def get_map3(df, selected_datetime_str, location_id, r):
    # Convertir la columna 'geometry' de objetos geométricos a WKT
    df['geometry'] = df['geometry'].apply(lambda geom: geom.wkt if isinstance(geom, MultiPolygon) else geom)

    # Convertir la columna 'geometry' de WKT a objetos geométricos
    df['geometry'] = df['geometry'].apply(wkt.loads)

    # Convertir el DataFrame a un GeoDataFrame con CRS WGS84
    geo_df = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")

    # Transformar el GeoDataFrame a un CRS métrico (UTM zona 18N para Nueva York)
    geo_df = geo_df.to_crs(epsg=32618)

    # Crear la columna logarítmica
    geo_df['log_k'] = np.log(geo_df['k'] + 1)  # Añadir 1 para evitar log(0)

    # Crear el mapa de calor con bordes negros
    fig, ax = plt.subplots(1, 1, figsize=(12, 10))  # Cambié el tamaño de la figura
    geo_df.plot(column='log_k', ax=ax, legend=True, cmap='OrRd', edgecolor='black', 
                legend_kwds={'label': "Logaritmo de Intensidad de K"})

    # Volver a transformar el GeoDataFrame a WGS84 para anotaciones en lat/lon
    geo_df_wgs = geo_df.to_crs(epsg=4326)

    # Colocar el valor de 'locationID' en el centro de cada locación (en coordenadas UTM)
    for idx, row in geo_df.iterrows():
        centroid = row.geometry.centroid  # Calcular el centroide
        centroid_x, centroid_y = centroid.x, centroid.y

        # Añadir la anotación en el mapa (usando coordenadas UTM)
        ax.text(
            centroid_x, centroid_y, 
            s=row['locationID'], 
            ha='center', va='center', 
            fontsize=6, color='black', weight='bold'
        )

    # Extraer la geometría del centroide de la ubicación seleccionada (en UTM)
    location_row = geo_df.loc[geo_df['locationID'] == location_id]
    if not location_row.empty:
        centroid = location_row.geometry.centroid.values[0]  # Obtener el centroide
        center_x, center_y = centroid.x, centroid.y

        # Dibujar un círculo con radio r (en metros)
        circle = plt.Circle((center_x, center_y), r * 1000, color='blue', alpha=0.2, label=f"Radio {r} km")
        ax.add_artist(circle)

    # Configurar el gráfico
    ax.set_title(f"Mapa de Calor - K (Log) - {selected_datetime_str}", fontsize=14)
    ax.set_xlabel("Coordenada X (UTM)")
    ax.set_ylabel("Coordenada Y (UTM)")
    ax.legend()

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
                dbc.Button("Calcular", id="submit-button", color="primary", className="mt-4 w-100", n_clicks=0)
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

# Configuración de logging para mayor detalle en los logs
logging.basicConfig(level=logging.DEBUG)
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
    if n_clicks is None:
        # Si el botón no ha sido presionado, no hacer nada.
        logging.debug("No se ha presionado el botón.")
        return dbc.Alert("Por favor, complete todos los campos.", color="warning")
    
    if not date or not time or not r or not location_id:
        logging.debug("Faltan campos: Date: %s, Time: %s, R: %s, Location ID: %s", date, time, r, location_id)
        return dbc.Alert("Por favor, complete todos los campos.", color="warning")
    
    try:
        selected_datetime_str = f"{date} {time}"
        df, loc_cercanos_df = get_prediction(selected_datetime_str, r, location_id)

        if df.empty:
            logging.warning("El DataFrame de predicción está vacío.")
            return dbc.Alert("No hay datos para la fecha y hora seleccionadas.", color="warning")

        if 'k' not in df.columns:
            logging.warning("La columna 'k' no existe en los datos.")
            return dbc.Alert("La columna 'k' no existe en los datos.", color="warning")
        
        
        img = get_map3(df, selected_datetime_str, location_id, r)
        # Convertir la imagen a formato base64 para incrustarla en HTML
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
        logging.debug("Imagen convertida a base64.")

        # Extraer las variables climáticas de la primera fila
        climatic_variables = {
            'relative_humidity': 'Humedad Relativa (%)',
            'apparent_temperature': 'Temperatura Aparente (°C)',
            'temperature': 'Temperatura (°C)',
            'cloud_cover': 'Cobertura de Nubes (%)',
            'wind_speed': 'Velocidad del Viento (m/s)',
            'wind_gusts': 'Ráfagas de Viento (m/s)'
        }
        
        first_row = df.iloc[0]

        # Formatear las variables climáticas
        formatted_values = {
            'Humedad Relativa (%)': f"{first_row['relative_humidity']}%",
            'Temperatura Aparente (°C)': f"{first_row['apparent_temperature'] - 273.15:.2f}",
            'Temperatura (°C)': f"{first_row['temperature'] - 273.15:.2f}",
            'Cobertura de Nubes (%)': f"{first_row['cloud_cover']}%",
            'Velocidad del Viento (m/s)': f"{first_row['wind_speed']:.2f}",
            'Ráfagas de Viento (m/s)': f"{first_row['wind_gusts']:.2f}"
        }

        # Crear la tabla HTML para las variables climáticas
        climatic_table = dbc.Table(
            [
                html.Thead(html.Tr([html.Th(col) for col in formatted_values.keys()])),
                html.Tbody(html.Tr([html.Td(val) for val in formatted_values.values()]))
            ],
            bordered=True,
            striped=True,
            hover=True,
            responsive=True,
            class_name="mb-4"
        )

        # Ordenar loc_cercanos_df por 'k' de mayor a menor
        loc_cercanos_df_sorted = loc_cercanos_df.sort_values(by='k', ascending=False)

        # Crear la tabla HTML para loc_cercanos_df
        loc_table = dbc.Table(
            [
                html.Thead(html.Tr([html.Th("ID de Ubicación"), html.Th("k")])),
                html.Tbody([
                    html.Tr([html.Td(row['locationID']), html.Td(f"{row['k']:.2f}")])
                    for _, row in loc_cercanos_df_sorted.iterrows()
                ])
            ],
            bordered=True,
            striped=True,
            hover=True,
            responsive=True,
            class_name="mb-4"
        )

        # Devolver el mapa, las variables climáticas y la tabla de locaciones cercanas
        return html.Div([
            html.Div([
                html.H5("Mapa Generado", className="font-weight-bold mb-3"),
                dbc.Card(
                    dbc.CardBody([
                        html.Img(src=f"data:image/png;base64,{img_str}",
                                 style={"width": "100%", "height": "auto", "border-radius": "8px"})
                    ]),
                    className="mb-4",
                    style={"box-shadow": "0 4px 8px rgba(0,0,0,0.1)", "border-radius": "8px", "background-color": "#ffffff"}
                ),
            ]),
            html.Div([
                html.H5(f"Condiciones climáticas de {first_row['borough']}", className="font-weight-bold mb-3"),
                climatic_table
            ]),
            html.Div([
                html.H5(f"Locaciones cercanas a la locación [{location_id}] a {r}km de distancia", className="font-weight-bold mb-3"),
                loc_table
            ])
        ])

    except Exception as e:
        logging.error("Error al procesar los datos: %s", str(e))
        return dbc.Alert(f"Error al procesar los datos: {str(e)}", color="danger")


if __name__ == "__main__":
    app.run_server(debug=True)
