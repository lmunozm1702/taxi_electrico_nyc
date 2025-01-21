from dash import dcc
from dash import html
from dash import register_page, callback
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from shapely import wkt
from shapely.geometry import MultiPolygon
#from shapely.ops import unary_union
#import pandas as pd
#import matplotlib.cm as cm
#import matplotlib.colors as mcolors
#import numpy as np


register_page(__name__, name='Home', path='/')


def cargar_data_graficos():
    credentials = service_account.Credentials.from_service_account_file(
        'driven-atrium-445021-m2-a773215c2f46.json')
    
    query_job = bigquery.Client(credentials=credentials).query(
                '''SELECT borough, zone, pickup_year, map_location, cantidad
                FROM project_data.trips_year_qtr_map
                WHERE borough <> 'EWR';''')
    results = query_job.result().to_dataframe()
    
    return results


tabla_viajes = cargar_data_graficos()

#tabla_viajes = pd.read_csv('tabla_viajes.csv')


#funcion para calcular el kpi1
def render_kpi(kpi_id, year, borough):
    kpi_titles = ['Taxi EV/HYB activo (+5%)', 'Aumento de Viajes (+5%)', 'KPI 3', 'KPI 4']
    kpi_subtitles = ['Trimestre actual:', 'Trimestre actual:', 'KPI 3', 'KPI 4']
    kpi_prefixes = ['', '', '', '']
    kpi_suffixes = ['', '', '', '']
    value, delta, traces = calculate_kpi(kpi_id, year, borough)
    fig = go.Figure(go.Indicator(
        mode = "number+delta",
        value = value,
        number = {"prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1]},
        delta = {"reference": delta, "valueformat": ".0f", "prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1]},
        #delta = {"reference": delta, "prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1], 'relative': True},
        title = {"text": f"{kpi_titles[kpi_id-1]}<br><span style='font-size:0.7em;color:gray'>{kpi_subtitles[kpi_id-1]}</span>"},
        domain = {'y': [0.15, 0.5], 'x': [0.25, 0.75]}))

    fig.add_trace(go.Scatter(
        y = traces, mode='lines', name='trend', line=dict(color='rgba(65,193,176,0.5)', width=2)))

    fig.update_layout(xaxis= {'showticklabels': False}, margin=dict(l=20, r=20, t=20, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(29,67,85,0.1)', autosize=True, height=220)

    return fig

def calculate_kpi(kpi_id, year, borough):
    print(kpi_id, year, borough)
    credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')

    if kpi_id == 1:
        if year == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT year, quarter, month, count FROM `driven-atrium-445021-m2.project_data.active_vehicles_count` WHERE vehicle_type = \'HYB\' or vehicle_type = \'BEV\'')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT year, quarter, month, count FROM `driven-atrium-445021-m2.project_data.active_vehicles_count` WHERE vehicle_type = \'HYB\' or vehicle_type = \'BEV\' and year = {year}')
        results = query_job.result().to_dataframe()

        #cuenta los valores distintos en la columna month para el year y month del último registro del dataframe
        count = results[(results['year'] == results['year'].values[-1]) & (results['quarter'] == results['quarter'].values[-1])]['month'].nunique()
    
        #agrupa los resultados por year y quarter
        results = results.groupby(['year', 'quarter']).sum().reset_index()

        #ordena los resultados por year y quarter
        results = results.sort_values(by=['year', 'quarter'], ascending=[True, True])

        #selecciona la columna f0_ en una lista
        traces = results['count'].map(lambda x: x / count).tolist()

        #value es el valor de la columna f0_ para el ultimo registro de los resultados dividido por la cantidad de months en un trimestre
        value = results['count'].values[-1] / count  

        #delta es el valor de la columna f0_ para el segundo registro de los resultados más el 5%
        delta = (results['count'].values[-2] / 3) * 1.05

        return value, delta, traces
    elif kpi_id == 2:
        if year == 'Todos':
            if borough == 'Todos':
                query_job = bigquery.Client(credentials=credentials).query('SELECT pickup_year, pickup_quarter, pickup_month, count(*) as count FROM `driven-atrium-445021-m2.project_data.trips` group by pickup_year, pickup_quarter, pickup_month order by pickup_quarter, pickup_year, pickup_month;')
            else:
                query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, pickup_quarter, pickup_month, count(*) as count FROM `driven-atrium-445021-m2.project_data.trips` WHERE pickup_location_id in (SELECT location_id FROM `driven-atrium-445021-m2.project_data.coordinates` WHERE borough = \'{borough}\') group by pickup_year, pickup_quarter, pickup_month order by pickup_quarter, pickup_year, pickup_month;')
        else:
            if borough == 'Todos':
                query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, pickup_quarter, pickup_month, count(*) as count FROM `driven-atrium-445021-m2.project_data.trips` WHERE pickup_year = {year} group by pickup_year, pickup_quarter, pickup_month order by pickup_quarter, pickup_year, pickup_month;')
            else:
                query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, pickup_quarter, pickup_month, count(*) as count FROM `driven-atrium-445021-m2.project_data.trips` WHERE pickup_location_id in (SELECT location_id FROM `driven-atrium-445021-m2.project_data.coordinates` WHERE borough = \'{borough}\') AND pickup_year = {year} group by pickup_year, pickup_quarter, pickup_month order by pickup_quarter, pickup_year, pickup_month;')
        results = query_job.result().to_dataframe()

        #cuenta los valores distintos en la columna month para el year y month del último registro del dataframe
        count = results[(results['pickup_year'] == results['pickup_year'].values[-1]) & (results['pickup_quarter'] == results['pickup_quarter'].values[-1])]['pickup_month'].nunique()
    
        #agrupa los resultados por year y quarter
        results = results.groupby(['pickup_year', 'pickup_quarter']).sum().reset_index()

        #ordena los resultados por year y quarter
        results = results.sort_values(by=['pickup_year', 'pickup_quarter'], ascending=[True, True])

        #selecciona la columna f0_ en una lista
        traces = results['count'].map(lambda x: x).tolist()

        #value es el valor de la columna f0_ para el ultimo registro de los resultados dividido por la cantidad de months en un trimestre
        value = results['count'].values[-1]  

        #delta es el valor de la columna f0_ para el segundo registro de los resultados más el 5%
        delta = (results['count'].values[-2] ) * 1.05

        return value, delta, traces  


def calculate_correlaciones(year, borough):
    credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')
    
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT coordinates.borough AS Distrito, trips.pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id GROUP BY coordinates.borough, trips.pickup_day_of_week;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT coordinates.borough AS Distrito, trips.pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id WHERE coordinates.borough = \'{borough}\' GROUP BY coordinates.borough, trips.pickup_day_of_week;')
    else:
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT coordinates.borough AS Distrito, trips.pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id WHERE trips.pickup_year = {year} GROUP BY coordinates.borough, trips.pickup_day_of_week;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT coordinates.borough AS Distrito, trips.pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id WHERE coordinates.borough = \'{borough}\' AND trips.pickup_year = {year} GROUP BY coordinates.borough, trips.pickup_day_of_week;')
        
    results = query_job.result().to_dataframe()
    
    results = results[results['Distrito'] != 'EWR']
    
    dias_semana = { 0: 'Domingo', 1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves', 5: 'Viernes', 6: 'Sábado' }
    results['Dia_de_la_Semana'] = results['Dia_de_la_Semana'].map(dias_semana)
    
    return results


def render_correlaciones(year, borough):
    viajes = calculate_correlaciones(year, borough)
    fig = px.scatter(
        viajes,
        x='Distrito',
        y='Dia_de_la_Semana',
        size='Cantidad',
        title= f'Cantidad de Viajes por Día de Año {year} y Distrito {borough}'
    )
    dias_de_semana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    fig.update_layout(yaxis=dict(categoryorder='array', categoryarray=dias_de_semana[::-1]))
    
    return fig


def calculate_mapa(year, borough, tabla):
    tabla['geometry'] = tabla['map_location'].apply(wkt.loads)
    
    if year == 'Todos':
        if borough == 'Todos':
            results = tabla.groupby(
                ['borough', 'zone']).agg({'cantidad': 'sum', 'geometry': 'first'}).reset_index()
        else:
            results = tabla[tabla['borough'] == borough].groupby(
                ['zone']).agg({'cantidad': 'sum', 'geometry': 'first'}).reset_index()
    else:
        if borough == 'Todos':
            results = tabla[tabla['pickup_year'] == year].groupby(
                ['borough', 'zone']).agg({'cantidad': 'sum', 'geometry': 'first'}).reset_index()
        else:
            results = tabla[(tabla['borough'] == borough) & (tabla['pickup_year'] == year)].groupby(
                ['zone']).agg({'cantidad': 'sum', 'geometry': 'first'}).reset_index()
    
    return results


def get_bounding_box(geometries):
    bounds = [geom.bounds for geom in geometries]
    min_x = min([b[0] for b in bounds])
    min_y = min([b[1] for b in bounds])
    max_x = max([b[2] for b in bounds])
    max_y = max([b[3] for b in bounds])
    return min_x, min_y, max_x, max_y

def calculate_center_and_zoom(geometries):
    min_x, min_y, max_x, max_y = get_bounding_box(geometries)
    center = {'lat': (min_y + max_y) / 2, 'lon': (min_x + max_x) / 2}
    # Ajustar el nivel de zoom dependiendo del tamaño del área
    area = (max_x - min_x) * (max_y - min_y)
    if area < 0.1:
        zoom = 9.5
        print('10')
    else:
        zoom = 8.5
        print('8')
    return center, zoom


def render_mapa(year, borough, tabla):

    viajes = calculate_mapa(year, borough, tabla)
    
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"zone": zone},
                "geometry": geom.__geo_interface__
            } for zone, geom in zip(viajes['zone'], viajes['geometry'])
        ]
    }
    
    geometries = viajes['geometry'].tolist()
    center, zoom = calculate_center_and_zoom(geometries)
    
    fig = px.choropleth_mapbox(
        viajes, 
        geojson=geojson_data, 
        locations='zone',
        featureidkey='properties.zone',
        color='cantidad', 
        color_continuous_scale="Viridis",
        #center=center, #dict(lat=40.7128, lon=-74.0060), 
        mapbox_style="carto-positron", 
        #zoom=zoom, #8,
        opacity=1
        )
    
    for _, row in viajes.iterrows():
        geom = row['geometry']
        
        if isinstance(geom, MultiPolygon):
            for polygon in geom.geoms:
                fig.add_trace(go.Scattermapbox(
                    lon=list(polygon.exterior.coords.xy[0]),
                    lat=list(polygon.exterior.coords.xy[1]),
                    mode='lines',
                    line=dict(width=1, color='black'),
                    text=f"{row['zone']}<br>Cantidad: {row['cantidad']}", 
                    hoverinfo='text'
                    )
                )
        else:
            fig.add_trace(go.Scattermapbox(
                lon=list(geom.exterior.coords.xy[0]),
                lat=list(geom.exterior.coords.xy[1]),
                mode='lines',
                line=dict(width=1, color='black'),
                text=f"{row['zone']}<br>Cantidad: {row['cantidad']}", 
                hoverinfo='text'
            ))
            
    fig.update_layout(
        showlegend=False,
        mapbox=dict(
            style="carto-positron",
            center=center, 
            zoom=zoom 
            )
        )

    return fig


dropdown_options = {
    'years': ['Todos',2023, 2024],
    'boroughs': ['Todos', 'Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'],
}

layout = html.Div([
    dbc.Row([
        dbc.Col([
            dbc.Row([
              dbc.Label("Año", html_for="year-dropdown", className='text-primary p-0 m-0 ms-1 fw-bold'),
              dbc.Select(
                  id='year-dropdown',
                  options=[{'label': year, 'value': year} for year in dropdown_options['years']],
                  value='Todos',
                ),  
                dbc.FormText("Selecciona un año", className='text-muted p-0 m-0 ms-1'),
            ], className='mb-3'),
            dbc.Row([
                dbc.Label("Barrio", html_for="year-dropdown", className='text-primary p-0 m-0 ms-1 fw-bold'),
                dbc.Select(
                    id='borough-dropdown',
                    options=[{'label': borough, 'value': borough} for borough in dropdown_options['boroughs']],
                    value='Todos',
                ),  
                dbc.FormText("Selecciona un barrio", className='text-muted p-0 m-0 ms-1'),
            ])            
            
        ], width=3),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    html.Div(dcc.Graph(figure=render_kpi(1, 'Todos', 'Todos'), id='kpi1'), className='kpi_card_border'),
                ], width=3),
                dbc.Col([
                    html.Div(dcc.Graph(figure=render_kpi(2, 'Todos', 'Todos'), id='kpi2'), className='kpi_card_border'),
                ], width=3),
                dbc.Col([
                    html.H2('', className='border-0'),
                ], width=3),
                dbc.Col([
                    html.H2('', className='border-0'),
                ], width=3),            
            ]),
            dbc.Row([
                dbc.Col([
                    html.H2(dcc.Graph(figure=render_correlaciones('Todos', 'Todos'), id='correlations'), className='text-primary border border-primary'),
                ], width=6),
                dbc.Col([
                    html.H2(dcc.Graph(figure=render_mapa('Todos', 'Todos', tabla_viajes), id='mapa'), className='text-primary border border-primary'),
                ], width=6),
                dbc.Col([
                    html.H2('Gráfico 3', className='text-primary border border-primary'),
                ], width=6),                           
                dbc.Col([
                    html.H2('Gráfico 4', className='text-primary border border-primary'),
                ], width=6),
            ])
        ], width=9)
    ], className='container-fluid'),
], className='container-fluid ')


#define callbacks
@callback(    
        [Output('kpi1', 'figure'),        
        Output('kpi2', 'figure'),
        Input('year-dropdown', 'value'),
        Input('borough-dropdown', 'value')
        ]    
)
def update_kpis(selected_year, selected_borough):
    kpi1 = render_kpi(1, selected_year, selected_borough)
    kpi2 = render_kpi(2, selected_year, selected_borough)
    return kpi1, kpi2


#define callbacks
@callback(    
        [Output('correlations', 'figure'),        
        Output('mapa', 'figure'),
        Input('year-dropdown', 'value'),
        Input('borough-dropdown', 'value')
        ]    
)
def update_graphics(selected_year, selected_borough):
    correlations = render_correlaciones(selected_year, selected_borough)
    mapa = render_mapa(selected_year, selected_borough, tabla_viajes)
    return correlations, mapa