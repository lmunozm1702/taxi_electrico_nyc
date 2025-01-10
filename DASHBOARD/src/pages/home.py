import requests

from dash import dcc
from dash import html
from dash import register_page, callback
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account


register_page(__name__, name='Home', path='/')

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
    credentials = service_account.Credentials.from_service_account_file('../../driven-atrium-445021-m2-a773215c2f46.json')

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
    print(year, borough)
    credentials = service_account.Credentials.from_service_account_file('../../driven-atrium-445021-m2-a773215c2f46.json')
    
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(
                '''
                SELECT borough AS Distrito, 
                pickup_day_of_week AS Dia_de_la_Semana, 
                count(*) AS Cantidad 
                FROM project_data.trips AS trips
                INNER JOIN project_data.coordinates AS coordinates 
                ON trips.pickup_location_id = coordinates.location_id
                GROUP BY coordinates.borough, pickup_day_of_week
                ''')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT borough AS Distrito, pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id WHERE borough = {borough} GROUP BY coordinates.borough, pickup_day_of_week;')
    else:
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT borough AS Distrito, pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id WHERE pickup_year = {year} GROUP BY coordinates.borough, pickup_day_of_week;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT borough AS Distrito, pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id WHERE borough = {borough} AND pickup_year = {year} GROUP BY coordinates.borough, pickup_day_of_week;')
        
    results = query_job.result().to_dataframe()
    dias_semana = { 0: 'Domingo', 1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves', 5: 'Viernes', 6: 'Sábado' }
    results['Dia_de_la_Semana'] = results['Dia_de_la_Semana'].map(dias_semana)
    return results


def render_correlaciones(year, borough):
    viajes = calculate_correlaciones(year, borough)
    # Crear el gráfico de dispersión
    fig = px.scatter(
        viajes,
        x='Distrito',
        y='Dia_de_la_Semana',
        size='Cantidad',
        title='Cantidad de Viajes por Distrito por Día'
    )
    fig.update_layout(yaxis=dict(categoryorder='array', categoryarray=['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']))
    
    return fig


def calculate_mapa(year, borough):
    credentials = service_account.Credentials.from_service_account_file('../../driven-atrium-445021-m2-a773215c2f46.json')
    query_job = bigquery.Client(
        credentials=credentials).query(
            '''
            SELECT borough AS Distrito, count(*) AS Cantidad 
            FROM project_data.trips AS trips
            INNER JOIN project_data.coordinates AS coordinates 
            ON trips.pickup_location_id = coordinates.location_id
            GROUP BY coordinates.borough
            ''')
    results = query_job.result().to_dataframe()
    return results

def render_mapa(year, borough):
    url = "https://raw.githubusercontent.com/dwillis/nyc-maps/master/boroughs.geojson"
    response = requests.get(url)
    geojson = response.json()
    viajes = calculate_mapa(year, borough)
    fig = px.choropleth_mapbox(viajes, geojson=geojson, locations='Distrito', featureidkey="properties.BoroName",
                               color='Cantidad', color_continuous_scale="Viridis",
                               center=dict(lat=40.7128, lon=-74.0060), mapbox_style="carto-positron", zoom=8)
    fig.update_layout(title='Cantidad de Inicios de Viaje por Distrito', margin={"r":0,"t":40,"l":0,"b":0})
    return fig


def calculate_torta(year, borough):
    credentials = service_account.Credentials.from_service_account_file('../../driven-atrium-445021-m2-a773215c2f46.json')
    query_job = bigquery.Client(
        credentials=credentials).query(
            '''
            SELECT vehicle_type AS Tipo_de_Motor, count(*) AS Cantidad 
            FROM project_data.active_vehicles_count AS actives
            GROUP BY actives.vehicle_type
            ''')
    results = query_job.result().to_dataframe()
    return results

def render_torta(year, borough):
    viajes = calculate_torta(year, borough)
    fig = px.pie(viajes, names='Tipo_de_Motor', values='Cantidad', title='Distribución por Tipo de Motor')
    return fig


dropdown_options = {
    'years': ['Todos',2023, 2024],
    'boroughs': ['Todos', 'Manhattan', 'Brooklyn', 'Queens', 'The Bronx', 'Staten Island'],
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
                    html.H2('KPI 3', className='text-primary border border-primary'),
                ], width=3),
                dbc.Col([
                    html.H2('KPI 4', className='text-primary border border-primary'),
                ], width=3),            
            ]),
            dbc.Row([
                dbc.Col([
                    html.H2(dcc.Graph(figure=render_correlaciones('Todos', 'Todos'), id='correlations'), className='text-primary border border-primary'),
                ], width=6),
                dbc.Col([
                    html.H2(dcc.Graph(figure=render_mapa('Todos', 'Todos'), id='mapa'), className='text-primary border border-primary'),
                ], width=6),
                dbc.Col([
                    html.H2(dcc.Graph(figure=render_torta('Todos', 'Todos'), id='cake'), className='text-primary border border-primary'),
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
        Output('cake', 'figure'),
        Input('year-dropdown', 'value'),
        Input('borough-dropdown', 'value')
        ]    
)
def update_graphics(selected_year, selected_borough):
    correlations = render_correlaciones(selected_year, selected_borough)
    mapa = render_mapa(selected_year, selected_borough)
    cake = render_torta(selected_year, selected_borough)
    return correlations, mapa, cake