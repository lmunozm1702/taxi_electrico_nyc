import pandas as pd
from dash import dcc
from dash import html, dash_table
from dash import callback, register_page
from dash.dependencies import Input, Output
from dash.dash_table.Format import Format
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from shapely import wkt
import textwrap
import geopandas as gpd

#hide userwarnings
import warnings
warnings.filterwarnings("ignore")


register_page(__name__, name='Home', path='/')

#funcion para calcular el kpi1
def render_kpi(kpi_id, year, borough):
    kpi_titles = ['Taxi EV/HYB activo (+5%)', 'Cantidad Viajes (+5%)', 'Ticket Promedio (+2%)', 'Cuota vs Taxi (+0.01%)']
    kpi_subtitles = ['Trimestre actual:', 'Trimestre actual:', 'Trimestre actual:', 'Mes Actual:']
    kpi_prefixes = ['', '', 'USD$', '']
    kpi_suffixes = ['', '', '', '%']
    delta_format = [',.0f', ',.0f', ',.2f', ',.2f']
    value, delta, traces = calculate_kpi(kpi_id, year, borough)
    fig = go.Figure(go.Indicator(
        mode = "number+delta",
        value = value,
        number = {"prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1]},
        delta = {"reference": delta, "valueformat": delta_format[kpi_id-1], "prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1]},
        #delta = {"reference": delta, "prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1], 'relative': True},
        title = {"text": f"{kpi_titles[kpi_id-1]}<br><span style='font-size:0.7em;color:gray'>{kpi_subtitles[kpi_id-1]}</span>"},
        domain = {'y': [0.15, 0.5], 'x': [0.25, 0.75]}))

    fig.add_trace(go.Scatter(
        y = traces, mode='lines', name='trend', line=dict(color='rgba(65,193,176,0.5)', width=2)))

    fig.update_layout(xaxis= {'showticklabels': False}, margin=dict(l=20, r=20, t=20, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(29,67,85,0.1)', autosize=True, height=220)

    return fig

def calculate_kpi(kpi_id, year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')

    if kpi_id == 1:
        if year == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT year, quarter, month, count FROM `driven-atrium-445021-m2.project_data.kpi1`;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT year, quarter, month, count FROM `driven-atrium-445021-m2.project_data.kpi1` WHERE year = {year}')
        results = query_job.result().to_dataframe()

        #elimina duplicados
        results = results.drop_duplicates()

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
    elif kpi_id == 3:
        if year == 'Todos':
            if borough == 'Todos':
                query_job = bigquery.Client(credentials=credentials).query('SELECT pickup_year, pickup_quarter, avg_fare_amount FROM `driven-atrium-445021-m2.project_data.kpi3_nb`;')
            else:
                query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, pickup_quarter, avg_fare_amount FROM `driven-atrium-445021-m2.project_data.kpi3_sb` WHERE borough = \'{borough}\';')
        else:
            if borough == 'Todos':
                query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, pickup_quarter, avg_fare_amount FROM `driven-atrium-445021-m2.project_data.kpi3_nb` WHERE pickup_year = {year};')
            else:
                query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, pickup_quarter, avg_fare_amount FROM`driven-atrium-445021-m2.project_data.kpi3_sb` WHERE borough = \'{borough}\' AND pickup_year = {year};')
        results = query_job.result().to_dataframe()

        #ordena los resultados por year y quarter
        results = results.sort_values(by=['pickup_year', 'pickup_quarter'], ascending=[True, True])

        #selecciona la columna avg_fare_amount en una lista
        traces = results['avg_fare_amount'].map(lambda x: x).tolist()

        #value es el valor de la columna avg_fare_amount para el ultimo registro de los resultados
        value = results['avg_fare_amount'].values[-1]

        #delta es el valor de la columna avg_fare_amount para el segundo registro de los resultados
        delta = results['avg_fare_amount'].values[-2] * 1.02
    else:
        if year == 'Todos':
            if borough == 'Todos':
                query_job = bigquery.Client(credentials=credentials).query('SELECT taxi_type, pickup_year, pickup_quarter, pickup_month, cantidad FROM `driven-atrium-445021-m2.project_data.kpi4_nb`;')
            else:
                query_job = bigquery.Client(credentials=credentials).query(f'SELECT taxi_type, pickup_year, pickup_quarter, pickup_month, cantidad FROM `driven-atrium-445021-m2.project_data.kpi4_sb` WHERE borough = \'{borough}\';')
        else:
            if borough == 'Todos':
                query_job = bigquery.Client(credentials=credentials).query(f'SELECT taxi_type, pickup_year, pickup_quarter, pickup_month, cantidad FROM `driven-atrium-445021-m2.project_data.kpi4_nb` WHERE pickup_year = {year};')
            else:
                query_job = bigquery.Client(credentials=credentials).query(f'SELECT taxi_type, pickup_year, pickup_quarter, pickup_month, cantidad FROM `driven-atrium-445021-m2.project_data.kpi4_sb` WHERE borough = \'{borough}\' AND pickup_year = {year};')
        results = query_job.result().to_dataframe()

        #ordena los resultados por year y quarter
        results = results.sort_values(by=['pickup_year', 'pickup_quarter', 'pickup_month'], ascending=[True, True, True])

        #agrega columna month_total cpon un apply a la funcion taxi_type_total_month(results)
        results['month_total'] = results.apply(lambda row: results[
            (results['pickup_year'] == row['pickup_year']) & 
            (results['pickup_quarter'] == row['pickup_quarter']) & 
            (results['pickup_month'] == row['pickup_month'])]['cantidad'].sum(), axis=1)

        #elimina la fila si taxi_type es distinto a high_volume
        results = results[results['taxi_type'] == 'high_volume']

        #agrega columna cuota en la cual para cada registro el valor es cantidad / month_total
        results['cuota'] = results.apply(lambda row: row['cantidad'] / row['month_total'], axis=1)

        #selecciona la columna cantidad en una lista
        traces = results['cuota'].map(lambda x: x).tolist()
        
        #value es el valor de la columna cantidad para el ultimo registro de los resultados
        value = results['cantidad'].values[-1] / results['month_total'].values[-1] * 100

        #delta es el valor de la columna cantidad para el segundo registro de los resultados
        delta = (results['cantidad'].values[-2] / results['month_total'].values[-2] * 100) + 0.001
        

    return value, delta, traces
    
def calculate_correlaciones(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')
    
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT coordinates.borough AS Distrito, trips.pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id GROUP BY coordinates.borough, trips.pickup_day_of_week ORDER BY Dia_de_la_Semana;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT coordinates.borough AS Distrito, trips.pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id WHERE coordinates.borough = \'{borough}\' GROUP BY coordinates.borough, trips.pickup_day_of_week  ORDER BY Dia_de_la_Semana;')
    else:
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT coordinates.borough AS Distrito, trips.pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id WHERE trips.pickup_year = {year} GROUP BY coordinates.borough, trips.pickup_day_of_week  ORDER BY Dia_de_la_Semana;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT coordinates.borough AS Distrito, trips.pickup_day_of_week AS Dia_de_la_Semana, count(*) AS Cantidad FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates ON trips.pickup_location_id = coordinates.location_id WHERE coordinates.borough = \'{borough}\' AND trips.pickup_year = {year} GROUP BY coordinates.borough, trips.pickup_day_of_week  ORDER BY Dia_de_la_Semana;')
        
    results = query_job.result().to_dataframe()
    
    results = results[results['Distrito'] != 'EWR']
    
    dias_semana = { 0: 'D', 1: 'L', 2: 'M', 3: 'X', 4: 'J', 5: 'V', 6: 'S' }
    results['Dia_de_la_Semana2'] = results['Dia_de_la_Semana'].map(dias_semana)

    return results

#Crea grafico de lieas utilizando plotly
def render_viajes_lineas(year, borough):
    viajes = calculate_correlaciones(year, borough)
    #ordenar por distrito y dia de la semana
    viajes = viajes.sort_values(by=['Distrito', 'Dia_de_la_Semana'])
    fig = px.line(
        viajes,
        x='Dia_de_la_Semana2',
        y='Cantidad',
        color='Distrito',
        markers=True,
        #specify color of each line
        color_discrete_sequence=['#1d4355', '#365b6d', '#41c1b0', '#6c9286', '#f2f1ec'],        
    )

    #ocultar titulo de eje x y eje y    
    fig.update_layout(
        title={
        'text': 'Viajes por Día',  
        'font': {
            'size': 14,  
            'color': '#41c1b0'  
        },
        'x': 0.1,  
        'xanchor': 'left',
        'yanchor': 'top'
        },
        margin=dict(l=20, r=20, t=50, b=20), 
        paper_bgcolor='rgba(0,0,0,0)', 
        plot_bgcolor='rgba(29,67,85,0.1)', 
        autosize=True, height=310, xaxis_title=None, yaxis_title=None, legend_yanchor="top", legend_xanchor="left", legend_orientation="h")
    return fig

def render_correlaciones(year, borough):
    viajes = calculate_correlaciones(year, borough)
    fig = px.scatter(
        viajes,
        x='Distrito',
        y='Dia_de_la_Semana',
        size='Cantidad',        
    )
    dias_de_semana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    fig.update_layout(yaxis=dict(categoryorder='array', categoryarray=dias_de_semana[::-1]), autosize=True, height=310, margin=dict(l=20, r=20, t=20, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(29,67,85,0.1)')
    
    return fig


def calculate_mapa(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(
                '''SELECT zone, ST_AsText(map_location) AS geometry, SUM(cantidad) AS cantidad
                FROM project_data.trips_year_qtr_map 
                GROUP BY zone, geometry;''')
        else:
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, ST_AsText(map_location) AS geometry, SUM(cantidad) AS cantidad 
                FROM project_data.trips_year_qtr_map 
                WHERE borough = \'{borough}\'
                GROUP BY zone, geometry;''')
    else:
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, ST_AsText(map_location) AS geometry, SUM(cantidad) AS cantidad
                FROM project_data.trips_year_qtr_map 
                WHERE pickup_year = {year}
                GROUP BY zone, geometry;''')
        else:
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, ST_AsText(map_location) AS geometry, SUM(cantidad) AS cantidad 
                FROM project_data.trips_year_qtr_map 
                WHERE borough = \'{borough}\'
                AND pickup_year = {year}
                GROUP BY zone, geometry;''')
            
    results = query_job.result().to_dataframe()
    
    return results


def calculate_mapa_destino(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(
                '''SELECT zone, ST_AsText(map_location) AS geometry, SUM(cantidad) AS cantidad 
                FROM project_data.dropoff_trips_year_qtr_map 
                GROUP BY zone, geometry;''')
        else:
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, ST_AsText(map_location) AS geometry, SUM(cantidad) AS cantidad 
                FROM project_data.dropoff_trips_year_qtr_map 
                WHERE borough = \'{borough}\'
                GROUP BY zone, geometry;''')
    else:
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, ST_AsText(map_location) AS geometry, SUM(cantidad) AS cantidad 
                FROM project_data.dropoff_trips_year_qtr_map 
                WHERE pickup_year = {year}
                GROUP BY zone, geometry;''')
        else:
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, ST_AsText(map_location) AS geometry, SUM(cantidad) AS cantidad 
                FROM project_data.dropoff_trips_year_qtr_map 
                WHERE borough = \'{borough}\'
                AND pickup_year = {year}
                GROUP BY zone, geometry;''')
            
    results = query_job.result().to_dataframe()
    
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
    area = (max_x - min_x) * (max_y - min_y)
    if area < 0.1:
        zoom = 9.5
    else:
        zoom = 8.5
    return center, zoom


def render_mapa(viajes, min_val, max_val, tipo):

    viajes['geometry'] = viajes['geometry'].map(wkt.loads)
    
    gdf = gpd.GeoDataFrame(viajes, geometry='geometry', crs="EPSG:4326")
    
    gdf['text'] = gdf.apply(lambda row: f"Zona: {row['zone']}<br>Cantidad: {row['cantidad']}", axis=1)
    
    center, zoom = calculate_center_and_zoom(gdf['geometry'].tolist())
    
    gdf_proj = gdf.to_crs(epsg=3857)
    
    gdf_proj['centroid'] = gdf_proj.geometry.centroid
    
    gdf['lon'] = gdf_proj['centroid'].to_crs(epsg=4326).x 
    gdf['lat'] = gdf_proj['centroid'].to_crs(epsg=4326).y
    
    max_size = 30 
    min_size = 5 
    gdf['size'] = (
        min_size 
        + (max_size - min_size) 
        * (gdf['cantidad'] - min_val) 
        / (max_val - min_val)
        )
    
    scattermapbox_data = go.Scattermapbox(
        lon=gdf['lon'],
        lat=gdf['lat'],
        mode='markers',
        marker=dict(
            size=gdf['size'],
            color=gdf['cantidad'],
            colorscale=['#1d4355', '#365b6d', '#41c1b0', '#6c9286'],
            showscale=True,
            opacity=1,
            cmin=min_val,
            cmax=max_val
        ),
        text=gdf['text'],
        hoverinfo='text'
    )
            
    layout = go.Layout(
        title={
        'text': f'{tipo} de Viajes',  
        'font': {
            'size': 14,  
            'color': '#41c1b0'  
        },
        'x': 0.1,  
        'xanchor': 'left',
        'yanchor': 'top'
        },
        showlegend=False,
        uirevision=True,
        mapbox=dict(
            style="carto-positron",
            center=center, 
            zoom=zoom 
            ),
        autosize=True,
        height=310,
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(29,67,85,0.1)'
        )

    fig = go.Figure(data=[scattermapbox_data], layout=layout)

    return fig

def calculate_max_min_table(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT pickup_year, borough, zone, cantidad FROM project_data.trips_year_qtr_map;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, borough, zone, cantidad FROM project_data.trips_year_qtr_map where borough = \'{borough}\';')
    else:
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, borough, zone, cantidad FROM project_data.trips_year_qtr_map WHERE pickup_year = {year};')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, borough, zone, cantidad FROM project_data.trips_year_qtr_map WHERE pickup_year = {year} AND borough = \'{borough}\';')
        
    df = query_job.result().to_dataframe()
    
    #Order df by year, quarter, month
    df = df.sort_values(by=['pickup_year', 'cantidad'], ascending=[False, False])

    #Eliminar columnas pickup_year, pickup_quarter, map_location
    df = df.drop(['pickup_year'], axis=1)
    
    #Eliminar duplicados por columna location_id dejando el primer registro
    df = df.drop_duplicates(subset=['zone'], keep='first')

    #wrap for column zone, permite wrapping over long words
    df['zone'] = df['zone'].apply(lambda x: textwrap.shorten(x, width=20, placeholder='...'))    

    df_max = df.head(5)
    df_min = df.tail(5)

    return pd.concat([df_max, df_min], axis=0).reset_index(drop=True)  

def card_total_viajes(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')

    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT pickup_year, borough, cantidad FROM project_data.card_total_viajes;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, borough, cantidad FROM project_data.card_total_viajes where borough = \'{borough}\';')
    else:
        if borough == 'Todos':            
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, borough, cantidad FROM project_data.card_total_viajes WHERE pickup_year = {year};')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, borough, cantidad FROM project_data.card_total_viajes WHERE pickup_year = {year} AND borough = \'{borough}\';')
    
    df = query_job.result().to_dataframe()
    results = df['cantidad'].sum()

    #return results with comma separator and grouped by thousands
    return "{:,}".format(results)

def card_viaje_promedio_dia(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT pickup_year, pickup_month, borough, cantidad FROM project_data.card_viaje_promedio_dia;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, pickup_month, borough, cantidad FROM project_data.card_viaje_promedio_dia where borough = \'{borough}\';')
    else:
        if borough == 'Todos':            
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, pickup_month, borough, cantidad FROM project_data.card_viaje_promedio_dia WHERE pickup_year = {year};')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, pickup_month, borough, cantidad FROM project_data.card_viaje_promedio_dia WHERE pickup_year = {year} AND borough = \'{borough}\';')
    df = query_job.result().to_dataframe()

    #Extract the unique pairs 'pickup_year' and 'pickup_month' and sum the 'pickup_month' column into a scalar variable
    df2 = df.groupby(['pickup_year', 'pickup_month']).sum(['pickup_month']).reset_index()    

    #print the toptal for column 'pickup_month'
    total_days = df2['pickup_month'].sum() * 30
    total_qty = df2['cantidad'].sum()

    #return results with comma separator and without decimals
    return "{:,.0f}".format(total_qty/total_days)

def card_total_vehiculos(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')
    if year == 'Todos':
        query_job = bigquery.Client(credentials=credentials).query('SELECT vehicle_type, year, month, count as cantidad FROM project_data.active_vehicles_count;')
    else:
        query_job = bigquery.Client(credentials=credentials).query(f'SELECT vehicle_type, year, month, count as cantidad FROM project_data.active_vehicles_count where year = {year};')

    df = query_job.result().to_dataframe()

    #eliminar duplicados
    df = df.drop_duplicates()

    #Agrupar registros por columna year y sumar la columna cantidad
    df = df.groupby(['year', 'month']).sum(['cantidad']).reset_index()

    #ordenar los registros por year y month
    df = df.sort_values(by=['year', 'month'], ascending=[False, False])

    #seleccionar el primer valor para la columna year
    df_month = df[df['month'] == df['month'].values[0]].reset_index(drop=True)

    return "{:,}".format(df_month['cantidad'].values[0])

def card_tiempo_promedio_viaje(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT pickup_year, borough, trip_duration FROM project_data.card_trip_duration_average;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, borough, trip_duration FROM project_data.card_trip_duration_average where borough = \'{borough}\';')
    else:
        if borough == 'Todos':            
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, borough, trip_duration FROM project_data.card_trip_duration_average WHERE pickup_year = {year};')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT pickup_year, borough, trip_duration FROM project_data.card_trip_duration_average WHERE pickup_year = {year} AND borough = \'{borough}\';')
    df = query_job.result().to_dataframe()

    #calculate the average for column trip_duration
    total_mean = df.groupby(['pickup_year']).mean(['trip_duration']).reset_index()

    #return total_mean in minutes and the rest as seconds
    result = "{:,.0f}".format(total_mean['trip_duration'].values[0]/60) + "m" + "{:,.0f}".format(total_mean['trip_duration'].values[0]%60) + "s"

    #return results as object
    return result

    

def render_population_rate(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')

    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT borough, pickup_day_of_week, cantidad FROM project_data.trips_week_day;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT borough, pickup_day_of_week, cantidad FROM project_data.trips_week_day where borough = \'{borough}\';')
    else:
        if borough == 'Todos':            
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT borough, pickup_day_of_week, cantidad FROM project_data.trips_week_day WHERE pickup_year = {year};')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT borough, pickup_day_of_week, cantidad FROM project_data.trips_week_day where borough = \'{borough}\' AND pickup_year = {year};')
    viajes = query_job.result().to_dataframe()

    borough_population = {
        'Manhattan': 3400000,
        'Brooklyn': 2500000,
        'Queens': 2200000,
        'Bronx': 1400000,
        'Staten Island': 450000
    }

    viajes['population_rate'] = viajes.apply(lambda row: row['cantidad'] / borough_population[row['borough']], axis=1)

    #agrupar por distrito
    viajes = viajes.groupby(['borough']).sum().reset_index()
    #viajes = viajes.groupby(['borough', 'pickup_day_of_week']).sum().reset_index()

    #ordenar por distrito
    viajes = viajes.sort_values(by=['borough'], ascending=[True])

    #grafico de barras por borough
    fig = px.bar(
        viajes,
        x='borough',
        y='population_rate',
        color='borough',
        #specify color of each bar
        color_discrete_sequence=['#1d4355', '#365b6d', '#41c1b0', '#6c9286', '#f2f1ec'],
    )

    #ocultar titulo de eje x y eje y    
    fig.update_layout(
        title={
        'text': 'Viajes c/100.000 habitantes',  
        'font': {
            'size': 14,  
            'color': '#41c1b0'  
        },
        'x': 0.1,  
        'xanchor': 'left',
        'yanchor': 'top'
        },
        showlegend=False,
        margin=dict(l=20, r=20, t=50, b=20), 
        paper_bgcolor='rgba(0,0,0,0)', 
        plot_bgcolor='rgba(29,67,85,0.1)', 
        autosize=True, 
        height=310, 
        xaxis_title=None, yaxis_title=None, legend_yanchor="top", legend_xanchor="left", legend_orientation="h")
    return fig

def render_hourly_pickup(year, borough):
    credentials = service_account.Credentials.from_service_account_file('/etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')

    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query('SELECT borough, pickup_hour_of_day, total FROM project_data.trips_hourly_pickup;')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT borough, pickup_hour_of_day, total FROM project_data.trips_hourly_pickup where borough = \'{borough}\';')
    else:
        if borough == 'Todos':            
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT borough, pickup_hour_of_day, total FROM project_data.trips_hourly_pickup WHERE pickup_year = {year};')
        else:
            query_job = bigquery.Client(credentials=credentials).query(f'SELECT borough, pickup_hour_of_day, total FROM project_data.trips_hourly_pickup where borough = \'{borough}\' AND pickup_year = {year};')
    viajes = query_job.result().to_dataframe()

    #ordenar por distrito y dia de la semana
    viajes = viajes.sort_values(by=['borough', 'pickup_hour_of_day'], ascending=[True, True])

    fig = px.line(
        viajes,
        x='pickup_hour_of_day',
        y='total',
        color='borough',
        markers=True,
        #specify color of each line
        color_discrete_sequence=['#1d4355', '#365b6d', '#41c1b0', '#6c9286', '#f2f1ec'],        
    )

    #ocultar titulo de eje x y eje y    
    fig.update_layout(
        title={
        'text': 'Viajes por Hora',  
        'font': {
            'size': 14,  
            'color': '#41c1b0'  
        },
        'x': 0.1,  
        'xanchor': 'left',
        'yanchor': 'top'
        },
        margin=dict(l=20, r=20, t=50, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(29,67,85,0.1)', autosize=True, height=310, xaxis_title=None, yaxis_title=None, legend_yanchor="top", legend_xanchor="left", legend_orientation="h")
    return fig

dropdown_options = {
    'years': [2024, 2023],
    'boroughs': ['Todos', 'Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'],
}

layout = html.Div([
    dcc.Store(id='viajes_origen'),
    dcc.Store(id='viajes_destino'),
    dcc.Store(id='min_val'),
    dcc.Store(id='max_val'),
    dbc.Row([
        dbc.Col([
            dbc.Row([
              dbc.Label("Año", html_for="year-dropdown", className='text-primary p-0 m-0 ms-1 fw-bold'),
              dbc.Select(
                  id='year-dropdown',
                  options=[{'label': year, 'value': year} for year in dropdown_options['years']],
                  value=2024,
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
            ]),
            dbc.Row([
                dbc.Card([                    
                    dbc.CardBody([
                        html.H4("Total Viajes", className="card-title text-center text-secondary p-0 m-0"),
                        html.H1(id='total_viajes', className="card-text text-center text-primary m-0 p-0"),
                    ])
                ], className='kpi_card_border py-2'),
                dbc.Card([                    
                    dbc.CardBody([
                        html.H4("Viajes Promedio Día", className="card-title text-center text-secondary p-0 m-0"),
                        html.H1(id='viaje_promedio_dia', className="card-text text-center text-primary m-0 p-0"),
                    ])
                ], className='kpi_card_border py-2'),
                dbc.Card([                    
                    dbc.CardBody([
                        html.H4("Vehiculos Activos", className="card-title text-center text-secondary p-0 m-0"),
                        html.H1(id='total_vehiculos', className="card-text text-center text-primary m-0 p-0"),
                    ])
                ], className='kpi_card_border py-2'),
                dbc.Card([                    
                    dbc.CardBody([
                        html.H4("Tiempo Promedio Viaje", className="card-title text-center text-secondary p-0 m-0"),
                        html.H1(id='tiempo_promedio_viaje', className="card-text text-center text-primary m-0 p-0"),
                    ])
                ], className='kpi_card_border py-2'),
                html.Div(                
                    html.Img(src='assets/sd_logo_transparente.png', className='img-fluid', style={'width': '60%', 'height': 'auto'}),
                    className='d-flex justify-content-center pt-3 mt-3'
                )
            ], className='px-5 mt-4'),                   
        ], width=3),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    html.Div(dcc.Graph(id='kpi1'), className='kpi_card_border'),
                ], width=3),
                dbc.Col([
                    html.Div(dcc.Graph(id='kpi2'), className='kpi_card_border'),
                ], width=3),
                dbc.Col([
                    html.Div(dcc.Graph(id='kpi3'), className='kpi_card_border'),
                ], width=3),
                dbc.Col([
                    html.Div(dcc.Graph(id='kpi4'), className='kpi_card_border'),
                ], width=3),            
            ]),
            dbc.Row([
                dbc.Col([                    
                    html.H2(dcc.Graph(id='viajes_linea'), className='kpi_card_border'),
                ], width=4),                
                dbc.Col([
                    html.H2(dcc.Graph(id='mapa_origen'), className='kpi_card_border'),
                ], width=4),
                dbc.Col([
                    html.H2(dcc.Graph(id='mapa_destino'), className='kpi_card_border'),
                ], width=4),
            ]),
            dbc.Row([
                dbc.Col([
                    html.H2(dcc.Graph(id='population_rate'), className='kpi_card_border'),
                ], width=4),              
                dbc.Col([
                    html.Div(
                        children=[
                            html.H4("Mejores y Peores Zonas", style={'fontSize': '15px', 'color': '#41c1b0', 'textAlign': 'left', 'fontWeight': 'normal'}),                            
                            dash_table.DataTable(               
                            id='max_min_table',
                            columns =[{'name': 'Zona', 'id': 'zone'},
                                      {'name': 'Barrio', 'id': 'borough'},
                                      {'name': 'Cantidad', 'id': 'cantidad', 'type': 'numeric', 'format': Format(group=',')},
                                ],                            
                            style_data={
                                'whiteSpace': 'normal',
                                'height': 'auto',
                                'lineHeight': '1px'
                            },
                            style_header={
                                'backgroundColor': 'white',
                                'fontWeight': 'bold'
                            },
                            fixed_rows={'headers': True},
                            style_table={'height': 255, 'overflowY': 'auto'},
                            style_cell_conditional=[                                
                                {'if': {'column_id': 'cantidad'}, 'textAlign': 'right'},
                            ],
                            style_as_list_view=True,
                            style_cell={'padding': '3px', 'fontSize': 12, 'textAlign': 'left'},                            
                        )],
                        className='table_card_border'),
                ], width=4),
                dbc.Col([                    
                    html.H2(dcc.Graph(id='hourly_pickup'), className='kpi_card_border'),
                ], width=4),
            ])
        ], width=9)
    ], className='container-fluid'),
], className='container-fluid ')


@callback(
    Output('viajes_origen', 'data'),
    [Input('year-dropdown', 'value'),
    Input('borough-dropdown', 'value')]
)
def obtener_viajes_origen(selected_year, selected_borough):
    origen = calculate_mapa(selected_year, selected_borough).to_json(orient='split')
    return origen


@callback(
    Output('viajes_destino', 'data'),
    [Input('year-dropdown', 'value'),
    Input('borough-dropdown', 'value')]
)
def obtener_viajes_destino(selected_year, selected_borough):
    destino = calculate_mapa_destino(selected_year, selected_borough).to_json(orient='split')
    return destino


@callback(
    [Output('min_val', 'data'),
     Output('max_val', 'data')],
    [Input('viajes_origen', 'data'),
    Input('viajes_destino', 'data')]
)
def obtener_min_max(origen, destino):
    origen = pd.read_json(origen, orient='split')
    destino = pd.read_json(destino, orient='split')
    minimo = min(origen['cantidad'].min(), destino['cantidad'].min())
    maximo = max(origen['cantidad'].max(), destino['cantidad'].max())
    return minimo, maximo


@callback(
    Output('kpi1', 'figure'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_kpi1(selected_year, selected_borough):
    return render_kpi(1, selected_year, selected_borough)


@callback(
    Output('kpi2', 'figure'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_kpi2(selected_year, selected_borough):
    return render_kpi(2, selected_year, selected_borough)


@callback(
    Output('kpi3', 'figure'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_kpi3(selected_year, selected_borough):
    return render_kpi(3, selected_year, selected_borough)


@callback(
    Output('kpi4', 'figure'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_kpi4(selected_year, selected_borough):
    return render_kpi(4, selected_year, selected_borough)


@callback(
    Output('viajes_linea', 'figure'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_viajes_linea(selected_year, selected_borough):
    return render_viajes_lineas(selected_year, selected_borough)


@callback(
    Output('mapa_origen', 'figure'),
    [Input('viajes_origen', 'data'), 
     Input('min_val', 'data'),
     Input('max_val', 'data')]
)
def update_mapa_origen(origen, minimo, maximo):
    origen = pd.read_json(origen, orient='split')
    return render_mapa(origen, minimo, maximo, 'Origenes')


@callback(
    Output('mapa_destino', 'figure'),
    [Input('viajes_destino', 'data'), 
     Input('min_val', 'data'),
     Input('max_val', 'data')]
)
def update_mapa_destino(destino, minimo, maximo):
    destino = pd.read_json(destino, orient='split')
    return render_mapa(destino, minimo, maximo, 'Destinos')


@callback(
    Output('max_min_table', 'data'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_max_min_table(selected_year, selected_borough):
    return calculate_max_min_table(selected_year, selected_borough).to_dict('records')


@callback(
    Output('total_viajes', 'children'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_total_viajes(selected_year, selected_borough):
    return card_total_viajes(selected_year, selected_borough)


@callback(
    Output('viaje_promedio_dia', 'children'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_viaje_promedio_dia(selected_year, selected_borough):
    return card_viaje_promedio_dia(selected_year, selected_borough)


@callback(
    Output('total_vehiculos', 'children'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_total_vehiculos(selected_year, selected_borough):
    return card_total_vehiculos(selected_year, selected_borough)


@callback(
    Output('tiempo_promedio_viaje', 'children'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_tiempo_promedio_viaje(selected_year, selected_borough):
    return card_tiempo_promedio_viaje(selected_year, selected_borough)


@callback(
    Output('population_rate', 'figure'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_population_rate(selected_year, selected_borough):
    return render_population_rate(selected_year, selected_borough)


@callback(
    Output('hourly_pickup', 'figure'),
    [Input('year-dropdown', 'value'), 
     Input('borough-dropdown', 'value')]
)
def update_hourly_pickup(selected_year, selected_borough):
    return render_hourly_pickup(selected_year, selected_borough)



                # dbc.Col([
                #     #<iframe width="600" height="450" src= frameborder="0" style="border:0" allowfullscreen sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"></iframe>
                #     html.Iframe(src="https://lookerstudio.google.com/embed/reporting/dfb5c74a-6d41-4767-b00c-a84154d7b9cf/page/ZEedE"),
                #     #html.Div('<iframe width="600" height="450" src="https://lookerstudio.google.com/embed/reporting/dfb5c74a-6d41-4767-b00c-a84154d7b9cf/page/ZEedE" frameborder="0" style="border:0" allowfullscreen sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"></iframe>', className='text-primary border border-primary'),
                #     #html.H2('Gráfico 3', className='text-primary border border-primary'),
                # ], width=6), 