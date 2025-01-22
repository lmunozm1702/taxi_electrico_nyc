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
from shapely.geometry import Polygon, MultiPolygon

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
    credentials = service_account.Credentials.from_service_account_file('./etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
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
    credentials = service_account.Credentials.from_service_account_file('./etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
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
    results['Dia_de_la_Semana'] = results['Dia_de_la_Semana'].map(dias_semana)

    return results

#Crea grafico de lieas utilizando plotly
def render_viajes_lineas(year, borough):
    viajes = calculate_correlaciones(year, borough)
    fig = px.line(
        viajes,
        x='Dia_de_la_Semana',
        y='Cantidad',
        color='Distrito',
        markers=True,
        #specify color of each line
        color_discrete_sequence=['#1d4355', '#365b6d', '#41c1b0', '#6c9286', '#f2f1ec'],        
        #title= f'Cantidad de Viajes por Mes {year} y Distrito {borough}'
    )

    #ocultar titulo de eje x y eje y    
    fig.update_layout(margin=dict(l=20, r=20, t=20, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(29,67,85,0.1)', autosize=True, height=330, xaxis_title=None, yaxis_title=None, legend_yanchor="top", legend_xanchor="left", legend_orientation="h")
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
    fig.update_layout(yaxis=dict(categoryorder='array', categoryarray=dias_de_semana[::-1]), autosize=True, height=330, margin=dict(l=20, r=20, t=20, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(29,67,85,0.1)')
    
    return fig


def calculate_mapa(year, borough):
    credentials = service_account.Credentials.from_service_account_file('./etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')
    
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(
                '''SELECT zone, SUM(cantidad) AS cantidad, ANY_VALUE(map_location) AS geometry 
                FROM project_data.trips_year_qtr_map 
                WHERE borough <> \'EWR\'
                GROUP BY zone;''')
        else:
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, SUM(cantidad) AS cantidad, ANY_VALUE(map_location) AS geometry 
                FROM project_data.trips_year_qtr_map 
                WHERE borough <> \'EWR\' AND borough = \'{borough}\'
                GROUP BY zone;''')
    else:
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, SUM(cantidad) AS cantidad, ANY_VALUE(map_location) AS geometry 
                FROM project_data.trips_year_qtr_map 
                WHERE pickup_year = {year}
                GROUP BY zone;''')
        else:
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, SUM(cantidad) AS cantidad, ANY_VALUE(map_location) AS geometry 
                FROM project_data.trips_year_qtr_map 
                WHERE borough = \'{borough}\'
                AND pickup_year = {year}
                GROUP BY zone;''')
            
    results = query_job.result().to_dataframe()
    
    #results['geometry'] = results['geometry'].apply(wkt.loads)
    
    return results


def calculate_mapa_destino(year, borough):
    credentials = service_account.Credentials.from_service_account_file('./etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
    #credentials = service_account.Credentials.from_service_account_file('driven-atrium-445021-m2-a773215c2f46.json')
    
    if year == 'Todos':
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(
                '''SELECT zone, SUM(cantidad) AS cantidad, ANY_VALUE(map_location) AS geometry 
                FROM project_data.dropoff_trips_year_qtr_map 
                WHERE borough <> \'EWR\'
                GROUP BY zone;''')
        else:
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, SUM(cantidad) AS cantidad, ANY_VALUE(map_location) AS geometry 
                FROM project_data.dropoff_trips_year_qtr_map 
                WHERE borough = \'{borough}\'
                GROUP BY zone;''')
    else:
        if borough == 'Todos':
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, SUM(cantidad) AS cantidad, ANY_VALUE(map_location) AS geometry 
                FROM project_data.dropoff_trips_year_qtr_map 
                WHERE pickup_year = {year}
                GROUP BY zone;''')
        else:
            query_job = bigquery.Client(credentials=credentials).query(
                f'''SELECT zone, SUM(cantidad) AS cantidad, ANY_VALUE(map_location) AS geometry 
                FROM project_data.dropoff_trips_year_qtr_map 
                WHERE borough = \'{borough}\'
                AND pickup_year = {year}
                GROUP BY zone;''')
            
    results = query_job.result().to_dataframe()
    
    #results['geometry'] = results['geometry'].apply(wkt.loads)
    
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


def render_mapa(year, borough, tipo_lugar):

    print('Año: ', year)

    if tipo_lugar == 'origen':
        viajes = calculate_mapa(year, borough)
    elif tipo_lugar == 'destino':
        viajes = calculate_mapa_destino(year, borough)
    
    viajes['geometry'] = viajes['geometry'].apply(lambda geom: geom.wkt if isinstance(geom, (Polygon, MultiPolygon)) else geom)

    viajes['geometry'] = viajes['geometry'].apply(wkt.loads)
    
    gdf = gpd.GeoDataFrame(viajes, geometry='geometry', crs="EPSG:4326")
    
    gdf['text'] = gdf.apply(lambda row: f"Zona: {row['zone']}<br>Cantidad: {row['cantidad']}", axis=1)
    
    center, zoom = calculate_center_and_zoom(gdf['geometry'].tolist())
    
    gdf_proj = gdf.to_crs(epsg=3857)
    
    gdf_proj['centroid'] = gdf_proj.geometry.centroid
    
    gdf['lon'] = gdf_proj['centroid'].to_crs(epsg=4326).x 
    gdf['lat'] = gdf_proj['centroid'].to_crs(epsg=4326).y
    
    max_size = 20 
    min_size = 5 
    gdf['size'] = min_size + (max_size - min_size) * (gdf['cantidad'] - gdf['cantidad'].min()) / (gdf['cantidad'].max() - gdf['cantidad'].min())
    
    scattermapbox_data = go.Scattermapbox(
        lon=gdf['lon'],
        lat=gdf['lat'],
        mode='markers',
        marker=dict(
            size=gdf['size'],
            color=gdf['cantidad'],
            colorscale=['#1d4355', '#365b6d', '#41c1b0', '#6c9286'],
            showscale=True,
            opacity=1
        ),
        text=gdf['text'],
        hoverinfo='text'
    )
            
    layout = go.Layout(
        showlegend=False,
        mapbox=dict(
            style="carto-positron",
            center=center, 
            zoom=zoom 
            ),
        autosize=True,
        height=330,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(29,67,85,0.1)'
        )

    fig = go.Figure(data=[scattermapbox_data], layout=layout)

    return fig

def calculate_max_min_table(year, borough):
    credentials = service_account.Credentials.from_service_account_file('./etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
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
    credentials = service_account.Credentials.from_service_account_file('./etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
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
    credentials = service_account.Credentials.from_service_account_file('./etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
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
    credentials = service_account.Credentials.from_service_account_file('./etc/secrets/driven-atrium-445021-m2-a773215c2f46.json')
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

dropdown_options = {
    'years': [2024, 2023],
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
            ]),
            dbc.Row([
                dbc.Card([                    
                    dbc.CardBody([
                        html.H4("Total Viajes", className="card-title text-center text-secondary p-0 m-0"),
                        html.H1(card_total_viajes(2024, 'Todos'), id='total_viajes', className="card-text text-center text-primary m-0 p-0"),
                    ])
                ], className='kpi_card_border py-3'),
                dbc.Card([                    
                    dbc.CardBody([
                        html.H4("Viajes Promedio Día", className="card-title text-center text-secondary p-0 m-0"),
                        html.H1(card_viaje_promedio_dia(2024, 'Todos'), id='viaje_promedio_dia', className="card-text text-center text-primary m-0 p-0"),
                    ])
                ], className='kpi_card_border py-3'),
                dbc.Card([                    
                    dbc.CardBody([
                        html.H4("Vehiculos Activos", className="card-title text-center text-secondary p-0 m-0"),
                        html.H1(card_total_vehiculos(2024, 'Todos'), id='total_vehiculos', className="card-text text-center text-primary m-0 p-0"),
                    ])
                ], className='kpi_card_border py-3'),
                html.Div(                
                    html.Img(src='assets/sd_logo_transparente.png', className='img-fluid', style={'width': '60%', 'height': 'auto'}),
                    className='d-flex justify-content-center pt-5 mt-5'
                )
            ], className='px-5 mt-4'),                   
        ], width=3),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    html.Div(dcc.Graph(figure=render_kpi(1, 2024, 'Todos'), id='kpi1'), className='kpi_card_border'),
                ], width=3),
                dbc.Col([
                    html.Div(dcc.Graph(figure=render_kpi(2, 2024, 'Todos'), id='kpi2'), className='kpi_card_border'),
                ], width=3),
                dbc.Col([
                    html.Div(dcc.Graph(figure=render_kpi(3, 2024, 'Todos'), id='kpi3'), className='kpi_card_border'),
                ], width=3),
                dbc.Col([
                    html.Div(dcc.Graph(figure=render_kpi(4, 2024, 'Todos'), id='kpi4'), className='kpi_card_border'),
                ], width=3),            
            ]),
            dbc.Row([
                dbc.Col([                    
                    html.H2(dcc.Graph(figure=render_viajes_lineas(2024, 'Todos'), id='viajes_linea'), className='kpi_card_border'),
                ], width=4),
                dbc.Col([
                    html.H2(dcc.Graph(figure=render_mapa(2024, 'Todos', 'origen'), id='mapa_origen'), className='kpi_card_border'),
                ], width=4),
                dbc.Col([
                    html.H2(dcc.Graph(figure=render_mapa(2024, 'Todos', 'destino'), id='mapa_destino'), className='kpi_card_border'),
                ], width=4),
                dbc.Col([
                    html.H2(dcc.Graph('Viaje por cada 100000 habitantes por distrito'), className='kpi_card_border'),
                ], width=4),
                dbc.Col([
                    html.Div(
                        children=dash_table.DataTable(  
                            data=calculate_max_min_table(2024, 'Todos').to_dict('records'),                    
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
                            style_table={'height': 300},
                            style_cell_conditional=[                                
                                {'if': {'column_id': 'cantidad'}, 'textAlign': 'right'},
                            ],
                            style_as_list_view=True,
                            style_cell={'padding': '3px', 'fontSize': 12, 'textAlign': 'left'},                            
                        ),
                        className='table_card_border'),
                ], width=4),
            ])
        ], width=9)
    ], className='container-fluid'),
], className='container-fluid ')

#define callbacks
@callback(    
        [Output('kpi1', 'figure'),        
        Output('kpi2', 'figure'),
        Output('kpi3', 'figure'),
        Output('kpi4', 'figure'),
        Output('viajes_linea', 'figure'),        
        Output('mapa_origen', 'figure'),
        Output('mapa_destino', 'figure'),
        Output('max_min_table', 'data'),
        Output('total_viajes', 'children'),
        Output('viaje_promedio_dia', 'children'),
        Output('total_vehiculos', 'children'),
        ],
        [Input('year-dropdown', 'value'),
        Input('borough-dropdown', 'value')],
        prevent_initial_call=True,
)
def update_kpis(selected_year, selected_borough):
    #correlations = render_correlaciones(selected_year, selected_borough)
    viajes_linea = render_viajes_lineas(selected_year, selected_borough)
    mapa_origen = render_mapa(selected_year, selected_borough, 'origen')
    mapa_destino = render_mapa(selected_year, selected_borough, 'destino')
    kpi1 = render_kpi(1, selected_year, selected_borough)
    kpi2 = render_kpi(2, selected_year, selected_borough)
    kpi3 = render_kpi(3, selected_year, selected_borough)
    kpi4 = render_kpi(4, selected_year, selected_borough)
    max_min_table = calculate_max_min_table(selected_year, selected_borough)
    total_viajes = card_total_viajes(selected_year, selected_borough)
    viaje_promedio_dia = card_viaje_promedio_dia(selected_year, selected_borough)
    total_vehiculos = card_total_vehiculos(selected_year, selected_borough)
    return kpi1, kpi2, kpi3, kpi4, viajes_linea, mapa_origen, mapa_destino, max_min_table.to_dict('records'), total_viajes, viaje_promedio_dia, total_vehiculos


                # dbc.Col([
                #     #<iframe width="600" height="450" src= frameborder="0" style="border:0" allowfullscreen sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"></iframe>
                #     html.Iframe(src="https://lookerstudio.google.com/embed/reporting/dfb5c74a-6d41-4767-b00c-a84154d7b9cf/page/ZEedE"),
                #     #html.Div('<iframe width="600" height="450" src="https://lookerstudio.google.com/embed/reporting/dfb5c74a-6d41-4767-b00c-a84154d7b9cf/page/ZEedE" frameborder="0" style="border:0" allowfullscreen sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"></iframe>', className='text-primary border border-primary'),
                #     #html.H2('Gráfico 3', className='text-primary border border-primary'),
                # ], width=6), 