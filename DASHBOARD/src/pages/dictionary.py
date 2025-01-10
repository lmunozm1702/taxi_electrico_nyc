import dash
from dash import dcc
from dash import html
from dash import register_page, callback
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account


register_page(__name__, name='Diccionario', path='/dictionary')

def json_table(table_name):
    print(table_name)

    #leer dataset desde archivo json en la variable df
    df = pd.read_json('../../assets/Data_dictionary/data_dictionary.json')

    df_t = df.T


    table_header = [
    html.Thead(html.Tr([html.Th("Tabla"), html.Th("Nombre"), html.Th("Tipo"), html.Th("Longitud"), html.Th("Descripción")]))]

    rows = []
    for index, row in df_t.iterrows():
        #iterar sobre row['campos']
        for column in row['campos']:
            items = []
            if (index == table_name) | (table_name == 'Todos'):
                items.append(html.Td([index]))
                for key, value in column.items():                
                    if key != 'valores':
                        items.append(html.Td([value]))
                rows.append(html.Tr(items))

    table = dbc.Table(
        table_header + [html.Tbody(rows)],
        bordered=True,
        dark=False,
        hover=True,
        responsive=True,
        striped=True,
    )  
    return table   

dropdown_options = {
    'tables': ['coordinates', 'yellow_taxi', 'green_taxi'],
}

layout = html.Div([
    dbc.Row([
        dbc.Col([
            dbc.Row([
                dbc.Label("Tabla", html_for="table-dropdown", className='text-primary p-0 m-0 ms-1 fw-bold'),
                dbc.Select(
                    id='table-dropdown',
                    options=[{'label': table, 'value': table} for table in dropdown_options['tables']],
                    value='coordinates',
                ),  
                dbc.FormText("Selecciona una tabla", className='text-muted p-0 m-0 ms-1'),
            ], className='mb-3'),                 
        ], width=3),
        dbc.Col([html.Div(
            json_table('coordinates'), id='table1')], width=9)                         
    ], className='container-fluid')
], className='container-fluid ')

@callback(
    [
        Output('table1', 'table'),        
        Input('table-dropdown', 'value')
    ] 
)
def update_table(selected_table):
    return json_table(selected_table)