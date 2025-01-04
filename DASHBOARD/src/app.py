import dash
from dash import dcc
from dash import html

from dash.dependencies import Output
from dash.dependencies import Input

import plotly.express as px
import dash_bootstrap_components as dbc

import os
import pandas as pd

import plotly.graph_objects as go

#funcion para calcular el kpi1
def render_kpi(kpi_id):
    kpi_titles = ['Taxi EV/HYB activo', 'KPI 2', 'KPI 3', 'KPI 4']
    kpi_subtitles = ['TRIMESTRE', 'KPI 2', 'KPI 3', 'KPI 4']
    kpi_prefixes = ['', '', '', '']
    kpi_suffixes = ['', '', '', '']
    value, delta, traces = calculate_kpi(kpi_id)
    fig = go.Figure(go.Indicator(
        mode = "number+delta",
        value = value,
        number = {"prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1]},
        #delta = {"reference": delta, "valueformat": ".0f", "prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1]},
        delta = {"reference": delta, "prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1], 'relative': True},
        title = {"text": f"{kpi_titles[kpi_id-1]}<br><span style='font-size:0.7em;color:gray'>{kpi_subtitles[kpi_id-1]}</span>"},
        domain = {'y': [0.15, 0.5], 'x': [0.25, 0.75]}))

    fig.add_trace(go.Scatter(
        y = traces, mode='lines', name='trend', line=dict(color='rgba(65,193,176,0.5)', width=2)))

    fig.update_layout(xaxis= {'showticklabels': False}, margin=dict(l=20, r=20, t=20, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(29,67,85,0.1)', autosize=True, height=220)

    return fig


def calculate_kpi(kpi_id):
    value = 492
    delta = 489
    traces = [325, 324, 405, 400, 424, 404, 417, 432, 419, 394, 410, 426, 413, 419, 404, 408, 401, 377, 368, 361, 356, 359, 375, 397, 394, 418, 437, 450, 430, 442, 424, 443, 420, 418, 423, 423, 426, 440, 437, 436, 447, 460, 478, 472, 450, 456, 436, 418, 429, 412, 429, 442, 464, 447, 434, 457, 474, 480, 499, 497, 480, 502, 512, 492]
    return value, delta, traces



# Load data
df = pd.read_parquet('../../assets/Datasets/green_tripdata_2024-09.parquet')

# Load external stylesheets BOOTSTRAP and Google Fonts Montserrat
external_stylesheets = [dbc.themes.BOOTSTRAP, {
                          "href": "https://fonts.googleapis.com/css2?"
                          "family=Lato:wght@400;700&display=swap",
                          "rel": "stylesheet",
                        }]

# Create the app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.title = 'NYC Taxi Dashboard'

# Define the layout with bootstrap components, 1 left sidebar using col-3 and main content using col-9
app.layout = html.Div([
    dbc.Row([
        dbc.Col([
            html.H2('Left sidebar', className='text-secondary'),            
        ], width=3),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    html.H2(dcc.Graph(figure=render_kpi(1)), className='kpi_card_border'),
                ], width=3),
                dbc.Col([
                    html.H2('KPI 2', className='text-primary border border-primary'),
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
                    html.H2('KPI 1', className='text-primary border border-primary'),
                ], width=5),
                dbc.Col([
                    html.H2('KPI 2', className='text-primary border border-primary'),
                ], width=5),
                dbc.Col([
                    html.H2('KPI 3', className='text-primary border border-primary'),
                ], width=2),                           
            ])
        ], width=9)
    ])
], className='container-fluid my-4')

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)


