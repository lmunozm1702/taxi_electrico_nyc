import dash
from dash import dcc
from dash import html, page_container
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account


# Load external stylesheets BOOTSTRAP and Google Fonts Montserrat
external_stylesheets = [dbc.themes.BOOTSTRAP, {
                          "href": "https://fonts.googleapis.com/css2?"
                          "family=Lato:wght@400;700&display=swap",
                          "rel": "stylesheet",
                        }]

# Create the app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, use_pages=True)

app.title = 'NYC Taxi Dashboard'

navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Inicio", href="/", active="exact")),
        dbc.NavItem(dbc.NavLink("Diccionario", href="/dictionary", active="exact")),
    ],
    brand="Dashboard NYC Taxi",
    brand_href="/",
    color="#41c1b0",
    dark=True,
)

# Define the layout with bootstrap components, 1 left sidebar using col-3 and main content using col-9
app.layout = html.Div([
    dbc.Row([
    navbar    
    ], className='mb-3 mt-0 py-0'),
    dbc.Row([
        page_container,
    ]),    
], className='container-fluid ')

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)