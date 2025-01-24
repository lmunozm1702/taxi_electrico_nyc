from dash import html, dash_table
from dash import register_page, callback
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

register_page(__name__, name='Diccionario', path='/fares')

layout = html.Div([
    html.Iframe(src="https://lookerstudio.google.com/embed/reporting/dfb5c74a-6d41-4767-b00c-a84154d7b9cf/page/ZEedE",
                width="80%",
                height="900px",
                style={
                    'border': 'none'
                },
                sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"
                ),
    # frameborder="0" style="border:0" allowfullscreen sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"></iframe>
], className='d-flex justify-content-center')