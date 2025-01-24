import dash
from dash import html, page_container, DiskcacheManager
import dash_bootstrap_components as dbc
import uuid
import diskcache

launch_uid = uuid.uuid4()

# Load external stylesheets BOOTSTRAP and Google Fonts Montserrat
external_stylesheets = [dbc.themes.BOOTSTRAP, {
                          "href": "https://fonts.googleapis.com/css2?"
                          "family=Lato:wght@400;700&display=swap",
                          "rel": "stylesheet",
                        }]

cache = diskcache.Cache("./cache")
background_callback_manager = DiskcacheManager(
    cache, cache_by=[lambda: launch_uid], expire=60
)

# Create the app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, use_pages=True, background_callback_manager=background_callback_manager)

app.title = 'NYC Taxi Dashboard'

navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Viajes", href="/", active="exact")),
        dbc.NavItem(dbc.NavLink("Tarifas", href="/fares", active="exact")),
        #dbc.NavItem(dbc.NavLink("Diccionario", href="/dictionary", active="exact")),
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