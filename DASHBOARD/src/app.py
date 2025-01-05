import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account

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
        delta = {"reference": delta, "valueformat": ".0f", "prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1]},
        #delta = {"reference": delta, "prefix": kpi_prefixes[kpi_id-1], "suffix": kpi_suffixes[kpi_id-1], 'relative': True},
        title = {"text": f"{kpi_titles[kpi_id-1]}<br><span style='font-size:0.7em;color:gray'>{kpi_subtitles[kpi_id-1]}</span>"},
        domain = {'y': [0.15, 0.5], 'x': [0.25, 0.75]}))

    fig.add_trace(go.Scatter(
        y = traces, mode='lines', name='trend', line=dict(color='rgba(65,193,176,0.5)', width=2)))

    fig.update_layout(xaxis= {'showticklabels': False}, margin=dict(l=20, r=20, t=20, b=20), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(29,67,85,0.1)', autosize=True, height=220)

    return fig


def calculate_kpi(kpi_id):
    credentials = service_account.Credentials.from_service_account_file('../../driven-atrium-445021-m2-6aa68b6352dd.json')
    query_job = bigquery.Client(credentials=credentials).query('SELECT year, quarter, month, total FROM `driven-atrium-445021-m2.taxi_historic_data.active_medallion_month_resume` WHERE vehicle_type = \'HYB\' or vehicle_type = \'BEV\'')
    results = query_job.result().to_dataframe()

    #cuenta los valores distintos en la columna month para el year y month del último registro del dataframe
    count = results[(results['year'] == results['year'].values[-1]) & (results['quarter'] == results['quarter'].values[-1])]['month'].nunique()
    #print(count)

    #agrupa los resultados por year y quarter
    results = results.groupby(['year', 'quarter']).sum().reset_index()
    #print(results)

    #ordena los resultados por year y quarter
    results = results.sort_values(by=['year', 'quarter'], ascending=[True, True])

    #selecciona la columna f0_ en una lista
    traces = results['total'].map(lambda x: x / count).tolist()

    #value es el valor de la columna f0_ para el ultimo registro de los resultados dividido por la cantidad de months en un trimestre
    value = results['total'].values[-1] / count  

    #delta es el valor de la columna f0_ para el segundo registro de los resultados más el 5%
    delta = (results['total'].values[-2] / 3) * 1.05
    #print(value, delta, traces)

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


