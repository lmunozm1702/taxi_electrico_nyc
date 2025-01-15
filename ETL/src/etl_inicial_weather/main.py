import functions_framework
import pandas as pd
import pandas_gbq
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

def format_count(count):
    """
    Format the number of rows in a BigQuery table

    Args:
    count (int): The number of rows in the table

    Returns:
    str: The formatted number of rows
    """
    return f'{count:,}'

def get_table_count(project_id, dataset_id, table_id):
    """
    Get the number of rows in a BigQuery table

    Args:
    project_id (str): The Google Cloud project ID
    dataset_id (str): The BigQuery dataset ID
    table_id (str): The Dataset table ID

    Returns:
    int: The number of rows in the table
    """
    
    client = bigquery.Client(project_id)
    query = f"""
        SELECT COUNT(*)
        FROM `{dataset_id}.{table_id}`
    """
    count = 0

    try:
        query_job = client.query(query)
        for row in query_job:
            count = row[0]
            break
        return count
    except Exception:
        return 0

def load_data_to_bigquery(df, client, table_id, filename):
    """
    Load a DataFrame to a BigQuery table

    Args:
    df (pd.DataFrame): The DataFrame to load
    client (bigquery.Client): The BigQuery client
    table_id (str): The table ID
    filename (str): The filename containing the data
    """

    rows_before_load = get_table_count("driven-atrium-445021-m2", "project_data", "weather")
    print(f'Registros en tabla WEATHER antes de la carga: {format_count(rows_before_load)}')
    print(f'Insertando {format_count(df.shape[0])} registros desde el Dataset {filename}')
    project_id = 'driven-atrium-445021-m2'
    table_id = 'project_data.weather'
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')
    rows_after_load = get_table_count("driven-atrium-445021-m2", "project_data", "weather")
    print(f'Registros en tabla weather después de la carga: {format_count(rows_after_load)}')
    print(f'Diferencia cuenta de registros en tabla y registros en dataset: {format_count(rows_after_load - rows_before_load - df.shape[0])}')        
    print('-----------------------------------')

    return {
        'rows_before_load': rows_before_load,
        'rows_after_load': rows_after_load,
        'rows_loaded': df.shape[0],
        'rows_difference': rows_after_load - rows_before_load - df.shape[0]
    }

def transform_data(df, filename):
    """
    Transform the data in a DataFrame

    Args:
    df (pd.DataFrame): The DataFrame to transform

    Returns:
    pd.DataFrame: The transformed DataFrame
    """

    #remove na
    df.dropna(inplace=True)

    #cambiar nombre de columnas
    df.columns = ['location_id', 'date', 'temperature', 'relative_humidity', 'dew_point', 'apparent_temperature', 'rain', 'snowfall', 'weather_code', 'pressure_msl', 'cloud_cover', 'wind_speed', 'wind_direction', 'wind_gusts']
    df.drop(columns=['rain', 'snowfall'], inplace=True)

    df['date'] = pd.to_datetime(df['date'])
    df['location_id'] = df['location_id'] + 1

    #Agregar columna 'year' con el año de la columna 'pickup_datetime'
    df['year'] = df['date'].dt.year
    #Agregar columna 'month' con el mes de la columna 'date'
    df['month'] = df['date'].dt.month
    #Agregar columna 'day' con el día de la columna 'date'
    df['day_of_month'] = df['date'].dt.day
    #agregar columna 'hour_of_day' 
    df['hour_of_day'] = df['date'].dt.hour
    #agregar columna 'day_of_week' 
    df['day_of_week'] = df['date'].dt.dayofweek

    df.drop(columns=['date'], inplace=True)

    #regenerar índice
    df.reset_index(drop=True, inplace=True)
    
    return df

@functions_framework.http
def etl_inicial_weather(request):
    print('**** Iniciando proceso ETL para WEATHER ****')
    initial_time = datetime.now()
    process_type = 'initial'
    result_json = {}

    result_json['process_type'] = process_type
    result_json['start_time'] = initial_time
    result_json['rows_before_load'] = get_table_count("driven-atrium-445021-m2", "project_data", "weather")

    if request.args and 'filename' in request.args:
        filename = request.args['filename']
        process_type = 'incremental'        
    else:
        #Load file list from GCS bucket
        client = storage.Client()
        bucket = client.get_bucket('ncy-taxi-bucket')
        blobs = list(bucket.list_blobs(prefix='raw_datasets/weather/raw', max_results=3))    

    print(f'Proceso de tipo {process_type}')

    client = bigquery.Client('driven-atrium-445021-m2')
    table_id = 'project_data.weather'
    
    if process_type == 'incremental':
        df = pd.read_csv(f'gs://ncy-taxi-bucket/{filename}', skiprows=7)
        df = transform_data(df, filename)
        result_json[filename] = load_data_to_bigquery(df, client, table_id, filename)
    else:
        for blob in blobs: 
            #Extract
            df = pd.read_csv(f'gs://ncy-taxi-bucket/{blob.name}', skiprows=7)       
            #Transform
            df = transform_data(df, blob.name)        
            #Load
            result_json[blob.name] = load_data_to_bigquery(df, client, table_id, blob.name)        

    print(f'Proceso terminado, total registros cargados en BigQuery: {format_count(get_table_count("driven-atrium-445021-m2", "project_data", "weather"))}')
    print(f'tiempo de ejecución: {datetime.now() - initial_time}')

    result_json['end_time'] = datetime.now()
    result_json['rows_after_load'] = get_table_count("driven-atrium-445021-m2", "project_data", "weather")

    return result_json