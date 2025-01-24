import functions_framework
import pandas as pd
import pandas_gbq
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
import uuid

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

    rows_before_load = get_table_count("driven-atrium-445021-m2", "project_data", "trips")
    print(f'Registros en tabla green-taxi antes de la carga: {format_count(rows_before_load)}')
    print(f'Insertando {format_count(df.shape[0])} registros desde el Dataset {filename}')
    project_id = 'driven-atrium-445021-m2'
    table_id = 'project_data.trips'
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')
    rows_after_load = get_table_count("driven-atrium-445021-m2", "project_data", "trips")
    print(f'Registros en tabla TRIPS después de la carga: {format_count(rows_after_load)}')
    print(f'Diferencia cuenta de registros en tabla y registros en dataset: {format_count(rows_after_load - rows_before_load - df.shape[0])}')        
    print('-----------------------------------')

    return {
        'rows_before_load': rows_before_load,
        'rows_after_load': rows_after_load,
        'rows_loaded': df.shape[0],
        'rows_difference': rows_after_load - rows_before_load - df.shape[0]
    }

def get_duplicated_rows(project_id, dataset_id, table_id, pickup_month, pickup_year):
    """
    Get the number of duplicated rows in a DataFrame

    Args:
    df (pd.DataFrame): The DataFrame to check

    Returns:
    int: The number of duplicated rows
    """

    client = bigquery.Client(project_id)
    query = f"""
        SELECT COUNT(*)
        FROM `{dataset_id}.{table_id}`
        WHERE pickup_month = {pickup_month} AND pickup_year = {pickup_year}
    """
    count = 0

    try:
        query_job = client.query(query)
        for row in query_job:
            count = row[0]
            print(f'Registros duplicados: {count}') 
            break
        return count
    except Exception:
        return 0
    

def transform_data(df, filename):
    """
    Transform the data in a DataFrame

    Args:
    df (pd.DataFrame): The DataFrame to transform

    Returns:
    pd.DataFrame: The transformed DataFrame
    """

    #cambiar nombre de columnas
    df.columns = ['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'store_and_forward_flag', 'rate_code_id', 'pickup_location_id', 'dropoff_location_id', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge']

    df.drop(columns=['vendor_id', 'store_and_forward_flag', 'rate_code_id', 'passenger_count', 'trip_distance', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge'], inplace=True)

    #eliminar registros con columna 'pickup_location_id' == na
    df = df.dropna(subset=['pickup_location_id'])
    df = df.dropna(subset=['dropoff_location_id'])

    #Eliminar valores que no corresponden al mes y año del dataset
    print(filename)
    dataset_month = filename.split('/')[-1].split('.')[0].split('_')[-1].split('-')[1]
    dataset_year = filename.split('/')[-1].split('.')[0].split('_')[-1].split('-')[0]
    df = df[(df['pickup_datetime'].dt.month == int(dataset_month)) & (df['pickup_datetime'].dt.year == int(dataset_year))]

    #controlar duplicados por mes y año en bigquery
    if get_duplicated_rows("driven-atrium-445021-m2", "project_data", "trips", dataset_month, dataset_year) > 0:
        return True

    #agregar uuid integer en columna 'trip_id'
    df['trip_id'] = uuid.uuid4()

    #agregar columna taxi_type con 'high_volume'
    df['taxi_type'] = 'green'

    #agregar columna 'motor_type' con 'n/a'
    df['motor_type'] = 'n/a'

    #Eliminar duplicados
    df = df.drop_duplicates()

    #Agregar columna 'year' con el año de la columna 'pickup_datetime'
    df['pickup_year'] = df['pickup_datetime'].dt.year

    #Agregar columna 'month' con el mes de la columna 'pickup_datetime'
    df['pickup_month'] = df['pickup_datetime'].dt.month

    #Agregar columna 'day' con el día de la columna 'pickup_datetime'
    df['pickup_day_of_month'] = df['pickup_datetime'].dt.day

    #Agregar columna 'weekday' con el día de la semana de la columna 'pickup_datetime'
    df['pickup_day_of_week'] = df['pickup_datetime'].dt.weekday

    #Agregar columna 'quarter' con el trimestre de la columna 'pickup_datetime'
    df['pickup_quarter'] = df['pickup_datetime'].dt.quarter

    #agregar columna 'hour_of_day' 
    df['pickup_hour_of_day'] = df['pickup_datetime'].dt.hour

    #agregar columna 'trip_duration' con la direrencia entre 'dropoff_datetime' y 'pickup_datetime' en segundos
    df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds()

    #pasar columnas tipo object a string
    df['trip_id'] = df['pickup_location_id'].astype(str)
    df['taxi_type'] = df['taxi_type'].astype(str)
    df['motor_type'] = df['motor_type'].astype(str)

    #pasar columnas tipo object a datetime
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])

    #regenerar índice
    df.reset_index(drop=True, inplace=True)
    
    return df

@functions_framework.http
def etl_inicial_green_taxi(request):
    print('**** Iniciando proceso ETL para GREEN TAXI ****')
    initial_time = datetime.now()
    process_type = 'initial'
    result_json = {}

    result_json['process_type'] = process_type
    result_json['start_time'] = initial_time
    result_json['rows_before_load'] = get_table_count("driven-atrium-445021-m2", "project_data", "trips")

    if request.args and 'filename' in request.args:
        filename = request.args['filename']
        process_type = 'incremental'        
    else:
        #Load file list from GCS bucket
        client = storage.Client()
        bucket = client.get_bucket('ncy-taxi-bucket')
        blobs = list(bucket.list_blobs(prefix='raw_datasets/trip_record_data/2023/green_tripdata_', max_results=12))    

    print(f'Proceso de tipo {process_type}')

    client = bigquery.Client('driven-atrium-445021-m2')
    table_id = 'project_data.trips'
    
    if process_type == 'incremental':
        #filename = raw_datasets/trip_record_data/2024/green_tripdata_2024-09.parquet
        df = pd.read_parquet(f'gs://ncy-taxi-bucket/{filename}')
        df = transform_data(df, filename)
        if df == True:
            result_json['Duplicated'] = "True"
            return result_json
        else:
            result_json['Duplicated'] = "False"

        result_json[filename] = load_data_to_bigquery(df, client, table_id, filename)
    else:
        for blob in blobs: 
            #Extract
            df = pd.read_parquet(f'gs://ncy-taxi-bucket/{blob.name}')       
            #Transform
            df = transform_data(df, blob.name)        
            #Load
            result_json[blob.name] = load_data_to_bigquery(df, client, table_id, blob.name)        

    print(f'Proceso terminado, total registros cargados en BigQuery: {format_count(get_table_count("driven-atrium-445021-m2", "project_data", "trips"))}')
    print(f'tiempo de ejecución: {datetime.now() - initial_time}')

    result_json['end_time'] = datetime.now()
    result_json['rows_after_load'] = get_table_count("driven-atrium-445021-m2", "project_data", "trips")

    return result_json