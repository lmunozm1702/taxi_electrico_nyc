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

    rows_before_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "green_taxi")
    print(f'Registros en tabla green-taxi antes de la carga: {format_count(rows_before_load)}')
    print(f'Insertando {format_count(df.shape[0])} registros desde el Dataset {filename}')
    project_id = 'driven-atrium-445021-m2'
    table_id = 'taxi_historic_data.green_taxi'
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')
    rows_after_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "green_taxi")
    print(f'Registros en tabla green-taxi después de la carga: {format_count(rows_after_load)}')
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

    #cambiar nombre de columnas
    df.columns = ['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'store_and_forward_flag', 'rate_code_id', 'start_location_id', 'end_location_id', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge']

    #Eliminar valores que no corresponden al mes y año del dataset
    print(filename)
    dataset_month = filename.split('/')[-1].split('.')[0].split('_')[-1].split('-')[1]
    dataset_year = filename.split('/')[-1].split('.')[0].split('_')[-1].split('-')[0]
    df = df[(df['pickup_datetime'].dt.month == int(dataset_month)) & (df['pickup_datetime'].dt.year == int(dataset_year))]

    #Imputar 'N' en valores nulos de la columna 'store_and_forward_flag'
    df['store_and_forward_flag'] = df['store_and_forward_flag'].fillna('N')

    #Imputar 0 en valores nulos de la columna 'rate_code_id'
    df['rate_code_id'] = df['rate_code_id'].fillna(1)

    #Imputar 1 = estándar, en valores nulos de la columna 'passenger_count'
    df['passenger_count'] = df['passenger_count'].fillna(0)

    #Imputar 0 en valores nulos de la columna 'ehail_fee'
    df['ehail_fee'] = df['ehail_fee'].fillna(0)

    #Imputar promedios en valores nulos de la columna 'trip_distance' agrupando por 'start_location_id' y 'end_location_id'
    df['trip_distance'] = df.groupby(['start_location_id', 'end_location_id'])['trip_distance'].transform(lambda x: x.fillna(x.mean()))

    #Imputar 0 en valores nulos de la columna 'congestion_surcharge'
    df['congestion_surcharge'] = df['congestion_surcharge'].fillna(0)

    #Imputar 1 = Street, en valores nulos de la columna 'trip_type'
    df['trip_type'] = df['trip_type'].fillna(1)

    #Imputar 2 = Efectivo, en valores nulos de la columna 'payment_type'
    df['payment_type'] = df['payment_type'].fillna(2)

    #Eliminar registros con valor 99 en columna 'rate_code_id'
    df = df[df['rate_code_id'] != 99]

    #Agregar columna 'year' con el año de la columna 'pickup_datetime'
    df['year'] = df['pickup_datetime'].dt.year

    #Agregar columna 'month' con el mes de la columna 'pickup_datetime'
    df['month'] = df['pickup_datetime'].dt.month

    #Agregar columna 'day' con el día de la columna 'pickup_datetime'
    df['day'] = df['pickup_datetime'].dt.day

    #Agregar columna 'weekday' con el día de la semana de la columna 'pickup_datetime'
    df['weekday'] = df['pickup_datetime'].dt.weekday

    #Agregar columna 'quarter' con el trimestre de la columna 'pickup_datetime'
    df['quarter'] = df['pickup_datetime'].dt.quarter

    #Eliminar duplicados
    df = df.drop_duplicates()

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
    result_json['rows_before_load'] = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "green_taxi")

    if request.args and 'filename' in request.args:
        filename = request.args['filename']
        process_type = 'incremental'        
    else:
        #Load file list from GCS bucket
        client = storage.Client()
        bucket = client.get_bucket('ncy-taxi-bucket')
        blobs = list(bucket.list_blobs(prefix='raw_datasets/trip_record_data/2022/green_tripdata_', max_results=3))    

    print(f'Proceso de tipo {process_type}')

    client = bigquery.Client('driven-atrium-445021-m2')
    table_id = 'taxi_historic_data.green_taxi'
    
    if process_type == 'initial':
        #Drop table if exists 
        try:
            client.delete_table(table_id, not_found_ok=True)
            print(f'Tabla {table_id} eliminada')
        except Exception:
            print(f'Tabla {table_id} no existe')
    
    if process_type == 'incremental':
        df = pd.read_parquet(f'gs://ncy-taxi-bucket/{filename}')
        df = transform_data(df, filename)
        result_json[filename] = load_data_to_bigquery(df, client, table_id, filename)
    else:
        for blob in blobs: 
            #Extract
            df = pd.read_parquet(f'gs://ncy-taxi-bucket/{blob.name}')       
            #Transform
            df = transform_data(df, blob.name)        
            #Load
            result_json[blob.name] = load_data_to_bigquery(df, client, table_id, blob.name)        

    print(f'Proceso terminado, total registros cargados en BigQuery: {format_count(get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "green_taxi"))}')
    print(f'tiempo de ejecución: {datetime.now() - initial_time}')

    result_json['end_time'] = datetime.now()
    result_json['rows_after_load'] = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "green_taxi")

    return result_json