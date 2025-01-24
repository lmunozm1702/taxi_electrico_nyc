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

    table_id = 'project_data.coordinates'
    rows_before_load = get_table_count("driven-atrium-445021-m2", "project_data", 'coordinates')
    print(f'Registros en tabla {table_id} antes de la carga: {format_count(rows_before_load)}')
    
    print(f'Insertando {format_count(df.shape[0])} registros desde el Dataset {filename}')
    project_id = 'driven-atrium-445021-m2'
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')
    rows_after_load = get_table_count("driven-atrium-445021-m2", "project_data", 'coordinates')
    print(f'Registros en tabla {table_id} después de la carga: {format_count(rows_after_load)}')
    print(f'Diferencia cuenta de registros en tabla y registros en dataset: {format_count(rows_after_load - rows_before_load - df.shape[0])}')        
    print('-----------------------------------')

    return {
        'rows_before_load': rows_before_load,
        'rows_after_load': rows_after_load,
        'rows_loaded': df.shape[0],
        'rows_difference': rows_after_load - rows_before_load - df.shape[0]
    }

def transform_data(df):
    """
    Transform the data in the DataFrame

    Args:
    df (pd.DataFrame): The DataFrame to transform

    Returns:
    pd.DataFrame: The transformed DataFrame
    """
    #Eliminar columnas con información que no será utilizada
    df.drop(columns=['LocationID', 'Shape_Leng', 'Shape_Area'], inplace=True) 

    #cambiar nombre de columnas
    df.columns = ['location_id', 'geom', 'zone', 'borough']

    #datatypes
    df['location_id'] = df['location_id'].astype(int)
    df['zone'] = df['zone'].astype(str)
    df['borough'] = df['borough'].astype(str)
    df['geom'] = df['geom'].astype(str)
    
    #Eliminar duplicados
    df.drop_duplicates(inplace=True)

    return df

@functions_framework.http
def etl_inicial_coordinates(request):
    print('**** Iniciando proceso ETL para COORDENADAS ****')
    initial_time = datetime.now()
    process_type = 'initial'
    result_json = {}

    result_json['process_type'] = process_type
    result_json['start_time'] = initial_time
    result_json['rows_before_load'] = get_table_count("driven-atrium-445021-m2", "project_data", "coordinates")

    if request.args and 'filename' in request.args:
        filename = request.args['filename']
        process_type = 'incremental'        
    else:
        #Load file list from GCS bucket
        client = storage.Client()
        bucket = client.get_bucket('ncy-taxi-bucket')
        blobs = list(bucket.list_blobs(prefix='raw_datasets/taxi_zones'))    
        blobs = [blob for blob in blobs if blob.name.endswith('.csv')]

    print(f'Proceso de tipo {process_type}')

    client = bigquery.Client('driven-atrium-445021-m2')    
    
    if process_type == 'incremental':
        df = pd.read_csv(f'gs://ncy-taxi-bucket/{filename}')
        df = transform_data(df)
        result_json[filename] = load_data_to_bigquery(df, client, 'project_data.coordinates', filename)
    else:
        for blob in blobs: 
            #Extract
            df = pd.read_csv(f'gs://ncy-taxi-bucket/{blob.name}')       
            #Transform
            df = transform_data(df)
            #Load
            result_json[blob.name] = load_data_to_bigquery(df, client, 'project_data.coordinates', blob.name)

    print(f'Proceso terminado, total registros cargados en BigQuery: {format_count(get_table_count("driven-atrium-445021-m2", "project_data", "coordinates"))}')
    print(f'tiempo de ejecución: {datetime.now() - initial_time}')
    result_json['end_time'] = datetime.now()
    result_json['rows_after_load'] = get_table_count("driven-atrium-445021-m2", "project_data", "coordinates")

    return result_json