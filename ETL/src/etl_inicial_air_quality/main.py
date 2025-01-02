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

    rows_before_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "air_quality")
    print(f'Registros en tabla air-quality antes de la carga: {format_count(rows_before_load)}')
    print(f'Insertando {format_count(df.shape[0])} registros desde el Dataset {filename}')
    project_id = 'driven-atrium-445021-m2'
    table_id = 'taxi_historic_data.air_quality'
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')
    rows_after_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "air_quality")
    print(f'Registros en tabla air-quality después de la carga: {format_count(rows_after_load)}')
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
    
    #Eliminar columnas no necesarias

    #Cambiar nombre de columnas
    df.columns = ['unique_id', 'indicator_id', 'name', 'measure', 'measure_info', 'geo_type_name', 'geo_join_id', 'geo_place_name', 'time_period', 'start_date', 'data_value', 'message']

    #Eliminar columna 'message'
    df.drop(columns=['message'], inplace=True)
    #Eliminar duplicados
    df.drop_duplicates(inplace=True)
    #Eliminar dupicados analizando columna 'unique_id'
    df.drop_duplicates(subset=['unique_id'], inplace=True)
    #Eliminar outliers en columna 'data_value'
    stats = df['data_value'].describe(percentiles=[.25, .5, .75])
    q1 = stats['25%']
    #median = stats['50%']
    q3 = stats['75%']
    iqr = q3 - q1
    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr
    outliers = df[(df['data_value'] < lower_fence) | (df['data_value'] > upper_fence)].shape[0]
    #within_box = df[(df['data_value'] >= q1) & (df['data_value'] <= q3)].shape[0]
    #within_whiskers = df[(df['data_value'] >= lower_fence) & (df['data_value'] <= upper_fence)].shape[0]
    #eliminar los registros que no estén dentro de los whiskers
    df = df[(df['data_value'] >= lower_fence) & (df['data_value'] <= upper_fence)]
    print(f'Outliers eliminados en columna data_value: {outliers}')   
    #Datatype conversion
    df['name'] = df['name'].astype('string')
    df['measure'] = df['measure'].astype('string')
    df['measure_info'] = df['measure_info'].astype('string')
    df['geo_type_name'] = df['geo_type_name'].astype('string')
    df['geo_place_name'] = df['geo_place_name'].astype('string')
    df['start_date'] = pd.to_datetime(df['start_date'])

    #regenerar índice
    df.reset_index(drop=True, inplace=True)  
    return df

@functions_framework.http
def etl_inicial_air_quality(request):
    print('**** Iniciando proceso ETL para AIR QUALITY ****')
    initial_time = datetime.now()
    process_type = 'initial'
    result_json = {}

    result_json['process_type'] = process_type
    result_json['start_time'] = initial_time
    result_json['rows_before_load'] = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "air_quality")

    if request.args and 'filename' in request.args:
        filename = request.args['filename']
        process_type = 'incremental'        
    else:
        #Load file list from GCS bucket
        client = storage.Client()
        bucket = client.get_bucket('ncy-taxi-bucket')
        blobs = list(bucket.list_blobs(prefix='raw_datasets/air_quality/', max_results=3))    

    print(f'Proceso de tipo {process_type}')

    client = bigquery.Client('driven-atrium-445021-m2')
    table_id = 'taxi_historic_data.air_quality'
    
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

    print(f'Proceso terminado, total registros cargados en BigQuery: {format_count(get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "air_quality"))}')
    print(f'tiempo de ejecución: {datetime.now() - initial_time}')

    result_json['end_time'] = datetime.now()
    result_json['rows_after_load'] = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "air_quality")

    return result_json