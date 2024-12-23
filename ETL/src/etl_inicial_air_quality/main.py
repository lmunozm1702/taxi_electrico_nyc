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

@functions_framework.http
def etl_inicial_air_quality(request):
    print('**** Iniciando proceso ETL para AIR QUALITY ****')
    initial_time = datetime.now()
    #Load file list from GCS bucket
    client = storage.Client()
    bucket = client.get_bucket('ncy-taxi-bucket')
    blobs = list(bucket.list_blobs(prefix='raw_datasets/air_quality/', max_results=3))    

    #Drop table if exists    
    client = bigquery.Client('driven-atrium-445021-m2')
    table_id = 'taxi_historic_data.air_quality'
    try:
        client.delete_table(table_id, not_found_ok=True)
        print(f'Tabla {table_id} eliminada')
    except Exception:
        print(f'Tabla {table_id} no existe')

    for blob in blobs: 
        #Extract
        df = pd.read_parquet(f'gs://ncy-taxi-bucket/{blob.name}')       
        
        #Transform
        
        #Load
        rows_before_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "air_quality")
        print(f'Registros en tabla air-quality antes de la carga: {format_count(rows_before_load)}')
        
        print(f'Insertando {format_count(df.shape[0])} registros desde el Dataset {blob.name}')
        project_id = 'driven-atrium-445021-m2'
        table_id = 'taxi_historic_data.air_quality'
        pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')

        rows_after_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "air_quality")
        print(f'Registros en tabla air-quality después de la carga: {format_count(rows_after_load)}')
        print(f'Diferencia cuenta de registros en tabla y registros en dataset: {format_count(rows_after_load - rows_before_load - df.shape[0])}')        
        print('-----------------------------------')
        

    print(f'Proceso terminado, total registros cargados en BigQuery: {format_count(get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "air_quality"))}')
    print(f'tiempo de ejecución: {datetime.now() - initial_time}')

    return 'Proceso terminado, revisar logs para detalles'