import functions_framework
import pandas as pd
import pandas_gbq
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

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

    print('****** Query ******')
    print(query)
    count = 0

    try:
        query_job = client.query(query)
        for row in query_job:
            count = row[0]
            break
        print('****** Query JOb ******')
        print(count)
        return count
    except Exception as e:
        print('****** Query JOb ******')
        print(f'excepción: {e}')
        return 0

@functions_framework.http
def etl_inicial_yellow_taxi(request):
    print('Iniciando proceso ETL para yellow taxi')
    initial_time = datetime.now()
    #Load file list from GCS bucket
    client = storage.Client()
    bucket = client.get_bucket('ncy-taxi-bucket')
    blobs = list(bucket.list_blobs(prefix='raw_datasets/trip_record_data/2022/yellow_tripdata_', max_results=3))    
    print(blobs)

    #Drop table if exists    
    client = bigquery.Client()
    table_id = 'taxi_historic_data.yellow_taxi'
    try:
        client.delete_table(table_id, not_found_ok=True)
        print(f'Tabla {table_id} eliminada')
    except Exception:
        print(f'Tabla {table_id} no existe')

    for blob in blobs: 
        #Extract
        df = pd.read_parquet(f'gs://ncy-taxi-bucket/{blob.name}')
        print(f'Leídos {df.shape[0]} registros desde el archivo {blob.name}')
        
        #Transform
        
        #Load
        rows_before_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "yellow_taxi")
        print(f'Registros en tabla yellow-taxi antes de la carga: {rows_before_load}')
        
        print(f'Insertando {df.shape[0]} registros en la tabla {blob.name}')
        project_id = 'driven-atrium-445021-m2'
        table_id = 'taxi_historic_data.yellow_taxi'
        pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')

        rows_after_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "yellow_taxi")
        print(f'Registros en tabla yellow-taxi después de la carga: {rows_after_load}')
        print(f'Diferencia cuenta de registros en tabla y registros en dataset: {rows_after_load - rows_before_load - df.shape[0]}')        
        print('-----------------------------------\n')
        

    print(f'Proceso terminado, total registros cargados en BigQuery: {get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "yellow_taxi")}')
    print(f'tiempo de ejecución: {datetime.now() - initial_time}')

    return 'Proceso terminado, revisar logs para detalles'