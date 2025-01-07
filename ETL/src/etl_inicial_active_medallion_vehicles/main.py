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

    table_id = 'taxi_historic_data.active_medallion_month_resume'
    rows_before_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", 'active_medallion_month_resume')
    print(f'Registros en tabla {table_id} antes de la carga: {format_count(rows_before_load)}')
    
    print(f'Insertando {format_count(df.shape[0])} registros desde el Dataset {filename}')
    project_id = 'driven-atrium-445021-m2'
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')
    rows_after_load = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", 'active_medallion_month_resume')
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
    #Eliminar columnas con información del conductor
    df.drop(columns=['agent_number', 'agent_name', 'agent_telephone_number', 'agent_address'], inplace=True)

    #Eliminar columnas de auditoría
    df.drop(columns=['last_updated_time'], inplace=True)

    #Eliminar columnas con información no requerida
    df.drop(columns=['name', 'type', 'current_status', 'medallion_type'], inplace=True)

    #Contar registros duplicados
    print(f'Registros duplicados: {df.duplicated().sum()}')

    #data types
    df['last_updated_date'] = pd.to_datetime(df['last_updated_date'])
    
    #Eliminar duplicados
    df.drop_duplicates(inplace=True)

    return df

def calculate_month_dataset(df):
    """
    Calculate the month totals of the dataset for vehicle_type column = 'HYB', 'WAV' and 'BEV'

    Args:
    df (pd.DataFrame): The DataFrame to calculate the month

    Returns:
    pd.DataFrame: The DataFrame with the month calculated
    """
    
    #Eliminar registros con vehicle_type != ['HYB','WAV','BEV']
    df = df[df['vehicle_type'].isin(['HYB','WAV','BEV'])]

    #Crear nuevo dataset con la suma de registros agrupados por 'vehicle_type'
    df_result = df.groupby('vehicle_type').size().reset_index(name='total')    

    #agregar columna 'month' al dataset df_result, con el mes del primer registro del dataset original
    df_result['month'] = df['last_updated_date'].min().month
    df_result['year'] = df['last_updated_date'].min().year
    df_result['quarter'] = df['last_updated_date'].min().quarter

    #regenerar el índice
    df_result.reset_index(drop=True, inplace=True)

    return df_result    

@functions_framework.http
def etl_inicial_active_medallion_vehicles(request):
    print('**** Iniciando proceso ETL para ACTIVE MEDALLION VEHICLES ****')
    initial_time = datetime.now()
    process_type = 'initial'
    result_json = {}

    result_json['process_type'] = process_type
    result_json['start_time'] = initial_time
    result_json['rows_before_load'] = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "active_medallion_month_resume")

    if request.args and 'filename' in request.args:
        filename = request.args['filename']
        process_type = 'incremental'        
    else:
        #Load file list from GCS bucket
        client = storage.Client()
        bucket = client.get_bucket('ncy-taxi-bucket')
        blobs = list(bucket.list_blobs(prefix='raw_datasets/active_medallion_vehicles'))    
        blobs = [blob for blob in blobs if blob.name.endswith('.csv')]

    print(f'Proceso de tipo {process_type}')

    client = bigquery.Client('driven-atrium-445021-m2')
    table_id = 'taxi_historic_data.active_medallion_vehicles'
    if process_type == 'initial':
        #Drop table if exists    
        try:
            client.delete_table('taxi_historic_data.active_medallion_month_resume', not_found_ok=True)
            print(f'Tablas {table_id} y taxi_historic_data.active_medallion_month_resume eliminada')
        except Exception:
            print(f'Tablas {table_id} y taxi_historic_data.active_medallion_month_resume no existen')

    if process_type == 'incremental':
        df = pd.read_csv(f'gs://ncy-taxi-bucket/{filename}')
        df = transform_data(df)
        result_json[filename] = load_data_to_bigquery(calculate_month_dataset(df), client, 'taxi_historic_data.active_medallion_month_resume', filename)
    else:
        for blob in blobs: 
            #Extract
            df = pd.read_csv(f'gs://ncy-taxi-bucket/{blob.name}')       
            #Transform
            df = transform_data(df)
            #Load
            result_json[blob.name] = load_data_to_bigquery(calculate_month_dataset(df), client, 'taxi_historic_data.active_medallion_month_resume', blob.name)

    print(f'Proceso terminado, total registros cargados en BigQuery: {format_count(get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "active_medallion_month_resume"))}')
    print(f'tiempo de ejecución: {datetime.now() - initial_time}')
    result_json['end_time'] = datetime.now()
    result_json['rows_after_load'] = get_table_count("driven-atrium-445021-m2", "taxi_historic_data", "active_medallion_month_resume")

    return result_json