import functions_framework
import pandas as pd
import pandas_gbq
import re
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

    rows_before_load = get_table_count("driven-atrium-445021-m2", "project_data", "emissions")
    print(f'Registros en tabla green-house-emissions antes de la carga: {format_count(rows_before_load)}')
    print(f'Insertando {format_count(df.shape[0])} registros desde el Dataset {filename}')
    project_id = 'driven-atrium-445021-m2'
    table_id = 'project_data.emissions'
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')
    rows_after_load = get_table_count("driven-atrium-445021-m2", "project_data", "emissions")
    print(f'Registros en tabla green-house-emissions después de la carga: {format_count(rows_after_load)}')
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

    melted_df = pd.melt(df, id_vars=['inventory_type', 'sectors_sector', 'category_full', 'category_label', 'source_full', 'source_label', 'source_units'], var_name='concept', value_name='value')

    #Tipos de datos
    melted_df['inventory_type'] = melted_df['inventory_type'].astype('string')
    melted_df['sectors_sector'] = melted_df['sectors_sector'].astype('string')
    melted_df['category_full'] = melted_df['category_full'].astype('string')
    melted_df['category_label'] = melted_df['category_label'].astype('string')
    melted_df['source_full'] = melted_df['source_full'].astype('string')
    melted_df['source_label'] = melted_df['source_label'].astype('string')
    melted_df['source_units'] = melted_df['source_units'].astype('string')
    melted_df['concept'] = melted_df['concept'].astype('string')
    melted_df['value'] = melted_df['value'].astype('float')

    #cambiar nombre de columnas
    melted_df.columns = ['inventory_type', 'sector', 'category_full', 'category_label', 'source_full', 'source_label', 'source_units', 'concept', 'value']

    #Eliminar valores con 'Total' en columna 'inventory_type'
    melted_df = melted_df[melted_df['inventory_type'] != 'Total']

    #Agregar columna year extraida de 'concept'
    melted_df['year'] = melted_df['concept'].str.extract(r'(\d{4})')

    #Elimina texto '_{year}' en columna 'concept'    
    melted_df['concept'] = melted_df['concept'].map(lambda x: re.sub(r'cy_\d{4}_', '', x))

    #eliminar valores que contengan 'Change' o 'change' en columna 'concept'
    melted_df = melted_df[~melted_df['concept'].str.contains('Change')]
    melted_df = melted_df[~melted_df['concept'].str.contains('change')]

    #Eliminar duplicados
    melted_df = melted_df.drop_duplicates()
    
    return melted_df

@functions_framework.http
def etl_inicial_green_house_emissions(request):
    print('**** Iniciando proceso ETL para GREEN HOUSE EMISSIONS ****')
    initial_time = datetime.now()
    process_type = 'initial'
    result_json = {}

    result_json['process_type'] = process_type
    result_json['start_time'] = initial_time
    result_json['rows_before_load'] = get_table_count("driven-atrium-445021-m2", "project_data", "emissions")

    if request.args and 'filename' in request.args:
        filename = request.args['filename']
        process_type = 'incremental'        
    else:
        #Load file list from GCS bucket
        client = storage.Client()
        bucket = client.get_bucket('ncy-taxi-bucket')
        blobs = list(bucket.list_blobs(prefix='raw_datasets/green_house_emissions/2', max_results=3))    

    print(f'Proceso de tipo {process_type}')

    client = bigquery.Client('driven-atrium-445021-m2')
    table_id = 'tproject_data.emissions'
    
    if process_type == 'incremental':
        df = pd.read_csv(f'gs://ncy-taxi-bucket/{filename}')
        df = transform_data(df, filename)
        result_json[filename] = load_data_to_bigquery(df, client, table_id, filename)
    else:
        for blob in blobs: 
            #Extract
            df = pd.read_csv(f'gs://ncy-taxi-bucket/{blob.name}')       
            #Transform
            df = transform_data(df, blob.name)        
            #Load
            result_json[blob.name] = load_data_to_bigquery(df, client, table_id, blob.name)        

    print(f'Proceso terminado, total registros cargados en BigQuery: {format_count(get_table_count("driven-atrium-445021-m2", "project_data", "emissions"))}')
    print(f'tiempo de ejecución: {datetime.now() - initial_time}')

    result_json['end_time'] = datetime.now()
    result_json['rows_after_load'] = get_table_count("driven-atrium-445021-m2", "project_data", "emissions")

    return result_json