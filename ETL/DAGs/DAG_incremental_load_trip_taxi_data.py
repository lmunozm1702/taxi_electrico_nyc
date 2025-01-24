import datetime
import google.auth.transport.requests
import json
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
from google.auth.transport.requests import AuthorizedSession




#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'DAG_incremental_load_trip_taxi_data'
project           = 'incremental-load-taxi-electrico-nyc'
owner             = 'juankaruna'
email             = ['juankarua@gmail.com']

raw_datasets_path = "raw_datasets/trip_record_data/"
current_year = '2024' #str(datetime.datetime.now().year)
current_year_month = '2024-10' #"-".join([current_year,str(datetime.datetime.now().month-2)])

kwargs_extract = {
    'function': "https://us-central1-driven-atrium-445021-m2.cloudfunctions.net/extract_incremental_load_trip_record_data",
    'params': ""
}
kwargs_transform_load_yellow_taxi = {
    'function': "https://us-central1-driven-atrium-445021-m2.cloudfunctions.net/etl_inicial_yellow_taxi",
    'params': {
                'filename': "".join([raw_datasets_path,current_year,'/yellow_tripdata_',current_year_month,'.parquet'])
            }
}
kwargs_transform_load_green_taxi = {
    'function': "https://us-central1-driven-atrium-445021-m2.cloudfunctions.net/etl_inicial_green_taxi",
    'params': {
                'filename': "".join([raw_datasets_path,current_year,'/green_tripdata_',current_year_month,'.parquet'])
            }
}
kwargs_transform_for_hire_taxi = {
    'function': "https://us-central1-driven-atrium-445021-m2.cloudfunctions.net/etl_inicial_for_hire_taxi",
    'params': {
                'filename': "".join([raw_datasets_path,current_year,'/fhv_tripdata_',current_year_month,'.parquet'])
            }
}
kwargs_transform_hvfhv_taxi = {
    'function': "https://us-central1-driven-atrium-445021-m2.cloudfunctions.net/etl_inicial_high_volume_taxi",
    'params': {
                'filename': "".join([raw_datasets_path,current_year,'/fhvhv_tripdata_',current_year_month,'.parquet'])
            }
}

#######################################################################################

default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': False,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2024, 12, 27),
    'email': email,
    #'email_on_failure': True,
    #'email_on_retry': True,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
    'dagrun_timeout': datetime.timedelta(minutes=3)
}

def invoke_function(**kwargs):
    url = kwargs['function'] #the url is also the target audience. 
    request = google.auth.transport.requests.Request()  #this is a request for obtaining the the credentials
    id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request) # If your cloud function url has query parameters, remove them before passing to the audience 

    resp = AuthorizedSession(id_token_credentials).request("GET", url=url, timeout=210, params=kwargs['params']) # the authorized session object is used to access the Cloud Function

    if kwargs['params']:
        json_resp= json.loads(resp.content.decode("utf-8"))

        if json_resp["duplicated"]:
            raise ValueError("Se encontraron datos duplicados. Marcando tarea como fallida")
        else:
            return True

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 3,
         schedule_interval = '0 2 18 * *') as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")
    
    extract_cf = PythonOperator(task_id='extract_cf',
                                provide_context=True,
                                python_callable=invoke_function,
                                op_kwargs=kwargs_extract
                            )
    
    transform_load_yellow_taxi = PythonOperator(task_id='transform_load_yellow_taxi',
                                provide_context=True,
                                python_callable=invoke_function,
                                op_kwargs=kwargs_transform_load_yellow_taxi
                            )
    
    transform_load_green_taxi = PythonOperator(task_id='transform_load_green_taxi',
                                provide_context=True,
                                python_callable=invoke_function,
                                op_kwargs=kwargs_transform_load_green_taxi
                            )
    
    transform_load_for_hire_taxi = PythonOperator(task_id='transform_load_for_hire_taxi',
                                provide_context=True,
                                python_callable=invoke_function,
                                op_kwargs=kwargs_transform_for_hire_taxi
                            )
    
    transform_load_hvfhv_taxi = PythonOperator(task_id='transform_load_hvfhv_taxi',
                                execution_timeout=datetime.timedelta(minutes=3),
                                provide_context=True,
                                python_callable=invoke_function,
                                op_kwargs=kwargs_transform_hvfhv_taxi
                            )
    
    Dummy_Reentrenar_ML = DummyOperator(task_id="Dummy_Reentrenar_ML", trigger_rule='all_success')

    t_end = DummyOperator(task_id="end", trigger_rule='all_success')

    #############################################################
    t_begin >> extract_cf
    extract_cf >> transform_load_yellow_taxi >> Dummy_Reentrenar_ML
    extract_cf >> transform_load_green_taxi >> Dummy_Reentrenar_ML
    extract_cf >> transform_load_for_hire_taxi >> Dummy_Reentrenar_ML
    extract_cf >> transform_load_hvfhv_taxi >> Dummy_Reentrenar_ML
    Dummy_Reentrenar_ML >> t_end
