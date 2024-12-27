import json
import datetime
import google.auth.transport.requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
from google.auth.transport.requests import AuthorizedSession




#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'DAG_incremental_load_trip_taxi_data'
project           = 'incremental-load-trip-taxi-data'
owner             = 'juankaruna'
email             = ['juankarua@gmail.com']
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
}

def invoke_extract_function():
    url = "https://us-central1-driven-atrium-445021-m2.cloudfunctions.net/extract_incremental_load_trip_record_data" #the url is also the target audience. 
    request = google.auth.transport.requests.Request()  #this is a request for obtaining the the credentials
    id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request) # If your cloud function url has query parameters, remove them before passing to the audience 

    resp = AuthorizedSession(id_token_credentials).request("GET", url=url) # the authorized session object is used to access the Cloud Function

    print(resp.status_code) # should return 200
    print(resp.content) # the body of the HTTP response

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 3,
         schedule_interval = None) as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")
    
    extract_cf = PythonOperator(task_id='extract_cf',
                    python_callable=invoke_extract_function
                )

    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> extract_cf >> t_end