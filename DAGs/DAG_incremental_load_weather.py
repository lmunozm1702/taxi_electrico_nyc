import datetime
import google.auth.transport.requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
from google.auth.transport.requests import AuthorizedSession

def get_year():
    """Retorna el año en curso
    """

    if datetime.datetime.now().month == 1:
        year = datetime.datetime.now().year - 1
    else:
        year = datetime.datetime.now().year

    return str(year)

def get_prev_month():
    """Retorna el año en curso
    """

    if datetime.datetime.now().month == 1:
        month = 12
    else:
        month = datetime.datetime.now().month - 1

    return str(month)


#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG           = 'DAG_incremental_load_weather'
project           = 'incremental-load-taxi-electrico-nyc'
owner             = 'juankaruna'
email             = ['juankarua@gmail.com']

raw_datasets_path = "raw_datasets/weather/"
current_year = get_year()
year_month = get_year() + '-' + get_prev_month()

kwargs_extract = {
    'function': "https://us-central1-driven-atrium-445021-m2.cloudfunctions.net/extract_incremental_load_weather",
    'params': ""
}
kwargs_transform_load_weather = {
    'function': "https://us-central1-driven-atrium-445021-m2.cloudfunctions.net/etl_inicial_weather",
    'params': {
                'filename': "".join([raw_datasets_path,current_year,'/',year_month,'_weather.csv'])
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

    resp = AuthorizedSession(id_token_credentials).request("GET", url=url, params=kwargs['params']) # the authorized session object is used to access the Cloud Function


with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 3,
         schedule_interval = None) as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    # FUENTE: CRONTRAB: https://crontab.guru/
    #############################################################
    
    t_begin = DummyOperator(task_id="begin")
    
    extract_cf = PythonOperator(task_id='extract_cf',
                                provide_context=True,
                                python_callable=invoke_function,
                                op_kwargs=kwargs_extract
                            )
    
    transform_load_weather = PythonOperator(task_id='transform_load_weather',
                                provide_context=True,
                                python_callable=invoke_function,
                                op_kwargs=kwargs_transform_load_weather
                            )

    t_end = DummyOperator(task_id="end", trigger_rule='all_success')

    #############################################################
    t_begin >> extract_cf >> transform_load_weather >> t_end

