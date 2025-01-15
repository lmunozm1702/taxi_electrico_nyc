import functions_framework
import csv

from datetime import datetime
from sodapy import Socrata
from google.cloud import storage

DATASET_URL = "data.cityofnewyork.us"
DATASET_ID = "rhe8-mgbb"

def get_current_year():
    """Retorna el año en curso
    """
    return str(datetime.now().year)

def get_current_month():
    """Retorna el año en curso
    """
    return str(datetime.now().month)

def get_current_day():
    """Retorna el año en curso
    """
    return str(datetime.now().day-1)

@functions_framework.http
def hello_http(request):
    client_soda = Socrata(DATASET_URL, None)
    current_day = get_current_year() + '-' + get_current_month() + '-' + get_current_day()
    print(current_day)
    where_query = "".join(["last_updated_date >= '",current_day,"T00:00:00.000'"])
    results = client_soda.get(DATASET_ID, where=where_query, limit=10000, order="last_updated_date DESC")
    field_names = results[0].keys()

    client_gcs = storage.Client()
    bucket = client_gcs.get_bucket('ncy-taxi-bucket')
    year = get_current_year()
    year_month = get_current_year() + '-' + get_current_month()
    csv_name = "_".join([year_month,"active_medallion_vehicles.csv"])
    blob = bucket.blob('/'.join(['raw_datasets/active_medallion_vehicles',year,csv_name])) 

    with blob.open(mode="w", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(results)
    print("El archivo se guardo")

    return 'check the results in the logs'
