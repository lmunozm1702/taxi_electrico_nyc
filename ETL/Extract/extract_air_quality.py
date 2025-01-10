import functions_framework
import csv

from datetime import datetime
from sodapy import Socrata
from google.cloud import storage

DATASET_URL = "data.cityofnewyork.us"
DATASET_ID = "c3uy-2p5r"

def get_current_year():
    """Retorna el año en curso
    """
    return str(datetime.now().year)

@functions_framework.http
def hello_http(request):
    client_soda = Socrata(DATASET_URL, None)
    results = client_soda.get(DATASET_ID, limit=20000)
    field_names = results[0].keys()

    client_gcs = storage.Client()
    bucket = client_gcs.get_bucket('ncy-taxi-bucket')
    year = get_current_year()
    csv_name = "_".join([year,"air_qualit.csv"])
    blob = bucket.blob('/'.join(['raw_datasets/air_quality',csv_name])) 

    with blob.open(mode="w", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(results)
    print("El archivo se guardo")

    return f'check the results in the logs'