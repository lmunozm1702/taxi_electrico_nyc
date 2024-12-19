import requests

from bs4 import BeautifulSoup

HEADERS = {
        'Accept': '*/*',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0 '
    }

URL = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'

if __name__ == '__main__':

    session = requests.session()
    response = session.get(URL, headers=HEADERS)

    print(response.status_code)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        div_year = soup.find('div', id='faq2024')

        a_labels_datasets = div_year('a')

        url_datasets = []

        for a_label in a_labels_datasets:
            url_datasets.append(a_label['href'].strip())

        file_response = requests.get(url_datasets[0], stream=True)

        if file_response.status_code == 200:
            ### Guardar en Local
            with open('assets/Datasets/yellow_tripdata_2024-01.parquet', 'wb') as file:
                for chunk in file_response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print('Se guardo correctamente')

            ### Guardar en Google Cloud Storage
            """
            # Step 4: Upload to Google Cloud Storage
            bucket_name = "your-gcs-bucket-name"  # Replace with your GCS bucket name
            destination_blob_name = "path/in/gcs/file.ext"  # Desired path and name in GCS

            # Initialize GCS client and upload the file
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            blob.upload_from_string(file_response.content, content_type='application/octet-stream')
            print(f"File uploaded to GCS as {destination_blob_name}")
            """
        else:
            print(f"No se logro acceder al archivo! Respuesta del Server:{file_response.status_code}")

    else:
        print(f"No se logro acceder al sitio! Respuesta del Server:{response.status_code}")
