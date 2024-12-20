import functions_framework
import pandas as pd
import pandas_gbq

@functions_framework.http
def etl_poc(request):
    #Extract
    df = pd.read_parquet('gs://ncy-taxi-bucket/static_files_for_etl/etl_poc/fhv_tripdata_2024-09.parquet')
    print(df.head())
    print(df.info())

    #Transform
    print(df['SR_Flag'].isna().sum())
    df.drop(columns=['SR_Flag'], inplace=True)
    print(df.info())

    #Load
    project_id = 'driven-atrium-445021-m2'
    table_id = 'poc_etl.fh_poc_etl'

    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')

    return 'check the results in the logs'