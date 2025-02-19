"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the 
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust 
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your 
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from airflow.operators.bash import BashOperator
import os
import json

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"retries": 3},
    tags=["players"],
)
def players_pipeline():
    
    @task
    def ingest_gsheet(payload: dict) -> None:
        headers = {
            'Content-Type': 'application/json'
        }             
        # Make the POST request
        response = requests.post("https://us-central1-data-eng-training-87b25bc6.cloudfunctions.net/apc-data-ingestion", headers=headers, json=payload)
        
        # Check the response
        if response.status_code != 200:
            raise ValueError(f"Request to Cloud Function failed: {response.status_code} {response.text}")

    _bucket = "apc-data-lake"
    _bucket_path_prefix = "raw/{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}/"
    
    ingest_provider1 = ingest_gsheet({
        "spreadsheet_id": "https://docs.google.com/spreadsheets/d/1BNS_pZVMt55wlqkv2qfZiW8LPdcefFHPoYRFY4WVjf8",
        "tab_name": "players",
        "bucket_name": _bucket,
        "file_name": _bucket_path_prefix+"airflow1.csv"
    })
    
    ingest_provider2 = ingest_gsheet({
        "spreadsheet_id": "https://docs.google.com/spreadsheets/d/1wNEh5U0YRerx2z_NXHdwpSgeFTXWtahakqw7iDWNQxo",
        "tab_name": "players",
        "bucket_name": _bucket,
        "file_name": _bucket_path_prefix+"airflow2.csv"
    })
    
    ingest_provider3 = ingest_gsheet({
        "spreadsheet_id": "https://docs.google.com/spreadsheets/d/1ewdBpIk8tJGZrUdnUUQPyyiFoECuBqm8qPonlw37Hp0",
        "tab_name": "players",
        "bucket_name": _bucket,
        "file_name": _bucket_path_prefix+"airflow3.csv"
    })
    

    provider1_tobq = aql.load_file(
        task_id='provider1_tobq',
        input_file=File(
            'gs://'+_bucket+"/"+_bucket_path_prefix+'airflow1.csv',
            conn_id='google_cloud_con',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_players1',
            conn_id='google_cloud_con',
            metadata=Metadata(schema='apc_dwh')
        ),
        use_native_support=False,
    )
    
    provider2_tobq = aql.load_file(
        task_id='provider2_tobq',
        input_file=File(
            'gs://'+_bucket+"/"+_bucket_path_prefix+'airflow2.csv',
            conn_id='google_cloud_con',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_players2',
            conn_id='google_cloud_con',
            metadata=Metadata(schema='apc_dwh')
        ),
        use_native_support=False,
    )
    
    provider3_tobq = aql.load_file(
        task_id='provider3_tobq',
        input_file=File(
            'gs://'+_bucket+"/"+_bucket_path_prefix+'airflow3.csv',
            conn_id='google_cloud_con',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_players3',
            conn_id='google_cloud_con',
            metadata=Metadata(schema='apc_dwh')
        ),
        use_native_support=False,
    )
    
    @task
    def create_gcloud_json() -> None:
        data = os.environ.get("GCLOUD_JSON")
        d = "{"+data+"}"
        gcloud_info = json.loads(d.replace("\n", "\\n"))
        with open("include/gcp/service_account.json", 'w') as json_file:
            json.dump(gcloud_info, json_file, indent=4)

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
        
    bash_task2 = BashOperator(
        task_id='dbt_transformations',
        bash_command='source /usr/local/airflow/dbt_venv/bin/activate && cd /usr/local/airflow/include/dbt/ && dbt deps && dbt run --project-dir /usr/local/airflow/include/dbt/ --profiles-dir /usr/local/airflow/include/dbt/',
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    ingest_provider1 >> provider1_tobq
    ingest_provider2 >> provider2_tobq
    ingest_provider3 >> provider3_tobq

    [provider1_tobq, provider2_tobq, provider3_tobq] >> create_gcloud_json() >> check_load() >> bash_task2 >> check_transform()
    
    
players_pipeline()