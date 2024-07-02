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
    tags=["club_elo"],
)
def club_elo_pipeline():
    
    @task
    def ingest_club_elo(payload: dict) -> None:
        headers = {
            'Content-Type': 'application/json'
        }             
        # Make the POST request
        response = requests.post("https://us-central1-data-eng-training-87b25bc6.cloudfunctions.net/apc-data-ingestion-api", headers=headers, json=payload)
        
        # Check the response
        if response.status_code != 200:
            raise ValueError(f"Request to Cloud Function failed: {response.status_code} {response.text}")

    _bucket = "apc-data-lake"
    _bucket_path_prefix = "raw/club_elo/{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}/"
    
    ingest_provider1 = ingest_club_elo({
        "url": f"https://api.clubelo.com/{{ ds }}",
        "bucket_name": _bucket,
        "file_name": _bucket_path_prefix+"club_elo.csv"
    })
        
    provider1_tobq = aql.load_file(
        task_id='club_elo_tobq',
        input_file=File(
            'gs://'+_bucket+"/"+_bucket_path_prefix+'club_elo.csv',
            conn_id='google_cloud_con',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_club_elo',
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
    def check_load(scan_name='check_load', checks_subpath='sources_api'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
        
    bash_task2 = BashOperator(
        task_id='dbt_transformations',
        bash_command='source /usr/local/airflow/dbt_venv/bin/activate && cd /usr/local/airflow/include/dbt/ && dbt deps && dbt run --project-dir /usr/local/airflow/include/dbt/ --profiles-dir /usr/local/airflow/include/dbt/',
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform_api'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    ingest_provider1 >> provider1_tobq >> create_gcloud_json() >> check_load() >> bash_task2 >> check_transform()
    
    
club_elo_pipeline()