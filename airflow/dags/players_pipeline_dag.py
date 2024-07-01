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
from airflow.operators.python import PythonOperator


def post_to_cloud_function(**kwargs):   
    headers = {
        'Content-Type': 'application/json'
    } 
    payload = {
        "spreadsheet_id": "https://docs.google.com/spreadsheets/d/1ewdBpIk8tJGZrUdnUUQPyyiFoECuBqm8qPonlw37Hp0",
        "tab_name": "players",
        "bucket_name": "apc-data-lake",
        "file_name": "raw/airflow1.csv"
    }
    
    # Make the POST request
    response = requests.post("https://us-central1-data-eng-training-87b25bc6.cloudfunctions.net/apc-data-ingestion", headers=headers, json=payload)
    
    # Check the response
    if response.status_code != 200:
        raise ValueError(f"Request to Cloud Function failed: {response.status_code} {response.text}")


#Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"retries": 3},
    tags=["players"],
)
def players_pipeline():
    call_cloud_function = PythonOperator(
        task_id='call_cloud_function',
        python_callable=post_to_cloud_function,
    )

players_pipeline()