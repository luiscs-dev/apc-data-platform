# APC Data Platform for Sport Analytics

This project aims to have a E2E pipeline to process/transform raw data about players, teams and to be the main source to take business decisions.

![Alt text](data/imgs/general_diagram.png?raw=true "APC - Architecture Diagram")


The Tech Stack to solve this use case is:
- [BigQuery - Data warehouse to store and query large datasets](https://cloud.google.com/bigquery?hl=en)
- [DBT - a SQL-first transformation workflow](https://www.getdbt.com/)
- [Astronomer - The fully-managed platform for Airflow](https://www.astronomer.io/)
- [Looker - Build the foundation for responsible data insights](https://lookerstudio.google.com/reporting/e3691221-9ff1-49f6-959e-39727d8b3bf2)
- [Soda IO - Monitor data quality health](https://www.soda.io/)
***

## Prerequisites 
- Astronomer Account
- Google Cloud Account
- DBT Cloud Account
- Soda IO Account

***

## Data Quality

**SODA** was used to include checks over the ingested and transformed data.

The definition can be found in 
```
airflow/include/soda
```

The configuration is set to use BigQuery
```
data_source apc_dwh:
  type: bigquery
  account_info_json_path: /usr/local/airflow/include/gcp/service_account.json
  auth_scopes:
  - https://www.googleapis.com/auth/bigquery
  - https://www.googleapis.com/auth/cloud-platform
  - https://www.googleapis.com/auth/drive
  project_id: 'data-eng-training-87b25bc6'
  dataset: apc_dwh
```


Check can be found in the directory
```
airflow/include/soda/checks/sources
```

This is an example
```
checks for raw_players1:
  - schema:
      fail:
        when required column missing: [provider_id, player_name, first_name, last_name, date_of_birth, gender, country]
        when wrong column type:
          provider_id: string
          player_name: string
          first_name: string
          last_name: string
          gender: string
```


## Data Transformation

**DBT** was used to transform raw data into a useful structure.

The definition can be found in 
```
airflow/include/dbt/models/transform
```

It is set to run on BigQuery.
```
apc_dwh:
 target: dev
 outputs:
  dev:
    type: bigquery
    method: service-account
    keyfile: /usr/local/airflow/include/gcp/service_account.json
    project: data-eng-training-87b25bc6
    dataset: apc_dwh
    threads: 1
    timeout_seconds: 300
    location: US
```
***


## Data Visualization

**Looker** was used to create a dashboard to present
information in a friendly way.

![Alt text](data/imgs/looker.png?raw=true "APC - Architecture Diagram")
