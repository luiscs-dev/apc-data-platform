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

## DBT

**DBT** allows us to transform data using SQL. It can be integrated in a CI/CD pipeline and it creates documentation about models.

Data Quality is set using the **schema.yml**. You can set foreach column its tests 
to be validated. For example the id column must be **unique** and **not_null**.

DBT has generic tests (unique, not_null, accepted_values, relationships) which you can use but if you need more expecific rules
you can take a look at
- [dbt_expectations](https://github.com/calogica/dbt-expectations)
- [soda](https://docs.soda.io/)

Commands:
- dbt run --select %my model name%
- dbt test --select %my model name%
- dbt docs generate


***
