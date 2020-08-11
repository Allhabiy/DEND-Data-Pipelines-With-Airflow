## Loading S3 file with Airflow to ETL with Redshift

The purpose of this project is to build an adapted data model thanks to python to load data in a JSON file format and wrangle them into a star schema (see the ERD) with the pipeline written as a code thanks to [AirFlow](https://airflow.apache.org/).

### Main Goal
The company Sparkify need to analyse their data to better understand the way users (free/paid) use theirs services. 
With this data pipeline we will be able to schedule, monitor and build more easily the ETL of this data.

### Data Pipeline
![Dag](./example-dag.png)

### Data Model

This pipeline finally is made to build this DB star schema below to make easier the data analysis

![ERD](./Song_ERD.png)

### Run it
After implementing your dags,
Go to the [AirFlow UI](http://localhost:8080/admin/) after several seconds then run the DAG.
