# Ingestion Data with Airbyte (Part 1)
## Introduction to Airbyte

Airbyte is an open-source platform designed to streamline data integration tasks. It allows you to extract data from various sources, transform it into a suitable format, and load it into your preferred destinations, such as databases, data warehouses, or cloud storage.

## Why using airbyte for data ingestion?

Airbyte simplifies complex data integration processes. It offers a user-friendly interface, supports a wide range of sources and destinations, and automates data synchronization.

## Deploy Postgresql, Citus and Airbyte Locally via Docker-Compose

- Go to [ingestion_airbyte](./ingestion_airbyte/) directory.
- Make sure port 5432 is not occupied by any docker container otherwise run this command to list all the active containers, then select `container_id` with port 5432  
    ```
    docker ps 
    ``` 
- Stop the container (using port 5432) with this command
    ```
    docker stop <container_id>
    ``` 
- Run Postgreql, Citus and Airbyte locally via docker-compose
    ```
    docker-compose -f ingestion_airbyte/docker-compose.yml up
    ```
- Make sure all the services are up and running.
    ![services-run](./img/ingestion_airbyte__all_services_up.png)

- Create connection on DBeaver to Postgresql and Citus with these credentials: 

```
    # Postgresql credential

    - Host: localhost:5432
    - Username: posgres
    - Password: pass
    - DB: store


    # Citus credential

    - Host: localhost:15432
    - Username: posgres
    - Password: pass
    - DB: store
```

## Setup Connection in Airbyte

- Open the Airbyte dashboard on [http://localhost:8000/](http://localhost:8000/) in browser, login with 
```
    - Username: airbyte
    - Password: password
```

- Then, create connection

    ![image-1](./img/ingestion_airbyte__create_connection.png)


## Ingest from API to Postgresql

We are going to ingest data from this [file url](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet) to Postgresql. 

First of all, let's define the new source from `File (CSV, JSON, Excel, Feather, Parquet)`. Then test and save it.

![setup-source-api](./img/ingestion_airbyte__setup_source_api.png)

Next, we should define `New Connection`, add the `green-taxi-data-parquet-url` as the source 

![green-taxi-data-as-source](./img/ingestion_airbyte__define_source_new_connection.png)

and Postgresql as the destination.

![destination-on-new-connection](./img/ingestion_airbyte__define_destination_new_connection.png)

Fill out the connection setup configuration.

![connection-setup](./img/ingestion_airbyte__connection_setup.png)

In the connection setup configuration: 
- set the frequency, to Manual. The options could be scheduled by Cron, Every 1, 2, 3, 6 to 24 hours.
- set the type of ingestion to overwrite or append.

Then start sync-ing the data stream from source to destination.

![sync-data-stream](./img/ingestion_airbyte__sync_data.png)

Once the data stream are succesfully ingested, the sync status will be shown like below:

![sync-success](./img/ingestion_airbyte__api_ingested_success.png)

Let's check data in Postgresql.

![pg-data](./img/ingestion_airbyte__result_on_pg_dbeaver.png)


## TASK
1. Ingest data from [local file json](./dataset/2017-10-02-1.json) to Postgresql with Airbyte.
2. Count the number of rows ingested.

# Ingestion Data with Airbyte (Part 2)

## Ingest data from Postgresql to Postgresql Citus

- Define source 

    ![image-2](./img/ingestion_airbyte__define_source.png)

- Define postgresql connection detail (see config on [docker-compose](./ingestion_airbyte/docker-compose.yml))

    ![image-3](./img/ingestion_airbyte__define_source_detail.png)

- define destination

    ![img-1](./img/ingestion_airbyte__destination.png)

    ![img-1](./img/ingestion_airbyte__destination_config.png)

![img-1](./img/ingestion_airbyte__destination_configuration.png)


- Start synchronizing data

![img-1](./img/ingestion_airbyte__destination_configuration_sync.png)

- Synchronizing status success 

![img-1](./img/ingestion_airbyte__destination_configuration_sync_success.png)

![img-1](./img/ingestion_airbyte__destination_configuration_sync_success_2.png)


## TASK
We have already learned how tol ingest data from source to destination using Airbyte. It is pretty much simpler and require no-code at all. In this section, we also learned to ingest data from Postgresql to [Citus](https://docs.citusdata.com/en/stable/get_started/what_is_citus.html) with Airbyte. 

Now, let's have some fun. The today task is, you should create an ingestion data from Postgresql to Citus using Python.