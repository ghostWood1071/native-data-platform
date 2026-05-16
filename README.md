# Data Platform Demo

## Data architecture
![enter image description here](https://lh3.googleusercontent.com/pw/AP1GczOu-dWkZa4VL-CGduMzDZI_ueVlKrfnHuvYWsIQ_RtMje2_hcrn82-OHDJvbl1ul63EMhqFSfqHvN2cbKDMqvuBwUU4W8z90VVK43IMiq9iZlAd5rZbyyXEhPCRecpmTjT2eW0gzF67Ba6_f75lBmZW=w621-h281-s-no-gm?authuser=0)  
I will ingest data from Postgres, store it in silver zone. Then I can make some transformation and save it to to gold zone

## How to run?

    cd native-data-platform setup.cmd 
    docker-compose-dev.yaml run.cmd source2silver_customer
after all container started, create bucket named "warehouse" in minio
## How to setup job?
### Ingestion job
- create config file like the ./config/job/source2silver_order.json
- create job file  ./jobs/source2silver_order.py
- run: run.cmd source2silver_order.py

### Aggregate job
- create config file like the ./config/job/silver2golden_top_user.json
- create job file ./jobs/silver2golden_top_user.py
- create transform file ./core/transformer/silver2golden_top_user.py
- zip the "src" folder then copy the .zip to infra/job-lib folder
- run: run.cmd silver2golden_top_user.py

## Code flow and CI/CD flow
### A. Human do
#### Step 1: Config job and work flow
in folder "/config/job" contains configuration to define how the engine execute ETL process. in folder "config/workflow" we define resource for the job and define the job dependency. Sometime when we need to upgrade the code, debug or define transform rules for difficult business.
#### Step 2: Commit job
After done job configuration and code, we commit and push to our branch. Now all our task done!
### B. CI/CD flow
#### step 1: prepare resource
- zip the "src" folder to minio
- push folder /config/job to minio
### step 2: prepare for deploy job
- go to postgresql run sql in file "orchestration/metadata-base/metadata-base.sql" to create metadata tables.
- in "orchestration" folder run file update_metadata.py. It can generate metadata from workflow config then insert to metadata tables in postgresql.
- copy file "orchestration/dags/dag-generator.py" to airflow's dags folder.
  Done !!!
## Summary
- The platform called native because it's easy to operate in both  on-premise and cloud environments. I deployed the same platform structure in previous projects
- It's easy to debug because of using pyspark to transform
- It's easy to scale, just config by json, duplicate it for the same type of job, same for the same table structure
- It's easy to develop just inherit from base classes