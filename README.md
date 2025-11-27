# Data Platform Demo
Hi, I'm Thinh

Because of some reason about security policies in my company, I can't share my Github.  So I created a demo data platform to demonstrate my skill.
I created a metadata driven data platform. It has two main elements: execution engine and metadata. We edit the metadata, then the execution engine executes ETL tasks as we defined.

## Data architecture
![enter image description here](https://lh3.googleusercontent.com/pw/AP1GczOu-dWkZa4VL-CGduMzDZI_ueVlKrfnHuvYWsIQ_RtMje2_hcrn82-OHDJvbl1ul63EMhqFSfqHvN2cbKDMqvuBwUU4W8z90VVK43IMiq9iZlAd5rZbyyXEhPCRecpmTjT2eW0gzF67Ba6_f75lBmZW=w621-h281-s-no-gm?authuser=0)
I will ingest data from Postgres, store it in silver zone. Then I can make some transformation and save it to to gold zone

## How to run?

    cd native-data-platform
    setup.cmd
    docker compose up -d
after all container started, create bucket named "warehouse" in minio
## How to setup job?
### Ingestion job
 - create config file like the ./config/job/source2silver_order.json
 - create job file  ./jobs/source2silver_order.py     
 -  access spark driver container: docker exec -it -u root spark-driver /bin/bash
 - submit job: /opt/spark/bin/spark-submit --py-files /opt/spark/jobs/core.zip /opt/spark/jobs/source2silver_order.py

### Aggregate job
-  create config file like the ./config/job/silver2golden_top_user.json
- create job file ./jobs/silver2golden_top_user.py
- create transform file ./core/transformer/silver2golden_top_user.py
- zip the "core" folder then copy the .zip to jobs folder
-  submit job: /opt/spark/bin/spark-submit --py-files /opt/spark/jobs/core.zip /opt/spark/jobs/silver2golden_top_user.py

## Summary
- The platform called native because it's easy to operate in both  on-premise and cloud environments. I deployed the same platform
- It's easy to debug because of using pyspark to transform
- It's easy to scale, just config by json, duplicate it for the same job, same table
- It's easy to develop just inherit from base classes
However, the limit of time, I can't deploy platform with orchestration. 