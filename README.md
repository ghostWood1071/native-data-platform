# Data Platform Demo

## Data Architecture
![enter image description here](https://lh3.googleusercontent.com/pw/AP1GczOu-dWkZa4VL-CGduMzDZI_ueVlKrfnHuvYWsIQ_RtMje2_hcrn82-OHDJvbl1ul63EMhqFSfqHvN2cbKDMqvuBwUU4W8z90VVK43IMiq9iZlAd5rZbyyXEhPCRecpmTjT2eW0gzF67Ba6_f75lBmZW=w621-h281-s-no-gm?authuser=0)  
The project implements data collection from Postgres, storage in the Silver zone, followed by transformation and storage into the Gold zone. The system uses Spark Engine (`ghostwood/spark-engine:1.0.0`) and the `neutronx` library to interact with infrastructure (MinIO, Kafka, Postgres).

## How to Run?

### Local Development
1. Navigate to the project directory and run setup:
   ```cmd
   setup.cmd
   ```
2. Launch the Docker environment:
   ```cmd
   docker-compose -f config/environment/docker-compose-dev.yaml up -d
   ```
3. Run a test job:
   ```cmd
   run.cmd source2silver_customer
   ```

## How to Setup Job?

### 1. Ingestion Job
- Create a configuration file at `./config/job/` (e.g., `source2bronze_*.json`).
- Define the workflow in `./config/workflow/`.
- Spark Engine will automatically read the configuration and execute.

### 2. Transformation Job
- Create a job configuration at `./config/job/`.
- If complex logic is required, create a transform file at `src/business_logic/py_transform/` or use SQL at `src/business_logic/sql_transform/`.
- Package the source code (`src` folder) into `src.zip` to send to the Spark cluster.

## CI/CD Flow

The system uses Jenkins to automate the deployment process:

### A. Developer Workflow
1. **Job & Workflow Configuration**: Define JSON files in `config/job` and `config/workflow`.
2. **Logic Development**: Write transformation code in `src/`.
3. **Commit & Push**: Push code to the repository to trigger the Jenkins Pipeline.

### B. Jenkins Pipeline (CI/CD)
1. **Zipping source**: Compresses the `src` directory into `src.zip`.
2. **Upload to MinIO**: 
   - Upload `src.zip` and `jobs/entry_point.py` to the `asset/spark-jobs/` bucket.
   - Recursively upload the `config/job/` directory to `asset/job-input/`.
   - Recursively upload the `config/workflow/` directory to `asset/workflow/`.
3. **Update Metadata**: Run the `orchestration/update_metadata.py` script to update configurations in the Airflow Database.
4. **Airflow DAGs**: Use `dag-generator.py` to automatically generate DAGs based on metadata in Postgres.

## Environment Configuration
The project uses environment variables to manage configuration (see `env-template`):
- `ONPREM_MINIO_*`: MinIO connection information.
- `DB_*`: Postgres connection information (Airflow Metadata).
- Spark image: `ghostwood/spark-engine:1.0.0`.

## Summary
- **Native**: Easy to operate on both On-premise and Cloud.
- **Scalable**: Easily extendable via JSON configuration.
- **Maintainable**: Uses the `neutronx` library to standardize infrastructure interaction.
- **Automated**: Complete CI/CD process from code to Airflow DAGs.