
wget -O /home/hadoop/landing/Yellow_tripdata_2021-01.parquet https://edvaibucket.blob.core.windows.net/data-engineer-edvai/yellow_tripdata_2021-01.parquet\?sp\=r\&st\=2023-11-06T12:52:39Z\&se\=2025-11-06T20:52:39Z\&sv\=2022-11-02\&sr\=c\&sig\=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/Yellow_tripdata_2021-01.parquet /ingest/airflow

wget -O /home/hadoop/landing/Yellow_tripdata_2021-02.parquet https://edvaibucket.blob.core.windows.net/data-engineer-edvai/yellow_tripdata_2021-02.parquet\?sp\=r\&st\=2023-11-06T12:52:39Z\&se\=2025-11-06T20:52:39Z\&sv\=2022-11-02\&sr\=c\&sig\=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/Yellow_tripdata_2021-02.parquet /ingest/airflow
