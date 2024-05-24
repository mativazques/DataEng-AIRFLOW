wget -O /home/hadoop/landing/drivers.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/drivers.csv

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/drivers.csv /ingest/f1
