wget -O /home/hadoop/landing/results.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/results.csv

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/results.csv /ingest/f1
