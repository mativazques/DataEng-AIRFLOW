wget -O /home/hadoop/landing/races.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/races.csv

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/races.csv /ingest/f1
