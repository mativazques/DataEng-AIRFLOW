wget -O /home/hadoop/landing/constructors.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/constructors.csv

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/constructors.csv /ingest/f1
