wget -O /home/hadoop/landing/results.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/results.csv

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/results.csv /ingest/f1

wget -O /home/hadoop/landing/drivers.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/drivers.csv

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/drivers.csv /ingest/f1

wget -O /home/hadoop/landing/constructors.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/constructors.csv

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/constructors.csv /ingest/f1

wget -O /home/hadoop/landing/races.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/races.csv

/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/landing/races.csv /ingest/f1
