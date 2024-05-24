from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

df01 = spark.read.option("header","true").parquet("hdfs://172.17.0.2:9000/ingest/airflow/Yellow_tripdata_2021-01.parquet")

df02 = spark.read.option("header","true").parquet("hdfs://172.17.0.2:9000/ingest/airflow/Yellow_tripdata_2021-02.parquet")

total_trips_df = df01.unionAll(df02)

total_trips_filtered_df = total_trips_df.filter("RatecodeID = 2 AND payment_type = 2")

final_total_trips_df = total_trips_filtered_df.select(total_trips_filtered_df.tpep_pickup_datetime.cast("date"), total_trips_filtered_df.airport_fee.cast("float"), total_trips_filtered_df.payment_type.cast("int"), total_trips_filtered_df.tolls_amount.cast("float"), total_trips_filtered_df.total_amount.cast("float"))

final_total_trips_df.show(10)

final_total_trips_df.createOrReplaceTempView("airport_trips_view")

hc.sql("insert into tripdb.airport_trips select * from airport_trips_view;")
