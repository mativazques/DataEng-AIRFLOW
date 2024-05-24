from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

df_results = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/f1/results.csv")
df_drivers = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/f1/drivers.csv")
df_constructors = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/f1/constructors.csv")
df_races = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/f1/races.csv")

df_results.createOrReplaceTempView("vw_results")
df_drivers.createOrReplaceTempView("vw_drivers")
df_constructors.createOrReplaceTempView("vw_constructors")
df_races.createOrReplaceTempView("vw_races")

df_driver_results = spark.sql( \
	"SELECT \
		cast(d.forename as string), \
		cast(d.surname as string), \
		cast(d.nationality as string), \
		sum(cast(r.points as int)) points \
	FROM \
		vw_results r, \
		vw_drivers d \
	WHERE \
		r.driverId = d.driverId \
	GROUP BY \
		d.driverId, d.forename, d.surname, d.nationality \
	ORDER BY \
		points DESC" \
	)

df_constructor_results = spark.sql( \
	"SELECT \
		cast(c.constructorRef as string) constructor_ref, \
		cast(c.name as string) cons_name, \
		cast(c.nationality as string) cons_nationality, \
		cast(c.url as string) url, \
		sum(cast(r.points as int)) points \
	FROM \
		vw_results r, \
		vw_constructors c, \
		vw_races ra \
	WHERE \
		r.constructorId = c.constructorId \
		AND r.raceId = ra.raceId \
		AND ra.year = '1991' \
	GROUP BY \
		c.constructorId, constructor_ref, cons_name, cons_nationality, c.url \
	ORDER BY \
		points DESC" \
	)

df_constructor_results.createOrReplaceTempView('vw_constructor_results')

hc.sql("insert into f1.constructor_results select * from vw_constructor_results;")

df_driver_results.createOrReplaceTempView("vw_driver_results")

hc.sql("insert into f1.driver_results select * from vw_driver_results;")
