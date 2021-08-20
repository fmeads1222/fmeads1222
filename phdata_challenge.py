from pyspark.sql import *
from pyspark.sql.functions import *

# Prepratory step for execution of later cells.
# Credetials would by stored in a databricks secret if this were not a demo.
options = {
  "sfUrl": "*****************",
  "sfUser": "*************",
  "sfPassword": "**************",
  "sfDatabase": "USER_XXXXX_XXXXX",
  "sfSchema": "STAGE",
  "sfWarehouse": "INTERVIEW_WH"
}
# Import data into databricks, and create views to simplify querying.
# Data is coming from a public bucket. Would use a secure bucket with IAM policies if this were not a demo.
airports = spark.read.format("CSV").option("inferSchema", "true").option("header","true").load("s3://mynorthwoodairlines/airports.csv")
airports.createOrReplaceTempView("AIRPORTS")
airlines = spark.read.format("CSV").option("inferSchema", "true").option("header","true").load("s3://mynorthwoodairlines/airlines.csv")
flights = spark.read.format("CSV").option("inferSchema", "true").option("header","true").load("s3://mynorthwoodairlines/partition*.csv")

# Create dim tables in Snowflake and a view to simplify querying in Databricks.
# First add a key column.
dim_airports = airports.withColumn("AIRPORT_KEY", monotonically_increasing_id()+ lit(1)) 
dim_airlines = airlines.withColumn("AIRLINE_KEY", monotonically_increasing_id() + lit(1))

# Rename the code columns.
dim_airports = dim_airports.withColumnRenamed("IATA_CODE","AIRPORT_CODE")
dim_airlines = dim_airlines.withColumnRenamed("IATA_CODE","AIRLINE_CODE")

# Put columns in the proper order
dim_airports = dim_airports.select("AIRPORT_KEY","AIRPORT_CODE","AIRPORT","CITY","STATE","COUNTRY","LATITUDE","LONGITUDE")
dim_airlines = dim_airlines.select("AIRLINE_KEY","AIRLINE_CODE","AIRLINE")

# Save to Snowflake.
dim_airports.write.format("snowflake").mode("overwrite").options(**options).option("dbtable","dw.dim_airports").save()
dim_airlines.write.format("snowflake").mode("overwrite").options(**options).option("dbtable","dw.dim_airlines").save()

# Create views
dim_airports.createOrReplaceTempView("DIM_AIRPORTS")
dim_airlines.createOrReplaceTempView("DIM_AIRLINES")

# Create cancellation reason lookup table.
columns = ["REASON_CODE","REASON"]
data = [("A","Airline/Carrier"),("B","Weather"),("C","National Air System"),("D","Security")]

rdd = spark.sparkContext.parallelize(data)
reasons = spark.createDataFrame(rdd).toDF(*columns)

# Create reasons view.

reasons.createOrReplaceTempView("REASONS")

# Create the flight fact table.
fact_flights = spark.sql("select al.airline_key,f.airline airline_id,oap.airport_key origin_airport_key,f.origin_airport origin_airport_id,dap.airport_key destination_airport_key,f.destination_airport destination_airport_id,f.year,f.month,f.day,f.flight_number,f.tail_number,ifnull(f.scheduled_departure,0) scheduled_departure,ifnull(f.departure_time,0) departure_time,ifnull(f.departure_delay,0) departure_delay,ifnull(f.taxi_out,0) taxi_out,ifnull(f.wheels_off,0) wheels_off,ifnull(f.scheduled_time,0) scheduled_time,ifnull(f.elapsed_time,0) elapsed_time,ifnull(f.air_time,0) air_time,ifnull(f.distance,0) distance,ifnull(f.wheels_on,0) wheels_on,ifnull(f.taxi_in,0) taxi_in,ifnull(f.scheduled_arrival,0) scheduled_arrival,ifnull(f.arrival_time,0) arrival_time,ifnull(f.arrival_delay,0) arrival_delay,case f.diverted when 1 then true else false end diverted, case f.cancelled when 1 then true else false end cancelled,ifnull(r.reason,'N/A') cancellation_reason,ifnull(f.air_system_delay,0) air_system_delay,ifnull(f.security_delay,0) security_delay,ifnull(f.airline_delay,0) airline_delay,ifnull(f.late_aircraft_delay,0) late_aircraft_delay,f.weather_delay from flights f join dim_airlines al on f.airline = al.airline_code join dim_airports oap on f.origin_airport = oap.airport_code join dim_airports dap on f.destination_airport = dap.airport_code left join reasons r on f.cancellation_reason=r.reason_code")
fact_flights.write.format("snowflake").mode("overwrite").options(**options).option("dbtable","dw.fact_flights").save()
flights.createOrReplaceTempView("FLIGHTS")
