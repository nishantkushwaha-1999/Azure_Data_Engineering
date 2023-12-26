# Databricks notebook source
flights = "dbfs:/mnt/xxairlinesprod/flights"
airlines = "dbfs:/mnt/xxairlinesprod/airlines"
airports = "dbfs:/mnt/xxairlinesprod/airports"

# COMMAND ----------

df_flights = spark.read.format("parquet").load(flights)
df_airports = spark.read.format("parquet").load(airports)
df_airlines = spark.read.format("parquet").load(airlines)

# COMMAND ----------

df_flights.createOrReplaceTempView("Flights")
df_airlines.createOrReplaceTempView("Airlines")
df_airports.createOrReplaceTempView("Airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total number of flights by airline on a monthly basis
# MAGIC SELECT F.YEAR, F.MONTH, F.AIRLINE, count(F.FLIGHT_NUMBER) AS n_flight
# MAGIC FROM Flights AS F
# MAGIC WHERE F.CANCELLED = 0
# MAGIC GROUP BY F.YEAR, F.MONTH, F.AIRLINE
# MAGIC ORDER BY F.YEAR, F.MONTH

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On time percentage of each airline for the year 2015
# MAGIC WITH CTE1 AS (
# MAGIC   SELECT F.YEAR, F.AIRLINE, count(F.FLIGHT_NUMBER) AS n_flight
# MAGIC   FROM Flights AS F
# MAGIC   WHERE F.YEAR = 2015
# MAGIC   GROUP BY F.AIRLINE, F.YEAR
# MAGIC ),
# MAGIC CTE2 AS (
# MAGIC   SELECT F.YEAR, F.AIRLINE, count(F.FLIGHT_NUMBER) AS n_flight_no_delay
# MAGIC   FROM Flights AS F
# MAGIC   WHERE F.YEAR = 2015 AND F.ARRIVAL_DELAY < 0
# MAGIC   GROUP BY F.AIRLINE, F.YEAR
# MAGIC )
# MAGIC
# MAGIC SELECT C1.YEAR, C1.AIRLINE, C1.n_flight, C2.n_flight_no_delay, ROUND(((C2.n_flight_no_delay/C1.n_flight)*100), 2) AS on_time_per
# MAGIC FROM CTE1 AS C1
# MAGIC LEFT JOIN CTE2 AS C2 ON C1.YEAR = C2.YEAR AND C1.AIRLINE = C2.AIRLINE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Airlines with the largest number of delays
# MAGIC SELECT F.YEAR, F.AIRLINE, count(F.FLIGHT_NUMBER) AS n_flight_delay
# MAGIC FROM Flights AS F
# MAGIC WHERE F.YEAR = 2015 AND F.ARRIVAL_DELAY > 0
# MAGIC GROUP BY F.AIRLINE, F.YEAR
# MAGIC ORDER BY n_flight_delay DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cancellation reasons by airport
# MAGIC SELECT DISTINCT F.ORIGIN_AIRPORT, F.CANCELLATION_REASON
# MAGIC FROM Flights AS F
# MAGIC WHERE F.CANCELLATION_REASON LIKE '%%'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Airline with the most unique routes
# MAGIC WITH CTE AS (
# MAGIC   SELECT DISTINCT F.AIRLINE, F.ORIGIN_AIRPORT, F.DESTINATION_AIRPORT
# MAGIC   FROM Flights AS F
# MAGIC )
# MAGIC
# MAGIC SELECT CTE.AIRLINE, count(CTE.AIRLINE) AS n_unique FROM CTE
# MAGIC GROUP BY CTE.AIRLINE
# MAGIC ORDER BY n_unique DESC
