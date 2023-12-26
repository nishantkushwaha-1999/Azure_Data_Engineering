# Databricks notebook source
import glob
import os
import datetime
import pandas as pd
import numpy as np

# COMMAND ----------

all_files = glob.glob("/dbfs/mnt/raw/flights/*.csv")
df_flights = pd.concat((pd.read_csv(f) for f in all_files))

# COMMAND ----------

df_flights = df_flights.drop_duplicates()

convert_dict = {
    'YEAR': int, 
    'MONTH': int,
    'DAY': int,
    'DAY_OF_WEEK': int,
    'AIRLINE': object, 
    'FLIGHT_NUMBER': int,
    'TAIL_NUMBER': object,
    'ORIGIN_AIRPORT': object,
    'DESTINATION_AIRPORT': object,
    'DEPARTURE_DELAY': float,
    'TAXI_OUT': float,
    'WHEELS_OFF': float,
    'SCHEDULED_TIME': float,
    'ELAPSED_TIME': float,
    'AIR_TIME': float,
    'DISTANCE': int,
    'WHEELS_ON': float,
    'TAXI_IN': float,
    'ARRIVAL_DELAY': float,
    'DIVERTED': int,
    'CANCELLED': int,
    'CANCELLATION_REASON': object,
    'AIR_SYSTEM_DELAY': float,
    'SECURITY_DELAY': float,
    'AIRLINE_DELAY': float,
    'LATE_AIRCRAFT_DELAY': float,
    'WEATHER_DELAY': float
}
df_flights = df_flights.astype(convert_dict)

# COMMAND ----------

def process_datetime(data):
    time, year, month, day = data
    # print(data)
    if str(time) == 'nan':
        return np.nan
    else:
        t_type = type(time)
        if t_type == int:
            time = list(str(int(time)))
            
            if len(time) != 4:
                time.insert(0, '0')
                time = ''.join(time)
            else:
                time = ''.join(time)
        elif t_type == float:
            time = str(time).split(".")[0]
            time = ''.join(time)
            time = list(str(int(time)))
            
            if len(time) != 4:
                time.insert(0, '0')
                time = ''.join(time)
            else:
                time = ''.join(time)
    
        # print(data, time, int(time[:2]), int(time[-2:]))
        hour = int(time[:2])
        if hour == 24:
            hour = 0
        else:
            hour = hour
        
        dt_time = datetime.datetime(year=int(year), month=int(month), day=int(day), hour=hour, minute=int(time[-2:]))
        return dt_time


df_flights['SCHEDULED_DEPARTURE'] = df_flights[['SCHEDULED_DEPARTURE', 'YEAR', 'MONTH', 'DAY']].apply(tuple, axis=1)
df_flights['SCHEDULED_DEPARTURE'] = df_flights['SCHEDULED_DEPARTURE'].apply(lambda row: process_datetime(row))

df_flights['DEPARTURE_TIME'] = df_flights[['DEPARTURE_TIME', 'YEAR', 'MONTH', 'DAY']].apply(tuple, axis=1)
df_flights['DEPARTURE_TIME'] = df_flights['DEPARTURE_TIME'].apply(lambda row: process_datetime(row))

df_flights['SCHEDULED_ARRIVAL'] = df_flights[['SCHEDULED_ARRIVAL', 'YEAR', 'MONTH', 'DAY']].apply(tuple, axis=1)
df_flights['SCHEDULED_ARRIVAL'] = df_flights['SCHEDULED_ARRIVAL'].apply(lambda row: process_datetime(row))

df_flights['ARRIVAL_TIME'] = df_flights[['ARRIVAL_TIME', 'YEAR', 'MONTH', 'DAY']].apply(tuple, axis=1)
df_flights['ARRIVAL_TIME'] = df_flights['ARRIVAL_TIME'].apply(lambda row: process_datetime(row))

# COMMAND ----------

df_flights.info()

# COMMAND ----------

df = spark.createDataFrame(df_flights)
df.createOrReplaceTempView("Flights")

# COMMAND ----------

df.write.parquet("dbfs:/mnt/xxairlinesprod/flights", mode="overwrite")

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcUsername = dbutils.secrets.get(scope="key-valut-student", key="db_username")
# MAGIC val jdbcPassword = dbutils.secrets.get(scope="key-valut-student", key="db_password")
# MAGIC val jdbcHostname = dbutils.secrets.get(scope="key-valut-student", key="db_HostName")
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = dbutils.secrets.get(scope="key-valut-student", key="db_Name")
# MAGIC
# MAGIC import java.util.Properties
# MAGIC
# MAGIC val jdbc_url = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
# MAGIC
# MAGIC val connectionProperties = new Properties()
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")
# MAGIC
# MAGIC
# MAGIC spark.sql("create or replace table Airport_hive as select * from Airports")
# MAGIC spark.table("Airport_hive").write.mode("overwrite").jdbc(jdbc_url, "Airports", connectionProperties)
