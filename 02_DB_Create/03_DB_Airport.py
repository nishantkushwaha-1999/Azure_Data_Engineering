# Databricks notebook source
import glob
import os
import datetime
import pandas as pd
import numpy as np

# COMMAND ----------

df_airports = pd.read_csv("/dbfs/mnt/raw/airports/airports.csv")

# COMMAND ----------

df_airports = df_airports.drop_duplicates()

convert_dict = {
    'IATA_CODE': object, 
    'AIRPORT': object,
    'CITY': object,
    'STATE': object,
    'COUNTRY': object,
    'LATITUDE': float,
    'LONGITUDE': float
}
df_airports = df_airports.astype(convert_dict)

# COMMAND ----------

df = spark.createDataFrame(df_airports)
df.createOrReplaceTempView("Airports")

# COMMAND ----------

df.write.parquet("dbfs:/mnt/xxairlinesprod/airports", mode="overwrite")

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
