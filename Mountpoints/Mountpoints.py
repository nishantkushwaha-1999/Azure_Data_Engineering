# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

tenant = dbutils.secrets.get(scope="xxairlilnes-key-vault", key="xxairlines-tenant")

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="xxairlilnes-key-vault", key="xxairlines-client-id"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="xxairlilnes-key-vault", key="xxairlines-secret-value"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{tenant}/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://raw@xxairlines.dfs.core.windows.net/flights",
  mount_point = "/mnt/raw/flights",
  extra_configs = configs)

# COMMAND ----------

tenant = dbutils.secrets.get(scope="xxairlilnes-key-vault", key="xxairlines-tenant")

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="xxairlilnes-key-vault", key="xxairlines-client-id"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="xxairlilnes-key-vault", key="xxairlines-secret-value"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{tenant}/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://raw@xxairlines.dfs.core.windows.net/airlines",
  mount_point = "/mnt/raw/airlines",
  extra_configs = configs)

# COMMAND ----------

tenant = dbutils.secrets.get(scope="xxairlilnes-key-vault", key="xxairlines-tenant")

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="xxairlilnes-key-vault", key="xxairlines-client-id"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="xxairlilnes-key-vault", key="xxairlines-secret-value"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{tenant}/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://raw@xxairlines.dfs.core.windows.net/airports",
  mount_point = "/mnt/raw/airports",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls mnt/raw/
