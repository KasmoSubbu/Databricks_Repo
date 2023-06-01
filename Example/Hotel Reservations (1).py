# Databricks notebook source
storage_account_name = "myfiles0107"
storage_account_access_key = "H9tX8zTUJoiAZK1VI14dmAD4DcZ2BdkFHnJLrGejJUuoyAHZJC6unMzcAkP8ko7pDRX0tESL0esZ+ASt/6lr3Q=="
container = "newcontainer"

# COMMAND ----------

file_location = "https://myfiles0107.blob.core.windows.net/newcontainer/Hotel Reservations.csv"
file_type = "csv"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

dbutils.fs.ls(f"wasbs://eco-karma-csv@insuranceanalytics.blob.core.windows.net/")
