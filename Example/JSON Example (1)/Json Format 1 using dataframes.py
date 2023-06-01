# Databricks notebook source

from pyspark.sql.functions import explode,col
testJsonData = spark.read.option("multiline","true").json("dbfs:/FileStore/Subhash/JSON/json_sampe1.json")
display(testJsonData)
display(testJsonData.select(explode("contactNumbers")))
display(testJsonData.select(explode("favoriteSports")))

# COMMAND ----------

samplejson1 = testJsonData.withColumn("Contact_Numbers",explode("contactNumbers"))\
                            .withColumn("Phone_Number",col("Contact_Numbers.number"))\
                            .withColumn("Phone_Type",col("Contact_Numbers.type"))\
                             .withColumn("Favorite Sports",explode("favoriteSports"))\
                             .drop("contactNumbers","favoriteSports","Contact_Numbers")
display(samplejson1)


# COMMAND ----------


