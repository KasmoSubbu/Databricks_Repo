# Databricks notebook source
dbutils.fs.put("/FileStore/Subhash/JSON/json_sample2.json","""[{"name":"Yin", "address":{"city":"Columbus","state":"Ohio"}}
{"name":"Michael", "address":{"city":null, "state":"California"}}]""",True)

# COMMAND ----------

df2_sample_json = spark.read.json("/FileStore/Subhash/JSON/json_sample2.json")

# COMMAND ----------



# COMMAND ----------

display(df2_sample_json)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table divvy_exploration.json_sample_format2;
# MAGIC create table  divvy_exploration.json_sample_format2
# MAGIC using json
# MAGIC options (path = "/FileStore/Subhash/JSON/json_sample2.json",multiline=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from divvy_exploration.json_sample_format2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   v1.name, v2.city, v2.state 
# MAGIC FROM divvy_exploration 
# MAGIC   LATERAL VIEW json_tuple(people.jsonObject, 'name', 'address') v1 
# MAGIC      as name, address
# MAGIC   LATERAL VIEW json_tuple(v1.address, 'city', 'state') v2
# MAGIC      as city, state;
