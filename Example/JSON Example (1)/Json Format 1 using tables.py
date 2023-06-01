# Databricks notebook source
testJsonData = spark.read.json("dbfs:/FileStore/Subhash/JSON/json_sampe1.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table divvy_exploration.json_format;
# MAGIC create table divvy_exploration.json_format
# MAGIC using json
# MAGIC OPTIONS (path = "/FileStore/Subhash/JSON/json_sampe1.json", multiline=true)

# COMMAND ----------

# DBTITLE 0,a
# MAGIC %sql
# MAGIC select * from divvy_exploration.json_format;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select age,
# MAGIC name,
# MAGIC spouse,phone_number, G as Game  from (
# MAGIC select 
# MAGIC age,
# MAGIC name,
# MAGIC spouse,
# MAGIC --contactNumbers.number,
# MAGIC --contactNumbers.type,
# MAGIC explode(contactNumbers.number) as phone_number,
# MAGIC -- from_json(to_json(e.favoriteSports), schema_of_json('["Football", "Cricket"]'))[0] as Football,
# MAGIC -- from_json(to_json(e.favoriteSports),schema_of_json('["Football", "Cricket"]'))[1] as Cricket,
# MAGIC explode(array(e.favoriteSports)) as G
# MAGIC from divvy_exploration.json_format e)
