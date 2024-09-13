# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 -  Read CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC show mounts
# MAGIC
# MAGIC display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC show files
# MAGIC
# MAGIC display(dbutils.fs.ls("/mnt/noahformula1dl/raw"))

# COMMAND ----------

# races_df = spark.read.option("header", "true").csv("/mnt/noahformula1dl/raw/races.csv")

# COMMAND ----------

# display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Schema

# COMMAND ----------

# races_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date",DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header", "true") \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Add ingestion date and race timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

races_added_df = races_df \
.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn('ingestion_date', current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select & Rename columns

# COMMAND ----------

races_selected_df = races_added_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date"), col("file_date"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet or Delta (newest version)

# COMMAND ----------

# MAGIC %md
# MAGIC .partitionBy("race_year") used to partition by year -> creates a folder for each value in that column (i.e. each year)

# COMMAND ----------

# races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{presentation_folder_path}/races")

# COMMAND ----------

# races_selected_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")
races_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/noahformula1dl/processed/races

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("success")
