# Databricks notebook source
# MAGIC %run ./SCD_SCHEMA

# COMMAND ----------

# DBTITLE 1,Reading the data
df_json = spark.read.format("json").option("multiline", "true").schema(schema).load("dbfs:/FileStore/shared_uploads/revathy.s@diggibyte.com/data_2-1.json")

# COMMAND ----------

# DBTITLE 1,semi flattening the data
df_flat = df_json.select(
    col("country"),
    explode(col("data")).alias("data")
).select(
    col("country"),
    col("data.date_time"),
    col("data.Key_in_date"),
    col("data.lastmodified_date"),
    col("data.task_id"),
    col("data.store-id").alias("store_id"),
    col("data.member_login_id"),
    col("data.check_in"),
    col("data.check_out"),
    col("data.user_location").alias("user_location"),
    col("data.distance"),
    explode(col("data.questions")).alias("questions")
)

# COMMAND ----------

# semi flatten json data
semi_flatten_json = df_flat.select(
    col("country"),
    col("date_time"),
    col("Key_in_date"),
    col("lastmodified_date"),
    col("task_id"),
    col("store_id"),
    col("member_login_id"),
    col("check_in"),
    col("check_out"),
    col("user_location"),
    col("distance"),
    col("questions.type"),
    col("questions.question"),
    col("questions.question_id"),
    explode(col("questions.answer")).alias("answers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Task_id is the PK in silver table (semi flatten table)**

# COMMAND ----------

silver_df = semi_flatten_json.dropDuplicates().filter(col("task_id").isNotNull())

# COMMAND ----------

# DBTITLE 1,writing the data
silver_table = silver_df.write.format("parquet").saveAsTable("silver.Silvertable")
