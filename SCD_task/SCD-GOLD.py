# Databricks notebook source
# MAGIC %run ./SCD_SCHEMA

# COMMAND ----------

gold_df = spark.read.table("silver.Silvertable")
display(gold_df)

# COMMAND ----------

final_flattened_df = gold_df.select(
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
    col("type"),
    col("question"),
    col("question_id"),
    col("answers.product_id"),
    col("answers.product_code"),
    col("answers.price_gross"),
    col("answers.price_net"),
    col("answers.price_discount"),
    col("answers.price_incentive"),
    col("answers.sales_unit"),
    col("answers.sales_gross"),
    col("answers.sales_net"),
    col("answers.promotion"),
    col("answers.answer_id"),
    col("user_location")[0].alias("latitude"),
    col("user_location")[1].alias("longitude"))

# COMMAND ----------

final_df = final_flattened_df.withColumn("is_current", lit(True).cast(StringType())) \
    .withColumn("valid_from", current_timestamp()) \
    .withColumn("valid_to", lit(None).cast(StringType())) \
    .dropDuplicates() \
    .filter(col("answer_id").isNotNull())


# COMMAND ----------

# Defining current timestamp 
current_time = current_timestamp()
output_path = "/dbfs/shared_uploads/revathy.s@diggibyte.com/scd/gold"

if not DeltaTable.isDeltaTable(spark, output_path):
        final_df.write.format("delta")\
                  .mode("overwrite")\
                  .option("path", output_path)\
                  .saveAsTable(f"gold.gold_data")
else:
    old_delta_table = DeltaTable.forPath(spark, output_path)
    old_delta_table.alias("target").merge(
    final_df.alias("source"),
    "target.answer_id = source.answer_id"
).whenMatchedUpdate(
    condition="target.is_current = true",
    set={
        "is_current": lit(False),
        "valid_to": current_time
    }
).whenNotMatchedInsertAll()\
.execute()

# COMMAND ----------

# Defining current timestamp
current_time = current_timestamp()
output_path = "/dbfs/shared_uploads/revathy.s@diggibyte.com/scd/gold"

if not DeltaTable.isDeltaTable(spark, output_path):
    final_df.write.format("delta") \
          .mode("overwrite") \
          .option("path", output_path) \
          .saveAsTable(f"gold.gold_data")
else:
    old_delta_table = DeltaTable.forPath(spark, output_path)
    old_delta_table.alias("target").merge(
        final_df.alias("source"),
        "target.answer_id = source.answer_id AND target.is_current = true"
    ).whenMatchedUpdate(
        set={
            "last_updated": current_time  
        }
    ).whenNotMatchedInsertAll() \
     .whenNotMatchedBySourceUpdate(
         set={
             "is_current": lit(False),
             "last_updated": current_time
         }
     ).execute()


# COMMAND ----------

# Defining current timestamp
current_time = current_timestamp()
output_path = "/dbfs/shared_uploads/revathy.s@diggibyte.com/scd/gold"

if not DeltaTable.isDeltaTable(spark, output_path):
    final_df.write.format("delta") \
          .mode("overwrite") \
          .option("path", output_path) \
          .saveAsTable(f"gold.gold_data")
else:
    old_delta_table = DeltaTable.forPath(spark, output_path)
    old_delta_table.alias("target").merge(
        final_df.alias("source"),
        "target.answer_id = source.answer_id AND target.is_current = true"
    ).whenMatchedUpdate(
        set={
            "last_updated": current_time  
        }
    ).whenNotMatchedInsertAll() \
    .whenNotMatchedBySourceDelete()\
    .execute()
