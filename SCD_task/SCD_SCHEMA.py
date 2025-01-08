# Databricks notebook source
from pyspark.sql.functions import col, lit, current_timestamp, when,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
from delta.tables import DeltaTable

# COMMAND ----------

schema = StructType([
    StructField("country", StringType(), True),
    StructField("data", ArrayType(
        StructType([
            StructField("date_time", StringType(), True),
            StructField("Key_in_date", StringType(), True),
            StructField("lastmodified_date", StringType(), True),
            StructField("task_id", StringType(), True),
            StructField("store-id", StringType(), True),
            StructField("member_login_id", StringType(), True),
            StructField("check_in", StringType(), True),
            StructField("check_out", StringType(), True),
            StructField("user_location", ArrayType(DoubleType()), True),
            StructField("distance", StringType(), True),
            StructField("questions", ArrayType(
                StructType([
                    StructField("type", StringType(), True),
                    StructField("question", StringType(), True),
                    StructField("question_id", StringType(), True),
                    StructField("answer", ArrayType(
                        StructType([
                            StructField("product_id", StringType(), True),
                            StructField("product_code", StringType(), True),
                            StructField("price_gross", DoubleType(), True),
                            StructField("price_net", DoubleType(), True),
                            StructField("price_discount", DoubleType(), True),
                            StructField("price_incentive", DoubleType(), True),
                            StructField("sales_unit", DoubleType(), True),
                            StructField("sales_gross", DoubleType(), True),
                            StructField("sales_net", DoubleType(), True),
                            StructField("promotion", StringType(), True),
                            StructField("answer_id", StringType(), True)
                        ])
                    ), True)
                ])
            ), True)
        ])
    ), True),
    StructField("total", StringType(), True),
    StructField("is_end", StringType(), True)
])


