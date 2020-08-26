# Import statements
from pyspark.sql import SparkSession, HiveContext
import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pandas as pd
from tabulate import tabulate
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window
import json
import sqlite3
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from datetime import datetime

# Create Spark handler
logging.info("Creating Spark Handler")
spark = SparkSession.builder.appName('JNAssignt').enableHiveSupport().getOrCreate()

# 1. Read CSV File and generate new columns, including with reserved keywords
dummy_dataset = spark.read.option("header", "true").csv("dataset\\dummyrecords.csv")
logging.info("Printing Dummy Dataset")
dummy_dataset.show(truncate=False)
logging.info(
    "Creating Full Name column by concatenating Prefix,FirstName,Middle Initial and Last Name. Add a constant using lit")
dummy_dataset_fullname = dummy_dataset.withColumn("FullName",
                                                  F.concat("Name Prefix", F.lit(" "), "First Name", F.lit(" "),
                                                           "Middle Initial", F.lit(" "), "Last Name"))

logging.info("Lowercase City Name")
dummy_dataset_lcasecity = dummy_dataset_fullname.withColumn("table",
                                                            udf(lambda col: col.lower(), StringType())("City"))
dummy_dataset_lcasecity.show(truncate=False)

# 2. Read Dataframe with reserved keyword

res_col_dataset = spark.read.option("header", "true").csv("dataset\\table_and_dbname.csv")
res_col_dataset.createOrReplaceTempView("res_col_tbl")
spark.sql("Select * from res_col_tbl").show(truncate=False)
# The above statement is not returning an exception as expected according to documntation below:
# https://docs.databricks.com/data/data-sources/sql-databases.html#write-data-to-jdbc

# In case you get error replicate with withColumnRenamed
res_col_dataset_rnamed = res_col_dataset.withColumnRenamed("table","table_name")
res_col_dataset_rnamed.createOrReplaceTempView("res_col_tbl_rnamed")
spark.sql("Select * from res_col_tbl_rnamed").show(truncate=False)