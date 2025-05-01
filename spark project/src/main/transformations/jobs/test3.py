import shutil
import sys
import time
from datetime import date
from http.cookiejar import offset_from_tz_string
from sys import prefix

from botocore.configprovider import SmartDefaultsConfigStoreFactory
from jmespath.compat import with_str_method
from pyspark.sql.functions import concat_ws, lit, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, DoubleType

from resources.dev.config import *
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.spark_session import spark_session

spark = spark_session()

output_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", DoubleType(), True),
    StructField("additional_column", StringType(), True)
])

# Define the schema for reading the CSV (adjust based on your actual CSV structure)
csv_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", StringType(), True),  # Read as String initially
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", DoubleType(), True),
    # Add potential extra columns here if you know their names
])

final_df_to_process = None
first_file_processed = False
mandatory_columns_set = set(mandatory_columns)  # Convert to set for faster lookup

correct_files = ['C:\\Users\\aksha\\Documents\\data_engineering\\spark_data\\file_from_s3\\sales_data.csv','C:\\Users\\aksha\\Documents\\data_engineering\\spark_data\\file_from_s3\\sales_data_2025-04-12.csv']
for file_path in correct_files:
    try:
        data_df = spark.read.format('csv') \
            .option("header", "true") \
            .schema(csv_schema)  \
            .load(file_path)

        data_df = data_df.withColumn("sales_date", col("sales_date").cast(DateType()))

        data_columns = set(data_df.columns)
        extra_columns = list(data_columns - mandatory_columns_set)
        logger.info(f"Extra columns available for file '{file_path}' are- {extra_columns}")

        if extra_columns:
            data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
                .select(*output_schema.names)  # Select columns based on the final schema
            logger.info(f"Processed {file_path} and added 'additional_column'.")
        else:
            logger.info(f"No extra columns found in '{file_path}'.")
            data_df = data_df.withColumn("additional_column", lit(None).cast("string")) \
                .select(*output_schema.names)

        if not first_file_processed:
            final_df_to_process = data_df
            first_file_processed = True
        else:
            final_df_to_process = final_df_to_process.unionByName(data_df, allowMissingColumns=True) # Use unionByName for schema evolution

    except Exception as e:
        logger.error(f"Error processing file '{file_path}': {e}")
        # Decide how to handle file processing errors

logger.info("******* Final dataframe from Source which is going to be processed *******")
if final_df_to_process is not None:
    try:
        final_df_to_process.printSchema()
        final_df_to_process.show(2000)  # Show a limited number of rows
    except Exception as e:
        logger.error(f"An error occurred during final_df_to_process.show(): {e}")
        print("Pausing for 60 seconds to allow Spark UI inspection...")
        time.sleep(60)
        input("Press Enter to terminate the application...")
else:
    logger.warning("No files were processed, final_df_to_process is None.")

