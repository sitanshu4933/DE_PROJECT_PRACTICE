import shutil
import sys


from pyspark.sql.functions import concat_ws, lit, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

from resources.dev.config import *
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_calculation_table_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import DataFrameWriter

s3_client_provider=S3ClientProvider(decrypt(aws_access_key),decrypt(aws_secret_key))
s3_client=s3_client_provider.get_client()
logger.info(f"Connected to S3 at :{s3_client}")
logger.info(f"List of buckets :{s3_client.list_buckets()['Buckets']}")


# check local directory already has a file.
# If yes then check if this file is present in stage table with status 'A'.
# If so then don't delete and try to re-run the file.
# Else give an error and don't process the next file.


csv_files=[file for file in os.listdir(local_directory) if file.endswith('.csv')]
mysql_connection=get_mysql_connection()
cursor=mysql_connection.cursor()

if csv_files:
    statement=f'''SELECT DISTINCT FILE_NAME FROM {database_name}.{product_staging_table} 
                    WHERE FILE_NAME IN ({str(csv_files)[1:-1]}) AND STATUS='A';'''

    logger.info(f"Dynamically statement created -{statement}")
    data=cursor.execute(statement)
    if data:
        logger.info("Your last run was failed, please check.")
    else:
        logger.info("No record matched.")
else:
    logger.info("Last run was successful.")

try:
    s3_reader=S3Reader()
    s3_absolute_file_path=s3_reader.list_files(s3_client,bucket_name,s3_source_directory)
    if s3_absolute_file_path:
        logger.info(f"Absolute path available on s3 bucket for csv files {s3_absolute_file_path}")
    else:
        logger.info(f"No files available at s3 path-'{s3_source_directory}'")
        raise Exception("No data available to process")
except Exception as e:
    logger.error(f"Exited with error :{e}")
    raise e

prefix=f"s3://{bucket_name}/"
file_paths=[url[len(prefix):] for url in s3_absolute_file_path]
logger.info(f"File path available on s3 under '{bucket_name}' bucket is -{file_paths}")
try:
    downloader=S3FileDownloader(s3_client,bucket_name,local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error(f"File download error- {e}")
    sys.exit()

downloaded_files=os.listdir(local_directory)
if downloaded_files:
    csv_files=[]
    error_files=[]
    for file in downloaded_files:
        if file.endswith('.csv'):
            csv_files.append(os.path.abspath(os.path.join(local_directory,file)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory,file)))
    if not csv_files:
        logger.error("No CSV data available to process the request.")
        raise Exception("No CSV data available to process the request.")
else:
    logger.error("There is no data to process.")
    raise Exception("No CSV data available to process.")

logger.info("**************** Listing the Files ******************")
logger.info(f"list of csv files needs to be processed -{csv_files}.")

logger.info("**************** Creating Spark session ******************")
spark=spark_session()
logger.info("**************** Spark Session started ******************")

# check the required columnin the schema of csv file.
# If not required columns keept it in a list or error files.
# Else union all the data into one dataframe.

logger.info("************** Checking Schema for data loaded in S3 **************")

correct_files=[]
for data in csv_files:
    data_schema=spark.read.format("csv")\
                .option("header","True")\
                .load(data).columns
    logger.info(f"Schema for {data} is {data_schema}.")
    logger.info(f"Mandatory columns schema are : {mandatory_columns}.")

    missing_columns=set(mandatory_columns)-set(data_schema)

    if missing_columns:
        logger.info(f"Missing columns are - {missing_columns}.")
        logger.info("-------------------------------------------------------------------------------------------------------------------------")
        error_files.append(data)
    else:
        logger.info(f"No missing columns for the '{data}'.")
        logger.info("-------------------------------------------------------------------------------------------------------------------------")
        correct_files.append(data)

logger.info(f"*********** List of correct files ***********{correct_files}")
logger.info(f"*********** List of missing files ***********{error_files}")
logger.info(f"*********** Moving error data to Error directory if any ***********")

if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name=os.path.basename(file_path)
            destination_path=os.path.join(error_folder_path_local,file_name)

            shutil.move(file_path,destination_path)
            logger.info(f"Successfully moved data from '{file_path}' to '{destination_path}'.")

            logger.info(f"*********** Moving error data at S3 to Error directory if any ***********")
            message=move_s3_to_s3(s3_client,bucket_name,s3_source_directory,s3_error_directory,file_name)
            logger.info(f"{message}")
        else:
            logger.info(f"'{file_path}' doesn't exist.")
else:
    logger.info("*********** No Error files in our dataset ***********")

# Additional columns needs to be taken care of
# Determine Extra columns
#
# Before running the process stage table needs to be updated with
# Active(A) or Inactive(I)
#
logger.info("******* Updating the product_stagging_table that we have started the process *******")
insert_statements=[]
if correct_files:
    for file_path in correct_files:
        statements=f"INSERT INTO {database_name}.{product_staging_table} "\
                    f"(FILE_NAME, FILE_LOCATION, CREATED_DATE, STATUS) "\
                    f"VALUES ('{os.path.basename(file_path)}','{file_path}',CURRENT_TIMESTAMP(),'A');"

        insert_statements.append(statements)
    logger.info(f"Inserted Statements created for staging table - {insert_statements}")
    logger.info("*************** Connecting with My Sql Server ***************")
    connection=get_mysql_connection()
    cursor=connection.cursor()
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("********** There is no files to process **********")
    raise Exception("No data available for correct files.")

logger.info("********** Staging Table updated successfully **********")
logger.info("********** Fixing Extra column in dataset **********")

schema=StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("store_id", IntegerType(), True),
    StructField("Product_name", StringType(), True),
    StructField("sales_data", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

final_df_to_process=spark.createDataFrame([],schema)
first_file_processed= False
for data in correct_files:
    data_df=spark.read.format('csv')\
            .option("header","true")\
            .option("inferSchema","true")\
            .load(data)
    data_schema=data_df.columns
    extra_columns=list(set(data_schema)-set(mandatory_columns))
    logger.info(f"Extra columns available for file '{data}' are- {extra_columns}")
    if extra_columns:
        data_df=data_df.withColumn("additional_column",concat_ws(", ",*extra_columns))\
                .select('customer_id', 'store_id', 'product_name', 'sales_date', 'sales_person_id', 'price', 'quantity', 'total_cost','additional_column')
        logger.info(f"Processed {data} and added 'additional_column'.")
    else:
        data_df=data_df.withColumn("additional_column", lit(None)) \
            .select('customer_id', 'store_id', 'product_name', 'sales_date', 'sales_person_id', 'price', 'quantity',
                    'total_cost', 'additional_column')
    if not first_file_processed:
        final_df_to_process=data_df
        first_file_processed=True
    else:
        final_df_to_process=final_df_to_process.union(data_df)

logger.info("******* Final dataframe from Source which is going to be processed *******")
final_df_to_process.show()

# Enrich the data from all dimension table
# also create datamart for sales_team and their incentive, address and all
# another datamart for Customer who bought how much  each days of month
# for every month there should be a file and inside that ther should be  a store_id segreation
# Read the data from paquet and generate a csv correct_files in which
# there will be sales_person_name,sales_person_store_id, sales_person_total_billing_done_for_each_month, total_incentive

# connecting with DatabaseReader
database_client=DatabaseReader(url,properties)

# Creating DataFrame for All tables
logger.info("*************** Loading Customer Table to customer_df ***************")
customer_df=database_client.create_dataframe(spark,customer_table_name)

logger.info("*************** Loading Product Table to product_df ***************")
product_df=database_client.create_dataframe(spark,product_table)

logger.info("*************** Loading staging Table into product_staging_table ***************")
product_staging_table_df=database_client.create_dataframe(spark,product_staging_table)

logger.info("*************** Loading Sales Team Table to sales_df ***************")
sales_df=database_client.create_dataframe(spark,sales_team_table)

logger.info("*************** Loading Store Table to store_df ***************")
store_df=database_client.create_dataframe(spark,store_table)

s3_customers_store_sales_df_join=dimesions_table_join(final_df_to_process,
                                                      customer_df,
                                                      store_df,
                                                      sales_df)

logger.info("************ Final enriched Data **************")
s3_customers_store_sales_df_join.show()

# write the customer data to customer data mart in parquet format
# File will be written to local first
# move the raw data to s3 for reporting
# Write reporting data to mssql also

logger.info("******** Write the data into Customer Data Mart ************")
final_customer_data_mart_df=s3_customers_store_sales_df_join.select(
    'ct.customer_id','ct.first_name','ct.last_name','ct.address','ct.pincode','phone_number','sales_date','total_cost')

logger.info("******* Final Data for Customer Data Mart ***********")
final_customer_data_mart_df.show()

parquet_writer=DataFrameWriter('overwrite','parquet')
parquet_writer.dataframe_writer(final_customer_data_mart_df,customer_data_mart_local_file)


#Move Customer data mart file from local to s3 bucket
logger.info("****** Moving Customer Data Mart file from local to S3 ********")
s3_uploader=UploadToS3(s3_client)
message=s3_uploader.upload_to_s3(s3_customer_datamart_directory, bucket_name, customer_data_mart_local_file)
logger.info(f"{message}")

logger.info("******** Write the data into Sales Data Mart ************")
final_sales_data_mart_df=s3_customers_store_sales_df_join.select(
    'store_id','sales_person_id','sales_person_first_name','sales_person_last_name','store_manager_name','manager_id','is_manager','sales_person_address',
    'sales_person_pincode','sales_date','total_cost',expr("substring(sales_date,1,7) as sales_month"))

logger.info("******* Final Data for Sales Data Mart ***********")
final_sales_data_mart_df.show()

parquet_writer=DataFrameWriter('overwrite','parquet')
parquet_writer.dataframe_writer(final_sales_data_mart_df,sales_team_data_mart_local_file)


#Move Sales data mart file from local to s3 bucket
logger.info("****** Moving Sales Data Mart file from local to S3 ********")
s3_uploader=UploadToS3(s3_client)
message=s3_uploader.upload_to_s3(s3_sales_datamart_directory, bucket_name, sales_team_data_mart_local_file)
logger.info(f"{message}")

logger.info("******* Also writing sales data to partitioned format ***********")

parquet_writer=DataFrameWriter('overwrite','parquet')
parquet_writer.dataframe_writer(final_sales_data_mart_df,sales_team_data_mart_partitioned_local_file,["sales_month","store_id"])


#Move Partitioned Sales data mart file from local to s3 bucket
logger.info("****** Moving Partitioned Sales Data Mart file from local to S3 ********")
s3_uploader=UploadToS3(s3_client)
message=s3_uploader.upload_to_s3(s3_sales_datamart_directory, bucket_name, sales_team_data_mart_partitioned_local_file)
logger.info(f"{message}")

# calculation for Customer mart
# find out the customer total purchase every month
# write the data into MySql table

logger.info("******** Calculating customer's purchased amount each month ***********")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("******* Calculation of customer mart done and written to the table ********")
#
# calculating for sales team mart
# find out total sales done by each sales peson every month
# Give the Top performer 1% incentive of total sales of the month
# rest sales person will get nothing
# write the data to MySql Table

logger.info("******** Calculating sales person's sale amount each month ***********")
sales_mart_calculation_table_write(final_sales_data_mart_df)
logger.info("******* Calculation of customer mart is done and written to the table ********")

############ Last Step ###############
# moving the file on s3 into processed folder and delete the local directory

message=move_s3_to_s3(s3_client,bucket_name,s3_source_directory,s3_processed_directory)
logger.info(message)

logger.info("********** Deleting sales team data from local *************")
delete_local_file(sales_team_data_mart_local_file)
logger.info("********** Successfully Deleting sales team data from local *************")

logger.info("********** Deleting sales team partitioned data from local *************")
delete_local_file(sales_team_data_mart_partitioned_local_file)
logger.info("********** Successfully deleting sales team partitioned data from local *************")

logger.info("********** Deleting Customer data from local *************")
delete_local_file(customer_data_mart_local_file)
logger.info("********** Successfully deleting Customer data from local *************")

logger.info("********** Deleting local data from local *************")
delete_local_file(local_directory)
logger.info("********** Successfully deleting local data from local *************")

#update staging table
update_statements=[]
if correct_files:
    for file_path in correct_files:
        statements=f"UPDATE {database_name}.{product_staging_table} "\
                    f"SET STATUS='I' ,UPDATED_DATE=CURRENT_TIMESTAMP() "\
                    f"WHERE FILE_NAME='{os.path.basename(file_path)}'"

        update_statements.append(statements)
    logger.info(f"Update Statements created for staging table - {update_statements}")
    logger.info("*************** Connecting with My Sql Server ***************")
    connection=get_mysql_connection()
    cursor=connection.cursor()
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("********** There is some erorr inprocess in between **********")
    sys.exit()

input("Press enter to terminate")