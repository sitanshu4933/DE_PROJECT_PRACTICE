import os
from resources.dev import config
from resources.dev.config import *
from src.main.utility.logging_config import logger
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *
s3_client_provider = S3ClientProvider(decrypt(config.aws_access_key), decrypt(config.aws_secret_key))
s3_client = s3_client_provider.get_client()

local_file_path = "C:\\Users\\aksha\\Documents\\data_engineering\\spark_data\\sales_data_to_s3\\"
def upload_to_s3(s3_directory, s3_bucket, local_file_path):
    s3_prefix = f"{s3_directory}"
    logger.info("****** Uploading sample sales data to S3 *********")
    try:
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}{file}"
                s3_client.upload_file(local_file_path, s3_bucket, s3_key)
                logger.info(f"****** Uploaded '{file}' successfully at '{s3_prefix}' folder of '{bucket_name}' bucket *********")
    except Exception as e:
        raise e

upload_to_s3(s3_source_directory,bucket_name,local_file_path)