import traceback
from src.main.utility.logging_config import *
class DataFrameWriter:
    def __init__(self,mode,data_format):
        self.mode = mode
        self.data_format = data_format

    def dataframe_writer(self,df, file_path,partition_column=None):
        try:
            if partition_column:
                partitioned_df=df.write.format(self.data_format) \
                    .option("header", "true") \
                    .mode(self.mode) \
                    .partitionBy(partition_column) \
                    .option("path", file_path)
            else:
                partitioned_df = df.write.format(self.data_format) \
                    .option("header", "true") \
                    .mode(self.mode) \
                    .option("path", file_path)
            partitioned_df.save()
        except Exception as e:
            logger.error(f"Error writing the data : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e