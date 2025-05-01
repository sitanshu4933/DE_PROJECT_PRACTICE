import traceback

from src.main.utility.logging_config import logger


class DatabaseReader:
    def __init__(self,url,properties):
        self.url = url
        self.properties = properties

    def create_dataframe(self,spark,table_name):
        try:
            df = spark.read.jdbc(url=self.url,
                                 table=table_name,
                                 properties=self.properties)
            if df:
                logger.info(f"Loaded {table_name} Table.")
                return df
            else:
                raise Exception
        except Exception as e:
            error_message = f"Error Loading Table: {e}"
            traceback_message = traceback.format_exc()
            logger.error("Got this error : %s", error_message)
            print(traceback_message)
            raise