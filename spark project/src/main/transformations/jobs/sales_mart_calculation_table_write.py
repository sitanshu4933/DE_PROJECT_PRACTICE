
from pyspark.sql import Window
from pyspark.sql.functions import lit, when, col, rank, concat, desc, sum, round

from resources.dev import config
from resources.dev.config import properties
from src.main.utility.logging_config import logger
from src.main.write.database_write import DatabaseWriter


def sales_mart_calculation_table_write(final_sales_data_mart_df):
    window=Window.partitionBy('store_id','sales_person_id','sales_month')
    final_sales_data_mart=final_sales_data_mart_df \
        .withColumn('total_sales_every_month',sum('total_cost').over(window)) \
        .select('store_id','sales_person_id',
                concat(col('sales_person_first_name'), lit(' '),col('sales_person_last_name')).alias('Full_Name'),
                'sales_month','total_sales_every_month').distinct()

    rank_window=Window.partitionBy('store_id','sales_month')\
        .orderBy(desc('total_sales_every_month'))
    final_sales_data_mart_df_table=final_sales_data_mart \
        .withColumn('rnk',rank().over(rank_window)) \
        .withColumn('incentive',round(when(col('rnk')==1,col('total_sales_every_month')*0.01).otherwise(lit(0)),2)) \
        .withColumn('total_sales',col('total_sales_every_month')) \
        .select('store_id','sales_person_id','Full_Name','sales_month','total_sales','incentive')

    logger.info("***** Writing the data into sales_team data mart ********")
    final_sales_data_mart_df_table.show()
    db_writer=DatabaseWriter(url=config.url,properties=properties)
    db_writer.write_dataframe(final_sales_data_mart_df_table,final_sales_data_mart_df_table)
