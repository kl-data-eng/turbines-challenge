from pyspark.sql import functions as F, SparkSession
from turbines.utils.utils import get_spark_session


class TurbinesBronze:
    def __init__(self):
        """
        Initialise class and check if output table already exists.
        """
        self.spark = get_spark_session()
        self.spark.sql('CREATE DATABASE IF NOT EXISTS bronze')
        self.bronze_exists = self.spark.catalog.tableExists('turbines', 'bronze')
        if self.bronze_exists:
            self.max_date = self.spark.read.table(
                'bronze.turbines'
            ).withColumn(
                'date',
                F.to_date(F.col('timestamp'))
            ).groupBy('date').max().collect()[0]['max_date']

    def extract(self):
        """
        Read raw data and combine
        :return: Raw dataframe
        """
        df_group_1 = self.spark.read.format('csv').option('header', 'true').load('data/raw/data_group_1.csv')
        df_group_2 = self.spark.read.format('csv').option('header', 'true').load('data/raw/data_group_2.csv')
        df_group_3 = self.spark.read.format('csv').option('header', 'true').load('data/raw/data_group_3.csv')
        df_raw = df_group_1.unionByName(
            df_group_2
        ).unionByName(
            df_group_3
        )
        return df_raw

    def transform(self, df_raw):
        """
        Cast columns to correct types
        and filter for new data if output table already exists

        :param df_raw: Raw dataframe
        :return: cleaned bronze dataframe
        """

        df_bronze_cast = df_raw.withColumn(
            'timestamp', F.col('timestamp').cast('timestamp'),
        ).withColumn(
            'turbine_id', F.col('turbine_id').cast('string'),
        ).withColumn(
            'wind_speed', F.col('wind_speed').cast('double'),
        ).withColumn(
            'wind_direction', F.col('wind_direction').cast('integer'),
        ).withColumn(
            'power_output', F.col('power_output').cast('double'),
        )

        if self.bronze_exists:
            # if the table already exists only ingest data since last ingestion
            df_bronze_cast = df_bronze_cast.filter(
                F.to_date(F.col('timestamp')) > self.max_date
            )

        df_bronze_audit = df_bronze_cast.withColumn(
            'load_date', F.current_date(),  # add load date for audit
        )

        return df_bronze_audit

    def load(self, df_bronze):
        """
        Appends data into bronze table

        :param df_bronze: Bronze dataframe
        :return: None
        """
        # append because we're assuming we're only ingesting new data
        df_bronze.write.mode(
            'append'
            ).format(
                'delta'
            ).saveAsTable('bronze.turbines')


if __name__ == "__main__":
    turbines_bronze = TurbinesBronze()
    df_raw = turbines_bronze.extract()
    df_bronze = turbines_bronze.transform(df_raw)
    turbines_bronze.load(df_bronze)
