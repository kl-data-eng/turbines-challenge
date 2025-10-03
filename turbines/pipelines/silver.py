from pyspark.sql import functions as F
from pyspark.ml.feature import Imputer
from delta.tables import DeltaTable

from turbines.utils.utils import get_spark_session, delta_upsert

class TurbinesSilver:
    def __init__(self):
        """
        Initialise class and check if output table already exists.
        """
        self.spark = get_spark_session()
        self.spark.sql('CREATE DATABASE IF NOT EXISTS silver')
        self.silver_exists = self.spark.catalog.tableExists('turbines', 'silver')
        if self.silver_exists:
            self.max_date = self.spark.read.table(
                'silver.turbines'
            ).withColumn(
                'date',
                F.to_date(F.col('timestamp'))
            ).groupBy('date').max().collect()[0]['max_date']

    def extract(self):
        """
        Read bronze table
        :return: Bronze dataframe
        """
        df_bronze = self.spark.read.table('bronze.turbines')
        return df_bronze

    def transform(self, bf_bronze):
        """
        Filter for new data,
        remove nulls and incorrect timestamps
        and impute missing data

        :param bf_bronze: Bronze dataframe
        :return: Silver dataframe
        """
        df_silver = bf_bronze.withColumn(
                'date', F.to_date(F.col('timestamp'))
            ).filter(
            (F.col('timestamp').isNotNull())
            & (F.col('date').isNotNull())
            & (F.col('turbine_id').isNotNull())
            & (F.col('wind_speed').isNotNull())
            & (F.col('wind_direction').isNotNull())
            & (F.col('timestamp') < F.current_timestamp())
            & (F.to_date(F.col('timestamp')) > self.max_date)
        )

        imputer = Imputer(
            inputCols=['power_output'],
            outputCols=['power_output_imputed'],
        )
        model = imputer.fit(df_silver)
        df_silver_imputed = model.transform(df_silver)
        df_silver_clean = df_silver_imputed.withColumn(
                'imputed',
                F.when(
                    F.col('power_output').isNull(),
                    1
                ).otherwise(0)
            ).withColumn(
            'power_output', F.coalesce(
                F.col('power_output'),
                F.col('power_output_imputed'),
            )
        )
        return df_silver_clean

    def load(self, df):
        """
        Upserts silver data into silver table

        :param df: Silver dataframe
        :return: None
        """
        delta_upsert(
            self.spark,
            df,
            self.silver_exists,
            'silver.turbines',
            ['date'],
            "source.turbine_id = target.turbine_id AND source.timestamp = target.timestamp",
        )


if __name__ == "__main__":
    turbines_silver = TurbinesSilver()
    df_bronze = turbines_silver.extract()
    df_silver = turbines_silver.transform(df_bronze)
    turbines_silver.load(df_silver)
