from pyspark.sql import functions as F
from pyspark.ml.feature import Imputer

from .bronze import TurbinesBronze
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
        else:
            self.max_date = None

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
        df_silver = bf_bronze.filter(
            (F.col('timestamp').isNotNull())
            & (F.col('turbine_id').isNotNull())
            & (F.col('timestamp') < F.current_timestamp())
        )

        if self.max_date:
            df_silver = df_silver.filter(
                (F.to_date(F.col('timestamp')) > self.max_date)
            )

        data_max_date = df_silver.select(F.to_date(F.max('timestamp'))).collect()[0][0]
        data_min_date = df_silver.select(F.to_date(F.min('timestamp'))).collect()[0][0]

        df_dummy = self.spark.createDataFrame([(1,)], ["dummy"])
        df_all_dates = df_dummy.select(
            F.explode(
                F.sequence(
                    F.to_timestamp(F.lit(f'{data_min_date} 00:00:00')),
                    F.to_timestamp(F.lit(f'{data_max_date} 00:00:00')),
                    F.expr('INTERVAL 1 HOUR')
                )
            ).alias("timestamp")
        )

        df_turbines = df_silver.select('turbine_id').distinct()
        df_all_dates_turbines = df_all_dates.join(df_turbines, how='cross')

        df_silver_missing_dates = df_all_dates_turbines.join(
            df_silver,
            on=['timestamp', 'turbine_id'],
            how='left'
        ).withColumn(
                'date', F.to_date(F.col('timestamp'))
            )

        impute_cols = [
                'wind_speed',
                'wind_direction',
                'power_output'
            ]
        imputer = Imputer(
            inputCols=impute_cols,
            outputCols=[
                'wind_speed_imputed',
                'wind_direction_imputed',
                'power_output_imputed'
            ],
        )

        model = imputer.fit(df_silver_missing_dates)
        df_silver_imputed = model.transform(df_silver_missing_dates)
        df_silver_imputed_with_cols = df_silver_imputed.withColumn(
            'imputed_cols',
            F.array(*[F.when(F.col(c).isNull(), F.lit(c)).otherwise(F.lit('null')) for c in impute_cols])
        ).withColumn(
            'imputed_cols',
            F.array_remove(
                F.col('imputed_cols'),
                'null'
            )
        )
        # combine imputed and non-imputed data and add load date for imputed rows
        df_silver_clean = df_silver_imputed_with_cols.withColumn(
            'power_output', F.coalesce(
                F.col('power_output'),
                F.col('power_output_imputed'),
            )
        ).withColumn(
            'wind_speed', F.coalesce(
                F.col('wind_speed'),
                F.col('wind_speed_imputed'),
            )
        ).withColumn(
            'wind_direction', F.coalesce(
                F.col('wind_direction'),
                F.col('wind_direction_imputed'),
            )
        ).withColumn(
            'load_date', F.coalesce(
                F.col('load_date'),
                F.current_date(),
            )
        ).drop(
            'power_output_imputed',
            'wind_speed_imputed',
            'wind_direction_imputed',
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
            'source.turbine_id = target.turbine_id AND source.timestamp = target.timestamp',
        )


if __name__ == "__main__":
    turbines_silver = TurbinesSilver()
    df_bronze = turbines_silver.extract()
    df_silver = turbines_silver.transform(df_bronze)
    turbines_silver.load(df_silver)
