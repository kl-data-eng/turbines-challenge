from pyspark.sql import functions as F

from turbines.utils.utils import get_spark_session, delta_upsert

class TurbinesGold:
    def __init__(self):
        """
        Initialises class and creates database if it already exists
        """
        self.spark = get_spark_session()
        self.spark.sql('CREATE DATABASE IF NOT EXISTS gold')

    def extract(self):
        """
        Read silver dataframe
        :return: Silver dataframe
        """
        df_silver = self.spark.read.table('silver.turbines')
        return df_silver

    def transform(self, df_silver):
        """
        Calculates daily aggregated stats and anomalies
        Removes anomalies before calculating aggregated stats
        :param df_silver:
        :return:
        """
        df_anomaly_threshold = df_silver.withColumn(
            'date', F.to_date(F.col('timestamp'))
        ).groupby(
            'turbine_id'
        ).agg(
            F.stddev('power_output').alias('stddev_power_output'),
            F.mean('power_output').alias('mean_power_output')
        ).withColumn(
            'anomaly_upper_threshold',
            F.col('mean_power_output') + 2 * F.col('stddev_power_output')
        ).withColumn(
            'anomaly_lower_threshold',
            F.col('mean_power_output') - 2 * F.col('stddev_power_output')
        ).drop('stddev_power_output', 'mean_power_output')

        df_anomaly_status = df_silver.join(
            df_anomaly_threshold,
            on='turbine_id',
            how='left'
        )
        anomaly_filter = ((
                    df_silver.power_output < df_anomaly_threshold.anomaly_lower_threshold
            ) |
            (
                    df_silver.power_output > df_anomaly_threshold.anomaly_upper_threshold
            ))
        df_anomalies = df_anomaly_status.filter(
            anomaly_filter
        )

        df_summary_stats = df_anomaly_status.filter(
            ~anomaly_filter
        ).withColumn(
            'date', F.to_date(F.col('timestamp'))
        ).groupby(  # aggregating over 24 hours
            'turbine_id',
            'date',
        ).agg(
            F.min('power_output').alias('min_power_output'),
            F.max('power_output').alias('max_power_output'),
            F.mean('power_output').alias('mean_power_output'),
        )
        return df_summary_stats, df_anomalies

    def load(self, df_summary_stats, df_anomalies):
        """
        Upserts aggregated stats dataframe and anomalies dataframe into output tables
        :param df_summary_stats: Summary stats dataframe
        :param df_anomalies: Anomalies dataframe
        :return:
        """
        delta_upsert(
            self.spark,
            df_summary_stats,
            self.spark.catalog.tableExists('turbine_summary_stats', 'gold'),
            'gold.turbine_summary_stats',
            ['date'],
            "source.turbine_id = target.turbine_id AND source.date = target.date",
        )

        delta_upsert(
            self.spark,
            df_anomalies,
            self.spark.catalog.tableExists('turbine_anomalies', 'gold'),
            'gold.turbine_anomalies',
            ['date'],
            "source.turbine_id = target.turbine_id AND source.timestamp = target.timestamp",
        )


if __name__ == "__main__":
    turbines = TurbinesGold()
    df_silver = turbines.extract()
    df_summary_stats, df_anomalies = turbines.transform(df_silver)
    turbines.load(df_summary_stats, df_anomalies)
