from pyspark.sql import SparkSession
from delta.tables import DeltaTable

def get_spark_session():
    return SparkSession.builder.master(
        'local[*]'
    ).config(
        'spark.sql.warehouse.dir', 'spark-warehouse'
    ).config(
        'spark.sql.sources.default', 'delta'
    ).getOrCreate()

def delta_upsert(
        spark,
        df,
        table_exists,
        path,
        partition_columns,
        merge_statement,
):
    if table_exists:
        target_table = DeltaTable.forPath(spark, path)
        (
            target_table.alias('target')
            .merge(
                df.alias('source'),
                merge_statement
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format(
            'delta'
        ).mode(
            'overwrite'
        ).partitionBy(
            *partition_columns
        ).saveAsTable(path)