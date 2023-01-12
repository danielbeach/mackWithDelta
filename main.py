from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import logging
import mack
from delta import DeltaTable


def read_hard_drive_data(spark: SparkSession, location: str) -> DataFrame:
    df = spark.read.csv(location, header='true')
    return df


def transform_data(input_df: DataFrame) -> DataFrame:
    output = input_df.select(F.col("date").cast("date"),
                             F.col("serial_number"),
                             F.col("model"),
                             F.col("capacity_bytes").cast("bigint"),
                             F.col("failure").cast("boolean")
                             )
    return output


def push_data_to_delta(input_df: DataFrame) -> None:
    input_df.write \
        .format("delta") \
        .mode("append") \
        .save("/app/spark-warehouse/test.db/backblaze_hard_drives")


def main():
    spark = SparkSession.builder.appName('TestMack') \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
        .config("spark.sql.legacy.setCommandRejectsSparkCoreConfs", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.warehouse.dir", "/app/dw") \
        .enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logging.basicConfig(format='%(asctime)s %(levelname)s - Test Mack - %(message)s', level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)

    spark.sql("CREATE DATABASE IF NOT EXISTS test;")
    spark.sql("DROP TABLE IF EXISTS test.backblaze_hard_drives;")
    spark.sql("""
                CREATE TABLE IF NOT EXISTS test.backblaze_hard_drives
                    (
                        date DATE NOT NULL,
                        serial_number STRING,
                        model STRING,
                        capacity_bytes BIGINT,
                        failure BOOLEAN
                    )
                    USING DELTA
                    LOCATION '/app/spark-warehouse/test.db/backblaze_hard_drives';
                """)

    df = read_hard_drive_data(spark, location='data/*.csv')
    df = transform_data(df)
    push_data_to_delta(df)

    # Duplicate Removal
    extra = spark.createDataFrame([('2022-07-01', 'PL1331LAGDJ5GH',
                                    'HGST HDS5C4040ALE630', 4000787030016,
                                    0)],
                                    ['date', 'serial_number', 'model',
                                     'capacity_bytes', 'failure'])
    extra = extra.withColumn('date', F.col('date').cast('date'))\
        .withColumn('failure', F.col('failure').cast('boolean'))
    push_data_to_delta(extra)

    temp = spark.sql("""SELECT * 
                        FROM test.backblaze_hard_drives 
                        WHERE serial_number = 'PL1331LAGDJ5GH' AND
                         date = CAST('2022-07-01' as DATE);
                         """)
    temp.show()
    delta_table = DeltaTable.forPath(spark, "/app/spark-warehouse/test.db/backblaze_hard_drives")
    mack.kill_duplicates(delta_table, ["date", "serial_number", "model", "capacity_bytes", "failure"])
    temp = spark.sql("""SELECT * 
                            FROM test.backblaze_hard_drives 
                            WHERE serial_number = 'PL1331LAGDJ5GH' AND
                             date = CAST('2022-07-01' as DATE);
                             """)
    temp.show()

    # Validate appends
    delta_table = DeltaTable.forPath(spark, "/app/spark-warehouse/test.db/backblaze_hard_drives")
    extra = spark.createDataFrame([('2022-07-01', 'PL1331LAGDJ5GH',
                                    'HGST HDS5C4040ALE630', 4000787030016,
                                    0, 1, 'whale')],
                                  ['date', 'serial_number', 'model',
                                   'capacity_bytes', 'failure', 'bag-o-bones', 'ahab'])
    extra = extra.withColumn('date', F.col('date').cast('date')) \
        .withColumn('failure', F.col('failure').cast('boolean'))

    mack.validate_append(
        delta_table,
        extra,
        required_cols=['date', 'serial_number', 'model', 'capacity_bytes', 'failure'],
        optional_cols=["ahab"],
    )




if __name__ == '__main__':
    main()
