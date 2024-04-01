from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os

#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/personal-gcp.json"

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    required_jars = [
        "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
        #"https://storage.googleapis.com/hadoop-lib/gcs/spark-bigquery-with-dependencies_2.12-0.36.1.jar"
        "https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.36.1.jar"
        #"./spark_dependencies/gcs-connector-hadoop3-latest.jar",
        #"./spark_dependencies/spark-avro_2.12-3.3.0.jar",
    ]

    # Create the spark session, if it doesn't
    spark = (
        SparkSession.builder
        .appName('load_to_bigquery')
        .config("spark.jars", ",".join(required_jars))
        #.config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
        #.config("spark.sql.repl.eagerEval.enabled", True) 
        .getOrCreate()
    )
    # Set GCS credentials if necessary
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    # Save the spark session in the context
    kwargs['context']['spark'] = spark

    # Set the GCS location to save the data
    bucket_name=os.environ['BUCKET_NAME']
    table_name=kwargs['table_name']
    source_root_path= f'gs://{bucket_name}/{kwargs["path"]}/{table_name}'
    print(source_root_path)
    
    # Read the csv data and save it into GCS in parquet format
    df_weeks= (
        spark.read
        .format("parquet")
        .load(source_root_path)
        .select(
            F.min("week").alias("min"),
            F.max("week").alias("max"),
        )
    )
    df= df_weeks.toPandas()

    print(df)
    print(df['min'][0])
    print(df['max'][0])
    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
