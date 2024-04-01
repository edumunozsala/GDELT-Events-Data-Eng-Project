from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types
import pyspark.sql.functions as F

import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/personal-gcp.json"

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
    # Create the spark session, if it doesn't
    spark = (
        SparkSession.builder
        .appName('pyspark-run-with-gcp-bucket')
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
        #.config("spark.sql.repl.eagerEval.enabled", True) 
        .getOrCreate()
    )
    # Set GCS credentials if necessary
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    # Save the spark session in the context
    kwargs['context']['spark'] = spark

    # Set the GCS location to save the data
    bucket_name="mage-dezoomcamp-ems"
    project_id="banded-pad-411315"

    table_name="events"
    source_root_path= f'gs://{bucket_name}/GDELT-Project/{table_name}'
    dest_root_path= f'gs://{bucket_name}/GDELT-Project/silver/{table_name}'
    
    #spark.sparkContext.addFile(url)
    # Read the csv data and save it into GCS in parquet format
    df = (spark.read
        .format("parquet")
        .load(source_root_path)
        #.withColumn("partition_date", F.to_date(F.col("SQLDATE").cast("string"), "yyyyMMdd"))
    #.write
    #.mode("overwrite")
    #.format("parquet")
    #.partitionBy("partition_date")
    #.save(dest_root_path)
    )    

    print(df.show(3))

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
