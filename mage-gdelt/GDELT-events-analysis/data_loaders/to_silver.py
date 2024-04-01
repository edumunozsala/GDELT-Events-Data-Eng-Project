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
# Specify your data exporting logic here
    # Create the spark session, if it doesn't
    spark = (
        SparkSession.builder
        .appName('spark project')
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
    #bucket_name="mage-dezoomcamp-ems"
    bucket_name=kwargs['bucket_name']
    table_name=kwargs['table_name']

    source_root_path= f'gs://{bucket_name}/{kwargs['path']}/*.CSV'
    dest_root_path= f'gs://{bucket_name}/GDELT-Project/silver/{table_name}'
    
    #spark.sparkContext.addFile(url)
    # Read the csv data and save it into GCS in parquet format
    df = (spark.read
        .schema(schema)
        .format("parquet")
        .load(source_root_path)
        #.withColumn("partition_date", F.to_date(F.col("SQLDATE").cast("string"), "yyyyMMdd"))
        .select("GLOBALEVENTID",
            "SQLDATE",
            "MonthYear",
            "Year",
            "FractionDate",
            "Actor1Code",
            "Actor1Name",
            "Actor1CountryCode",
            "Actor1KnownGroupCode",
            "Actor1EthnicCode",
            "Actor1Religion1Code",
            "Actor1Type1Code",
            "Actor1Type2Code",
            "Actor1Type3Code",
            "Actor2Code",
            "Actor2Name",
            "Actor2CountryCode",
            "Actor2KnownGroupCode",
            "Actor2EthnicCode",
            "Actor2Religion1Code",
            "Actor2Type1Code",
            "Actor2Type2Code",
            "Actor2Type3Code",
            "IsRootEvent",
            "EventCode",
            "EventBaseCode",
            "EventRootCode",
            "NumMentions",
            "NumSources",
            "NumArticles",
            "AvgTone",
            "Actor1Geo_Type",
            "Actor1Geo_FullName",
            "Actor1Geo_CountryCode",
            "Actor1Geo_Lat",
            "Actor1Geo_Long",
            "Actor2Geo_Type",
            "Actor2Geo_FullName",
            "Actor2Geo_CountryCode",
            "Actor2Geo_Lat",
            "Actor2Geo_Long",
            "Actor2Geo_FeatureID",
            "ActionGeo_Type",
            "ActionGeo_FullName",
            "ActionGeo_CountryCode",
            "ActionGeo_Lat",
            "ActionGeo_Long",
            "ActionGeo_FeatureID",
            "DATEADDED",
            "SOURCEURL",
            "week"
    )
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("Year", "week")
    .save(dest_root_path)
    )    

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
