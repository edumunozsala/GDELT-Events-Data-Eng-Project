from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.types import *

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
    # Create the pipeline spark session and save in context
    print("Credentials: ", os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    """
    builder = (SparkSession
                .builder.appName('GCSFilesRead')
                .config("google.cloud.auth.service.account.enable", "true")\
                .config("google.cloud.auth.service.account.json.keyfile","/home/jovyan/work/gcs_admin.json")\
                .config('fs.gs.auth.type','SERVICE_ACCOUNT_JSON_KEYFILE')
    )
    spark = builder.getOrCreate()
    

    spark = SparkSession.builder.appName("titanic_to_gs").getOrCreate()
    path_to_key = SparkFiles.get("mygcpkey.json")
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("google.cloud.auth.service.account.json.keyfile", path_to_key)
    spark.conf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")

    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \

    spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")    
    
    spark = (
        SparkSession
        .builder
        .appName('Test spark')
        .getOrCreate()
    )
    """

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


    kwargs['context']['spark'] = spark
    # Create the schema for pyspark DataFrame
    fields = [T.StructField(dtype[0], globals()[f'{dtype[1]}Type']()) for dtype in dtypes]
    schema = StructType(fields)

    # Specify your data exporting logic here
    bucket_name="mage-dezoomcamp-ems"
    project_id="banded-pad-411315"

    table_name="events"
    root_path= f'gs://{bucket_name}/GDELT-Project/{table_name}'
    print(root_path)
    
    # Specify your data loading logic here
    url = "https://gdelt-open-data.s3.amazonaws.com/v2/events/202403230000.export.csv"

    spark.sparkContext.addFile(url)

    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(SparkFiles.get("20240318230000.export.csv"))
        #.load()
        #.select("PassengerId","Survived","Pclass","Name","Sex","Age")
        .write
        .mode("overwrite")
        .format("parquet")
        .save(root_path)
    )


    