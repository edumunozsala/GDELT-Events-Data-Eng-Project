from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql import types
from pyspark.sql.types import *
from pyspark.sql import functions as F

import os

from mage_ai.orchestration.triggers.api import trigger_pipeline

#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/personal-gcp.json"

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
    # Create the schema
    dtypes = data.to_records(index=False).tolist() 
    fields = [types.StructField(dtype[0], globals()[f'{dtype[1]}Type']()) for dtype in dtypes]
    schema = StructType(fields)
    print(schema)
    # Extract days to collect data
    # IMPORTANT: We include a limit of 90 days,
    # You can remove or change it
    days_to_collect=int(os.environ['DAYS_TO_COLLECT'])
    if days_to_collect>90:
        days_to_collect=90

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
    bucket_name=os.environ['BUCKET_NAME']
    project_id=os.environ['PROJECT_ID']

    table_name=kwargs['table_name']#"events"
    source_files= f'gs://{bucket_name}/{kwargs["path"]}/*.csv'
    print(source_files)
    root_path= f'gs://{bucket_name}/{kwargs["dest_path"]}/{table_name}'
    print(root_path)
    
    # Set the url where the csv data is
    #url = "https://gdelt-open-data.s3.amazonaws.com/v2/events/20240318230000.export.csv"
    #url = "https://gdelt-open-data.s3.amazonaws.com/v2/events/20240324161500.gkg.csv"
    #url="/home/src/extracted/20240324160000.export.CSV"

    #spark.sparkContext.addFile(url)
    # Read the csv data and save it into GCS in parquet format
    (spark.read
        .schema(schema)
        .csv(source_files, sep="\t")
        .withColumn("week", F.weekofyear(F.to_date("SQLDATE","yyyyMMdd")))
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
        .where(F.datediff(F.to_date(F.current_timestamp()), F.to_date("SQLDATE","yyyyMMdd")) <=days_to_collect)
        .repartition("week")
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("week")
        .save(root_path)
    )
    

