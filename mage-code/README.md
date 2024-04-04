# Code Explanation

The project is built on Mage and consists of the next stages and pipelines, let's describe them:

## Load Bronze
Here, we create our bronze stage in the data lake on Cloud Storage

- **Load_bronze_gcs Pipeline**

    1. **Load_zip_gcs**: 
        - Read the file with all the CSV zipped files in the GDELT Project
        - Extract from this list the filenames of the files created in the last n days (n is a variable, in our example, n=45).
            You can identify the files because they include the date in the filename
        - For each filename we download it, unzipped it and concatenate its csv content to a new file. These operations are executed on memory for a fast execution.
            **Note**: we concatenate 30 files in one file before saving it to the data lake. The source csv files are very small and it is much faster to read them in memory and group 30 of them in one file.
        - Save the CSV generated files to a bucket in Cloud Storage and the path `GDELT-Project/bronze/csv`.

    2. **run_to_silver**:
        - This block triggers the next pipeline to execute: load_silver.

## Load Silver
Here, we create our silver stage in the data lake on Cloud Storage

- **load_silver Pipeline**

    1. **load_events_schema**:
        - Download a file from the URL https://raw.githubusercontent.com/linwoodc3/gdelt2HeaderRows/master/schema_csvs/GDELT_2.0_Events_Column_Labels_Header_Row_Sep2016.csv that contains the schema of the CSV files of the GDELT Project.
        - Create a pandas Dataframe with the content of that file, remove unnecessary fields.

    2. **export_silver**:
        - Using the dataframe from the previous step, create an array of `StructFields`, the schema to be used when reading the CSV files to a Spark Dataframe.
        - Create the Spark Session
        - Read the CSV files from the bronze stage, `GDELT-Project/bronze/csv` folder, into a Spark Dataframe using the schema defined
        - Create a new column named "week", containing the week number
        - Select the columns of the dataframe we are interested in
        - Filter the dataframe: select rows with the date between today and today-n days (n=45)
            *There are some rows from dates out of the desired range, we want to exclude them.*
        - Repartition the dataframe on the week column, created previously.
            If not, every partition has several very small files and this is not very efficient.
        - Write the dataframe to the Silver stage, `GDELT-Project/Silver/events`, in parque format and partitioned by week

    3. **run_to_gold**:
        - Triggers the next Pipeline: load_gold_bq

## Load Gold:
Here, we create our gold stage in the data warehouse on BigQuery

- **load_gold_bq Pipeline**

    1. **min_max_week**: 
        - Create the Spark session with the required jars
        - Read the data in the silver stage in parquet format into a Spark dataframe
        - Extract the min and max value of the week column
        - Return a pandas dataframe with those two values

    2. **export_gold_bq**:
        - Read the data from the silver stage in parquet format into a Spark dataframe
        - Write the dataframe into a BigQuery Table named `Events` 
        - Partitioned by `week`, setting the range between min and max week (we get these values from the pandas dataframe)

    3. **run_lookups**:
        - Triggers the next pipeline: load_lookups

## Load lookup
Here, we read some lookup tables from the GDELT Project website to enrich some fields of our main data table: events. We save this data into our bronze and silver stage in CSV format and into our gold stage in the format of a BigQuery Table.

- **load_lookups Pipeline**
    1. **read_lookups**:
        - Create a list of URLs pointing to all the lookup tables. They all consist of a code or type and a label fields.
        - Download every lookup table in CSV format and concatenate them all.
        - Return a pandas dataframe containing all the tables.

    2. **export_df_bq**:
        - Write the pandas dataframe into a BigQuery table, `Lookup` in the gold stage.

    3. **export_df_gcs**:
        - Write the pandas dataframe into a CSV file in the bronze stage, `GDELT-Project/bronze/lookup`, and into the silver stage, `GDELT-Project/silver/lookup`.

    4. **run_staging_dm**:
        - Run the next pipeline: staging_dm

## Load Datamart
Here, we prepare and create some views and fact tables to aggregate values that can help to create the reports. For our example, we only create two dimension tables, Actor Type and Event Code, and two facts table, one table aggregates by the source actor type (Actor1) and the destination actor type (Actor2) summing up the events, articles, sources and mentions. The other one aggregates by event code.

- **staging_dm Pipeline**
    This pipeline creates a "staging area" that consists of a view for both the dimension and facts tables. The idea is to use these views to validate what will be created in the next stage, the tables that our final users will use in their reports.


    In our example, the views are very simple and can be easy understand:
        - `staging/stg_actor_type`: extracts the code and label from the lookup table where table is 'type'
        - `staging/stg_event_codes`: extracts the code and label from the lookup table where table is 'eventcodes'
        - `staging/stg_events_by_code`: group by date, week, root event code, base event code, event code and sum up events, articles, mentions and sources on the Events table.
        - `staging/stg_events_types`: group by date, week, Actor1 type and Actor2 type and sum up events, articles, mentions and sources on the Events table.
        - `staging/schema`

    - **run_load_dm**
        - run the next pipeline: load_dm
        This time we create the dimension and fact tables from the views created previously.



- **load_dm Pipeline**
    This time we create the dimension and fact tables from the views created previously. The facts tables are joined to the dimension tables to include the description or labels of the codes, so users can understand what the fields contain.
    - `core_dm/dim_actor_types`
    - `core_dm/dim_event_codes`
    - `core_dm/fact_events_actor_type`: Joins the stg_events_types to the stg_actor_types for both the source or actor1 and the destination or actor2.
    - `core_dm/fact_events_event_code`: Joins the stg_events_by_code to the stg_event_codes for the root event, base event and event codes.

And that's all!!!