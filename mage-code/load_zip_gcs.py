import requests
import zipfile
from io import BytesIO
from google.cloud import storage
from google.oauth2 import service_account
import os
import pandas as pd

import datetime
import shutil


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Extract string date in format YYYYMMDD starting at today until n days before
def get_dates(n):
    today= datetime.datetime.utcnow()

    date_range = []
    for i in range(n):
            date_range.append((today - datetime.timedelta(days=i)).strftime("%Y%m%d"))
    return date_range    # return [today - datetime.timedelta(days=i) for i in range(n)]


def download_and_upload_to_gcs(urls, csv_per_file=20, path=None, bucket=None, client=None):
    # Download the zip file from the URL
    files_included=0
    
    for url in urls:
        # Open a new file
        #print(url)
        day=url.split("/")[-1].split(".")[0]
        if files_included == csv_per_file:
            # SAve the file to disk
                        # SAve to disk
            filename_to_save=day_ini+"_"+url.split("/")[-1].split(".")[0]+".csv"
            print(filename_to_save)
            
            #with open(filename_to_save, 'wb') as fd:
            dest_file.seek(0)
            upload_to_gcs(dest_file, path+filename_to_save, bucket, client)
                #shutil.copyfileobj(dest_file, fd)

            files_included = 0
            
        if files_included == 0:
            day_ini=url.split("/")[-1].split(".")[0]
            dest_file= BytesIO()

        response = requests.get(url)
        
        if response.status_code == 200:
            # Extract the contents of the zip file
            with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
                file_list = zip_ref.namelist()
                for file_name in file_list:
                    with zip_ref.open(file_name) as file:
                        # Upload extracted files to Google Cloud Storage
                        #upload_to_gcs(file, path+file_name, bucket, client)
                        dest_file.write(file.read())
                        files_included+=1

    # Save the last file to disk
    if files_included > 0:
        filename_to_save=day_ini+"_"+day.split("/")[-1].split(".")[0]+".csv"
        print(filename_to_save)

        #with open(filename_to_save, 'wb') as fd:
        dest_file.seek(0)
        upload_to_gcs(dest_file, path+filename_to_save, bucket, client)
            #shutil.copyfileobj(dest_file, fd)
                                
#upload_to_gcs(file, path+file_name, bucket, client)

def upload_to_gcs(file_content, file_name, bucket, client):
    blob = storage.Blob(file_name, bucket)
    blob.upload_from_file(file_content, content_type='application/csv')

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    # Set the GCS location to save the data
    # Read the list of files in the project
    #url_file_list='http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
    print(kwargs['url_file_list'])
    url_file_list=kwargs['url_file_list']
    df = pd.read_csv(url_file_list, sep=' ', header=None)
    df.columns = ['ID', 'md5sum', 'file_name']
    # REmove some rows with null values
    df = df.dropna()
    # Extract days to collect data
    # IMPORTANT: We include a limit of 90 days,
    # You can remove or change it
    days_to_collect=int(os.environ['DAYS_TO_COLLECT'])
    if days_to_collect>90:
        days_to_collect=90

    date_range=get_dates(days_to_collect)
    files=[]
    for day in date_range:
        #df_files = df[(df['file_name'].str.contains(month_to_download) & (df['file_name'].str.contains('export')))]
        files+=df[(df['file_name'].str.contains(day) & (df['file_name'].str.contains('export')))]['file_name'].values.tolist()

    #bucket_name="mage-dezoomcamp-ems"
    bucket_name=os.environ['BUCKET_NAME']
    #project_id="banded-pad-411315"
    project_id=os.environ['PROJECT_ID']
    #path="GDELT-Project/bronze/csv/"
    path=kwargs['path']
    root_path= f'gs://{bucket_name}/{path}'
    print(root_path)

    credentials = service_account.Credentials \
        .from_service_account_file(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    client = storage.Client(
        project=project_id,
        credentials=credentials
    )

    bucket = client.bucket(bucket_name)
    #url="http://data.gdeltproject.org/gdeltv2/20240324161500.export.CSV.zip"
    #for url in zip_urls:
    print("Files:", len(files))
    download_and_upload_to_gcs(files, int(kwargs['csv_per_file']), path, bucket, client)

    return {}
