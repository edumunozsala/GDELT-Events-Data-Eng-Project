import os
import requests
import zipfile
import pandas as pd

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def download_and_extract_zips(zip_urls, download_dir, extract_dir):
    try:
        # Create directories if they don't exist
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(extract_dir, exist_ok=True)

        for url in zip_urls:
            # Extract filename from URL
            filename = url.split('/')[-1]
            download_path = os.path.join(download_dir, filename)

            # Download the ZIP file
            #print(f"Downloading {filename}...")
            response = requests.get(url)
            with open(download_path, 'wb') as file:
                file.write(response.content)

            # Extract the ZIP file
            #print(f"Extracting {filename}...")
            with zipfile.ZipFile(download_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            print(f"Extraction completed for {filename}")

        print("All files downloaded and extracted successfully.")
    except Exception as e:
        print("An error occurred:", e)


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here
    # Specify your data exporting logic here
    # Read the list of files in the project
    df = pd.read_csv('http://data.gdeltproject.org/gdeltv2/masterfilelist.txt', sep=' ', header=None)
    df.columns = ['ID', 'hash', 'file_name']
    df = df.dropna()
    # Select the files in 2024-03-01
    df_202403 = df[(df['file_name'].str.contains('20240301') & (df['file_name'].str.contains('export')))]
    print(len(df_202403))
    """
    zip_urls = [
        "http://data.gdeltproject.org/gdeltv2/20240324161500.export.CSV.zip",
        "http://data.gdeltproject.org/gdeltv2/20240324160000.export.CSV.zip"
    ]
    """
    zip_urls = df_202403['file_name'].values.tolist()
    
    download_dir = "/home/src/downloads"
    extract_dir = "/home/src/extracted"

    download_and_extract_zips(zip_urls, download_dir, extract_dir)


    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
