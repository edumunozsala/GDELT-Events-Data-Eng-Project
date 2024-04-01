import os
import requests
import zipfile


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


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
            print(f"Downloading {filename}...")
            response = requests.get(url)
            with open(download_path, 'wb') as file:
                file.write(response.content)

            # Extract the ZIP file
            print(f"Extracting {filename}...")
            with zipfile.ZipFile(download_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            print(f"Extraction completed for {filename}")

        print("All files downloaded and extracted successfully.")
    except Exception as e:
        print("An error occurred:", e)

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
    zip_urls = [
        "http://data.gdeltproject.org/gdeltv2/20240324161500.export.CSV.zip",
        "http://data.gdeltproject.org/gdeltv2/20240324160000.export.CSV.zip"
    ]
    download_dir = "/home/src/downloads"
    extract_dir = "/home/src/extracted"

    download_and_extract_zips(zip_urls, download_dir, extract_dir)

