import pandas as pd
import requests
from io import StringIO

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
    #url='https://raw.githubusercontent.com/linwoodc3/gdelt2HeaderRows/master/schema_csvs/GDELT_2.0_Events_Column_Labels_Header_Row_Sep2016.csv'
    #'https://github.com/linwoodc3/gdelt2HeaderRows/blob/master/schema_csvs/GDELT_2.0_Events_Column_Labels_Header_Row_Sep2016.csv'
        # Download the CSV file from the URL
    mapping = {'INTEGER': 'Integer', 'STRING':'String', 'FLOAT':'Double'}

    response = requests.get(kwargs['schema_url'])
    if response.status_code == 200:
            # Read the content of the response as a string
            csv_content = response.content.decode('utf-8')
            
            # Parse the string content into a pandas DataFrame
            df = pd.read_csv(StringIO(csv_content))
            # Convert type names to pyspark datatypes
            df['dataType'] = df['dataType'].map(mapping)
            # Optionally, you can print the DataFrame
            df = df.drop(['Empty','Description'],axis=1)
            
            return df
    else:
            print("Failed to download CSV file. Status code:", response.status_code)
            return None
    

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
