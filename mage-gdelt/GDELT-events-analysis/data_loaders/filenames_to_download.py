import pandas as pd

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


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    print(kwargs['url_file_list'])
    url_file_list=kwargs['url_file_list']
    # Read the list of csv files in the GDELT Project
    df = pd.read_csv(url_file_list, sep=' ', header=None)
    df.columns = ['ID', 'md5sum', 'file_name']
    # REmove some rows with null values
    df = df.dropna()
    # Select the files from today - days to collect
    date_range=get_dates(kwargs['days_to_collect'])
    # Create the list of files to download
    files=[]
    for day in date_range:
        #df_files = df[(df['file_name'].str.contains(month_to_download) & (df['file_name'].str.contains('export')))]
        files+=df[(df['file_name'].str.contains(day) & (df['file_name'].str.contains('export')))]['file_name'].values.tolist()

    return files


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert len(output> > 0, 'The output is undefined'
