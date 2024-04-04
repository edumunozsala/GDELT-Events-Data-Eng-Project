import pandas as pd

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
    #url_file_list='http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
    url_lookups=['https://www.gdeltproject.org/data/lookups/CAMEO.type.txt',
    'https://www.gdeltproject.org/data/lookups/CAMEO.religion.txt',
    'https://www.gdeltproject.org/data/lookups/CAMEO.knowngroup.txt',
    'https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt',
    'https://www.gdeltproject.org/data/lookups/CAMEO.ethnic.txt',
    'https://www.gdeltproject.org/data/lookups/CAMEO.country.txt']


    df_lookups= pd.DataFrame(columns=['code','label','table'],dtype=str)
    print(df_lookups.info())

    for url in url_lookups:
        #print(url)
        table= url.split("CAMEO.")[1].split(".txt")[0]
        df= pd.read_csv(url, sep='\t', header=0, names=['code','label'])
        df['table']= table
        print(df.shape)
        df_lookups = pd.concat([df_lookups, df], axis=0, ignore_index=True)
        print(df_lookups.shape)
        #print(df_lookups.head(3))
    return df_lookups


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
