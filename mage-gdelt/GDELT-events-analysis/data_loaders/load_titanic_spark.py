from pyspark import SparkFiles

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
    url="https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv?raw=True"

    kwargs['spark'].sparkContext.addFile(url)

    df = (kwargs['spark'].read
        .option("header", "true")
        .csv(SparkFiles.get("titanic.csv"))
    )

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
