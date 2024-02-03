if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def cols_to_rename():
    """Rename camelcase columns to snakecase"""

    return {
        "VendorID": "vendor_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "RatecodeID": "ratecode_id"
    }



@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    data = data[(data["passenger_count"] > 0) & (data["trip_distance"] > 0)].reset_index(drop=True)
    data["lpep_pickup_date"] = data["lpep_pickup_datetime"].dt.date
    data["lpep_dropoff_date"] = data["lpep_dropoff_datetime"].dt.date
    data = data.rename(columns=cols_to_rename())

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert all(output["passenger_count"] > 0), 'passenger_count is 0'
    assert all(output["trip_distance"] > 0), 'trip_distance is 0'
    assert all(output["vendor_id"].isin([1, 2])), 'vendor_id is invalid'
