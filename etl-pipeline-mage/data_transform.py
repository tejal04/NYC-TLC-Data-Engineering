import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        df: The output from the upstream parent block (data)
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    
    #creating unique index
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    #converting datetime variables in order to extract relevant features
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #DATETIME DIMENSION
    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)

    #datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
    datetime_dim['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    datetime_dim['pickup_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pickup_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pickup_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pickup_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    #datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
    datetime_dim['dropoff_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['dropoff_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['dropoff_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['dropoff_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['dropoff_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index

    datetime_dim =datetime_dim[['datetime_id','tpep_dropoff_datetime', 'tpep_pickup_datetime', 'pickup_hour',
        'pickup_day', 'pickup_month', 'pickup_year', 'pickup_weekday',
        'dropoff_hour', 'dropoff_day', 'dropoff_month', 'dropoff_year',
        'dropoff_weekday']]

    #RATECODE DIMENSION
    ratecode_dict = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    ratecode_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    ratecode_dim['ratecode_id'] = ratecode_dim.index
    ratecode_dim['ratecode_name'] = ratecode_dim['RatecodeID'].map(ratecode_dict)
    ratecode_dim = ratecode_dim[['ratecode_id','RatecodeID','ratecode_name']]

    #PAYMENT DIMENSION
    payment_dict = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }

    payment_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_dim['payment_id'] = payment_dim.index
    payment_dim['payment_name'] = payment_dim['payment_type'].map(payment_dict)
    payment_dim = payment_dim[['payment_id','payment_type','payment_name']]
   
    #LOCATION DIMENSION
    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 

    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]


    #VENDOR DIMENSION
    vendor_dict = {
        1:"Creative Mobile Technologies, LLC",
        2:"VeriFone Inc.",
    }

    vendor_dim = df[['VendorID']].drop_duplicates().reset_index(drop=True)
    vendor_dim['vendor_id'] = vendor_dim.index
    vendor_dim['vendor_name'] = vendor_dim['VendorID'].map(vendor_dict)
    vendor_dim = vendor_dim[['vendor_id','VendorID','vendor_name']]


    #CREATE FACT TABLE
    fact_table = df.merge(datetime_dim, on=['tpep_pickup_datetime','tpep_dropoff_datetime'], how='left') \
            .merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude'], how='left') \
            .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude'], how='left') \
            .merge(ratecode_dim, on='RatecodeID', how='left') \
            .merge(payment_dim, on='payment_type', how='left') \
            .merge(vendor_dim, on='VendorID', how='left') 

    fact_table = fact_table[['trip_id', 'passenger_count', 'trip_distance','vendor_id', 'datetime_id', \
                'pickup_location_id', 'dropoff_location_id', 'ratecode_id', 'payment_id', \
                'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', \
                'improvement_surcharge', 'total_amount']]
        
    result = {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "ratecode_dim":ratecode_dim.to_dict(orient="dict"),
    "pickup_location_dim":pickup_location_dim.to_dict(orient="dict"),
    "dropoff_location_dim":dropoff_location_dim.to_dict(orient="dict"),
    "payment_dim":payment_dim.to_dict(orient="dict"),
    "vendor_dim" : vendor_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}

    return result

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
