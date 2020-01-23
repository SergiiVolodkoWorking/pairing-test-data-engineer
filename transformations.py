import pandas as pd

def add_date(df):
    df['date'] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'minute']])
    return df

def count_column_groups(df, column_from_df):
    groups_df = df.groupby(column_from_df).size()

    count = groups_df.shape[0] # Rows count in pandas

    return count

def merge_detasets(df1, df2, common_column):
    return pd.merge(df1, df2, on=common_column)

def extract_cities_from_airports(airports_df):
    cities_df = airports_df[['IATA_CODE', 'CITY']].copy()
    return cities_df

def merge_flights_with_cities(flights_df, cities_df):
    df = flights_df[['origin', 'dest']].copy()

    df = pd.merge(df, cities_df[['IATA_CODE', 'CITY']], how='left', left_on='origin', right_on='IATA_CODE')
    df = df.rename({'CITY': 'origin_city'}, axis='columns')
    df = df.drop('IATA_CODE', axis='columns')

    df = pd.merge(df, cities_df[['IATA_CODE', 'CITY']], how='left', left_on='dest', right_on='IATA_CODE')
    df = df.rename({'CITY': 'dest_city'}, axis='columns')
    df = df.drop('IATA_CODE', axis='columns')

    return df

def add_connection_id(flights_with_cities_df):
    def combine_origin_and_destination(origin, dest):
        origin_and_dest = [origin, dest]
        origin_and_dest.sort()
        return '-'.join(origin_and_dest)

    df = flights_with_cities_df
    df['connection_id'] = [combine_origin_and_destination(x, y)\
                                for x, y in zip(df['origin_city'].map(str), df['dest_city'].map(str))]
    return df

def aggregate_connections_between_cities(connections_df):

    df = connections_df.groupby('connection_id').size().sort_index().reset_index()

    df.columns = ['connection_id','flights_count']

    df = df.sort_values(by='flights_count', ascending=False)

    return df