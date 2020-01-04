import pandas as pd

def add_date(df):
    df['date'] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'minute']])
    return df

def count_column_groups(df, column_from_df):
    groups_df = df.groupby(column_from_df).size()

    count = groups_df.shape[0] # Rows count in pandas

    return count