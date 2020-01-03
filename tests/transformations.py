import pandas as pd

def add_date(df):
    df['date'] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'minute']])
    return df