import unittest
import pandas as pd

from pandas.testing import assert_frame_equal

from parameterized import parameterized

from transformations import add_date,\
                            count_column_groups,\
                            merge_detasets,\
                            extract_cities_from_airports,\
                            merge_flights_with_cities,\
                            add_connection_id,\
                            aggregate_connections_between_cities

class TestTransformations(unittest.TestCase):
    def test_add_date(self):
        df = pd.DataFrame({
            'year':  [1990, 2005, 2019],
            'month': [1,   2,    12],
            'day':   [30,  15,   1],
            'hour':  [5.0, 17.0, 0.0],
            'minute':[0.0, 45.0, 12.0],
            'other': ['some','other','data']
        })

        expected = pd.DataFrame({
            'year':  [1990, 2005, 2019],
            'month': [1,   2,    12],
            'day':   [30,  15,   1],
            'hour':  [5.0, 17.0, 0.0],
            'minute':[0.0, 45.0, 12.0],
            'other': ['some','other','data'],
            'date':  [
                pd.Timestamp('1990-1-30 05:00:00'),
                pd.Timestamp('2005-2-15 17:45:00'),
                pd.Timestamp('2019-12-1 00:12:00'),
            ]
        })
        assert_frame_equal(expected, add_date(df))

        
    @parameterized.expand([
        [1, 1, 2],
        ["text1", "text1", "text2"],
        [pd.Timestamp('1990-1-30'), pd.Timestamp('1990-1-30'), pd.Timestamp('2019-12-1')]
        ])
    def test_count_groups_of_column(self, v1, v2, v3):
        df = pd.DataFrame({
            'column1': [v1,v2,v3],
            'column2': ['some','other','data']
        })
        self.assertEqual(2, count_column_groups(df, df['column1']))

    @parameterized.expand([
        [1, 2, 3],
        ["text1", "text2", "text3"],
        [pd.Timestamp('1990-1-30'), pd.Timestamp('1999-1-30'), pd.Timestamp('2019-12-1')]
        ])
    def test_combine_data_frames(self, v1, v2, v3):
        df1 = pd.DataFrame({
            'column1': [v1,v2,v3],
            'column2': ['first', None, 'dataset']
        })
        df2 = pd.DataFrame({
            'column1': [v1,v2,v3],
            'column3': ['second', 'dataset', None]
        })

        expected = pd.DataFrame({
            'column1': [v1,v2,v3],
            'column2': ['first', None, 'dataset'],
            'column3': ['second','dataset', None]
        })
        assert_frame_equal(expected, merge_detasets(df1, df2, 'column1'))

    def test_extract_cities_df_from_airports(self):
        airports_df = pd.DataFrame({
            'IATA_CODE':    ['LCY','ODS'],
            'CITY':         ['London', 'Odessa'],
            'other_columns':['some','data']
        })
        expected = pd.DataFrame({
            'IATA_CODE':    ['LCY','ODS'],
            'CITY':         ['London', 'Odessa']
        })
        assert_frame_equal(expected, extract_cities_from_airports(airports_df))

    def test_merge_flights_with_cities(self):
        cities_df = pd.DataFrame({
            'IATA_CODE':    ['LCY','ODS','RTM'],
            'CITY':         ['London','Odessa','Rotterdam'],
            'other':        ['any','other','airport']
        })

        flights_df = pd.DataFrame({
            'origin':   ['LCY','ODS'],
            'dest':     ['ODS','RTM'],
            'other':    ['other', 'data']
        })

        expected = pd.DataFrame({
            'origin':               ['LCY', 'ODS'],
            'dest':                 ['ODS', 'RTM'],
            'origin_city':          ['London', 'Odessa'],
            'dest_city':            ['Odessa', 'Rotterdam']
        })
        assert_frame_equal(expected, merge_flights_with_cities(flights_df, cities_df))


    def test_add_connection_id_to_flights_with_cities(self):
        flights_with_cities_df = pd.DataFrame({
            'origin':               ['LCY', 'ODS', 'LHR'],
            'dest':                 ['ODS', 'RTM', 'ODS'],
            'origin_city':          ['London', 'Odessa', 'London'],
            'dest_city':            ['Odessa', 'Rotterdam','Odessa'],
            'connection_id':        ['London-Odessa', 'Odessa-Rotterdam', 'London-Odessa']
        })
        expected = pd.DataFrame({
            'origin':               ['LCY', 'ODS', 'LHR'],
            'dest':                 ['ODS', 'RTM', 'ODS'],
            'origin_city':          ['London', 'Odessa', 'London'],
            'dest_city':            ['Odessa', 'Rotterdam','Odessa'],
            'connection_id':        ['London-Odessa', 'Odessa-Rotterdam', 'London-Odessa']
        })
        assert_frame_equal(expected, add_connection_id(flights_with_cities_df))

    def test_aggregate_connections_between_cities(self):
        connections_df = pd.DataFrame({
            'origin':               ['LCY', 'ODS', 'LHR'],
            'dest':                 ['ODS', 'RTM', 'ODS'],
            'origin_city':          ['London', 'Odessa', 'London'],
            'dest_city':            ['Odessa', 'Rotterdam','Odessa'],
            'connection_id':        ['London-Odessa', 'Odessa-Rotterdam', 'London-Odessa']
        })

        expected = pd.DataFrame({
            'connection_id':['London-Odessa', 'Odessa-Rotterdam'],
            'flights_count':[2, 1]
        })
        actual = aggregate_connections_between_cities(connections_df)
        assert_frame_equal(expected, actual)