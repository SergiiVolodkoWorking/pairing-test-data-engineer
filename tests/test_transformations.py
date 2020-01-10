import unittest
import pandas as pd

from pandas.testing import assert_frame_equal

from parameterized import parameterized

from transformations import add_date,\
                            count_column_groups,\
                            merge_detasets

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
        actual = merge_detasets(df1, df2, 'column1')
        assert_frame_equal(expected, actual)