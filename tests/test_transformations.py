import unittest
import pandas as pd

from transformations import add_date

from pandas.testing import assert_frame_equal

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

