import datetime
import pytz
import unittest

import waterflow

class WaterFlowTests(unittest.TestCase):

    def test_tz_check(self):
        # UTC ok
        waterflow.check_utc_or_unaware(datetime.datetime.utcnow())
        waterflow.check_utc_or_unaware(datetime.datetime(2010, 6, 26, tzinfo=pytz.UTC))

        # No TZ ok
        waterflow.check_utc_or_unaware(datetime.datetime(2010, 6, 26))

        # other TZs not ok
        with self.assertRaises(ValueError):
            waterflow.check_utc_or_unaware(datetime.datetime(2010, 6, 26, tzinfo=pytz.timezone('US/Eastern')))

