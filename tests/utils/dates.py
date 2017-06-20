# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta
import unittest

from airflow.utils import dates

class Dates(unittest.TestCase):

    def test_days_ago(self):
        today = datetime.today()
        today_midnight = datetime.fromordinal(today.date().toordinal())

        self.assertTrue(dates.days_ago(0) == today_midnight)

        self.assertTrue(
            dates.days_ago(100) == today_midnight + timedelta(days=-100))

        self.assertTrue(
            dates.days_ago(0, hour=3) == today_midnight + timedelta(hours=3))
        self.assertTrue(
            dates.days_ago(0, minute=3)
            == today_midnight + timedelta(minutes=3))
        self.assertTrue(
            dates.days_ago(0, second=3)
            == today_midnight + timedelta(seconds=3))
        self.assertTrue(
            dates.days_ago(0, microsecond=3)
            == today_midnight + timedelta(microseconds=3))

    def test_to_iso(self):
        execution_date = datetime(2017, 1, 1, 12, 34, 56)
        iso_expected = '2017-01-01T12:34:56'

        iso = dates.to_iso(execution_date)
        self.assertEqual(iso, iso_expected)

        execution_date = '2017-01-01 12:34:56'
        iso = dates.to_iso(execution_date)
        self.assertEqual(iso, iso_expected)

        execution_date = None
        with self.assertRaises(TypeError):
            iso = dates.to_iso(execution_date)

        execution_date = 'some randome string'
        with self.assertRaises(ValueError):
            iso = dates.to_iso(execution_date)
