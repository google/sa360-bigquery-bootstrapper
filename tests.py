# /***********************************************************************
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Note that these code samples being shared are not official Google
# products and are not formally supported.
# ************************************************************************/
import os
import unittest

import pandas as pd

import app_settings
from csv_decoder import Decoder
from utilities import ViewTypes
from utilities import get_view_name


class AppSettingsTest(unittest.TestCase):
    def test_columns(self):
        s = app_settings.AppSettings()
        self.assertEqual(
            s.columns['account_column_name'].default,
            'account_name'
        )
        s.columns['account_column_name'].value = 'Account'
        dict_map = {val.value:val.default for val in s.columns.values()}
        self.assertIn('Account', dict_map)


class UtilitiesTest(unittest.TestCase):
    def test_types(self):
        self.assertEqual(
            get_view_name(ViewTypes.KEYWORD_MAPPER, '123'),
            'KeywordMapper_123'
        )


class CSVDecoderTest(unittest.TestCase):
    def test_size(self):
        file = Decoder('utf-8', './testdata/dirA', map={
            'A': 'only_column'
        }).run()
        self.assertTrue(os.path.isfile(file))
        df: pd.DataFrame = pd.read_csv(file)
        self.assertEqual(len(df.columns), 1)
        self.assertTrue('only_column' in df)
        desired_list = ['I','a','c','e','g','k','m']
        self.assertListEqual(sorted(df['only_column'].values), desired_list)


if __name__ == '__main__':
    unittest.main()
