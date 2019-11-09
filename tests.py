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

from absl import flags
import unittest

import flagmaker.settings
import app_flags
from csv_decoder import Decoder
from flagmaker.tests import TestSettings
from utilities import ViewTypes
from utilities import get_view_name


class UtilitiesTest(unittest.TestCase):
    def test_types(self):
        self.assertEqual(
            get_view_name(ViewTypes.KEYWORD_MAPPER, '123'),
            'KeywordMapper_123'
        )


class CSVDecoderTest(unittest.TestCase):
    def test_size(self):
        output_dir = Decoder('utf-8', './testdata/dirA').run()
        self.assertEqual(len(os.listdir(output_dir)), 7)


if __name__ == '__main__':
    unittest.main()
