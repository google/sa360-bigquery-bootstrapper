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
from absl import flags
import unittest

from .settings import AbstractSettings
from .settings import SettingOption


class TestSettings(AbstractSettings):
    def settings(self):
        return {}


class AppFlagsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = TestSettings()

    def test_settings(self):
        test = SettingOption.create(self.settings, "Hint")
        test.value = "Test"

        self.assertTrue(test[0].isalnum())
        self.assertTrue(test[-1].isalnum())
        self.assertEqual(test[0], 'T')
        self.assertEqual(test[-1], 't')

    def test_assign(self):
        def test_bool(value, check):
            boolean = SettingOption.create(
                self.settings,
                "Hint",
                method=flags.DEFINE_bool)
            boolean.set_value(value=value)
            self.assertEqual(boolean.value, check)

        test_bool('true', True)
        test_bool('1', True)
        test_bool(True, True)
        test_bool('True', True)

        test_bool('false', False)
        test_bool('0', False)
        test_bool(False, False)
        test_bool('False', False)

        string = SettingOption.create(
            self.settings,
            "Hint",
        )
        string.value = 'string'

        self.assertEqual(type(string.value), str)


