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

import flagmaker.settings
import app_flags

settings = app_flags.AppSettings()
test = flagmaker.settings.SettingOption.create(settings, "Hint")
test.value = "Test"

assert(test[0].isalnum())
assert(test[-1].isalnum())
assert(test[0] == "T")
assert(test[-1] == "t")


def test_bool(value, check):
    boolean = flagmaker.settings.SettingOption.create(settings,
                                                      "Hint",
                                                      method=flags.DEFINE_bool)


    boolean.set_value(value=value)
    assert(boolean.value == check)

test_bool('true', True)
test_bool('1', True)
test_bool(True, True)
test_bool('True', True)

test_bool('false', False)
test_bool('0', False)
test_bool(False, False)
test_bool('False', False)

