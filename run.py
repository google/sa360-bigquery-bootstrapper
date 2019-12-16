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
from subprocess import check_output
from utilities import lib_mappings

counter = -1
last_package = None
while True:
    try:
        import bootstrapper
        break
    except ImportError as err:
        if counter == -1:
            check_output(['git', 'checkout', '--', 'Pipfile'])
            counter += 1
            continue
        if last_package == err.name:
            counter += 1
        else:
            last_package = err.name
            counter = 0
        if counter > 0:
            raise
        print('Installing required package {}...'.format(err.name))
        package_name = (lib_mappings[err.name]
                        if err.name in lib_mappings
                        else err.name)
        check_output(['pipenv', 'install', package_name])

from absl import flags
import flagmaker.settings as settings

if __name__ == '__main__':
    bootstrap = bootstrapper.Bootstrap()
    flags.adopt_module_key_flags(settings)
    bootstrap.run()
