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
try:
    import bootstrapper
except ImportError:
    print('Installing required packages...')
    from pip._internal.utils import subprocess
    subprocess.call_subprocess(['pipenv', 'install'], show_stdout=False)
    import bootstrapper

from absl import flags
import flagmaker.settings as settings

if __name__ == '__main__':
    bootstrap = bootstrapper.Bootstrap()
    flags.adopt_module_key_flags(settings)
    bootstrap.run()
