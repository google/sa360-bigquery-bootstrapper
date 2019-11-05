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
from typing import ClassVar
from typing import List

from absl import flags
from termcolor import cprint

from flagmaker.settings import SettingBlock
from .building_blocks import SettingsInterface
from .exceptions import FlagMakerConfigurationError
from .exceptions import FlagMakerInputError
from .sanity import Validator
from .settings import SettingOption
from .settings import SettingOptions

FLAGS = flags.FLAGS


class AbstractSettings(SettingsInterface):
    """Settings Base Class

    Loaded from the Config class. Used to generate flags for an app.
    """

    args: List[SettingBlock] = None
    flattened_args: dict = {}

    def start(self):
        """Bootstraps the settings loading process

        Called from load_settings. Should not be called directly.
        """
        for block in self.args:
            for k, s in block.settings.items():
                s.set_value(init=FLAGS.get_flag_value(k, None))
                self.flattened_args[k] = s

    def load_settings(self):
        self.start()
        first = True
        interactive_mode = self.args[0].settings.pop('interactive')
        for block in self.args:
            header_shown = False
            if block.conditional is not None and not block.conditional(self):
                continue
            for k, setting in block.settings.items():
                if setting.maybe_needs_input():
                    if not interactive_mode and setting.default:
                        setting.set_value(init=setting.default)
                        continue
                    if first:
                        cprint('Interactive Setup', attrs=['bold', 'underline'])
                        cprint(
                            '===============================',
                            attrs=['bold'],
                        )
                        first = False
                    if not header_shown:
                        cprint(block.name, attrs=['underline'])
                        header_shown = True
                    if setting.include_in_interactive:
                        setting.set_value(prompt=setting.get_prompt(k))
        return self

    def assign_flags(self) -> flags:
        for block in self.args:
            for k, setting in block.settings.items():
                kwargs = {
                    'default': None,
                }
                if not setting.include_in_interactive:
                    kwargs['default'] = setting.default
                setting.method(k, help=setting.help, **kwargs)
                self.flattened_args[k] = setting
        return FLAGS

    def __getitem__(self, item):
        return self.flattened_args[item]

    def get_settings(self):
        # system settings
        settings = [SettingBlock('System Settings', {
            'interactive': SettingOption.create(
                self,
                'Enter Interactive Mode even to verify default values',
                default=False,
                include_in_interactive=False,
                method=flags.DEFINE_bool,
            ),
        })]
        settings += self.settings()
        return settings

    def install(self):
        self.args = self.get_settings()
        self.assign_flags()

    def __repr__(self):
        return str(self.args)


AbstractSettingsClass = ClassVar[AbstractSettings]


class Config(object):
    """The entry point for setting the Settings class for an app.

    Example: Config(MySettingsClass)

    This will bootstrap the settings class correctly.
    """
    def __init__(self, s: AbstractSettingsClass):
        self.s = s
        self.instance = None
        self.instance: AbstractSettings = s()
        self.instance.install()

    def get(self) -> AbstractSettings:
        if not FLAGS.is_parsed():
            raise FlagMakerConfigurationError('Do not call this '
                                              'method until after app.run()')
        self.instance.load_settings()
        return self.instance
