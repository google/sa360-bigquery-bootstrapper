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
from typing import Dict
from typing import List

from absl import flags
from prompt_toolkit import prompt

from flagmaker.exceptions import FlagMakerPromptInterruption
from .building_blocks import SettingOptionInterface
from .building_blocks import SettingsInterface
from .building_blocks import Value
from .exceptions import FlagMakerInputError
from .hints import StringKeyDict
from .sanity import Validator
from termcolor import cprint
from absl import flags
from .exceptions import FlagMakerConfigurationError

FLAGS = flags.FLAGS


class SettingOption(SettingOptionInterface):
    settings: SettingsInterface = None
    default = None
    help = None
    method: callable = None
    _value: Value = None
    required: bool = False
    validation: callable = None
    conditional: callable = None
    after: callable = None
    prompt: callable or str = None
    custom_data: StringKeyDict = {}
    include_in_interactive: bool = True
    called: dict = {}
    _error: bool = False

    def __init__(self):
        self._value = Value()

    @classmethod
    def create(cls, settings: SettingsInterface, helptext=None, default=None,
               method=flags.DEFINE_string, required=True, validation=None,
               conditional=None, after=None, prompt=None,
               include_in_interactive=True):
        fl = cls()
        fl.settings = settings
        fl.default = default
        fl.help = helptext
        fl.method = method
        fl.required = required
        fl.validation = validation
        fl.conditional = conditional
        fl.after = after
        fl.prompt = prompt
        fl.include_in_interactive = include_in_interactive
        return fl

    def get_prompt(self, k):
        default = ' [{0}]'.format(
            self.default
        ) if self.default is not None else ''
        prompt_val = ''
        if self.prompt is not None:
            prompt_val += '\n'
            if self.prompt is str:
                prompt_val += self.prompt
            if callable(self.prompt):
                prompt_val += self.prompt(self)
            prompt_val += '\nInput'
        return '{} ({}){}{}: '.format(self.help, k, default, prompt_val)

    @property
    def value(self):
        return self._value.get_val()

    @value.setter
    def value(self, value):
        if value is None:
            self._value.set_val(None)
            return
        if self.method == flags.DEFINE_boolean:
            if value in ['1', 'true', 'True', True]:
                value = True
            elif value in ['0', 'false', 'False', False]:
                value = False
        elif self.method == flags.DEFINE_integer:
            value = int(value)
        self._value.set_val(value)
        # perform actions

        if self.after is None:
            self._error = False
            return

        in_called = (self, self.after) not in self.called
        if in_called:
            self.called[(self, self.after)] = True
            self.after(self)

    def set_value(self, value: str = '', ask: str = '', init: str = ''):
        while True:
            num_opts = int(value != '') + int(ask != '') + int(init != '')
            if num_opts != 1:
                raise FlagMakerInputError('Need to choose either '
                                          'init, value or ask')

            if init is None:
                return
            elif init != '':
                self.value = init
                return

            if ask != '':
                if ask is None:
                    # we intentionally set ask to None. A conditional prompt
                    # doesn't want this to continue
                    return
                val = prompt(ask)
                if val == '' and self.default is not None:
                    self.value = self.default
                else:
                    try:
                        self.value = val
                    except FlagMakerConfigurationError as err:
                        cprint(err.message, 'red')
                        continue
            else:
                self.value = value
            if not Validator.validate(self) or self._error:
                continue
            if self.value_explicitly_set() or not self.required:
                return
            else:
                cprint('Required Field', 'red')

    def value_explicitly_set(self) -> bool:
        return bool(self._value)

    def maybe_needs_input(self):
        return not self.value_explicitly_set() and (
            self.conditional is None or self.conditional(self.settings))

    def get_method(self):
        return self.method

    def __str__(self):
        return self.value if self.value is not None else ''

    def __repr__(self):
        return '[{0}{1}]'.format(
            self.help, ' (' + str(self.value) + ')' if self.value else ''
        )

    def __bool__(self):
        return bool(self.value)

    def __index__(self):
        return self.value

    def __getitem__(self, item):
        return self.value.__getitem__(item)


SettingOptions = Dict[str, SettingOption]


class SettingBlock:
    def __init__(self, block: str,
                 settings: SettingOptions,
                 conditional: callable = None):
        self.name = block
        self.settings = settings
        self.conditional = conditional

    def get(self):
        cprint('{}'.format(self.name), None, attrs=['bold'])
        cprint('==========================', attrs=['bold'])


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

    @property
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
                        try:
                            setting.set_value(ask=setting.get_prompt(k))
                        except FlagMakerPromptInterruption as err:
                            setting.set_value(value=err.value)
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
        self.instance.load_settings
        return self.instance
