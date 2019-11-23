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
from typing import Union

import yaml

from yaml.parser import ParserError

from collections.abc import Iterable
from enum import EnumMeta
from typing import ClassVar
from typing import Dict
from typing import Generic
from typing import List
from typing import TypeVar

from absl import flags
from prompt_toolkit import ANSI
from prompt_toolkit import prompt
from prompt_toolkit.shortcuts import CompleteStyle
from termcolor import colored
from termcolor import cprint

from flagmaker.building_blocks import list_to_string_list
from flagmaker.exceptions import FlagMakerPromptInterruption
from flagmaker.validators import ChoiceValidator
from .building_blocks import SettingOptionInterface
from .building_blocks import SettingsInterface
from .building_blocks import Value
from .exceptions import FlagMakerConfigurationError
from .exceptions import FlagMakerInputError
from .hints import StringKeyDict
from .sanity import Validator

FLAGS = flags.FLAGS

T = TypeVar('T', bound=SettingsInterface)


class SettingConfig(object):
    cache_file: str = '{}/.sa360bq'.format(os.environ['HOME'])
    cache_dict: dict = {}


class SettingOption(SettingOptionInterface, Generic[T]):
    settings: T = None
    default = None
    cache = None
    help = None
    method: callable = None
    _value: Value = None
    required: bool = False
    validation: callable = None
    conditional: callable = None
    after: callable = None
    prompt: Union[callable, str]
    custom_data: StringKeyDict
    include_in_interactive: bool = True
    called: dict
    _options: EnumMeta = None
    _error: bool = False
    attrs: dict

    def __init__(self):
        self._value = Value()
        self.called = {}

    @classmethod
    def create(cls, settings: T, helptext=None, default=None,
               method=flags.DEFINE_string, required=True, validation=None,
               conditional=None, after=None, prompt=None,
               include_in_interactive=True, options=None, attrs=None):
        if options is None:
            options = []
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
        fl._options = options
        fl.attrs = attrs or {}

        return fl

    @property
    def options(self):
        return (list(map(lambda x: x.value, self._options))
                if self._options is not None else None)

    def get_prompt(self, k):
        d = self.get_default_or_cache()
        default = ' [default={0}]'.format(
            d if not isinstance(d, bool) else int(d)
        ) if d is not None else ''
        prompt_val = ''
        if self.prompt is not None:
            prompt_val += '\n'
            if self.prompt is str:
                prompt_val += self.prompt
            if callable(self.prompt):
                prompt_val += self.prompt(self)
            prompt_val += '\nInput'
        if self.method != flags.DEFINE_enum:
            method = self.get_basic_prompt
        else:
            method = self.get_option_prompt
        return method(k, default, prompt_val)

    def get_option_prompt(self, k, default, prompt_val):
        if not isinstance(self._options, EnumMeta):
            raise FlagMakerConfigurationError('Need to add options for ' + k)
        options = list_to_string_list(self.options)
        return (
            '{0}\n'
            '{1}\n'
            '{2}\n'
            'Choices{3}: '
        ).format(
            k,
            colored('Options:', attrs=['underline']),
            options,
            default, prompt_val
        )

    def get_basic_prompt(self, k, default, prompt_val):
        return '{}{}{}'.format(k, default, prompt_val)

    @property
    def value(self):
        return self._value.get_val()

    @value.setter
    def value(self, value):
        while True:
            try:
                self._set_value(value)
                break
            except FlagMakerConfigurationError as err:
                cprint(str(err), 'red')

    def _set_value(self, value):
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
        elif self.method == flags.DEFINE_enum:
            options = self.options
            is_iterable = isinstance(options, Iterable)
            if not (is_iterable and value in options):
                raise FlagMakerInputError(
                    'Need to choose one of [{}]'.format(', '.join(options))
                )
        self._value.set_val(value)
        # perform actions

        if self.after is None:
            self._error = False
            return

        in_called = (self, self.after) not in self.called
        if in_called:
            self.called[(self, self.after)] = True
            self.after(self)

    def get_default_or_cache(self) -> str:
        if self.cache is not None:
            default_or_cache = self.cache
        else:
            default_or_cache = self.default
        return default_or_cache

    def set_value(self, key: str = '', value: str = '', 
                  ask: str = '', init: str = ''):
        while True:
            num_opts = int(value != '') + int(ask != '') + int(init != '')
            if num_opts != 1:
                raise FlagMakerInputError('Need to choose either '
                                          'init, value or ask')
            default_or_cache = self.get_default_or_cache()
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
                kwargs = {
                    'bottom_toolbar': ANSI(self.help)
                }
                if self.method == flags.DEFINE_enum:
                    choices = [str(i[0])
                               for i in
                               enumerate(self.options, start=1)]
                    kwargs['validator'] = ChoiceValidator(choices)
                    kwargs['complete_style'] = CompleteStyle.READLINE_LIKE
                    selection = prompt(ANSI(ask), **kwargs)
                    if selection == '':
                        val = default_or_cache
                    else:
                        val = self.options[int(selection)-1]
                elif self.method == flags.DEFINE_multi_string:
                    val = []
                    i = 0
                    while True:
                        i += 1
                        res = prompt(ANSI(
                            "{} #{} (Empty Value to finish): ".format(ask, i)
                        ), **kwargs)
                        if res == '':
                            break
                        val.append(res)
                else:
                    val = prompt(ANSI(ask + ": "), **kwargs)
                if val == '' and default_or_cache is not None:
                    self.value = default_or_cache
                else:
                    self.value = val
                if self.value is not None:
                    SettingConfig.cache_dict[key] = self.value
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
        return self.value or ''

    def __repr__(self):
        return '[{0}: {1}]'.format(
            self.default,
            str(self.value) if self.value else '',
        )

    def __bool__(self):
        return bool(self.value)

    def __index__(self):
        return self.value

    def __getitem__(self, item) -> SettingOptionInterface:
        return self.value.__getitem__(item)


class SettingBlock:
    def __init__(self, block: str,
                 settings: Dict[str, SettingOption],
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

    def load_settings(self):
        self.start()
        first = True
        interactive_mode = self.args[0].settings.pop('interactive')
        cache: dict = {}
        if os.path.exists(SettingConfig.cache_file):
            try:
                with open(SettingConfig.cache_file, 'r') as fh:
                    cache = yaml.load(
                        fh.read(), Loader=yaml.Loader
                    ) or {}
            except ParserError:
                cache = {}
                os.remove(SettingConfig.cache_file)

        for block in self.args:
            header_shown = False
            if block.conditional is not None and not block.conditional(self):
                continue
            for k, setting in block.settings.items():
                setting.cache = cache[k] if k in cache else None

                if setting.maybe_needs_input():
                    if not interactive_mode and setting.default:
                        setting.set_value(k, init=setting.default)
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
                            setting.set_value(k, ask=setting.get_prompt(k))
                        except FlagMakerPromptInterruption as err:
                            setting.set_value(k, value=err.value)
        with open(SettingConfig.cache_file, 'w+') as fh:
            fh.write(yaml.dump(
                SettingConfig.cache_dict, Dumper=yaml.Dumper
            ))
        return self

    def assign_flags(self) -> flags:
        for block in self.args:
            for k, setting in block.settings.items():
                kwargs = {
                    'default': None,
                }
                if not setting.include_in_interactive:
                    kwargs['default'] = setting.default
                if setting.method == flags.DEFINE_enum:
                    kwargs['enum_values'] = setting.options
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

    def __enter__(self):
        return self

    def __exit__(self, err, value, traceback):
        with open(SettingConfig.cache_file, 'a+') as fh:
            fh.write(yaml.dump(
                SettingConfig.cache_dict, Dumper=yaml.Dumper
            ))


AbstractSettingsClass = ClassVar[T]


class Config(Generic[T]):
    """The entry point for setting the Settings class for an app.

    Example: Config(MySettingsClass)

    This will bootstrap the settings class correctly.
    """

    def __init__(self, s: ClassVar[T]):
        self.s: ClassVar[T] = s
        self.instance = s()
        self.instance.install()

    def get(self) -> T:
        if not FLAGS.is_parsed():
            raise FlagMakerConfigurationError(
                'Do not call this '
                'method until after app.run()'
            )
        with self.instance as instance:  
            instance.load_settings()
        return self.instance
