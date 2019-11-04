from typing import Dict

from absl import flags
from termcolor import cprint

from .building_blocks import SettingsInterface
from flagmaker.building_blocks import SettingOptionInterface
from flagmaker.hints import StringKeyDict
from .exceptions import FlagMakerInputError
from .sanity import Validator
from .building_blocks import Value


class SettingOption(SettingOptionInterface):
    settings: SettingsInterface = None
    default = None
    help = None
    method: callable = None
    _value: Value = None
    required: bool = False
    validation: callable = None
    show: callable = None
    after: callable = None
    prompt: callable or str = None
    custom_data: StringKeyDict = {}
    include_in_interactive: bool = True
    called: dict = {}
    __error: bool = False

    def __init__(self):
        self._value = Value()

    @classmethod
    def create(cls, settings: SettingsInterface, helptext=None, default=None,
               method=flags.DEFINE_string, required=True, validation=None,
               show=None, after=None, prompt=None,
               include_in_interactive=True):
        fl = cls()
        fl.settings = settings
        fl.default = default
        fl.help = helptext
        fl.method = method
        fl.required = required
        fl.validation = validation
        fl.show = show
        fl.after = after
        fl.prompt = prompt
        fl.include_in_interactive = include_in_interactive
        return fl

    def get_prompt(self, k):
        default = ' [{0}]'.format(
            self.default
        ) if self.default is not None else ''
        prompt = ''
        if self.prompt is not None:
            prompt += '\n'
            if self.prompt is str:
                prompt += self.prompt
            if callable(self.prompt):
                prompt += self.prompt(self)
            prompt += '\nInput'
        return '{} ({}){}{}: '.format(self.help, k, default, prompt)

    @property
    def value(self):
        return self._value.get_val()

    @value.setter
    def value(self, value):
        if self.method == flags.DEFINE_boolean:
            if value == '1' or value == 'true':
                value = True
            elif value == '0' or value == 'false':
                value = False
                return
        elif self.method == flags.DEFINE_integer:
            value = int(value)
        self._value.set_val(value)
        # perform actions
        if self.after is not None and self.after not in self.called:
            self.called[self.after] = True
            self.__error = not self.after(self)
        else:
            self.__error = False

    def set_value(self, value: str = '', prompt: str = '', init: str = ''):
        while True:
            num_opts = int(value != '') + int(prompt != '') + int(init != '')
            if num_opts != 1:
                raise FlagMakerInputError('Need to choose either '
                                          'init, value or prompt')

            if init is None:
                return
            elif init != '':
                self.value = init
                return

            if prompt != '':
                val = input(prompt)
                if val == '' and self.default is not None:
                    self.value = self.default
                else:
                    self.value = val
                if self.__error:
                    self.value = None
                    continue
            else:
                self.value = value

            if not Validator.validate(self):
                continue
            if self.value_explicitly_set() or not self.required:
                return
            else:
                cprint('Required Field', 'red')

    def value_explicitly_set(self) -> bool:
        return bool(self._value)

    def maybe_needs_input(self):
        return not self.value_explicitly_set() and (
            self.show is None or self.show())

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
