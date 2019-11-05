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
        in_called = self.after is not None and self.after not in self.called
        if in_called or self._error:
            self.called[self.after] = True
            self._error = not self.after(self)
        else:
            self._error = False

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
                if self._error:
                    self.value = None
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
        print('==========================')


