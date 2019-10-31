from absl import flags
from google.cloud.storage import Bucket
from typing import Dict
from typing import List
from termcolor import cprint
from google.cloud import storage
from google.api_core import exceptions
from enum import Enum

from exceptions import BootstrapperInputError
from exceptions import BootstrapperInternalError

FLAGS = flags.FLAGS

StringList = List[str]
StringKeyDict = Dict[str, any]
Buckets = List[Bucket]

class ValueType(Enum):
    STRING = 0
    INTEGER = 1
    BOOLEAN = 2
    LIST = 3

class Value(object):
    value_type: ValueType = ValueType.STRING
    b_val: bool = None
    i_val: int = None
    s_val: str = None
    l_val: list = None
    __value_set: bool = False

    def set_val(self, val):
        t = type(val)

        def set():
            if t is int:
                self.i_val = val
                self.value_type = ValueType.INTEGER
                return
            if t is str:
                self.s_val = val
                return
            if t is bool:
                self.b_val = val
                self.value_type = ValueType.BOOLEAN
            if t is list:
                self.l_val = val
                self.value_type = ValueType.LIST
        set()
        self.__value_set = val != '' and val is not None

    def get_val(self):
        if self.value_type == ValueType.STRING:
            return self.s_val
        if self.value_type == ValueType.BOOLEAN:
            return self.b_val
        if self.value_type == ValueType.INTEGER:
            return self.i_val
        if self.value_type == ValueType.LIST:
            return self.l_val

    def __bool__(self):
        return self.__value_set


class SettingOptions(object):
    default = None
    help = None
    method: callable = None
    __value: Value = None
    required: bool = False
    validation: callable = None
    show: callable = None
    after: callable = None
    prompt: callable or str = None
    custom_data: StringKeyDict = {}
    __error: bool = False

    def __init__(self):
        self.__value = Value()

    @classmethod
    def create(cls, helptext=None, default=None, method=flags.DEFINE_string,
               required=True, validation=None, show=None, after=None,
               prompt=None):
        fl = cls()
        fl.default = default
        fl.help = helptext
        fl.method = method
        fl.required = required
        fl.validation = validation
        fl.show = show
        fl.after = after
        fl.prompt = prompt
        return fl

    @staticmethod
    def dash(v: str) -> str:
        return '- ' + v

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

    def set_bool(self, value):
        self.value_type = ValueType.BOOLEAN
        self.bool_value = value

    @property
    def value(self):
        return self.__value.get_val()

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
        self.__value.set_val(value)
        # perform actions
        if self.after is not None:
            self.__error = not self.after(self)
        else:
            self.__error = False

    def set_value(self, value: str = '', prompt: str = '', init: str = ''):
        while True:
            num_opts = int(value != '') + int(prompt != '') + int(init != '')
            if num_opts != 1:
                raise BootstrapperInputError('Need to choose either '
                                             'init, value or prompt')
            if init is None:
                return
            elif init != '':
                self.__value.set_val(init)
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
        return bool(self.__value)

    def maybe_needs_input(self):
        return not self.value_explicitly_set() and (
            self.show is None or self.show())

    def __str__(self):
        return self.value

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


class Validator:
    @staticmethod
    def check_bool(setting: SettingOptions, errors: list):
        value = setting.value
        if isinstance(value, bool):
            return
        if value == 'true' or value == '1' or value == 'false' or value == '0':
            return
        errors.append('Invalid option '
                      + setting.value
                      + '. Expecting true/false or 0/1')

    @staticmethod
    def validate(s: SettingOptions) -> bool:
        if s.method == flags.DEFINE_boolean and s.validation is None:
            s.validation = Validator.check_bool
        if s.validation is not None:
            errors: StringList = []
            s.validation(s, errors)
            if len(errors) > 0:
                cprint('Error{0}:'.format(
                    's' if len(errors) > 1 else ''
                ), 'red', attrs=['bold'])
                cprint('\n'.join(map(SettingOptions.dash, errors)), 'red')
                return False
        return True


class Hooks:
    @staticmethod
    def create_bucket(setting: SettingOptions):
        class ChooseAnother:
            toggle = False
        while True:
            if 'buckets' not in setting.custom_data:
                raise BootstrapperInternalError('No buckets set.')
            if setting.value.isnumeric():
                val = int(setting.value)
                if val <= len(setting.custom_data['buckets']):
                    setting.value = setting.custom_data['buckets'][val - 1]
                else:
                    cprint('Invalid selection', 'red', attrs=['bold'])
                    return False
            elif setting.value == 'c':
                setting.value = input('Select project name: ')
            elif not ChooseAnother.toggle:
                cprint('Select a valid input option', 'red')
                return False
            if not setting:
                return True
            client = storage.Client(project=args['gcp_project_name'].value)

            ChooseAnother.toggle = False
            try:
                return client.get_bucket(setting.value)
            except exceptions.NotFound as e:
                r = input(
                    'Cannot find bucket {0}. Create [y/n]? '.format(setting)
                )
                if r.lower() == 'y':
                    ChooseAnother.toggle = True
                    result = client.create_bucket(
                        setting.value,
                        project=args['gcp_project_name'].value
                    )
                    cprint('Created ' + setting.value, 'green', attrs=['bold'])
                    print(result)
                    return result
                break
            except exceptions.Forbidden as e:
                ChooseAnother.toggle = True
                cprint('-------------------------------------\n'
                       'Please select a GCP bucket you own and have access to '
                       'or double check your permissions. \n'
                       'If you are having trouble finding an unclaimed unique '
                       'name, consider adding your project name as a prefix.\n'
                       '-------------------------------------',
                       'red')
            if ChooseAnother.toggle:
                setting.value = input(
                    'Press Ctrl+C to cancel or choose a different input: '
                )

    @staticmethod
    def bucket_options(setting: SettingOptions):
        client = storage.Client(project=args['gcp_project_name'].value)
        buckets = setting.custom_data['buckets'] = list(client.list_buckets())
        bucket_size = len(buckets)
        result = '\n'.join(['{}: {}'.format(b+1, buckets[b])
                            for b in range(bucket_size)])
        result += '\nc: Create New Bucket'
        return result


SimpleFlags = Dict[str, SettingOptions]

args: SimpleFlags = {
    'gcp_project_name': SettingOptions.create('GCP Project Name'),
    'raw_dataset': SettingOptions.create(
        'Dataset where raw data will be stored',
        default='raw'
    ),
    'view_dataset': SettingOptions.create(
        'Dataset where view data will be generated and stored',
        default='views'
    ),
    'agency_id': SettingOptions.create('SA360 Agency ID'),
    'advertiser_id': SettingOptions.create(
        'SA360 Advertiser IDs',
        method=flags.DEFINE_list
    ),
    'historical_data': SettingOptions.create(
        'Include Historical Data?',
        method=flags.DEFINE_boolean
    ),
    'storage_bucket': SettingOptions.create(
        'Storage Bucket Name',
        prompt=Hooks.bucket_options,
        after=Hooks.create_bucket,
    ),
    'historical_table_name': SettingOptions.create(
        'Name of historical table',
        show=lambda: args['historical_data'].value
    ),
}


class Settings(SimpleFlags):
    def __init__(self):
        super().__init__(args)
        for k in args.keys():
            self[k].set_value(init=FLAGS.get_flag_value(k, None))


def assign_flags() -> flags:
    for k in args:
        args[k].method(k, None, args[k].help)
    return flags.FLAGS


def load_settings():
    settings: Settings = Settings()
    first = True
    for k in settings.keys():
        setting: SettingOptions = settings[k]
        if setting.maybe_needs_input():
            if first:
                cprint('Interactive Setup', attrs=['bold'])
                first = False
            setting.set_value(prompt=setting.get_prompt(k))
    return settings
