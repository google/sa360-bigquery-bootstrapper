from absl import flags
from typing import Dict
from typing import List
from termcolor import cprint
from google.cloud import storage
from typing import overload

FLAGS = flags.FLAGS

StringList = List[str]


class SimpleFlag(str):
    default = None
    help = None
    method: callable = None
    value: str = None
    required: bool = False
    validation: callable = None
    show: callable = None
    after: callable = None

    def __init__(self):
        super().__init__()

    @classmethod
    def create(cls, helptext=None, default=None, method=flags.DEFINE_string,
                 required=True, validation=None, show=None, after=None):
        fl = cls()
        fl.__value_set = False
        fl.default = default
        fl.help = helptext
        fl.method = method
        fl.required = required
        fl.validation = validation
        fl.show = show
        fl.after = after
        return fl

    @staticmethod
    def dash(v: str) -> str:
        return '- ' + v

    def set_value(self, value: str):
        if self.method == flags.DEFINE_boolean:
            if value == '1' or value == 'true':
                value = True
            elif value == '0' or value == 'false':
                value = False
        self.value = value
        self.__value_set = value is not None
        # perform actions
        if self.after is not None:
            self.after(self)

    def value_explicitly_set(self) -> bool:
        return self.__value_set

    def maybe_needs_input(self):
        return not self.value_explicitly_set() and (
            self.show is None or self.show())

    def __str__(self):
        return self.value


class Validator:
    @staticmethod
    def check_bool(setting: SimpleFlag, errors: list):
        value = setting.value
        if isinstance(value, bool):
            return
        if value == 'true' or value == '1' or value == 'false' or value == '0':
            return
        errors.append('Invalid option '
                      + setting.value
                      + '. Expecting true/false or 0/1')

    @staticmethod
    def validate(s: SimpleFlag):
        if s.method == flags.DEFINE_boolean and s.validation is None:
            s.validation = Validator.check_bool
        if s.validation is not None:
            errors: StringList = []
            s.validation(s, errors)
            if len(errors) > 0:
                cprint('Error{0}:'.format(
                    's' if len(errors) > 1 else ''
                ), 'red', attrs=['bold'])
                cprint('\n'.join(map(SimpleFlag.dash, errors)), 'red')
                return False
        return True


class Hooks:
    @staticmethod
    def create_bucket(setting: SimpleFlag):
        if not setting.value:
            return
        client = storage.Client()
        bucket = client.get_bucket(setting.value)
        print(bucket)
        client.create_bucket(setting.value,
                             project=args['gcp_project_name'].value)


SimpleFlags = Dict[str, SimpleFlag]

args: SimpleFlags = {
    'gcp_project_name': SimpleFlag.create('GCP Project Name'),
    'raw_dataset': SimpleFlag.create(
        'Where all raw BigQuery data is stored',
        default='raw'
    ),
    'view_dataset': SimpleFlag.create(
        'Where all formatted BigQuery data is stored',
        default='views'
    ),
    'agency_id': SimpleFlag.create('SA360 Agency ID'),
    'advertiser_id': SimpleFlag.create(
        'SA360 Advertiser IDs',
        method=flags.DEFINE_list
    ),
    'historical_data': SimpleFlag.create(
        'Include Historical Data?',
        method=flags.DEFINE_boolean
    ),
    'storage_bucket': SimpleFlag.create(
        'Storage Bucket Name',
        after=Hooks.create_bucket
    ),
    'historical_table_name': SimpleFlag.create(
        'Name of historical table',
        show=lambda: args['historical_data'].value
    ),
}


class Settings(SimpleFlags):
    def __init__(self):
        super().__init__(args)
        for k in args.keys():
            self[k].set_value(getattr(FLAGS, k))


def assign_flags() -> flags:
    for k in args:
        args[k].method(k, None, args[k].help)
    return flags.FLAGS


def load_settings():
    settings: Settings = Settings()
    first = True
    for k in settings.keys():
        setting: SimpleFlag = settings[k]
        if setting.maybe_needs_input():
            if first:
                cprint('Interactive Setup', attrs=['bold'])
                first = False
            default = ' [{0}]'.format(
                setting.default
            ) if setting.default is not None else ''
            while True:
                setting.set_value(input(
                    '{0} ({1}){2}: '.format(k, setting.help, default)
                ))
                if setting.value == '' and setting.default is not None:
                    setting.value = setting.default
                validated = Validator.validate(setting)
                if not validated:
                    continue
                if setting.value != '' or not setting.required:
                    break
                cprint('Required Field', 'red')
    return settings
