from absl import flags
from google.cloud.storage import Bucket
from typing import Dict
from typing import List
from termcolor import cprint
from google.cloud import storage
from google.api_core import exceptions
from typing import overload

FLAGS = flags.FLAGS

StringList = List[str]
StringKeyDict = Dict[str, any]
Buckets = List[Bucket]

class SettingOptions(object):
    default = None
    help = None
    method: callable = None
    value: str = None
    required: bool = False
    validation: callable = None
    show: callable = None
    after: callable = None
    prompt: callable or str = None
    __value_set: bool = False
    custom_data: StringKeyDict = {}

    def __init__(self):
        super().__init__()

    @classmethod
    def create(cls, helptext=None, default=None, method=flags.DEFINE_string,
               required=True, validation=None, show=None, after=None,
               prompt=None):
        fl = cls()
        fl.__value_set = False
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
            prompt += '\nInput: '
        return input(
            '{0} ({1}){2}: '.format(self.help, k, default, prompt)
        )

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

    def __repr__(self):
        return '[{0}{1}]'.format(
            self.help, ' (' + self.value + ')' if self.value else ''
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
    def validate(s: SettingOptions):
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
        if not setting:
            return
        client = storage.Client(project=args['gcp_project_name'].value)

        class ChooseAnother:
            toggle = False

        ChooseAnother.toggle = False
        try:
            return client.get_bucket(setting)
        except exceptions.NotFound as e:
            r = input(
                "Cannot find bucket {0}. Create [y/n]? ".format(setting)
            )
            if r.lower() == 'y':
                ChooseAnother.toggle = True
                return client.create_bucket(
                    setting,
                    project=args['gcp_project_name'].value
                )
        except exceptions.Forbidden as e:
            ChooseAnother.toggle = True
            cprint("Please select a GCP bucket you own and have access to or "
                   "double check your permissions. If you are having trouble "
                   "finding an unclaimed unique name, consider adding your "
                   "project name as a prefix.", "red")
        if ChooseAnother.toggle:
            setting.value = input(
                "Press Ctrl+C to cancel or choose a different input: "
            )
            Hooks.create_bucket(setting)

    @staticmethod
    def bucket_options(setting: SettingOptions):
        client = storage.Client(project=args['gcp_project_name'].value)
        buckets = setting.custom_data['buckets'] = list(client.list_buckets())
        i = 0
        result = '\n'.join(['{}: {}'.format(i, buckets[i])
                            for b in range(len(buckets))])
        print(result)
        return result


SimpleFlags = Dict[str, SettingOptions]

args: SimpleFlags = {
    'gcp_project_name': SettingOptions.create('GCP Project Name'),
    'raw_dataset': SettingOptions.create(
        'Where all raw BigQuery data is stored',
        default='raw'
    ),
    'view_dataset': SettingOptions.create(
        'Where all formatted BigQuery data is stored',
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
        after=Hooks.create_bucket
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
            self[k].set_value(getattr(FLAGS, k))


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
            while True:
                setting.set_value(setting.get_prompt(k))
                if setting.value == '' and setting.default is not None:
                    setting.value = setting.default
                validated = Validator.validate(setting)
                if not validated:
                    continue
                if setting.value != '' or not setting.required:
                    break
                cprint('Required Field', 'red')
    return settings
