from absl import flags
from typing import Dict
from termcolor import colored, cprint

FLAGS = flags.FLAGS

class SimpleFlag:
    default = None
    help = None
    method: callable = None
    value: str = None

    def __init__(self, helptxt=None, default=None, method=flags.DEFINE_string):
        self.__value_set = False
        self.default = default
        self.help = helptxt
        self.method = method

    def set_value(self, value: str):
        self.value = value
        self.__value_set = True

    def value_explicitly_set(self) -> bool:
        return self.__value_set is True

    def __str__(self):
        return self.value


SimpleFlags = Dict[str, SimpleFlag]

args: SimpleFlags = {
    'gcp_project_name': SimpleFlag('GCP Project Name'),
    'raw_dataset': SimpleFlag('Where all raw BigQuery data is stored'),
    'view_dataset': SimpleFlag('Where all formatted BigQuery data is stored',
                               default='views'),
    'historical_table_name': SimpleFlag('Name of historical table (if any)'),
    'agency_id': SimpleFlag('SA360 Agency ID'),
    'advertiser_id': SimpleFlag('SA360 Advertiser IDs',
                                method=flags.DEFINE_list),
}


class Settings(SimpleFlags):
    def __init__(self):
        super().__init__(args)
        for k in args.keys():
            self[k].set_value(FLAGS.__getattr__(k))


def assign_flags() -> flags:
    for k in args:
        args[k].method(k, args[k].default, args[k].help)
    return flags.FLAGS


def load_settings():
    settings: Settings = Settings()
    first = True
    for k in settings.keys():
        setting: SimpleFlag = settings[k]
        if not setting.value_explicitly_set():
            if first:
                cprint('Interactive Setup', attrs=['bold'])
                first = False
            default = ' [{0}]'.format(
                setting.default
            ) if hasattr(setting.default) else ''

            setting.value = input('{0} ({1}){2}: '.format(k,
                                                          setting.help,
                                                          default))
    return settings
