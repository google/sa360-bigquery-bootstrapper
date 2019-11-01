from absl import flags
from google.api_core import exceptions
from google.cloud import storage
from termcolor import cprint

from flagmaker import AbstractSettings
from flagmaker import SettingOption
from flagmaker.building_blocks import SettingsInterface
from flagmaker.exceptions import FlagMarkerInternalError


class AppSettings(AbstractSettings):
    """Settings for the BQ bootstrapper

    Add all flags under settings()
    """
    def settings(self) -> dict:
        hooks = Hooks()
        args = {
            'gcp_project_name': SettingOption.create('GCP Project Name'),
            'raw_dataset': SettingOption.create(
                self,
                'Dataset where raw data will be stored',
                default='raw'
            ),
            'view_dataset': SettingOption.create(
                self,
                'Dataset where view data will be generated and stored',
                default='views'
            ),
            'agency_id': SettingOption.create(self, 'SA360 Agency ID'),
            'advertiser_id': SettingOption.create(
                self,
                'SA360 Advertiser IDs',
                method=flags.DEFINE_list
            ),
            'historical_data': SettingOption.create(
                self,
                'Include Historical Data?',
                method=flags.DEFINE_boolean
            ),
            'storage_bucket': SettingOption.create(
                self,
                'Storage Bucket Name',
                prompt=Hooks.bucket_options,
                after=hooks.create_bucket,
            ),
            'historical_table_name': SettingOption.create(
                self,
                'Name of historical table',
                show=lambda: args['historical_data'].value
            ),
        }
        return args


class Hooks:
    """Convenience class to add all hooks

    Includes validation hooks, after hooks, and more.
    """

    def __init__(self):
        self.valid_bucket = False

    def create_bucket(self, setting: SettingOption) -> bool:
        if self.valid_bucket:
            print('done')
            return True
        settings = setting.settings

        class ChooseAnother:
            toggle = False
        while True:
            if 'buckets' not in setting.custom_data:
                raise FlagMarkerInternalError('No buckets set.')
            if setting.value.isnumeric():
                val = int(setting.value)
                if val <= len(setting.custom_data['buckets']):
                    setting.value = setting.custom_data['buckets'][val - 1].name
                    self.valid_bucket = True
                    print(self.valid_bucket)
                    return True
                else:
                    cprint('Invalid selection', 'red', attrs=['bold'])
                    return False
            elif setting.value == 'c':
                setting.value = input('Select project name: ')
            elif not ChooseAnother.toggle:
                print(self.valid_bucket)
                if self.valid_bucket:
                    return True
                cprint('Select a valid input option', 'red')
                return False
            if not setting:
                self.valid_bucket = True
                return True
            client = storage.Client(project=settings['gcp_project_name'].value)

            ChooseAnother.toggle = False
            try:
                setting.value = client.get_bucket(setting.value).name
                self.valid_bucket = True
                return True
            except exceptions.NotFound as e:
                r = input(
                    'Cannot find bucket {0}. Create [y/n]? '.format(setting)
                )
                if r.lower() == 'y':
                    ChooseAnother.toggle = True
                    result = client.create_bucket(
                        setting.value,
                        project=settings['gcp_project_name'].value
                    )
                    cprint('Created ' + setting.value, 'green', attrs=['bold'])
                    setting.value = result
                    self.valid_bucket = True
                    return True
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
    def bucket_options(setting: SettingOption):
        settings = setting.settings
        client = storage.Client(project=settings['gcp_project_name'].value)
        buckets = setting.custom_data['buckets'] = list(client.list_buckets())
        bucket_size = len(buckets)
        result = '\n'.join(['{}: {}'.format(b+1, buckets[b])
                            for b in range(bucket_size)])
        result += '\nc: Create New Bucket'
        return result

