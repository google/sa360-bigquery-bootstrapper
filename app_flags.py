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

    def __init__(self):
        self.hooks = Hooks()

    def settings(self) -> dict:
        args = {
            'gcp_project_name': SettingOption.create(
                self,
                'GCP Project Name',
                after=self.hooks.set_clients,
            ),
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
            'location': SettingOption.create(
                self,
                'Cloud Location (2 letter country code)',
                default='US'
            ),
            'agency_id': SettingOption.create(self, 'SA360 Agency ID'),
            'advertiser_id': SettingOption.create(
                self,
                'SA360 Advertiser IDs',
                method=flags.DEFINE_list,
            ),
            'has_historical_data': SettingOption.create(
                self,
                'Include Historical Data?',
                method=flags.DEFINE_boolean,
            ),
            'storage_bucket': SettingOption.create(
                self,
                'Storage Bucket Name',
                prompt=self.hooks.bucket_options,
                after=self.hooks.create_bucket,
            ),
            'historical_table_name': SettingOption.create(
                self,
                'Name of historical table',
                show=lambda: args['has_historical_data'].value,
            ),
            'file_path': SettingOption.create(
                self,
                'Provide your CSV file path. \n'
                'If in storage, provide a full URL starting with gs://. '
                'Otherwise, drag and drop the file here '
                'and just specify the file name.\n'
                'File Location',
                show=lambda: args['has_historical_data'].value,
                after=self.hooks.ensure_utf8
            )
        }
        return args


class Hooks:
    """Convenience class to add all hooks

    Includes validation hooks, after hooks, and more.
    """

    def __init__(self):
        self.storage = None

    def set_clients(self, setting: SettingOption):
        settings = setting.settings
        self.storage = storage.Client(
            project=settings['gcp_project_name'].value
        )

    def create_bucket(self, setting: SettingOption) -> bool:
        settings = setting.settings

        class ChooseAnother:
            toggle = False

        while True:
            if 'buckets' not in setting.custom_data:
                return
            if setting.value.isnumeric():
                val = int(setting.value)
                if val <= len(setting.custom_data['buckets']):
                    value = setting.custom_data['buckets'][val - 1].name
                    setting.value = value
                    return True
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
            ChooseAnother.toggle = False
            try:
                setting.value = self.storage.get_bucket(setting.value).name
                return True
            except exceptions.NotFound as e:
                r = input(
                    'Cannot find bucket {0}. Create [y/n]? '.format(setting)
                )
                if r.lower() == 'y':
                    ChooseAnother.toggle = True
                    result = self.storage.create_bucket(
                        setting.value,
                        project=settings['gcp_project_name'].value
                    )
                    cprint('Created ' + setting.value, 'green', attrs=['bold'])
                    setting.value = result
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

    def bucket_options(self, setting: SettingOption):
        settings = setting.settings
        buckets = setting.custom_data['buckets'] = list(self.storage.list_buckets())
        bucket_size = len(buckets)
        result = '\n'.join(['{}: {}'.format(b+1, buckets[b])
                            for b in range(bucket_size)])
        result += '\nc: Create New Bucket'
        return result

    def ensure_utf8(self, setting: SettingOption):
        settings = setting.settings
        filename = setting.value
        bucket_name = settings['storage_bucket'].value
        bucket = self.storage.get_bucket(settings['storage_bucket'].value)
        file = None
        if filename.startswith('gs://'):
            filename = filename.replace('gs://{}/'.format(bucket_name), '')
            file = bucket.blob(filename)
            with open('/tmp/' + filename, 'w+b') as fh:
                file.download_to_file(fh, self.storage)
        else:
            file = bucket.blob(filename)

        def try_decode(fname, encoding):
            with open('/tmp/' + fname, 'rb') as fh:
                if encoding == 'utf-8':
                    contents = fh.read()
                    contents.decode(encoding)
                    return contents
                contents = fh.read().decode(encoding).encode('utf-8')
                cprint('Found a {}-'.format(encoding) +
                       'encoded file and turned to utf-8', 'cyan')
                return contents

        for encoding in ['utf-8', 'utf-16', 'latin-1']:
            try:
                contents = try_decode(filename, encoding)
                with open('/tmp/' + filename, 'w+b') as fh:
                    fh.write(contents)
                    fh.seek(0)
                    file.upload_from_file(fh)
                break
            except UnicodeDecodeError:
                continue
        return True
