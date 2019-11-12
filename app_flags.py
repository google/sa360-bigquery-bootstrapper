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
import re
from enum import Enum

from absl import flags
from datetime import datetime
from dateutil.parser import parse as parse_date
from google.api_core import exceptions
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud.storage import Bucket
from prompt_toolkit.completion import PathCompleter
from termcolor import cprint
from prompt_toolkit import prompt
from xlrd import open_workbook
import os

from csv_decoder import Decoder
from flagmaker import settings
from typing import List

from flagmaker.exceptions import FlagMakerPromptInterruption
from flagmaker.exceptions import FlagMakerConfigurationError


class FileLocationOptions(Enum):
    GCS_BUCKET = 'GCS Bucket'
    CLOUD_SHELL = 'Cloud Shell Upload'


class ReportLevelOptions(Enum):
    CONVERSION = 'conversion'
    KEYWORD = 'keyword'
    CAMPAIGN = 'campaign'


class AppSettings(settings.AbstractSettings):
    """Settings for the BQ bootstrapper

    Add all flags under settings()
    """

    def __init__(self):
        self.hooks = Hooks()

    def settings(self) -> List[settings.SettingBlock]:
        args = [
            settings.SettingBlock('General Settings', {
                'gcp_project_name': settings.SettingOption.create(
                    self,
                    'GCP Project Name',
                    after=self.hooks.set_clients,
                ),
                'raw_dataset': settings.SettingOption.create(
                    self,
                    'Dataset where raw data will be stored',
                    default='raw'
                ),
                'view_dataset': settings.SettingOption.create(
                    self,
                    'Dataset where view data will be generated and stored',
                    default='views'
                ),
                'location': settings.SettingOption.create(
                    self,
                    'Cloud Location (2 letter country code)',
                    default='US'
                ),
                'agency_id': settings.SettingOption.create(self, 'SA360 Agency ID'),
                'advertiser_id': settings.SettingOption.create(
                    self,
                    'SA360 Advertiser',
                    method=flags.DEFINE_string,
                ),
                'has_historical_data': settings.SettingOption.create(
                    self,
                    'Include Historical Data?\n'
                    'Note: This will be true as long as any advertisers '
                    'are going to have historical data included.\nIf you are '
                    'uploading multiple advertisers, any of them with '
                    'historical data should have the same format. If not, '
                    'upload individual advertisers\nInput',
                    method=flags.DEFINE_boolean,
                ),
                'storage_bucket': settings.SettingOption.create(
                    self,
                    'Storage Bucket Name',
                    prompt=self.hooks.bucket_options,
                    after=self.hooks.create_bucket,
                ),
                'file_location': settings.SettingOption.create(
                    self,
                    'Is your data in GCS or did you upload here?',
                    method=flags.DEFINE_enum,
                    options=FileLocationOptions,
                    required=True,
                ),
                'file_path': settings.SettingOption.create(
                    self,
                    'Historical Data CSV File Path',
                    after=self.hooks.handle_csv_paths,
                    method=flags.DEFINE_multi_string,
                    conditional=lambda s: s['has_historical_data'].value,
                ),
                'first_date_conversions': settings.SettingOption.create(
                    self,
                    'The first date of conversions',
                    after=self.hooks.convert_to_date,
                    conditional=lambda s: s['has_historical_data'].value,
                )
            }),
            settings.SettingBlock('Historical Columns', {
                'account_column_name': settings.SettingOption.create(
                    self,
                    'Column name for the Account Name',
                    default='account_name',
                    after=self.hooks.map_historical_column,
                ),
                'campaign_column_name': settings.SettingOption.create(
                    self,
                    'Campaign Column Name for Historical Data '
                    '(only if including historical data)',
                    default='campaign_name',
                    after=self.hooks.map_historical_column,
                ),
                'conversion_count_column': settings.SettingOption.create(
                    self,
                    'Specify the conversion column',
                    default='conversions',
                    after=self.hooks.map_historical_column,
                ),
                'has_revenue_column': settings.SettingOption.create(
                    self,
                    'Does the report show revenue?',
                    default=True,
                    method=flags.DEFINE_bool,
                ),
                'revenue_column_name': settings.SettingOption.create(
                    self,
                    'Column name for revenue gained from conversion(s)\n '
                    'If left blank, then no revenue will be recorded.\n'
                    'Column Name',
                    default='revenue',
                    required=False,
                    conditional=lambda s: s['has_revenue_column'].value,
                    after=self.hooks.map_historical_column,
                ),
                'has_device_segment': settings.SettingOption.create(
                    self,
                    'Does the report have a device segment column?',
                    default=True,
                    method=flags.DEFINE_bool,
                ),
                'device_segment_column_name': settings.SettingOption.create(
                    self,
                    'Device Segment Column Name (if there is none, omit)',
                    default='device_segment',
                    conditional=lambda s: s['has_device_segment'].value,
                    after=self.hooks.map_historical_column,
                ),
                'report_level': settings.SettingOption.create(
                    self,
                    'Specify the report level of the '
                    'advertisers being uploaded (conversion/keyword/campaign)',
                    default='keyword',
                    method=flags.DEFINE_enum,
                    options=ReportLevelOptions,
                ),
                'date_column_name': settings.SettingOption.create(
                    self,
                    'Column name for the Date Value',
                    default='date',
                    after=self.hooks.map_historical_column,
                ),
                'adgroup_column_name': settings.SettingOption.create(
                    self,
                    'Column name for the ad group name',
                    default='ad_group_name',
                    conditional=lambda s: s['report_level'].value != 'campaign',
                    after=self.hooks.map_historical_column,
                ),
                'keyword_match_type': settings.SettingOption.create(
                    self,
                    'Keyword Match Type Column '
                    '(omit if uploading a campaign-level report)',
                    default='match_type',
                    after=self.hooks.map_historical_column,
                    conditional=lambda s: s['report_level'].value != 'campaign',
                ),
                'keyword_column_name': settings.SettingOption.create(
                    self,
                    'Keyword Column Name',
                    default='keyword',
                    conditional=lambda s: s['report_level'].value != 'campaign',
                    after=self.hooks.map_historical_column,
                )
            }, conditional=lambda s: s['has_historical_data'].value)
        ]
        return args


class Hooks:
    """Convenience class to add all hooks

    Includes validation hooks, after hooks, and more.
    """

    def __init__(self):
        self.storage = None

    def set_clients(self, setting: settings.SettingOption):
        s = setting.settings
        if 'storage_client' in s.custom:
            return True
        s.custom['storage_client'] = self.storage = storage.Client(
            project=s['gcp_project_name'].value
        )
        return True

    def create_bucket(self, setting: settings.SettingOption) -> bool:
        class ChooseAnother:
            toggle = False
        s = setting.settings

        while True:
            if 'buckets' not in s.custom:
                return True
            if setting.value.isnumeric():
                val = int(setting.value)
                if val <= len(s.custom['buckets']):
                    value = s.custom['buckets'][val - 1].name
                    setting.value = value
                    return True
                else:
                    raise FlagMakerConfigurationError('Invalid selection')
            elif setting.value == 'c':
                setting.value = prompt('Select project name: ')
            elif not ChooseAnother.toggle:
                raise FlagMakerConfigurationError('Select a valid option')
            if not setting:
                return True
            ChooseAnother.toggle = False
            try:
                setting.value = self.storage.get_bucket(setting.value).name
                return True
            except exceptions.NotFound as e:
                r = prompt(
                    'Cannot find bucket {0}. Create [y/n]? '.format(setting)
                )
                if r.lower() == 'y':
                    ChooseAnother.toggle = True
                    result = self.storage.create_bucket(
                        setting.value,
                        project=s['gcp_project_name'].value
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
                setting.value = prompt(
                    'Press Ctrl+C to cancel or choose a different input: '
                )

    def bucket_options(self, setting: settings.SettingOption):
        s = setting.settings
        buckets = s.custom['buckets'] = list(self.storage.list_buckets())
        bucket_size = len(buckets)
        result = '\n'.join(['{}: {}'.format(b+1, buckets[b])
                            for b in range(bucket_size)])
        result += '\nc: Create New Bucket'
        return result

    def get_file_paths(self, setting: settings.SettingOption):
        """
        :param setting: SettingOption object
        :raise FlagMakerPromptInterruption Returns '1' interrupting the prompt
               if there is only one valid answer.
        :return: Prompt string
        """
        s = setting.settings
        advertisers = s['advertiser_id'].value

        cprint('If in storage, provide a full URL starting with gs://. '
               'Otherwise, drag and drop the file(s) here '
               'and just specify the file name(s).\n'
               'You can leave any value without historical data'
               'blank.', 'blue')
        if not isinstance(advertisers, list) and len(advertisers) > 1:
            raise FlagMakerPromptInterruption(value='1')
        return (
            'Do you want to:\n' +
            '1. Enter each value separately?\n'
            '2. Enter comma separated values to map each advertiser ID'
        )

    def handle_csv_paths(self, setting: settings.SettingOption):
        choice = setting.value
        advertiser = setting.settings['advertiser_id'].value
        if isinstance(choice, list):
            file_map = {}
            for i in range(len(choice)):
                option = choice[i]
                file_map[advertiser] = option
                if option != '':
                    self.ensure_utf8(setting, option)
            setting.settings.custom['file_map'] = file_map
            return True
        options = []
        if not choice.isnumeric():
            options = choice.split(',')

    def ensure_utf8(self, setting: settings.SettingOption, filename: str):
        s = setting.settings
        bucket_name = s['storage_bucket'].value
        file_location = s['file_location'].value
        bucket: Bucket = None
        try:
            bucket = self.storage.get_bucket(bucket_name)
        except NotFound:
            cprint('Could not find bucket named ' + bucket_name, 'red',
                   attrs=['bold'])
            cprint('Please double-check existence, or remove the '
                   'storage_bucket flag so we can help you create the storage '
                   'bucket interactively.', 'red')
            exit(1)
        local = False
        if file_location == 'GCS Bucket':
            file = bucket.blob(filename)
            path = dirname = '/tmp/in-{}/'.format(str(
                datetime.now().strftime('%Y%m%d%H%m%S%f')
            ))
            os.mkdir(path)
            if not file.exists():
                files = list(bucket.list_blobs(prefix=filename))
                single = False
                for file in files:
                    file_parts = file.name.split('/')
                    with open(dirname + file_parts[-1], 'w+b') as fh:
                        file.download_to_file(fh, self.storage)
                        file.delete()
            else:
                files = [file]
                single = True
        else:  #todo(seancjones: Make sure this section below works)
            single = not os.path.isdir(filename)
            path = filename
        result_dir = Decoder(desired_encoding='utf-8', path=path).run()
        s.custom['blobs'] = []
        for file in os.listdir(result_dir):
            if single:
                blob = bucket.blob(filename)
            else:
                blob = bucket.blob(filename + '/' + file)
            blob.upload_from_filename(result_dir + '/' + file)
            s.custom['blobs'].append(blob.name)
    def map_historical_column(self, setting: settings.SettingOption):
        settings = setting.settings
        setting.value = re.sub(r'[^a-zA-Z0-9]', '_', setting.value)
        if 'historical_map' not in settings.custom:
            settings.custom['historical_map'] = {}
        settings.custom['historical_map'][setting.value] = setting.default

    def convert_to_date(self, setting: settings.SettingOption):
        try:
            location: str = setting.settings['location'].value
            try:
                value = datetime.strptime(setting.value, '%Y-%m-%d')
            except ValueError:
                kwargs = {}
                kwargs['dayfirst'] = not location.lower().startswith('us')
                value = parse_date(setting.value, **kwargs)
                while True:
                    result = prompt(
                        'Date not in yyyy-mm-dd format. '
                        'Converted to {}. Correct? [y/n]'.format(
                            value.strftime('%Y-%m-%d')
                        )
                    )
                    if result == 'n':
                        raise FlagMakerConfigurationError()
                    if result == 'y':
                        break
                    cprint('Invalid option. Select y/n or set value to '
                           'yyyy-mm-dd. Example: 1999-12-31', 'red')
            setting._error = False
            setting.value = value
            return True
        except ValueError:
            raise FlagMakerConfigurationError(
                'Invalid Date Selection. Use either y-m-d or m/d/y'
            )
