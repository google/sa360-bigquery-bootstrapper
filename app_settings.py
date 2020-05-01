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
from datetime import datetime
from enum import Enum
from typing import Dict
from typing import List

import numpy as np
from absl import flags
from dateutil.parser import parse as parse_date
from google.api_core import exceptions
from google.cloud import storage
from prompt_toolkit import prompt
from termcolor import cprint

from flagmaker import settings
from flagmaker.exceptions import FlagMakerConfigurationError
from utilities import Locale


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
        self.columns: Dict[str, settings.SettingOption] = self.init_columns()

    def settings(self) -> List[settings.SettingBlock]:
        args = [
            settings.SettingBlock('General Settings', {
                'overwrite_storage_csv': settings.SettingOption.create(
                    self,
                    'Override unified default storage CSV if it exists? '
                    'this would be sa360-bq-[advertiser ID].csv. '
                    'If it exists, then it will be re-used to save you '
                    'money, unless you want to start from scratch.',
                    method=flags.DEFINE_bool,
                    default=False,
                    include_in_interactive=False
                ),
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
                    default='US',
                    after=self.hooks.set_locale,
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
                    'are going to have historical data included.',
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
                    default=FileLocationOptions.GCS_BUCKET.value,
                    required=True,
                ),
                'file_path': settings.SettingOption.create(
                    self,
                    'Historical Data CSV File Path '
                    '(can be a directory of file)',
                    conditional=lambda s: s['has_historical_data'].value,
                ),
                'first_date_conversions': settings.SettingOption.create(
                    self,
                    'The first date of conversions',
                    after=self.hooks.convert_to_date,
                    conditional=lambda s: s['has_historical_data'].value,
                )
            }),
            settings.SettingBlock('Data Gathering for Historical Data', {
                'has_revenue_column': settings.SettingOption.create(
                    self,
                    'Does the report show revenue?',
                    default=True,
                    method=flags.DEFINE_bool,
                ),
                'has_device_segment': settings.SettingOption.create(
                    self,
                    'Does the report have a device segment column?',
                    default=False,
                    method=flags.DEFINE_bool,
                ),
                'report_level': settings.SettingOption.create(
                    self,
                    'Specify the report level of the '
                    'advertisers being uploaded (conversion/keyword/campaign).\n'
                    'Difference between keyword and conversion: Conversion '
                    'reports only have one conv. per row so '
                    'no column is needed.',
                    default='keyword',
                    method=flags.DEFINE_enum,
                    options=ReportLevelOptions,
                ),
            }, conditional=lambda s: s['has_historical_data'].value),
            settings.SettingBlock(
                'Historical Data Columns',
                self.columns,
                conditional=lambda s: s['has_historical_data'].value
            ),
        ]
        return args

    def init_columns(self) -> dict:
        return {
            'account_column_name': settings.SettingOption.create(
                self,
                'Column name for the Account Name',
                default='account_name',
                after=self.hooks.map_historical_column,
                attrs={'dtype': np.object},
            ),
            'campaign_column_name': settings.SettingOption.create(
                self,
                'Campaign Column Name for Historical Data '
                '(only if including historical data)',
                default='campaign_name',
                after=self.hooks.map_historical_column,
                attrs={'dtype': np.object},
            ),
            'conversion_count_column': settings.SettingOption.create(
                self,
                'Specify the conversion column',
                default='conversions',
                after=self.hooks.map_historical_column,
                attrs={'dtype': np.float64},
                conditional=lambda s: s['report_level'].value == 'keyword',
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
                attrs={'dtype': np.float64},
            ),
            'device_segment_column_name': settings.SettingOption.create(
                self,
                'Device Segment Column Name (if there is none, omit)',
                default='device_segment',
                conditional=lambda s: s['has_device_segment'].value,
                after=self.hooks.map_historical_column,
                attrs={'dtype': np.object},
            ),
            'date_column_name': settings.SettingOption.create(
                self,
                'Column name for the Date Value',
                default='date',
                after=self.hooks.map_historical_column,
                attrs={'dtype': np.datetime64},
            ),
            'adgroup_column_name': settings.SettingOption.create(
                self,
                'Column name for the ad group name',
                default='ad_group',
                conditional=lambda s: s['report_level'].value != 'campaign',
                after=self.hooks.map_historical_column,
                attrs={'dtype': np.object},
            ),
            'keyword_match_type': settings.SettingOption.create(
                self,
                'Keyword Match Type Column '
                '(omit if uploading a campaign-level report)',
                default='match_type',
                after=self.hooks.map_historical_column,
                conditional=lambda s: s['report_level'].value != 'campaign',
                attrs={'dtype': np.object},
            ),
            'keyword_column_name': settings.SettingOption.create(
                self,
                'Keyword Column Name',
                default='keyword',
                conditional=lambda s: s['report_level'].value != 'campaign',
                after=self.hooks.map_historical_column,
                attrs={'dtype': np.object},
            )
        }


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

    def create_new_bucket(self, bucket: str, project_name: str) -> None:
        result = self.storage.create_bucket(
                bucket,
                project=project_name
                )
        return result

    def create_bucket(self, setting: settings.SettingOption) -> bool:
        class ChooseAnother:
            toggle = False
        s = setting.settings

        while True:
            if 'buckets' not in s.custom:
                break
            if setting.value.isnumeric():
                val = int(setting.value)
                if val <= len(s.custom['buckets']):
                    value = s.custom['buckets'][val - 1].name
                    setting.value = value
                    break
                else:
                    raise FlagMakerConfigurationError('Invalid selection')
            elif setting.value == 'c':
                setting.value = prompt('Select new bucket name: ')
            elif not ChooseAnother.toggle and setting.default is None:
                raise FlagMakerConfigurationError('Select a valid option')
            if not setting:
                break
            ChooseAnother.toggle = False
            try:
                setting.value = self.storage.get_bucket(setting.value).name
                break
            except exceptions.NotFound as e:
                r = prompt(
                    'Cannot find bucket {0}. Create [y/n]? '.format(setting)
                )
                if r.lower() == 'y':
                    ChooseAnother.toggle = True
                    self.settings.create_new_bucket(setting.value, s['project_name'].value)
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
        cprint('Add any files/folders to:', attrs=['bold'])
        cprint(
            'https://console.cloud.google.com/'
            'storage/browser/{}?project={}'.format(
                setting.value,
                s['gcp_project_name'].value
            ), 
            attrs=['bold', 'underline'])

    def set_locale(self, setting: settings.SettingOption):
        s = setting.settings
        s.custom['locale'] = Locale.get(setting.value)

    def bucket_options(self, setting: settings.SettingOption):
        s = setting.settings
        buckets = s.custom['buckets'] = list(self.storage.list_buckets())
        bucket_size = len(buckets)
        result = '\n'.join(['{}: {}'.format(b+1, buckets[b])
                            for b in range(bucket_size)])
        result += '\nc: Create New Bucket'
        default = setting.get_default_or_cache()
        if default is not None and default > '':
            result += '\n[default={}]'.format(default)
        return result

    def map_historical_column(self, setting: settings.SettingOption):
        settings = setting.settings
        if 'historical_map' not in settings.custom:
            settings.custom['historical_map'] = {}
        settings.custom['historical_map'][setting.value] = setting

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
