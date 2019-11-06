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
from absl import flags
from datetime import datetime
from dateutil.parser import parse as parse_date
from google.api_core import exceptions
from google.api_core.exceptions import NotFound
from google.cloud import storage
from prompt_toolkit.completion import PathCompleter
from termcolor import cprint
from prompt_toolkit import prompt
import os

from flagmaker import settings
from typing import List


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
                    'SA360 Advertiser IDs',
                    method=flags.DEFINE_list,
                ),
                'has_historical_data': settings.SettingOption.create(
                    self,
                    'Include Historical Data?\n'
                    'Note: This will be true as long as any advertisers '
                    'are going to have historical data included.\nIf you are '
                    'uploading multiple advertisers, any of them with '
                    'historical data should have the same format. If not, '
                    'upload individual advertisers\n Input',
                    method=flags.DEFINE_boolean,
                ),
                'storage_bucket': settings.SettingOption.create(
                    self,
                    'Storage Bucket Name',
                    prompt=self.hooks.bucket_options,
                    after=self.hooks.create_bucket,
                ),
                'file_path': settings.SettingOption.create(
                    self,
                    'Historical Data CSV File Path',
                    after=self.hooks.handle_csv_paths,
                    method=flags.DEFINE_list,
                    prompt=self.hooks.get_file_paths,
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
                    method=AppSettings.define_enum_helper(
                        choices=['conversion','keyword','campaign']
                    ),
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
            }, conditional=lambda s: s['has_historical_data'].value)
        ]
        return args

    @staticmethod
    def define_enum_helper(choices):
        def inner(*args, **kwargs):
            kwargs['enum_values'] = choices
            return flags.DEFINE_enum(*args, **kwargs)
        return inner


class Hooks:
    """Convenience class to add all hooks

    Includes validation hooks, after hooks, and more.
    """

    def __init__(self):
        self.storage = None

    def set_clients(self, setting: settings.SettingOption):
        settings = setting.settings
        settings.custom['storage_client'] = self.storage = storage.Client(
            project=settings['gcp_project_name'].value
        )
        return True

    def create_bucket(self, setting: settings.SettingOption) -> bool:
        class ChooseAnother:
            toggle = False
        settings = setting.settings

        while True:
            if 'buckets' not in settings.custom:
                return True
            if setting.value.isnumeric():
                val = int(setting.value)
                if val <= len(settings.custom['buckets']):
                    value = settings.custom['buckets'][val - 1].name
                    setting.value = value
                    return True
                else:
                    cprint('Invalid selection', 'red', attrs=['bold'])
                    return False
            elif setting.value == 'c':
                setting.value = prompt('Select project name: ')
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
                r = prompt(
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
                setting.value = prompt(
                    'Press Ctrl+C to cancel or choose a different input: '
                )

    def bucket_options(self, setting: settings.SettingOption):
        settings = setting.settings
        buckets = settings.custom['buckets'] = list(self.storage.list_buckets())
        bucket_size = len(buckets)
        result = '\n'.join(['{}: {}'.format(b+1, buckets[b])
                            for b in range(bucket_size)])
        result += '\nc: Create New Bucket'
        return result

    def get_file_paths(self, setting: settings.SettingOption):
        settings = setting.settings
        advertisers = settings['advertiser_id'].value

        cprint('If in storage, provide a full URL starting with gs://. '
               'Otherwise, drag and drop the file(s) here '
               'and just specify the file name(s).\n'
               'You can leave any value without historical data'
               'blank.', 'blue')

        return ('Do you want to:\n'
                '1. Enter each value separately?\n'
                '2. Enter comma separated values to map'
                ' each advertiser ID\n')

    def handle_csv_paths(self, setting: settings.SettingOption):
        choice = setting.value
        advertisers = setting.settings['advertiser_id'].value
        if not isinstance(advertisers, list):
            advertisers = [advertisers]
        if isinstance(choice, list):
            file_map = {}
            for i in range(len(choice)):
                option = choice[i]
                advertiser = advertisers[i]
                file_map[advertiser] = option
                if option != '':
                    self.ensure_utf8(setting, option)
            setting.settings.custom['file_map'] = file_map
            return True
        options = []
        if not choice.isnumeric():
            options = choice.split(',')

        while True:
            if choice == '2':
                options = prompt(
                    'Add comma-separated file '
                    'locations\n'
                    'IDs:    {}\n'
                    'Values: '.format(
                        ','.join(advertisers)
                    )
                ).split(',')
                break
            elif choice != '1':
                cprint('Invalid option', 'red', attrs=['bold'])
                setting.value = None
                return False
            for advertiser in advertisers:
                options.append(prompt(
                    'Advertiser #{}: '.format(advertiser),
                    completer=PathCompleter(),
                ))
            break

        if len(options) > len(advertisers):
            cprint('Invalid mapping. '
                   'Cannot have more filenames ({}) '.format(len(options)) +
                   'than advertisers ({}).'.format(len(advertisers)))
            return False

        file_map = {}
        results = []
        for i in range(len(advertisers)):
            filename = options[i] if i < len(options) else '-'
            results.append('{}:   {}'.format(
                advertisers[i],
                filename,
            ))
            file_map[advertisers[i]] = filename
        while True:
            result = prompt(
                'Confirm Map:\n{}\nCorrect? [y/n]: '.format('\n'.join(results))
            )
            if result == 'y':
                break
            if result == 'n':
                return False
        setting.settings.custom['file_map'] = file_map
        setting.value = options
        return True

    def ensure_utf8(self, setting: settings.SettingOption, filename: str):
        settings = setting.settings
        bucket_name = settings['storage_bucket'].value
        bucket = None
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
        if filename.startswith('gs://'):
            filename = filename.replace('gs://{}/'.format(bucket_name), '')
            file = bucket.blob(filename)
            with open('/tmp/' + filename, 'w+b') as fh:
                file.download_to_file(fh, self.storage)
        else:
            file = bucket.blob('{}'.format(filename))
            local = True

        def try_decode(fname, encoding):
            with open(fname, 'rb') as fh:
                if encoding == 'utf-8':
                    file_data = fh.read()
                    file_data.decode(encoding)
                    return file_data
                file_data = fh.read().decode(encoding).encode('utf-8')
                cprint('Found a {}-'.format(encoding) +
                       'encoded file and turned to utf-8', 'cyan')
                return file_data

        for encoding in ['utf-8', 'utf-16', 'latin-1']:
            if local:
                full_filename = '{}/{}'.format(os.environ['HOME'], filename)
            else:
                full_filename = '/tmp/{}'.format(filename)
            try:
                contents = try_decode(full_filename, encoding)
                with open('/tmp/' + filename, 'w+b') as fh:
                    fh.write(contents)
                    fh.seek(0)
                    file.upload_from_file(fh)
                break
            except UnicodeDecodeError:
                continue
        return True

    def map_historical_column(self, setting: settings.SettingOption):
        settings = setting.settings
        setting.value = setting.value.replace(' ', '_')
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
                        return False
                    if result == 'y':
                        break
                    cprint('Invalid option. Select y/n or set value to '
                           'yyyy-mm-dd. Example: 1999-12-31', 'red')
            setting._error = False
            setting.value = value
            return True
        except ValueError:
            cprint('Invalid Date Selection. Use either y-m-d or m/d/y', 'red')
            return False
