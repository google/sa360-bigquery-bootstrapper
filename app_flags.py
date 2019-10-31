from flagmaker import SettingOptions
from flagmaker import SettingOption
from absl import flags

args: SettingOptions = {
    'gcp_project_name': SettingOption.create('GCP Project Name'),
    'raw_dataset': SettingOption.create(
        'Dataset where raw data will be stored',
        default='raw'
    ),
    'view_dataset': SettingOption.create(
        'Dataset where view data will be generated and stored',
        default='views'
    ),
    'agency_id': SettingOption.create('SA360 Agency ID'),
    'advertiser_id': SettingOption.create(
        'SA360 Advertiser IDs',
        method=flags.DEFINE_list
    ),
    'historical_data': SettingOption.create(
        'Include Historical Data?',
        method=flags.DEFINE_boolean
    ),
    'storage_bucket': SettingOption.create(
        'Storage Bucket Name',
        prompt=Hooks.bucket_options,
        after=Hooks.create_bucket,
    ),
    'historical_table_name': SettingOption.create(
        'Name of historical table',
        show=lambda: args['historical_data'].value
    ),
}
