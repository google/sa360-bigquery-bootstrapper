import csv
from datetime import datetime

from dateutil.parser import parse as parse_date
from absl import app
from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import Conflict
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
from google.cloud import storage
from google.cloud.bigquery.dataset import Dataset
from google.protobuf.struct_pb2 import Struct
from termcolor import cprint

import app_flags
from utilities import *
from flagmaker import AbstractSettings
from flagmaker import Config


class Datasets:
    raw: bigquery.dataset.Dataset = None
    views: bigquery.dataset.Dataset = None


class Bootstrap:
    settings: AbstractSettings = None
    config: Config = None

    def __init__(self):
        self.config: Config = Config(app_flags.AppSettings)
        self.s = None
    def run(self):
        app.run(self.exec)

    def exec(self, args):
        try:
            self.settings: AbstractSettings = self.config.get()
            self.s = SettingUtil(self.settings)
            project = str(self.s.unwrap('gcp_project_name'))
            client = bigquery.Client(
                project=project, location='US'
            )  # type : bigquery.Client
            self.load_datasets(client, project)
            for advertiser in self.s.unwrap('advertiser_id'):
                cprint('Advertiser ID: {}'.format(advertiser),
                       'blue', attrs=['bold', 'underline'])
                self.load_transfers(client, project, advertiser)
                self.load_historical_tables(client, project, advertiser)
                CreateViews(self.settings, client, project, advertiser).run()

        except BadRequest as err:
            cprint(
                'Error. Please ensure you have enabled all requested '
                'APIs and try again.',
                'red',
                attrs=['bold']
            )
            cprint(err.message, 'red')

    def load_datasets(self, client, project):
        for k, v in (('raw', 'raw_dataset'), ('views', 'view_dataset')):
            dataset = str(self.settings[v])
            dataset = Dataset.from_string('{0}.{1}'.format(project, dataset))
            try:
                setattr(Datasets, k, client.get_dataset(dataset))
                cprint('Already have dataset {}'.format(dataset), 'green')
            except NotFound:
                setattr(Datasets, k, client.create_dataset(dataset))
                cprint('Created dataset {}'.format(dataset), 'green')

    def load_transfers(self, cli: bigquery.Client, project, advertiser):
        client = bigquery_datatransfer.DataTransferServiceClient()
        parent = client.location_path(project, self.s.unwrap('location'))
        config = {}
        params = Struct()
        display_name= 'SA360 Transfer {}'.format(advertiser)
        config = Bootstrap.config_exists(client, parent, display_name)
        if config is not None:
            cprint(
                'Schedule already exists for {}. Skipping'.format(advertiser),
                'cyan'
            )
            return config
        params['agency_id'] = self.s.unwrap('agency_id')
        params['advertiser_id'] = advertiser
        params['include_removed_entities'] = False
        config = {
            'display_name': display_name,
            'destination_dataset_id': Datasets.raw.dataset_id,
            'data_source_id': SystemSettings.SERVICE_NAME,
            'schedule': 'every day {}'.format(
                datetime.strftime(datetime.now(), '%H:%M')
            ),
            'params': params,
            'disabled': False,
        }
        result = client.create_transfer_config(parent, config)
        cprint(
            'Created schedule for {}'.format(advertiser),
            'cyan',
            attrs=['bold']
        )
        return result

    def guess_schema(self, file):
        if file.startswith('gs://'):
            file = file.replace('gs://{}/'.format(
                self.s.unwrap('storage_bucket')), ''
            )
        s_cli = self.settings.custom['storage_client']  # type: storage.Client
        bucket = s_cli.get_bucket(self.s.unwrap('storage_bucket'))
        blob = bucket.blob(file)
        blob.name = file
        result = blob.download_as_string(s_cli, 0, 1000)
        rows = result.decode().split('\n')[0:2]
        schema = []
        data = list(csv.reader(rows, delimiter=','))
        for col in range(len(data[0])):
            val = data[1][col]
            key = data[0][col]
            if val.isnumeric() and len(val) < 5:
                schema.append(bigquery.SchemaField(
                    key,
                    'INT64',
                ))
            else:
                try:
                    parse_date(val)
                    schema.append(bigquery.SchemaField(
                        key,
                        'DATE',
                    ))
                except ValueError:
                    schema.append(bigquery.SchemaField(
                        key,
                        'STRING',
                    ))
        return schema

    def load_historical_tables(self, client, project, advertiser):
        file_map = self.settings.custom['file_map']
        in_map = advertiser in file_map
        if not in_map or not file_map[advertiser]:
            cprint('No historical file provided for {}'.format(advertiser),
                   'red')
            return
        file = file_map[advertiser]
        dataset_ref: bigquery.dataset.Dataset = Datasets.raw
        dataset: str = dataset_ref.dataset_id
        table_name = 'historical_{}'.format(advertiser)
        full_table_name = '{}.{}.{}'.format(
            project,
            dataset,
            table_name,
        )
        uri = 'gs://{}/{}'.format(self.s.unwrap('storage_bucket'), file)

        job_config = bigquery.LoadJobConfig()
        job_config.schema = self.guess_schema(file)
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.ExternalSourceFormat.CSV
        load_job = client.load_table_from_uri(
            uri, dataset_ref.table(table_name), job_config=job_config
        )
        try:
            load_job.result()
            cprint(
                'Created table {}'.format(full_table_name),
                'green'
            )
        except Conflict:
            cprint(
                'Table {} already exists. Skipping'.format(full_table_name),
                'red'
            )
            pass

    @staticmethod
    def config_exists(client, parent, display_name):
        configs = {
            l.display_name: l
            for l in client.list_transfer_configs(parent)
            if l.data_source_id == SystemSettings.SERVICE_NAME
        }
        if display_name in configs:
            return configs[display_name]


class SystemSettings(object):
    SERVICE_NAME = 'doubleclick_search'


class CreateViews:
    client: bigquery.Client = None
    project: str = None
    advertiser: str = None
    settings: AbstractSettings = None

    def __init__(self, config, client, project, advertiser):
        self.settings = config
        self.client = client
        self.project = project
        self.advertiser = advertiser
        self.s = SettingUtil(config)

    def run(self):
        report_level = self.s.unwrap('report_level')
        if report_level == 'campaign':
            self.view('campaign')  # not created yet
        else:
            if self.s.unwrap('has_historical_data'):
                self.view('keyword_mapper')
                self.view('historical_keywords')

    def view(self, view_name, func_name=None):
        for adv in self.s.unwrap('advertiser_id'):
            adv_view = view_name + '_' + adv
            view_ref = Datasets.views.table(adv_view)
            view = bigquery.Table(view_ref)
            print(adv, view_name)
            view.view_query = getattr(
                self,
                func_name if func_name is not None else view_name
            )(adv)
            print(view.view_query)
            self.client.create_table(view, exists_ok=True)
            cprint('+ {}'.format(adv_view), 'green')
            self.keyword_mapper(adv)

    def historical_keywords(self, advertiser):
        views = ViewGetter(advertiser)

        sql = """SELECT 
            h.{date} date,
            a.keywordId{deviceSegment},
            a.keywordMatchType MatchType,
            h.{adgroup_column_name} AdGroup,
            {conversions} conversions,
            {revenue} revenue
          FROM `{project}.{raw}.{historical_table_name}` h
          INNER JOIN (
            SELECT keywordId,
                keyword,
                campaign,
                keywordMatchType,
                adGroup,
                account
            FROM `{project}.{views}.{keyword_mapper}`
            GROUP BY
                keywordId,
                keyword,
                campaign,
                account,
                adGroup,
                keywordMatchType
          ) a
            ON a.keyword=h.keyword
            AND a.campaign=h.{campaign_column_name} 
            AND a.account=h.{account_column_name}
            AND a.adGroup=h.{adgroup_column_name}
            AND LOWER(a.keywordMatchType) = LOWER(h.{keyword_match_type})
          GROUP BY
            h.{date},
            a.keywordId, 
            a.keyword, 
            a.keywordMatchType,
            h.{adgroup_column_name},
            a.campaign{device_segment_column_name},
            a.account""".format(
              date=self.s.unwrap('date_column_name'),
              project=self.s.unwrap('gcp_project_name'),
              deviceSegment=(
                  ',\n' + 'h.'
                  + self.s.unwrap('device_segment_column_name')
                  + ' AS device_segment'
              ) if self.s.unwrap('has_device_segment') else '',
              device_segment_column_name = (
                  ',\n'
                  + 'h.' + self.s.unwrap('device_segment_column_name')
              ) if self.s.unwrap('has_device_segment') else '',
              raw=self.s.unwrap('raw_dataset'),
              adgroup_column_name=self.s.unwrap('adgroup_column_name'),
              views=self.s.unwrap('view_dataset'),
              historical_table_name=views.get(ViewTypes.HISTORICAL),
              keyword_mapper=views.get(ViewTypes.KEYWORD_MAPPER),
              campaign_column_name=self.s.unwrap('campaign_column_name'),
              account_column_name=self.s.unwrap('account_column_name'),
              date_column_name=self.s.unwrap('date_column_name'),
              keyword_match_type=self.s.unwrap('keyword_match_type'),
              conversions=aggregate_if(
                  Aggregation.SUM,
                  'h', self.s.unwrap('conversion_count_column'),
                  'SUM(1)'
              ),
              revenue=aggregate_if(
                  Aggregation.SUM,
                  'h', self.s.unwrap('revenue_column_name'),
                  0
              ),
          )
        return sql

    def keyword_mapper(self, advertiser):
        sql = """SELECT 
            k.keywordId, 
            k.keywordText keyword,
            k.keywordMatchType,
            c.campaign, 
            a.account,
            g.adGroup,
            a.accountType
            FROM (
                SELECT keywordText,
                keywordId,
                keywordEngineId,
                campaignId,
                accountId,
                adgroupId,
                keywordMatchType,
                RANK() OVER (
                  PARTITION BY keywordText, keywordMatchType, campaignId, 
                      accountId, adGroupId
                    ORDER BY CASE WHEN status='Active' THEN 0 ELSE 1 END,
                    CASE WHEN keywordEngineId IS NOT NULL THEN 0 ELSE 1 END
                  ) rank
              FROM `{project}.{raw_data}.Keyword_{advertiser_id}` c
            ) k
            INNER JOIN (
              SELECT campaignId, campaign 
              FROM `{project}.{raw_data}.Campaign_{advertiser_id}` 
              GROUP BY campaignId, campaign
              ) c ON c.campaignId = k.campaignId
            INNER JOIN (
              SELECT accountId, account, accountType 
              FROM `{project}.{raw_data}.Account_{advertiser_id}` 
              GROUP BY accountId, account, accountType
              ) a ON a.accountId = k.accountId
            INNER JOIN (
              SELECT adGroupId, adGroup
              FROM `{project}.{raw_data}.AdGroup_{advertiser_id}`
              GROUP BY adGroupId, adGroup
            ) g ON g.adGroupId = k.adGroupId
            WHERE keywordText IS NOT NULL
            AND rank = 1
            GROUP BY
                k.keywordId, 
                k.keywordText,
                k.keywordMatchType, 
                c.campaign, 
                a.account,
                a.accountType,
                g.adGroup""".format(
                    project=self.s.unwrap('gcp_project_name'),
                    raw_data=self.s.unwrap('raw_dataset'),
                    advertiser_id=advertiser,
                )
        return sql
