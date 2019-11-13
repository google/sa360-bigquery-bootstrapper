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
import csv
import os
import traceback
from datetime import datetime
from typing import Union

from absl import logging
from dateutil.parser import parse as parse_date
from absl import app
from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import Conflict
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
from google.cloud import storage
from google.cloud.bigquery import Table
from google.cloud.bigquery.dataset import Dataset
from google.cloud.storage import Blob
from google.cloud.storage import Bucket
from google.protobuf.struct_pb2 import Struct
from termcolor import cprint

import app_settings
from csv_decoder import Decoder
from flagmaker.settings import SettingOption
from utilities import *
from flagmaker.settings import AbstractSettings
from flagmaker.settings import Config


class DataSets:
    raw: bigquery.dataset.Dataset = None
    views: bigquery.dataset.Dataset = None


class Bootstrap:
    settings: app_settings.AppSettings = None
    config: Config = None
    storage_cli: storage.Client
    bucket: Bucket

    def __init__(self):
        self.config: Config[app_settings.AppSettings] = Config(
            app_settings.AppSettings
        )
        self.s = None

    def run(self):
        app.run(self.exec)

    def get_storage_cli(self):
        return self.settings.custom['storage_client']

    def get_bucket(self) -> Bucket:
        bucket_name = self.s.unwrap('storage_bucket')
        try:
            return self.storage_cli.get_bucket(bucket_name)
        except NotFound:
            cprint('Could not find bucket named ' + bucket_name, 'red',
                   attrs=['bold'])
            cprint('Please double-check existence, or remove the '
                   'storage_bucket flag so we can help you create the storage '
                   'bucket interactively.', 'red')
            exit(1)

    def exec(self, args):
        try:
            self.settings: app_settings.AppSettings = self.config.get()
            self.s = SettingUtil(self.settings)
            self.storage_cli = self.get_storage_cli()
            self.bucket = self.get_bucket()
            project = str(self.s.unwrap('gcp_project_name'))
            client = bigquery.Client(
                project=project, location='US'
            )  # type : bigquery.Client
            self.load_datasets(client, project)
            advertisers = self.s.unwrap('advertiser_id')
            if not isinstance(advertisers, list):
                advertisers = [advertisers]
            for advertiser in advertisers:
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
            cprint(str(err), 'red')
            logging.debug('%s\n%s', err.errors, traceback.format_exc())

    def load_datasets(self, client, project):
        for k, v in (('raw', 'raw_dataset'), ('views', 'view_dataset')):
            dataset = self.settings[v].value
            dataset = Dataset.from_string('{0}.{1}'.format(project, dataset))
            try:
                setattr(DataSets, k, client.get_dataset(dataset))
                cprint('Already have dataset {}'.format(dataset), 'green')
            except NotFound:
                setattr(DataSets, k, client.create_dataset(dataset))
                cprint('Created dataset {}'.format(dataset), 'green')

    def load_transfers(self, cli: bigquery.Client, project, advertiser):
        client = bigquery_datatransfer.DataTransferServiceClient()
        location = self.s.unwrap('location')
        project = self.s.unwrap('gcp_project_name')
        data_source = 'doubleclick_search'
        name = client.location_data_source_path(project, location, data_source)
        parent = client.location_path(project, location)
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
            'destination_dataset_id': DataSets.raw.dataset_id,
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

    def ensure_utf8(self) -> Blob:
        file_path = self.settings['file_path']
        dest_filename = file_path.value
        setting: SettingOption[app_settings.AppSettings] = file_path
        s: app_settings.AppSettings = setting.settings
        storage_cli = self.storage_cli
        file_location = s['file_location'].value
        bucket = self.bucket
        now_str = datetime.now().strftime('%Y%m%d%H%m%S%f')
        dict_map = {v: k for v,k in s.custom['historical_map'].items()}
        if file_location == 'GCS Bucket':
            file = bucket.blob(dest_filename)
            path = dirname = '/tmp/in-{}/'.format(now_str)
            os.mkdir(path)
            if not file.exists():
                files = list(bucket.list_blobs(prefix=dest_filename))
            else:
                files = [file]
            for file in files:
                file_parts = file.name.split('/')
                if file_parts[-1] == '':
                    continue
                with open(dirname + file_parts[-1], 'w+b') as fh:
                    file.download_to_file(fh, storage_cli)
        else:
            path = dest_filename
        dest_filename = 'sa360-bq-{}.csv'.format(s['advertiser_id'])
        result_dir = Decoder(
            desired_encoding='utf-8',
            dest=dest_filename,
            path=path,
            out_type=Decoder.SINGLE_FILE,
            dict_map=dict_map,
        ).run()
        dest_blob = bucket.blob(dest_filename)
        dest_blob.upload_from_filename(result_dir)
        return dest_blob

    def load_historical_tables(self, client, project, advertiser):
        s = self.settings
        dataset_ref: bigquery.dataset.Dataset = DataSets.raw
        dataset: str = dataset_ref.dataset_id
        table_name = get_view_name(ViewTypes.HISTORICAL, advertiser)
        full_table_name = '{}.{}.{}'.format(
            project,
            dataset,
            table_name,
        )
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.source_format = bigquery.ExternalSourceFormat.CSV
        try:
            # if the table exists - then skip this part.
            client.get_table(full_table_name)
            cprint('Table {} exists. Skipping'.format(full_table_name), 'red')
            return
        except NotFound as err:
            pass

        try:
            table = dataset_ref.table(table_name)
            blob = self.bucket.blob('sa360-bq-{}.csv'.format(advertiser))
            if not blob.exists() or self.s.unwrap('overwrite_storage_csv'):
                blob = self.ensure_utf8()
            uri = 'gs://{}/{}'.format(
                self.s.unwrap('storage_bucket'),
                blob.name
            )
            load_job = client.load_table_from_uri(
                uri, table, job_config=job_config
            )
            result = load_job.result()
            logging.info("Job result: %s", result)
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
        except BadRequest as err:
            cprint(str(err), 'red', attrs=['bold'])
            if len(err.errors) > 0:
                for e in err.errors:
                    cprint('- {}'.format(e['debugInfo']), 'red')
            logging.info(traceback.format_exc())
            exit(1)

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


class MethodNotCreated(Exception):
    pass


class CreateViews:
    client: bigquery.Client = None
    project: str = None
    advertiser: str = None
    settings: AbstractSettings = None

    def __init__(self, config, client, project, advertiser):
        self.settings: app_settings.AppSettings = config
        self.client = client
        self.project = project
        self.advertiser = advertiser
        self.s = SettingUtil(config)

    def run(self):
        report_level = self.s.unwrap('report_level')
        if report_level == 'campaign':
            raise MethodNotCreated('Methods for campaign-only views'
                                   ' not implemented.')
        else:
            if self.s.unwrap('has_historical_data'):
                self.view(
                    ViewTypes.KEYWORD_MAPPER,
                    'keyword_mapper'
                )
                self.view(
                    ViewTypes.HISTORICAL_CONVERSIONS,
                    'historical_conversions'
                )
                self.view(
                    ViewTypes.REPORT_VIEW,
                    'report_view',
                )

    def view(self, view_name: ViewTypes, func_name):
        adv = self.s.unwrap('advertiser_id')
        logging.debug(view_name.value)
        adv_view = get_view_name(view_name, adv)
        view_ref = DataSets.views.table(adv_view)
        view_query = getattr(
            self,
            func_name if func_name is not None else view_name.value
        )(adv)
        logging.debug(view_query)
        try:
            logging.debug(view_ref)
            view: Table = self.client.get_table(view_ref)
            view.view_query = view_query
            cprint('= updated {}'.format(adv_view), 'green')
        except NotFound as err:
            try:
                logging.debug('error:\n-----\n%s\n-----\n', err)
                view = bigquery.Table(view_ref)
                logging.info('%s.%s', view.dataset_id, view.table_id)
                view.view_query = view_query
                self.client.create_table(view, exists_ok=True)
                cprint('+ created {}'.format(adv_view), 'green')
            except NotFound as err:
                cprint('Error: {}'.format(str(err)), 'red')
                logging.info(traceback.format_exc())
        self.keyword_mapper(adv)

    def historical_conversions(self, advertiser):
        views = ViewGetter(advertiser)

        sql = """SELECT 
            h.date,
            a.keywordId{deviceSegment},
            a.keywordMatchType MatchType,
            h.ad_group AdGroup,
            {conversions},
            {revenue}
          FROM `{project}`.`{raw}`.`{historical_table_name}` h
          INNER JOIN (
            SELECT keywordId,
                keyword,
                campaign,
                keywordMatchType,
                adGroup,
                account
            FROM `{project}`.`{views}`.`{keyword_mapper}`
            GROUP BY
                keywordId,
                keyword,
                campaign,
                account,
                adGroup,
                keywordMatchType
          ) a
            ON a.keyword=h.keyword
            AND a.campaign=h.campaign_name
            AND a.account=h.account_name
            AND a.adGroup=h.ad_group
            AND LOWER(a.keywordMatchType) = LOWER(h.match_type)
          GROUP BY
            h.date,
            a.keywordId, 
            a.keyword, 
            a.keywordMatchType,
            h.ad_group,
            a.campaign{device_segment_column_name},
            a.account""".format(
              project=self.s.unwrap('gcp_project_name'),
              deviceSegment=(
                  ',\n' + 'h.device_segment'
                  + ' AS device_segment'
              ) if self.s.unwrap('has_device_segment') else '',
              device_segment_column_name = (
                  ',\n'
                  + 'h.' + self.s.unwrap('device_segment_column_name')
              ) if self.s.unwrap('has_device_segment') else '',
              raw=self.s.unwrap('raw_dataset'),
              views=self.s.unwrap('view_dataset'),
              historical_table_name=views.get(ViewTypes.HISTORICAL),
              keyword_mapper=views.get(ViewTypes.KEYWORD_MAPPER),
              conversions=aggregate_if(
                  Aggregation.SUM,
                  'conversions',
                  'SUM(1)',
                  prefix='h',
              ),
              revenue=aggregate_if(
                  Aggregation.SUM,
                  'revenue',
                  0,
                  prefix='h',
              ),
          )
        return sql

    def keyword_mapper(self, advertiser):
        sql = '''SELECT 
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
              FROM `{project}`.`{raw_data}`.`Keyword_{advertiser_id}` c
            ) k
            INNER JOIN (
              SELECT campaignId, campaign 
              FROM `{project}`.`{raw_data}`.`Campaign_{advertiser_id}` 
              GROUP BY campaignId, campaign
              ) c ON c.campaignId = k.campaignId
            INNER JOIN (
              SELECT accountId, account, accountType 
              FROM `{project}`.`{raw_data}`.`Account_{advertiser_id}` 
              GROUP BY accountId, account, accountType
              ) a ON a.accountId = k.accountId
            INNER JOIN (
              SELECT adGroupId, adGroup
              FROM `{project}`.`{raw_data}`.`AdGroup_{advertiser_id}`
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
                g.adGroup'''.format(
                    project=self.s.unwrap('gcp_project_name'),
                    raw_data=self.s.unwrap('raw_dataset'),
                    advertiser_id=advertiser,
                )
        return sql

    def report_view(self, advertiser):
        views = ViewGetter(advertiser)

        sql = """SELECT 
        d.date Date, 
        m.keywordId,
        m.keyword Keyword{deviceSegment}, 
        m.campaign_name Campaign, 
        m.account_name Engine, 
        m.accountType Account_Type,
        SUM(clicks) Clicks, 
        SUM(impr) Impressions, 
        SUM(weightedPos) Weighted_Pos,
        COALESCE(SUM(cost), 0) Cost,
        COALESCE(SUM(c.revenue), 0) + COALESCE(SUM(h.revenue), 0) Revenue,
        COALESCE(SUM(c.conversions), 0) + COALESCE(SUM(h.conversions), 0) Conversions
        FROM (
          SELECT date, 
            keywordId,
            SUM(clicks) clicks, 
            SUM(impr) impr, 
            SUM(avgPos*impr) weightedPos,
            SUM(cost) cost{deviceSegment}
          FROM `{project}.{raw_data}.KeywordDeviceStats_{advertiser}`
          GROUP BY 
            date, 
            keywordId{deviceSegment}
        ) d
        INNER JOIN `{project}.{view_data}.{keyword_mapper}` m
          ON m.keywordId = d.keywordId
        LEFT JOIN (
          SELECT
            date,
            keywordId{deviceSegment},
            SUM(revenue) revenue,
            SUM(conversions) conversions
          FROM `{project}.{view_data}.{historical_conversions}` o
          GROUP BY 
            date, 
            keywordId{deviceSegment}
        ) h 
            ON h.keywordId=d.keywordId 
            AND h.date=d.date
        LEFT JOIN (
          SELECT 
            date, 
            keywordId, 
            SUM(dfaRevenue) revenue,
            SUM(dfaTransactions) conversions
          FROM 
            `{project}.{raw_data}.KeywordFloodlightAndDeviceStats_{advertiser}`
          GROUP BY date, keywordId
        ) c 
            ON c.keywordId=d.keywordId 
            AND c.date=d.date 
            AND c.date > '{date}'
        GROUP BY
            d.date, 
            m.keywordId, 
            m.keyword, 
            m.campaign_name, 
            m.account_name{deviceSegment},
            m.accountType""".format(
            view_data=self.s.unwrap('view_dataset'),
            keyword_mapper=views.get(ViewTypes.KEYWORD_MAPPER),
            deviceSegment=(',\n' + 'd.deviceSegment AS Device_Segment')
                          if self.s.unwrap('has_device_segment') else '',
            project=self.s.unwrap('gcp_project_name'),
            raw_data=self.s.unwrap('raw_dataset'),
            advertiser=advertiser,
            date=self.s.unwrap('first_date_conversions'),
            historical_conversions=views.get(ViewTypes.HISTORICAL_CONVERSIONS),
        )
        return sql

