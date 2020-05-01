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
import os
import shutil
import sys
import time
import traceback
from datetime import datetime
from typing import Dict

import numpy as np
from absl import app
from absl import logging
from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import Conflict
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
from google.cloud.bigquery_datatransfer_v1 import types
from google.cloud import storage
from google.cloud.bigquery.dataset import Dataset
from google.cloud.storage import Blob
from google.cloud.storage import Bucket
from google.protobuf.struct_pb2 import Struct
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from termcolor import cprint, colored
from typing import Dict

import app_settings
from csv_decoder import Decoder
from flagmaker.settings import Config
from flagmaker.settings import SettingOption
from prompt_toolkit import prompt
from utilities import *
from views import CreateViews
from views import DataSets


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
        self.path = '/tmp/in-upload/'

    def run(self):
        app.run(self.exec)

    def get_storage_cli(self):
        return self.settings.custom['storage_client']

    def get_bucket(self) -> Bucket:
        bucket_name = self.s.unwrap('storage_bucket')
        try:
            return self.storage_cli.get_bucket(bucket_name)
        except NotFound:
            if self.settings.hooks.create_new_bucket(bucket_name, self.s.unwrap('gcp_project_name')) is None:
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
            advertiser = str(self.s.unwrap('advertiser_id'))
            cprint('Advertiser ID: {}'.format(advertiser),
                   'blue', attrs=['bold', 'underline'])
            self.load_service(project)
            self.load_transfers(client, project, advertiser)
            if (self.s.unwrap('has_historical_data')):
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

    def load_service(self, project):
        def create_service_account(project_id, name, display_name):
            service = discovery.build('iam', 'v1')

            my_service_account = service.projects().serviceAccounts().create(
                name='projects/' + project_id,
                body={
                    'accountId': name,
                    'serviceAccount': {
                        'displayName': display_name
                    }
                }).execute()
            key = service.projects().serviceAccounts().keys().create(
                name='projects/-/serviceAccounts/' + my_service_account['email'], body={}
            ).execute()
            cprint('Created service account: ' + my_service_account['email'])
            return my_service_account

        try:
            create_service_account(
                project, 
                'bq-bootstrapper', 
                'BigQuery Bootstrapper Service Account'
            )
        except HttpError:
            return

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

    def wait_for_transfer(self, client, config, wait_for_all=False):
        sys.stdout.write(colored('Waiting for transfers to succeed. This might take a while.', 'red'))
        sys.stdout.flush()
        success = bigquery_datatransfer.enums.TransferState.SUCCEEDED
        while True:
            ts = [t for t in client.list_transfer_runs(config.name)]
            transfers_complete = wait_for_all
            for t in ts:
                if not wait_for_all and t.state == success.value:
                    transfers_complete = True
                    return

                if t.state != success.value:
                    transfers_complete = False
                else:
                    continue
                if not transfers_complete:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                    time.sleep(10)
            if transfers_complete: 
                cprint('\nDone', 'green')
                return

    def load_transfers(self, cli: bigquery.Client, project: str, advertiser):
        """
        Bootstrap step to create BigQuery data transfers.

        :param cli: BigQuery client instance
        :param project: Name of the project
        :param advertiser: Numeric value of an SA360 advertiser
        :return: The result of the client transfer configuration.
        """
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
            print('start transfer')
            cprint(
                'Schedule already exists for {}. Skipping'.format(advertiser),
                'cyan'
            )
            self.wait_for_transfer(client, config)
            return config
        params['agency_id'] = str(self.s.unwrap('agency_id'))
        params['advertiser_id'] = str(advertiser)
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
        self.wait_for_transfer(client, result)
        return result

    def combine_folder(self, delete=False) -> Blob:
        """
        Prepares and creates a GCS blob from a folder with multiple files.

        - Converts utf-8, utf-16, latin-1
        - Converts Excel
        - Reads CSV, TSV, and Pipe Delimited file

        :param delete: boolean - If set to True, removes original files
        :return: A GCS blob to upload
        """
        file_path = self.settings['file_path']
        dest_filename = file_path.value
        setting: SettingOption[app_settings.AppSettings] = file_path
        s: app_settings.AppSettings = setting.settings
        storage_cli = self.storage_cli
        file_location = s['file_location'].value
        bucket = self.bucket
        dict_map = {v: k for v, k in s.custom['historical_map'].items()}
        if file_location == 'GCS Bucket':
            file = bucket.blob(dest_filename)
            path = dirname = self.path
            if os.path.exists(path) and delete:
                shutil.rmtree(path)
            if not os.path.exists(path):
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
        dest_filename = 'sa360-bq-{}.csv'.format(s['advertiser_id'].value)
        with Decoder(
            desired_encoding='utf-8',
            locale=s.custom['locale'],
            dest=dest_filename,
            path=path,
            out_type=Decoder.SINGLE_FILE,
            dict_map=dict_map,
        ) as decoder:
            result_dir = decoder.run()
            dest_blob = bucket.blob(dest_filename)
            dest_blob.upload_from_filename(result_dir)
            return dest_blob

    def schema(self):
        schemas = []
        type_map = {
            np.object: 'STRING',
            np.datetime64: 'DATE',
            np.int64: 'INT64',
            np.float64: 'FLOAT64',
        }
        historical_map: Dict[str, SettingOption] = self.settings.custom['historical_map']
        for setting in historical_map.values():
            schemas.append(bigquery.SchemaField(
                setting.default,
                type_map.get(setting.attrs['dtype'])
            ))
        return schemas

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
        job_config.field_delimiter = ','
        job_config.quote = '""'
        job_config.skip_leading_rows = 1
        delete_table = False
        job_config.schema = self.schema()
        overwrite_storage_csv: bool = self.s.unwrap('overwrite_storage_csv')
        job_config.source_format = bigquery.ExternalSourceFormat.CSV
        try:
            # if the table exists - then skip this part.
            if not self.s.unwrap('overwrite_storage_csv'):
                client.get_table(full_table_name)
                if self.s.unwrap('interactive'):
                    while True:
                        res = prompt('Table {} exists. '.format(full_table_name)
                                     + 'Replace with new data? [y/N] ')
                        if res.lower() == 'y' or res == '1':
                            delete_table = True
                            overwrite_storage_csv = True
                            break
                        else:
                            return
                else:
                    cprint(
                        'Table {} exists. Skipping'.format(
                            full_table_name
                        ), 'red'
                    )
                    return
            else:
                delete_table = True
        except NotFound as err:
            pass

        try:
            if not delete_table:
                table = dataset_ref.table(table_name)
                blob = self.bucket.blob('sa360-bq-{}.csv'.format(advertiser))
            if delete_table or not blob.exists():
                blob = self.combine_folder(delete=delete_table)
            uri = 'gs://{}/{}'.format(
                self.s.unwrap('storage_bucket'),
                blob.name
            )
            if delete_table:
                client.delete_table(full_table_name, not_found_ok=True)
                table = dataset_ref.table(table_name)
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
            if len(err.errors) > 0:
                i = 1
                for e in err.errors:
                    cprint(e.keys(), attrs=['bold'])
                    cprint('Error #{}: {}'.format(i, e['debugInfo']), 'red')
                    i += 1
            else:
                cprint(str(err), 'red', attrs=['bold'])
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



