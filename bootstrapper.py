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

    def run(self):
        app.run(self.exec)

    def exec(self, args):
        try:
            self.settings: AbstractSettings = self.config.get()
            project = str(self.settings['gcp_project_name'])
            client = bigquery.Client(
                project=project, location='US'
            )  # type : bigquery.Client
            self.load_datasets(client, project)
            for advertiser in self.settings['advertiser_id']:
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
        parent = client.location_path(project, self.settings['location'])
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
        params['agency_id'] = self.settings['agency_id'].value
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
        s_cli = self.settings.custom['storage_client']  # type: storage.Client
        bucket = s_cli.get_bucket(self.settings['storage_bucket'].value)
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
        dataset = self.settings['raw_dataset']
        dataset_ref: bigquery.dataset.Dataset = client.get_dataset(dataset)
        table_name = '{}.{}.{}_{}'.format(
            project,
            dataset,
            self.settings['historical_table_name'].value,
            advertiser
        )
        uri = 'gs://{}/{}'.format(self.settings['storage_bucket'], file)

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
                'Created table {}'.format(table_name),
                'green'
            )
        except Conflict:
            cprint(
                'Table {} already exists. Skipping'.format(table_name),
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

    def run(self):
        pass