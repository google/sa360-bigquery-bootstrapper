from absl import app
from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.cloud.bigquery.dataset import DatasetListItem
from termcolor import cprint

import app_flags
from flagmaker import AbstractSettings
from flagmaker import Config
from google.cloud.bigquery.dataset import Dataset

class Datasets:
    raw = None
    views = None

class Bootstrap:
    settings: AbstractSettings = None
    config: Config = None

    def __init__(self):
        self.config: Config = Config(app_flags.AppSettings)

    def run(self):
        app.run(self.exec)

    def exec(self, args):
        self.settings: AbstractSettings = self.config.get()
        project = str(self.settings['gcp_project_name'])

        client = bigquery.Client(project=project)  # type : bigquery.Client

        for k, v in (('raw', 'raw_dataset'), ('views', 'view_dataset')):
            dataset = str(self.settings[v])
            dataset = Dataset.from_string('{0}.{1}'.format(project, dataset))

            try:
                setattr(Dataset, k, client.create_dataset(dataset))
                cprint('Created dataset {}'.format(dataset))
            except Conflict:
                cprint('Already have dataset {}'.format(dataset), 'green')