from absl import app
from google.cloud import bigquery
from google.cloud.bigquery.dataset import DatasetListItem

import app_flags
from flagmaker import AbstractSettings
from flagmaker import Config
from google.cloud.bigquery.dataset import Dataset

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
        dataset = str(self.settings['raw_dataset'])
        dataset = Dataset.from_string('{0}.{1}'.format(project, dataset))
        datasets = client.list_datasets(project)
        # type : DatasetListItem
        for d in datasets:
            dd = d # type : DatasetListItem
            print(dd.friendly_name)
        print(datasets)
        result = client.create_dataset(dataset)