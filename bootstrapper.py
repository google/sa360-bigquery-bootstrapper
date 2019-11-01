from absl import app
from google.cloud import bigquery

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
        client = bigquery.Client()
        val = self.settings['raw_dataset']
        dataset = Dataset(val)
        print(self.settings)
        result = client.create_dataset(dataset)
        print(result)