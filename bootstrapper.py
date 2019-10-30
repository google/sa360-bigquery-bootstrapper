from absl import flags
from absl import app
from google.cloud import bigquery
import app_flags


class Bootstrap:
  def __init__(self, settings: flags):
    self.settings = settings
    app.run(self.run)

  def run(self, args):
    print(self.settings.advertiser_id)
    app_flags.check_settings(self.settings)
    client = bigquery.Client()
    result = client.create_dataset(self.settings.raw_dataset)
    print(result)