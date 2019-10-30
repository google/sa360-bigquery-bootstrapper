from absl import flags
from absl import app
from google.cloud import bigquery
import app_flags


class Bootstrap:
  settings: app_flags.Settings = None

  def __init__(self):
    app.run(self.run)

  def run(self, args):
    settings: app_flags.Settings = app_flags.load_settings()
    client = bigquery.Client()
    result = client.create_dataset(settings.raw_dataset)
    print(result)