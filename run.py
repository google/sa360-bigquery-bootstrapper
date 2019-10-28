from absl import flags
from absl import app
from typing import Dict

FLAGS = flags.FLAGS


class SimpleFlag:
  default = None
  help = None
  method: callable = None

  def __init__(self, helptxt=None, default=None, method=flags.DEFINE_string):
    self.default = default
    self.help = helptxt
    self.method = method


SimpleFlags = Dict[str, SimpleFlag]

args: SimpleFlags = {
    'gcp_project_name': SimpleFlag('GCP Project Name'),
    'raw_dataset': SimpleFlag('Where all raw BigQuery data is stored'),
    'view_dataset': SimpleFlag('Where all formatted BigQuery data is stored'),
    'historical_table_name': SimpleFlag('Name of historical table (if any)'),
    'advertiser_id': SimpleFlag('SA360 Advertiser IDs',
                                method=flags.DEFINE_list),
}

for k in args:
  args[k].method(k, args[k].default, args[k].help)


def main(argv):
  print(FLAGS.advertiser_id)


if __name__ == '__main__':
  app.run(main)
