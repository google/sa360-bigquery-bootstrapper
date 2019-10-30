from absl import flags
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
    'agency_id': SimpleFlag('SA360 Agency ID'),
    'advertiser_id': SimpleFlag('SA360 Advertiser IDs',
                                method=flags.DEFINE_list),
}


def assign_flags() -> flags:
  for k in args:
    args[k].method(k, args[k].default, args[k].help)
  return flags.FLAGS

