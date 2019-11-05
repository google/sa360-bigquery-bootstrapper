from enum import Enum


def printspecial(content, style):
  print('{0}{1}{2}'.format(style, content, NORMAL))

def normalize(content):
  return content.replace(' ', '_').lower()

def aggregate_if(type, prefix, value, value_empty_alt):
  type_str = ''
  if type == Aggregation.SUM:
    type_str = 'SUM'
  if type == Aggregation.COUNT:
    type_str = 'COUNT'
  return ("{0}({1}.{2})".format(type_str, prefix, value)
        if value
        else value_empty_alt)

class SettingUtil(object):
    def __init__(self, settings):
        self.settings = settings

    def unwrap(self, key):
        return self.settings[key].value


class Aggregation(Enum):
    SUM = 1
    COUNT = 2


class ViewTypes(Enum):
    HISTORICAL = 'historical'
    KEYWORD_MAPPER = 'keyword_mapper'


class ViewGetter(object):
    def __init__(self, advertiser):
        self.advertiser = advertiser

    def get(self, type: ViewTypes):
        return '{}_{}'.format(type.value, self.advertiser)
