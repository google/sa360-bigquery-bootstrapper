from enum import Enum


class Aggregation(Enum):
    SUM = 1
    COUNT = 2


def normalize(content):
  return content.replace(' ', '_').lower()


def aggregate_if(agg: Aggregation, column: str, value_empty_alt: any,
                 value_if_null: int = 0, prefix: str = ''):
    """Helper creates an {@link Aggregation} and a fallback value if missing
    Handles NULL by assigning a value of value_empty_alt

    :param agg: The aggregation type (using {@link Aggregation} enum)
    :param column: The column name
    :param value_empty_alt: Fallback value if NULL or missing
    :param value_if_null: Default 0: Replaces NULL for aggregation purposes.
    :param prefix: The prefix to differentiate when >1 column
                   with the same name exists.
    :return: An aggregation in the format of SUM(COALESCE(COLUMN, FALLBACK)) or
             COUNT(COALESCE(COLUMN, FALLBACK)) or CONSTANT.

             Example: SUM(h.revenue, 0)
    """
    return "{0}(COALESCE({1}{2}, {3}))".format(
        agg.name, prefix + '.' if prefix else '', column, value_if_null
    ) if column else value_empty_alt


class SettingUtil(object):
    def __init__(self, settings):
        self.settings = settings

    def unwrap(self, key):
        return self.settings[key].value


class ViewTypes(Enum):
    HISTORICAL = 'Historical'
    KEYWORD_MAPPER = 'KeywordMapper',
    HISTORICAL_CONVERSIONS = 'HistoricalConversions',
    REPORT_VIEW = 'ReportView',


class ViewGetter(object):
    def __init__(self, advertiser):
        self.advertiser = advertiser

    def get(self, type: ViewTypes):
        return '{}_{}'.format(type.value, self.advertiser)
