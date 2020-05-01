# /***********************************************************************
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Note that these code samples being shared are not official Google
# products and are not formally supported.
# ************************************************************************/
import traceback

from absl import logging
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import Table
from termcolor import cprint

import app_settings
from flagmaker.settings import AbstractSettings
from utilities import Aggregation
from utilities import SettingUtil
from utilities import ViewGetter
from utilities import ViewTypes
from utilities import aggregate_if
from utilities import get_view_name


class DataSets:
    raw: bigquery.dataset.Dataset = None
    views: bigquery.dataset.Dataset = None


class CreateViews:
    client: bigquery.Client = None
    project: str = None
    advertiser: str = None
    settings: AbstractSettings = None

    def __init__(self, config, client, project, advertiser):
        self.settings: app_settings.AppSettings = config
        self.client = client
        self.project = project
        self.advertiser = advertiser
        self.s = SettingUtil(config)

    def run(self):
        report_level = self.s.unwrap('report_level')
        if report_level == 'campaign':
            raise MethodNotCreated('Methods for campaign-only views'
                                   ' not implemented.')
        else:
            self.view(
                ViewTypes.KEYWORD_MAPPER,
                'keyword_mapper'
            )
            if self.s.unwrap('has_historical_data'):
                self.view(
                    ViewTypes.HISTORICAL_CONVERSIONS,
                    'historical_conversions'
                )
                self.view(
                    ViewTypes.HISTORICAL_REPORT,
                    'historical_report',
                )
            self.view(
                ViewTypes.REPORT_VIEW,
                'report_view',
            )

    def view(self, view_name: ViewTypes, func_name):
        adv = str(self.s.unwrap('advertiser_id'))
        logging.debug(view_name.value)
        adv_view = get_view_name(view_name, adv)
        view_ref = DataSets.views.table(adv_view)
        view_query = getattr(
            self,
            func_name if func_name is not None else view_name.value
        )(adv)
        logging.debug(view_query)
        try:
            logging.debug(view_ref)
            view: Table = self.client.get_table(view_ref)
            view.view_query = view_query
            self.client.update_table(view, ['view_query'])
            cprint('= updated {}'.format(adv_view), 'green')
        except NotFound as err:
            try:
                logging.debug('error:\n-----\n%s\n-----\n', err)
                view = bigquery.Table(view_ref)
                logging.info('%s.%s', view.dataset_id, view.table_id)
                view.view_query = view_query
                self.client.create_table(view, exists_ok=True)
                cprint('+ created {}'.format(adv_view), 'green')
            except NotFound as err:
                cprint('Error: {}'.format(str(err)), 'red')
                logging.info(traceback.format_exc())
        self.keyword_mapper(adv)

    def historical_conversions(self, advertiser):
        views = ViewGetter(advertiser)

        sql = """SELECT 
            h.date,
            a.keywordId{deviceSegment},
            a.keywordMatchType MatchType,
            h.ad_group AdGroup,
            {conversions} conversions,
            {revenue} revenue
          FROM `{project}`.`{raw}`.`{historical_table_name}` h
          INNER JOIN (
            SELECT keywordId,
                keyword,
                campaign,
                keywordMatchType,
                adGroup,
                account
            FROM `{project}`.`{views}`.`{keyword_mapper}`
            GROUP BY
                keywordId,
                keyword,
                campaign,
                account,
                adGroup,
                keywordMatchType
          ) a
            ON a.keyword=h.keyword
            AND a.campaign=h.campaign_name
            AND a.account=h.account_name
            AND a.adGroup=h.ad_group
            AND LOWER(a.keywordMatchType) = LOWER(h.match_type)
          GROUP BY
            h.date,
            a.keywordId, 
            a.keyword, 
            a.keywordMatchType,
            h.ad_group,
            a.campaign{device_segment_column_name},
            a.account""".format(
              project=self.s.unwrap('gcp_project_name'),
              deviceSegment=(
                  ',\n' + 'h.device_segment'
                  + ' AS device_segment'
              ) if self.s.unwrap('has_device_segment') else '',
              device_segment_column_name = (
                  ',\n'
                  + 'h.' + self.s.unwrap('device_segment_column_name')
              ) if self.s.unwrap('has_device_segment') else '',
              raw=self.s.unwrap('raw_dataset'),
              views=self.s.unwrap('view_dataset'),
              historical_table_name=views.get(ViewTypes.HISTORICAL),
              keyword_mapper=views.get(ViewTypes.KEYWORD_MAPPER),
              conversions=aggregate_if(
                  Aggregation.SUM,
                  'conversions',
                  'SUM(1)',
                  prefix='h',
              ),
              revenue=aggregate_if(
                  Aggregation.SUM,
                  'revenue',
                  0,
                  prefix='h',
              ),
          )
        return sql

    def keyword_mapper(self, advertiser):
        sql = '''SELECT 
            k.keywordId, 
            k.keywordText keyword,
            k.keywordMatchType,
            c.campaign, 
            a.account,
            g.adGroup,
            a.accountType
            FROM (
                SELECT keywordText,
                keywordId,
                keywordEngineId,
                campaignId,
                accountId,
                adgroupId,
                keywordMatchType,
                RANK() OVER (
                  PARTITION BY keywordText, keywordMatchType, campaignId, 
                      accountId, adGroupId
                    ORDER BY CASE WHEN status='Active' THEN 0 ELSE 1 END,
                    CASE WHEN keywordEngineId IS NOT NULL THEN 0 ELSE 1 END
                  ) rank
              FROM `{project}`.`{raw_data}`.`Keyword_{advertiser_id}` c
            ) k
            INNER JOIN (
              SELECT campaignId, campaign 
              FROM `{project}`.`{raw_data}`.`Campaign_{advertiser_id}` 
              GROUP BY campaignId, campaign
              ) c ON c.campaignId = k.campaignId
            INNER JOIN (
              SELECT accountId, account, accountType 
              FROM `{project}`.`{raw_data}`.`Account_{advertiser_id}` 
              GROUP BY accountId, account, accountType
              ) a ON a.accountId = k.accountId
            INNER JOIN (
              SELECT adGroupId, adGroup
              FROM `{project}`.`{raw_data}`.`AdGroup_{advertiser_id}`
              GROUP BY adGroupId, adGroup
            ) g ON g.adGroupId = k.adGroupId
            WHERE keywordText IS NOT NULL
            AND rank = 1
            GROUP BY
                k.keywordId, 
                k.keywordText,
                k.keywordMatchType, 
                c.campaign, 
                a.account,
                a.accountType,
                g.adGroup'''.format(
                    project=self.s.unwrap('gcp_project_name'),
                    raw_data=self.s.unwrap('raw_dataset'),
                    advertiser_id=advertiser,
                )
        return sql

    def report_view(self, advertiser):
        views = ViewGetter(advertiser)
        deviceSegment = (',\n' + 'd.deviceSegment AS Device_Segment'
                if self.s.unwrap('has_device_segment') else '')
        historical_conversions = views.get(ViewTypes.HISTORICAL_CONVERSIONS)
        project = self.s.unwrap('gcp_project_name')
        view_data = self.s.unwrap('view_dataset')
        raw_data = self.s.unwrap('raw_dataset')
        keyword_mapper = views.get(ViewTypes.KEYWORD_MAPPER)
        date = self.s.unwrap('first_date_conversions')
        maybe_historical_data = 'LEFT JOIN ('

        if self.s.unwrap('has_historical_data'):
            maybe_historical_data += f"""
              SELECT
                date,
                keywordId{deviceSegment},
                SUM(revenue) revenue,
                SUM(conversions) conversions
              FROM `{project}.{view_data}.{historical_conversions}` o
              GROUP BY 
                date, 
                keywordId{deviceSegment}
            """
        else:
            maybe_historical_data += """
                SELECT CURRENT_DATE() as date, 
                '0' as keywordId, 
                NULL as device_segment,
                NULL as revenue,
                NULL AS conversions
            """
        maybe_historical_data += ') h ON h.keywordId = d.keywordId' \
                                 ' AND h.date = d.date'
        sql = f"""SELECT 
        d.date Date, 
        m.keywordId,
        m.keyword Keyword{deviceSegment}, 
        m.campaign Campaign,
        a.advertiser Advertiser, 
        m.account Engine, 
        m.accountType Account_Type,
        SUM(clicks) Clicks, 
        SUM(impr) Impressions, 
        SUM(weightedPos) Weighted_Pos,
        COALESCE(SUM(cost), 0) Cost,
        COALESCE(SUM(c.revenue), 0) + COALESCE(SUM(h.revenue), 0) Revenue,
        COALESCE(SUM(c.conversions), 0) + COALESCE(SUM(h.conversions), 0) Conversions
        FROM (
          SELECT date, 
            keywordId,
            SUM(clicks) clicks, 
            SUM(impr) impr, 
            SUM(avgPos*impr) weightedPos,
            SUM(cost) cost{deviceSegment}
          FROM `{project}.{raw_data}.KeywordDeviceStats_{advertiser}`
          GROUP BY 
            date, 
            keywordId{deviceSegment}
        ) d
        INNER JOIN (
            SELECT advertiser 
            FROM `{project}.{raw_data}.Advertiser_{advertiser}`
            LIMIT 1
        ) a ON 1=1
        INNER JOIN `{project}.{view_data}.{keyword_mapper}` m
          ON m.keywordId = d.keywordId
        {maybe_historical_data}
        LEFT JOIN (
          SELECT 
            date, 
            keywordId, 
            SUM(dfaRevenue) revenue,
            SUM(dfaTransactions) conversions
          FROM 
            `{project}.{raw_data}.KeywordFloodlightAndDeviceStats_{advertiser}`
          GROUP BY date, keywordId
        ) c 
            ON c.keywordId=d.keywordId 
            AND c.date=d.date 
            {"AND c.date > '{date}'" if date else ''}
        GROUP BY
            d.date, 
            m.keywordId, 
            m.keyword, 
            m.campaign,
            a.advertiser,
            m.account{deviceSegment},
            m.accountType"""
        return sql

    def historical_report(self, advertiser):
        views = ViewGetter(advertiser)
        sql = """WITH conversions AS (
                    SELECT
                        SPLIT(
                            REPEAT(CONCAT(keywordId, ","), 
                            CAST(FLOOR(conversions) AS INT64)), 
                            ","
                        ) keywords
                    FROM `{project}.{view_data}.{historical_conversions}`
                )
                SELECT
                    'conversions' as Row_Type,
                    'create' as Action,
                    'active' as Status,
                    a.advertiserId as Advertiser_ID,
                    a.agencyId as Agency_ID,
                    k.accountId as Account_ID,
                    a.accountType as Account_Type,
                    null as Floodlight_activity_ID,
                    null as Floodlight_activity_group_ID,
                    c1.date as Conversion_Date,
                    c1.keywordId as Keyword_ID,
                    c1.MatchType as Match_type,
                    c1.AdGroup,
                    c1.revenue/c1.conversions Conversion_Revenue
                FROM `{project}.{view_data}.{historical_conversions}` c1
                INNER JOIN (
                    SELECT keywordId
                        FROM conversions
                        CROSS JOIN UNNEST(conversions.keywords) keywordId
                        WHERE keywordId IS NOT NULL
                ) keywords 
                ON keywords.keywordId=c1.keywordId 
                AND FLOOR(c1.conversions) >= 1
                INNER JOIN (
                    SELECT
                        accountId, advertiserId, agencyId,
                        keywordId, keywordMatchType
                    FROM `{project}`.`{raw_data}`.`Keyword_{advertiser_id}`
                    GROUP BY 
                        accountId, advertiserId, agencyId,
                        keywordId, keywordMatchType
                ) k
                ON keywords.keywordId=k.keywordId
                INNER JOIN (
                    SELECT accountId, accountType, advertiserId, agencyId
                    FROM `{project}.{raw_data}.Account_{advertiser_id}`
                    GROUP BY accountId, accountType, advertiserId, agencyId
                ) a
                ON a.accountId=k.accountId
        """.format(
            advertiser_id=advertiser,
            project=self.s.unwrap('gcp_project_name'),
            raw_data=self.s.unwrap('raw_dataset'),
            view_data=self.s.unwrap('view_dataset'),
            keyword_mapper=views.get(ViewTypes.KEYWORD_MAPPER),
            historical_conversions=views.get(ViewTypes.HISTORICAL_CONVERSIONS),
        )
        return sql


class MethodNotCreated(Exception):
    pass
