# SA360 BigQuery Bootstrapper for Merchant Data Warehouse

## Walkthrough

### License

Copyright 2019 Google Inc.

Licensed under the Apache License, Version 2.0 (the 'License');
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an 'AS IS' BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Note that these code samples being shared are not official Google
products and are not formally supported.

## Select your cloud project

Select an existing Google Cloud project or create a new one.

<walkthrough-project-billing-setup key="project-id">
</walkthrough-project-billing-setup>

## Set project ID

Run the following command:

    gcloud config set project {{project-id}}

## One-time setup
If you run this command more than once, nothing bad will happen, 
but you should only have to run it one time.
    
    source startup.sh {{project-id}}
    
## Run deploy script

Now we will run the deploy script. It will be interactive, so be sure to follow the prompts below.

```bash
pipenv run python run.py --gcp_project_name={{project-id}} --interactive
```

You can copy the above command into the interactive shell below (or click on the **>** icon and then press enter below)

### Prompts

You will be prompted to answer many questions.
To see all prompts and hints you can run the command:

    pipenv run python.py --help

You can also fill in flags using this to avoid the interactive prompts.

### Check your columns

If you're uploading historical data, 
the easiest approach is to change your column headers.

Alternatively, you can fill in the prompts for your file.

*Note*: At the moment, this script only allows one file
configuration type at a time. If each advertiser you want
to upload has its own specific file format (or different headers)
and you can't get the header names to match, then you'll have to
run this script multiple times.

The script expects the following headers by default:

- Account Name: account_name
- Campaign Name: campaign_name
- Conversions: conversions
- Date: date
- Ad Group: ad_group_name
- Keyword Match Type: match_type

If there is revenue:
- Revenue Column: revenue

If there is a device segment:
- device_segment

## More Queries to Try

In your [BigQuery Console](https://pantheon.corp.google.com/bigquery?project={{project-id}}) you
can enter in any SQL query you want, and get your data in any format you want. Here are a few
examples:

### Historical Conversion Report
This view displays one line per conversion (so if a keyword report has 5 conversions in one line,
this report will have 5 lines with one conversion each).

    WITH conversions AS (
        SELECT 
            SPLIT(REPEAT(CONCAT(keywordId, ","), CAST(FLOOR(conversions) AS INT64)), ",") keywords
        FROM `conversion-extractor.elui_views.historical_conversions`
    )
    SELECT 
        c1.date,c1.keywordId,c1.MatchType,
        c1.AdGroup,1 conversions,revenue 
    FROM `[PROJECT].[DATASET].HistoricalConversions_[Advertiser ID]` c1 
    INNER JOIN (
        SELECT keywordId 
        FROM conversions
        CROSS JOIN UNNEST(conversions.keywords) keywordId 
        WHERE keywordId IS NOT NULL
    ) keywords ON keywords.keywordId=c1.keywordId AND FLOOR(c1.conversions) >= 1;
