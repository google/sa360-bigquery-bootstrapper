# SA360 BigQuery Bootstrapper
Provides support in creating helpful views and merging historical data from CSV files with current
platform data.

## Disclaimer
This is not an officially supported Google product. It is provided strictly AS-IS.
There is absolutley NO WARRANTY provided for using this code.

## How to install

Installation is easy on Cloud Shell. Because this project relies on Google Cloud to work, the 
recommended way to install is by clicking the button below and following the tutorial.

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://ssh.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2Fgoogle%2Fsa360-bigquery-bootstrapper&cloudshell_tutorial=docs%2Ftutorial.md)


## FAQ

### How do I update the code?

Need to update this software? Easiest way is to click on the button above to Open in Cloud Shell, and then select option 2 to open the directory and pull from git. This will give you the latest code.

## I want to launch the tutorial. How do I do that?
 
The tutorial provides step-by-step guides on how to run this script. 
You can access within cloudshell by running:

    cloudshell launch-tutorial -d docs/tutorial.md

## What if I want to upload historical conversions back to Search Ads 360?

We provide a useful view for this at the end of the [tutorial](docs/tutorial.md). 
You will need to change the headers and may need to do more data massaging, but this view gets
you a conversion level report.

## How do I link this to DataStudio for visualization?

You can create a new DataStudio instance and link it to BigQuery. [Docs](https://cloud.google.com/bigquery/docs/visualize-data-studio)

Currently there are no publicly available templates.


## CHANGES

2019-11-22 - Allow overwriting of existing Historical table 
by passing the `--overwrite_storage_csv` option