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

## How to use

First install dependencies:

    source startup.sh
    
The tutorial provides step-by-step guides on how to run this command. Outside of that,
the most helpful way to see what is possible is by running the following command:

    pipenv run python run.py --help
    
