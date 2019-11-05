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

You will be prompted many questions, including the following:


## Opening the editor

You can edit a file stored in Cloud Shell using Cloud Shell’s built-in text editor.

*  To start, open the editor by clicking on the <walkthrough-cloud-shell-editor-icon></walkthrough-cloud-shell-editor-icon> icon.
*  Look at the source file for this tutorial by opening `tutorial.md`.
*  Try making a change to the file for this tutorial, then saving it using the <walkthrough-editor-spotlight spotlightId="fileMenu">file menu</walkthrough-editor-spotlight>.

To restart the tutorial with your changes, run:
```bash
cloudshell launch-tutorial -d tutorial.md
```

