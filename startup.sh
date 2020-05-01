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

[[ -z $(which pipenv) ]] && pip3 install pipenv --user
export GOOGLE_CLOUD_PROJECT=$1
gcloud config set project $1
gcloud config get-value project

gcloud services enable doubleclicksearch.googleapis.com
gcloud services enable storage-component.googleapis.com
gcloud services enable bigquery-json.googleapis.com
gcloud services enable bigquerystorage.googleapis.com
gcloud services enable bigquerydatatransfer.googleapis.com
cmd="PATH=\$PATH:$HOME/.local/bin"
if [[ $(grep -c "$cmd" $HOME/.bashrc) == 0 ]]
then
    echo "PATH=\$PATH:$HOME/.local/bin" >> $HOME/.bashrc
    source ~/.bashrc
fi
