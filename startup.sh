pip3 install pipenv --user
gcloud config set project $1
gcloud services enable doubleclicksearch.googleapis.com
gcloud services enable storage-component.googleapis.com
gcloud services enable bigquery-json.googleapis.com
gcloud services enable bigquerystorage.googleapis.com
if [[ $(grep -c "$HOME/.local/bin" <(echo $PATH)) == 0 ]]
then
    echo "PATH=\$PATH:$HOME/.local/bin" >> $HOME/.bashrc
    source ~/.bashrc
fi