[[ -z $(which pipenv) ]] && pip3 install pipenv --user
[[ -n $1 && $(gcloud config get-value project) != $1 ]] && gcloud config set project $1 && echo "Project set"

gcloud services enable doubleclicksearch.googleapis.com
gcloud services enable storage-component.googleapis.com
gcloud services enable bigquery-json.googleapis.com
gcloud services enable bigquerystorage.googleapis.com
cmd="PATH=\$PATH:$HOME/.local/bin"
if [[ $(grep -c "$cmd" $HOME/.bashrc) == 0 ]]
then
    echo "PATH=\$PATH:$HOME/.local/bin" >> $HOME/.bashrc
    source ~/.bashrc
fi
