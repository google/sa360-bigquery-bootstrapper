pip3 install pipenv --user
if [[ $(grep -c "$HOME/.local/bin" <(echo $PATH)) == 0 ]]
then
    echo "PATH=\$PATH:$HOME/.local/bin" >> $HOME/.bashrc
    source ~/.bashrc
    echo "echo 'Already ran this - no further action necessary.'" > $0
fi