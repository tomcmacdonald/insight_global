#!/bin/bash

set -e

# current directory
DIR=$(pwd)

# create database
sqlite3 control.db <<EOF
create table if not exists control (
    id text
    , status text not null
    , last_modified text not null
    , primary key (id, last_modified)
);
EOF

# create a virtualenv
python3 -m venv env

# install requirements
"${DIR}/env/bin/pip" install -r requirements.txt

# create log and data directories
mkdir -p "${DIR}/logs"
mkdir -p "${DIR}/data"

# add crontab to run daily at midnight and redirect logs to directory
echo "0 0 * * * ${DIR}/env/bin/python ${DIR}/main.py >> ${DIR}/logs/\$(date +'\%Y\%m\%d\%H\%M').log 2>&1" | crontab -


