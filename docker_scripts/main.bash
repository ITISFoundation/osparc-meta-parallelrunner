#!/bin/bash

set -e

echo "Starting webserver"
cd /docker/http/server
npm start &

echo "Starting parallelrunner python code"
cd ${HOME}
source ./venv/bin/activate
python /docker/main.py

echo "Closing parallelrunner"
