#!/bin/bash

echo "Starting parallelrunner python code"
source ./venv/bin/activate
python /docker/main.py
echo "Closing parallelrunner python code"
