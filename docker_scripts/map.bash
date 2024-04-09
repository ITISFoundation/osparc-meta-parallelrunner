#!/bin/bash

pip install -r /docker/requirements.txt
echo "Starting map python code"
python3 /docker/map.py
echo "Closing map python code"
