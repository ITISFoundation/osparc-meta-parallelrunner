#!/bin/bash

pip install -r /docker/requirements.txt
echo "Starting map python code"
python3 /docker/main.py
echo "Closing map python code"
