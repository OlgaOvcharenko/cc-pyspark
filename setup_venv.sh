#!/bin/bash

module load python/3.7.4

if [[ ! -d "python_env" ]]; then
    echo "Created venv on $HOSTNAME"

    module load python

    python3 -m venv python_venv

    source "python_venv/bin/activate"

    pip install --upgrade pip
    pip install --upgrade pip

    pip3 install -r requirements.txt
fi
