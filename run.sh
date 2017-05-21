#!/bin/bash
cd `dirname $0`

if [ ! -d "./.venv" ]; then
    virtualenv .venv -p python3.6
    source .venv/bin/activate && pip install -q -r requirements.txt
fi

source .venv/bin/activate && time python main.py generate
source .venv/bin/activate && time python main.py process

set -x
wc -l id_object.csv
wc -l id_level.csv
head -n20 id_level.csv
