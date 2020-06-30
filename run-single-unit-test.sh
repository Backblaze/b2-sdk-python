#!/bin/bash -e

python setup.py nosetests -a "__name__=$1" --processes=0 -s
