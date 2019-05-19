#!/bin/bash -eu
sudo fatrace | awk '/.* CW .*\/b2-sdk-python\/.*\.(rst|py)$/ {print;system("./regenerate.sh")}'
