#!/bin/bash -eu
cd ..
sudo fatrace | awk '/.* CW .*\/b2-sdk-python\/.*\.(rst|py)$/ {print;system("cd doc; ./regenerate.sh")}'
