#!/bin/bash -eu
sudo fatrace | awk '/.* CW \/home\/rare\/b2\/b2-sdk-python\/.*\.(rst|py)$/ {print;system("./regenerate.sh")}'
