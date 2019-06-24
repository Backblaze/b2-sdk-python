#!/bin/bash -u

#
# USAGE: ./pre-commit.sh [remote_name] [branch_name] [remote_branch_name] [testname...]
# if the first test name to execute is "quick", then integration tests are skipped completely
#
# for example: ./pre-commit.sh origin master master basic account download
#


base_remote="${1:-origin}"
base_branch="${2:-master}"
base_remote_branch="${3:-master}"

# get a list of tests to run into $*
shift
shift
shift


function header
{
    echo
    echo "#"
    echo "# $*"
    echo "#"
    echo
}

header Unit Tests

if ./run-unit-tests.sh
then
    echo "Unit tests PASSED"
else
    echo "Unit tests FAILED"
    exit 1
fi

header Checking Formatting

if ! type yapf &> /dev/null
then
    echo "Please install yapf, then try again."
    exit 1
fi

if [ "$(git rev-parse ${base_branch})" != "$(git rev-parse ${base_remote}/${base_remote_branch})" ]; then
    echo """running yapf in full mode, because an assumption that master and origin/master are the same, is broken. To fix it, do this:
git checkout master
git pull --ff-only

then checkout your topic branch and run $0.
If the base branch on github is not called 'origin', invoke as $0 proper_origin_remote_name. Then your remote needs to be synched with your master too.
"""
    yapf --in-place --parallel --recursive --exclude '*eggs/*' .
else
    echo 'running yapf in incremental mode'
    head=`mktemp`
    master=`mktemp`
    git rev-list --first-parent HEAD > "$head"  # list of commits being a history of HEAD branch, but without commits merged from master after forking
    git rev-list origin/master > "$master"  # list of all commits on history of master

    changed_files=`git diff --name-only "$(git rev-parse --abbrev-ref HEAD)..${base_remote}/${base_remote_branch}"`
    dirty_files=`git ls-files -m`
    files_to_check="$((echo "$changed_files"; echo "$dirty_files") | grep '\.py$' | sort -u)"
    if [ -z "$files_to_check" ]; then
        echo 'nothing to run yapf on after all'
    else
        echo -n 'running yapf... '
        echo "$files_to_check" | (while read file
        do
            if [ -e "$file" ]; then
                # in case file was added since master, but then was removed
                yapf --in-place "$file" &
            fi
        done
        wait
        )

        echo 'done'
    fi
fi

header Check Licenses

missing="$(
    (
        grep -l 'All Rights Reserved' $(git ls-files | grep .py)
        git ls-files | grep .py
    ) | sort | uniq -c | sort -n | awk '$1 == 1'
)"
if [ -n "$missing" ]; then
    echo 'license is missing from:' >&2
    echo "$missing" >&2
    exit 1
fi

failing=0
for file in $(git ls-files | grep .py)
do
    if [ ! -f "$file" ]; then
        echo "file with a newline in the name or space or something? \"$file\""
        exit 1
    fi
	license_path="$(grep -B3 'All Rights Reserved' "$file" | awk '/# File: / {print $3}')"
    if [ "$file" != "$license_path" ]; then
        failing=1
        echo "$file contains an inappropriate path in license header: \"$license_path\""

        # uncomment for naive autofix
        #data=`sed 3d "$file" | sed "3i\# File: $file"`
        #echo "$data" > "$file"
        #failing=0
    fi
done
if [ "$failing" == 1 ]; then
	echo "license checker FAILED"
	exit 1
else
	echo "license checker passed"
fi


header Pyflakes

for d in b2sdk test *.py
do
    # pyflakes does not ignore lines tagged with  # noqa
    output="$(pyflakes "$d" | egrep -v "(b2sdk/v[0-9]+/(__init__|exception)\.py|test/v[0-9]+/deps(_exception)?.py):[0-9]+: ('from b2sdk.v[0-9]+(\.exception)? import \*' used; unable to detect undefined names|'(b2sdk\..*|\..*)' imported but unused)")"

    if [ -z "$output" ]
    then
        echo "pyflakes passed on $d"
    else
        echo "$output"
        echo "pyflakes FAILED on $d"
        exit 1
    fi
done


header test_raw_api

if TEST_APPLICATION_KEY_ID="$(head -n 1 ~/.b2_auth)" TEST_APPLICATION_KEY="$(tail -n 1 ~/.b2_auth)" python -m b2sdk.raw_api
then
    echo "raw API test PASSED"
else
    echo "raw API test FAILED"
    exit 1
fi
