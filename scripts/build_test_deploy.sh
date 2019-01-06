#!/usr/bin/env bash

exit_if_prev_failed () {
    if [[ $? -eq 0 ]]; then
        printf "$1 succeeded"
    else
        printf "$1 failed!"
        exit 1
    fi
}

# go to the repo root
printf "\n upgrading wheel and twine...\n"
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
cd ${SCRIPT_DIR}/..
pip install --upgrade setuptools wheel twine
exit_if_prev_failed 'upgrade'

# test
python -m unittest discover -v
exit_if_prev_failed 'tests'

# build
printf "\n building ...\n";
python setup.py sdist bdist_wheel
exit_if_prev_failed 'building'

# upload
upload_to_test () {
   printf "\n uploading to test ...\n";
   twine upload --repository-url https://test.pypi.org/legacy/ dist/*
   exit_if_prev_failed 'upload to test'
}

upload_to_prod () {
   printf "\n uploading to prod ...\n";
   twine upload dist/*
   exit_if_prev_failed 'upload to prod'
}

upload_to_test
#upload_to_prod
