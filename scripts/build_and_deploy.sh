#!/usr/bin/env bash

# go to the repo root
printf "\n upgrading wheel and twine...\n"
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
cd ${SCRIPT_DIR}/..
pip install --upgrade setuptools wheel twine

# build
printf "\n building ...\n";
python setup.py sdist bdist_wheel
if [[ $? -eq 0 ]]; then
    printf "build succeeded"
else
    printf "build failed!"
    exit 1
fi

# upload
upload_to_test () {
   printf "\n uploading to test ...\n";
   twine upload --repository-url https://test.pypi.org/legacy/ dist/*
}

upload_to_prod () {
   printf "\n uploading to prod ...\n";
   twine upload dist/*
}

upload_to_test
#upload_to_prod
