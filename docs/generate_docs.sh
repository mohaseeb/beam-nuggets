#!/usr/bin/env bash

DOCS_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
SCRIPT_NAME="$(basename "$0")"

PACKAGE_ROOT="$DOCS_DIR/../beam_nuggets/"
PACKAGE_TEST_REGEX="$DOCS_DIR/../**/test"

# remove old html files
cd "$DOCS_DIR"  # going to the html files dir (also where this script lives)
OLD_HTML_DOCS=$(ls | grep -v -e ${SCRIPT_NAME} -e sphinx)
rm -rf $OLD_HTML_DOCS

printf "Generating code documentation .rst files"
cd sphinx
sphinx-apidoc -d 4 -e -f -o . $PACKAGE_ROOT $PACKAGE_TEST_REGEX

printf "\nBuilding html"
make clean html

printf "\nInstalling"
cp -r _build/html/* ../.
