#!/usr/bin/env bash
cd $(dirname "$0")  # going to the same dir as the script
cd ../sphinx

printf "Generating code documentation .rst files"
sphinx-apidoc -d 4 -e -f -o . ../beam_nuggets/. ../**/test

printf "\nBuilding html"
make clean html

printf "\nInstalling"
rm -rf ../docs
cp -r _build/html ../docs
