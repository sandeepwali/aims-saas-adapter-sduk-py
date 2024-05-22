#!/usr/bin/env bash

# This script randomizes the activation date for the CSV files in test/zip_contents
# Packs them up into a zip and uploads them into Azure Blob Store for testing
# the correct processing of the adapter + Queuing

# Set the date of the ZipFile
_zip_date="$(date +%y%m%d%H%M%S)"

cd "$(dirname "$0")" || exit 0

# Remove previous run data
rm -rf SD/random_contents
mkdir -p SD/random_contents

for file in SD/zip_contents/*; do
    dos2unix "$file"
    _pre="$(head -n1 "$file" | cut -d\| -f-5)"
    _post="$(head -n1 "$file" | cut -d\| -f7-)"
    _date="$(TZ=Europe/London date --date=@"$(echo $(date +%s) - 300 + $((RANDOM % 600)) | bc)" "+%d/%m/%Y %H:%M:%S")"
    _rand_file="$(echo $file | sed s,SD/zip_contents,SD/random_contents,)"
    echo "$_pre|$_date|$_post" >"$_rand_file"
    tail -n+2 "$file" >>"$_rand_file"
    unix2dos "$_rand_file"
done

cd SD/random_contents || exit

# Making Zip
zip "../SD$_zip_date.zip" *

cd ../../..

echo Uploading Zip Into BLOB
python -m modules.sduk.blob upload "scripts/SD/SD$_zip_date.zip" || :

rm scripts/SD/SD*.zip
# echo Starting app.py with once

# while python app.py once; do
#     sleep 30
# done
