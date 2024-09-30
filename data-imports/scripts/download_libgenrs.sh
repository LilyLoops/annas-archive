#!/bin/bash

set -Eeuxo pipefail
# https://stackoverflow.com/a/3355423
cd "$(dirname "$0")"

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_libgenrs.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

# Delete everything so far, so we don't confuse old and new downloads.
rm -f libgen.rar fiction.rar

aria2c -c -x4 -s4 -j4 'http://libgen.is/dbdumps/libgen.rar'
aria2c -c -x4 -s4 -j4 'http://libgen.is/dbdumps/fiction.rar'
