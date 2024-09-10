#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_mariadb.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.

# Load from /temp-dir/imports (aa-data-import--temp-dir/imports on host).
# Add the -o option to overwrite tables
# Add --tables-list database.tablename,database.tablename2 etc to only import specific tables
# --tables-list allthethings.libgenli_editions_to_files for example

# Decompress dump
find /temp-dir/imports/mariadb -name "*.sql.gz" | parallel pigz -d {}

# Load into database
myloader --threads 32 --host ${MARIADB_HOST:-aa-data-import--mariadb} --user allthethings --password password --database allthethings --verbose 3 -d /temp-dir/imports/mariadb
