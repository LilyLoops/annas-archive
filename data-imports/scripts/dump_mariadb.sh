#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/dump_elasticsearch.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Dump scripts are idempotent, and can be rerun without losing too much work.

# Make core dumps and other debug output to go to /temp-dir.
cd /temp-dir

rm -rf /exports/mariadb
mkdir /exports/mariadb
cd /exports/mariadb
mydumper --threads 32 --omit-from-file /app/data-imports/scripts/dump_mariadb_omit_tables.txt --exit-if-broken-table-found --tz-utc --host ${MARIADB_HOST:-mariadb} --user allthethings --password password --database allthethings --compress --verbose 3 --long-query-guard 999999 --no-locks --compress-protocol --order-by-primary --outputdir /exports/mariadb

# Not as acutely necessary to verify gzip integrity here (compared to elasticdump scripts), but might as well.
time ls *.gz | parallel 'echo {}: $(zcat {} | wc -l)'
