#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_scihub.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir

# NOTE! Collation utf8mb4_general_ci (case insensitive) because DOIs are case insensitive.
7zr e -so -bd dois-2022-02-12.7z | sed -e 's/\\u0000//g' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword allthethings --local-infile=1 --show-warnings -vv -e "DROP TABLE IF EXISTS scihub_dois; CREATE TABLE scihub_dois (doi VARCHAR(250) NOT NULL, PRIMARY KEY(doi)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci; LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE scihub_dois FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '';"
