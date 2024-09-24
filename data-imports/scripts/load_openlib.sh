#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_openlib.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir

pv ol_dump_latest.txt.gz | zcat | sed -e 's/\\u0000//g' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword allthethings --local-infile=1 --show-warnings -vv -e "DROP TABLE IF EXISTS ol_base; CREATE TABLE ol_base (type CHAR(40) NOT NULL, ol_key CHAR(250) NOT NULL, revision INTEGER NOT NULL, last_modified DATETIME NOT NULL, json JSON NOT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin; LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE ol_base FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '';"

echo 'ALTER TABLE allthethings.ol_base ADD PRIMARY KEY(ol_key);' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword allthethings --show-warnings -vv
