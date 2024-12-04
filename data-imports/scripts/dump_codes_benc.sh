#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/dump_codes_benc.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Dump scripts are idempotent, and can be rerun without losing too much work.

# Make core dumps and other debug output to go to /temp-dir.

rm -rf /exports/codes_benc
mkdir /exports/codes_benc
cd /exports/codes_benc
flask cli dump_isbn13_codes_benc
