#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_aac_nexusstc_records.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir/aac_nexusstc_records

rm -f /file-data/annas_archive_meta__aacid__nexusstc_records*
mv annas_archive_meta__aacid__nexusstc_records*.jsonl.seekable.zst /file-data/
