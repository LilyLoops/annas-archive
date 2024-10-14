#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_aac_upload_records.sh
# Download scripts are idempotent but will RESTART the download from scratch!

rm -rf /temp-dir/aac_upload_records
mkdir /temp-dir/aac_upload_records

cd /temp-dir/aac_upload_records

curl -C - -O https://annas-archive.li/dyn/torrents/latest_aac_meta/upload_records.torrent

# Tried ctorrent and aria2, but webtorrent seems to work best overall.
webtorrent --verbose download upload_records.torrent
