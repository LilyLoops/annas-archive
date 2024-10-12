#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_aac_other_metadata.sh
# Download scripts are idempotent but will RESTART the download from scratch!

rm -rf /temp-dir/aac_ebscohost_records
mkdir /temp-dir/aac_ebscohost_records

cd /temp-dir/aac_ebscohost_records

curl -C - -O https://annas-archive.li/dyn/torrents/latest_aac_meta/ebscohost_records.torrent

# Tried ctorrent and aria2, but webtorrent seems to work best overall.
webtorrent --verbose download ebscohost_records.torrent
