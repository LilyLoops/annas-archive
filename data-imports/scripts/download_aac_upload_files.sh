#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_aac_upload_files.sh
# Download scripts are idempotent but will RESTART the download from scratch!

rm -rf /temp-dir/aac_upload_files
mkdir /temp-dir/aac_upload_files

cd /temp-dir/aac_upload_files

curl -C - -O https://annas-archive.li/dyn/torrents/latest_aac_meta/upload_files.torrent

# Tried ctorrent and aria2, but webtorrent seems to work best overall.
webtorrent --verbose download upload_files.torrent
