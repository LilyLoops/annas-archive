#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_aac_ia2_acsmpdf_files.sh
# Download scripts are idempotent but will RESTART the download from scratch!

rm -rf /temp-dir/aac_ia2_acsmpdf_files
mkdir /temp-dir/aac_ia2_acsmpdf_files

cd /temp-dir/aac_ia2_acsmpdf_files

curl -C - -O https://annas-archive.li/dyn/torrents/latest_aac_meta/ia2_acsmpdf_files.torrent

# Tried ctorrent and aria2, but webtorrent seems to work best overall.
webtorrent --verbose download ia2_acsmpdf_files.torrent
