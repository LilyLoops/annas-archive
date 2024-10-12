#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_worldcat.sh
# Download scripts are idempotent but will RESTART the download from scratch!

rm -rf /temp-dir/worldcat
mkdir /temp-dir/worldcat

cd /temp-dir/worldcat

# aria2c -c -x16 -s16 -j16 https://archive.org/download/WorldCatMostHighlyHeld20120515.nt/WorldCatMostHighlyHeld-2012-05-15.nt.gz

curl -C - -O https://annas-archive.li/dyn/torrents/latest_aac_meta/worldcat.torrent
webtorrent worldcat.torrent
