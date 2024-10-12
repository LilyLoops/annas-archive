#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_torrentsjson.sh
# Download scripts are idempotent but will RESTART the download from scratch!

rm -rf /temp-dir/torrents_json
mkdir /temp-dir/torrents_json

cd /temp-dir/torrents_json

curl -O https://annas-archive.li/dyn/torrents.json
