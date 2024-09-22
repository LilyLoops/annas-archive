#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/decompress_aac_files.sh
# This script is OPTIONAL. Keeping the compressed files works fine, though it might be a bit slower.

cd /file-data/

for f in *.seekable.zst; do
    if [ ! -f ${f%.seekable.zst} ]; then
        unzstd --keep -o ${f%.seekable.zst} ${f}
    fi
done
