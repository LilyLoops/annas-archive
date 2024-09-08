#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_elasticsearch.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.

# Load from /temp-dir/imports (aa-data-import--temp-dir/imports on host).
cd /temp-dir

# https://github.com/elasticsearch-dump/elasticsearch-dump/issues/651#issuecomment-564545317
export NODE_OPTIONS="--max-old-space-size=16384"
# Don't set parallel= too high, might run out of memory.
multielasticdump --direction=load --input=imports/elasticsearch --output=${ELASTICSEARCH_HOST:-http://aa-data-import--elasticsearch:9200} --parallel=6 --limit=10000 --fsCompress --includeType=data,mapping,analyzer,alias,settings,template
