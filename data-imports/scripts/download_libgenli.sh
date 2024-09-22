#!/bin/bash

set -Eeuxo pipefail

# For a faster method, see `download_libgenli_proxies_template.sh`.

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_libgenli.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

# Delete everything so far, so we don't confuse old and new downloads.
rm -rf libgenli_db

mkdir libgenli_db
cd /temp-dir/libgenli_db

# rclone -vP --include 'libgen_new.*' --max-depth 1 --check-first --checkers 1 --transfers 1 --size-only copy --retries=100 --low-level-retries=1000 --http-url="https://libgen.li/dbdumps/" :http: /temp-dir/libgenli_db/

# for i in $(seq -w 1 5); do # retries
#     rclone copy :ftp:/upload/db/ /temp-dir/libgenli_db/ --ftp-host=ftp.libgen.lc --ftp-user=anonymous --ftp-pass=$(rclone obscure dummy) --size-only --progress --multi-thread-streams=1 --transfers=1
# done

curl --fail -L -O "https://libgen.li/dbdumps/libgen_new.zip" || curl --fail -L -O "https://libgen.gs/dbdumps/libgen_new.zip" || curl --fail -L -O "https://libgen.vg/dbdumps/libgen_new.zip" || curl --fail -L -O "https://libgen.pm/dbdumps/libgen_new.zip"
for i in $(seq -w 1 64); do
    # Using curl here since it only accepts one connection from any IP anyway,
    # and this way we stay consistent with `libgenli_proxies_template.sh`.

    # Server doesn't support resuming??
    # curl -L -C - -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar"
    
    # Try bewteen these:
    # *.li, *.gs, *.vg, *.pm
    # curl --fail -L -O "https://libgen.lc/dbdumps/libgen_new.part0${i}.rar" || curl --fail -L -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar" || curl --fail -L -O "https://libgen.gs/dbdumps/libgen_new.part0${i}.rar" || curl --fail -L -O "https://libgen.vg/dbdumps/libgen_new.part0${i}.rar" || curl --fail -L -O "https://libgen.pm/dbdumps/libgen_new.part0${i}.rar"
    curl --fail -L -O "https://libgen.li/dbdumps/libgen_new.z${i}" || curl --fail -L -O "https://libgen.gs/dbdumps/libgen_new.z${i}" || curl --fail -L -O "https://libgen.vg/dbdumps/libgen_new.z${i}" || curl --fail -L -O "https://libgen.pm/dbdumps/libgen_new.z${i}"
done

#for i in $(seq -w 6 47); do curl --fail -L -O "https://libgen.lc/dbdumps/libgen_new.part0${i}.rar" || curl --fail -L -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar" || curl --fail -L -O "https://libgen.gs/dbdumps/libgen_new.part0${i}.rar" || curl --fail -L -O "https://libgen.vg/dbdumps/libgen_new.part0${i}.rar" || curl --fail -L -O "https://libgen.pm/dbdumps/libgen_new.part0${i}.rar"; done
