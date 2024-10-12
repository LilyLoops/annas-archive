Importing the data has been mostly automated, but it's still advisable to run the individual scripts yourself. It can take a week to run everything on a high-end machine (64 core CPU, 1TB RAM, 10TB NVMe), but we also support only updating part of the data.

Roughly the steps are:
- (optional) make a copy of the existing MariaDB database, if you want to keep existing data.
- Download new data.
- Import data into MariaDB.
- Generate derived data (mostly ElasticSearch).
- Swap out the new data in production.

Many steps can be skipped by downloading our [precalculated data](https://annas-archive.li/torrents#aa_derived_mirror_metadata). For more details on that, see below.

```bash
# First navigate to this data-imports directory.
cd /my/path/to/annas-archive/data-imports

[ -e ../../aa-data-import--allthethings-mysql-data ] && (echo '../../aa-data-import--allthethings-mysql-data already exists; aborting'; exit 1)
[ -e ../../aa-data-import--allthethings-elastic-data ] && (echo '../../aa-data-import--allthethings-elastic-data already exists; aborting'; exit 1)
[ -e ../../aa-data-import--allthethings-elasticsearchaux-data ] && (echo '../../aa-data-import--allthethings-elasticsearchaux-data already exists; aborting'; exit 1)
# If you wish to download everything from scratch, you should make sure the aa-data-import--temp-dir dir is deleted.
# [ -e ../../aa-data-import--temp-dir ] && (echo '../../aa-data-import--temp-dir already exists; aborting'; exit 1)

mkdir ../../aa-data-import--allthethings-elastic-data
chown 1000 ../../aa-data-import--allthethings-elastic-data
mkdir ../../aa-data-import--allthethings-elasticsearchaux-data
chown 1000 ../../aa-data-import--allthethings-elasticsearchaux-data

# Run this you want to start off with the existing MariaDB data, e.g. if you only want to run a subset of the scripts.
sudo rsync -av --append ../../allthethings-mysql-data/ ../../aa-data-import--allthethings-mysql-data/

# You might need to adjust the size of ElasticSearch's heap size, by changing `ES_JAVA_OPTS` in `data-imports/docker-compose.yml`.
# If MariaDB wants too much RAM: comment out `key_buffer_size` in `data-imports/mariadb-conf/my.cnf`
docker compose up -d --no-deps --build

# It's a good idea here to look at the Docker logs:
# docker compose logs --tail=200 -f

# Download the data. You can skip any of these scripts if you have already downloaded the data and don't want to repeat it.
# You can also run these in parallel in multiple terminal windows.
# We recommend looking through each script in detail before running it.
docker exec -it aa-data-import--web /scripts/download_libgenli.sh # Can be skipped when using aa_derived_mirror_metadata.
# Look at data-imports/scripts/download_libgenli_proxies_template.sh to speed up downloading.
# E.g.: docker exec -it aa-data-import--web /scripts/download_libgenli_proxies.sh; docker exec -it aa-data-import--web /scripts/download_libgenli.sh
docker exec -it aa-data-import--web /scripts/download_libgenrs.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/download_openlib.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/download_pilimi_isbndb.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/download_pilimi_zlib.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/download_aa_various.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/download_aac_duxiu_files.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_duxiu_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_ia2_acsmpdf_files.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_ia2_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_magzdb_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_nexusstc_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_upload_files.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_upload_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_worldcat.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_zlib3_files.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_zlib3_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/download_aac_other_metadata.sh # CANNOT BE SKIPPED

# Load the data.
docker exec -it aa-data-import--web /scripts/load_libgenli.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/load_libgenrs.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/load_openlib.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/load_pilimi_isbndb.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/load_pilimi_zlib.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/load_aa_various.sh # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web /scripts/load_aac_duxiu_files.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_duxiu_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_ia2_acsmpdf_files.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_ia2_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_magzdb_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_nexusstc_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_upload_files.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_upload_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_worldcat.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_zlib3_files.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_zlib3_records.sh # CANNOT BE SKIPPED
docker exec -it aa-data-import--web /scripts/load_aac_other_metadata.sh # CANNOT BE SKIPPED

# Index AAC files.
docker exec -it aa-data-import--web /scripts/decompress_aac_files.sh # OPTIONAL: only run this if you have enough disk space and want to speed up calculating derived data. The decompressed files are not recommended to keep for use in production (waste of space).
docker exec -it aa-data-import--web flask cli mysql_reset_aac_tables # OPTIONAL: mysql_build_aac_tables will recreate tables as necessary, but this can be useful if you suspect data corruption.
docker exec -it aa-data-import--web flask cli mysql_build_aac_tables # RECOMMENDED even when using aa_derived_mirror_metadata, in case new AAC files have been loaded since the data of aa_derived_mirror_metadata was generated. AAC files that are the same will automatically be skipped.

# To manually keep an eye on things, run SHOW PROCESSLIST; in a MariaDB prompt:
docker exec -it aa-data-import--mariadb mariadb -u root -ppassword allthethings

# First sanity check to make sure the right tables exist.
docker exec -it aa-data-import--web /scripts/check_after_imports.sh

# Sanity check to make sure the tables are filled.
docker exec -it aa-data-import--mariadb mariadb -u root -ppassword allthethings --show-warnings -vv -e 'SELECT table_name, ROUND(((data_length + index_length) / 1000 / 1000 / 1000), 2) AS "Size (GB)" FROM information_schema.TABLES WHERE table_schema = "allthethings" ORDER BY table_name;'

# Calculate derived data:
docker exec -it aa-data-import--web flask cli mysql_build_computed_all_md5s # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web flask cli elastic_reset_aarecords # Can be skipped when using aa_derived_mirror_metadata. Only necessary for full reset.
docker exec -it aa-data-import--web flask cli elastic_build_aarecords_all # Can be skipped when using aa_derived_mirror_metadata. Only necessary for full reset; see the code for incrementally rebuilding only part of the index.
docker exec -it aa-data-import--web flask cli elastic_build_aarecords_forcemerge # Can be skipped when using aa_derived_mirror_metadata.
docker exec -it aa-data-import--web flask cli mysql_build_aarecords_codes_numbers # Can be skipped when using aa_derived_mirror_metadata. Only run this when doing full reset.

# Gracefully shut down MariaDB
docker exec -it aa-data-import--web /scripts/mariadb_graceful_shutdown.sh

# Make sure to fully stop the databases, so we can move some files around.
docker compose down

# Quickly swap out the new MariaDB+ES folders in a production setting.
cd ..
docker compose stop mariadb elasticsearch elasticsearchaux kibana web
export NOW=$(date +"%Y_%m_%d_%H_%M")
mv ../allthethings-mysql-data ../allthethings-mysql-data--backup-$NOW
mv ../allthethings-elastic-data ../allthethings-elastic-data--backup-$NOW
mv ../allthethings-elasticsearchaux-data ../allthethings-elasticsearchaux-data--backup-$NOW
rsync -a --progress ../aa-data-import--allthethings-mysql-data/ ../allthethings-mysql-data
rsync -a --progress ../aa-data-import--allthethings-elastic-data/ ../allthethings-elastic-data
rsync -a --progress ../aa-data-import--allthethings-elasticsearchaux-data/ ../allthethings-elasticsearchaux-data
docker compose up -d --no-deps --build; docker compose stop web
docker compose logs --tail 20 --follow
docker compose start web

# To restore the backup:
docker compose stop mariadb elasticsearch elasticsearchaux kibana
mv ../allthethings-mysql-data ../allthethings-mysql-data--didnt-work
mv ../allthethings-elastic-data ../allthethings-elastic-data--didnt-work
mv ../allthethings-elasticsearchaux-data ../allthethings-elasticsearchaux-data--didnt-work
mv ../allthethings-mysql-data--backup-$NOW ../allthethings-mysql-data
mv ../allthethings-elastic-data--backup-$NOW ../allthethings-elastic-data
mv ../allthethings-elasticsearchaux-data--backup-$NOW ../allthethings-elasticsearchaux-data
docker compose up -d --no-deps --build
docker compose logs --tail 20 --follow
```

## Importing from aa_derived_mirror_metadata

For answers to questions about this, please see [this Reddit post and comments](https://www.reddit.com/r/Annas_Archive/comments/1dtb4qz/comment/lbbo3ys/).

```bash
# First, download the torrents from https://annas-archive.li/torrents#aa_derived_mirror_metadata to aa-data-import--temp-dir/imports.
# Then run these before the commands mentioned above:
docker exec -it aa-data-import--web /scripts/load_elasticsearch.sh
docker exec -it aa-data-import--web /scripts/load_elasticsearchaux.sh
docker exec -it aa-data-import--web /scripts/load_mariadb.sh
# Make sure to still run the download_aac_*, load_aac_* (download_aac_* and load_aac_* can be run in parallel with the 3 scripts above),
# and mysql_build_aac_tables scripts (can NOT be run in parallel), since those download and move into position the AAC files,
# which are necessary for some more unusual operations (such as the /db endpoints). This will not rebuild any MariaDB tables, since the system
# will detect that the AAC files are already up to date (unless there have since been newer AAC files) and will use the imported AAC
# tables (which point to byte offsets in the compressed AAC files).
# We also recommend still running check_after_imports.sh.
# If you have more questions, please first check out this Reddit post and comments: https://www.reddit.com/r/Annas_Archive/comments/1dtb4qz/comment/lbbo3ys/
```
