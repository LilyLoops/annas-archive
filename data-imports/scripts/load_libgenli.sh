#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_libgenli.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /aa-data-import--allthethings-mysql-data

echo 'DROP DATABASE IF EXISTS libgen_new;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv
rm -rf libgen_new

7z x /temp-dir/libgenli_db/libgen_new.zip
# unrar x /temp-dir/libgenli_db/libgen_new.part001.rar
chown -R 999:999 libgen_new

mysqlcheck -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --auto-repair --check libgen_new

# Used this to generate this list: SELECT Concat('DROP TRIGGER ', Trigger_Name, ';') FROM  information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = 'libgen_new';
# (from https://stackoverflow.com/a/30339930)
echo 'DROP TRIGGER libgen_new.authors_before_ins_tr; DROP TRIGGER libgen_new.authors_add_descr_before_ins_tr; DROP TRIGGER libgen_new.authors_add_descr_before_upd_tr; DROP TRIGGER libgen_new.authors_add_descr_before_del_tr1;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job1pid=$!
echo 'DROP TRIGGER libgen_new.editions_before_ins_tr1; DROP TRIGGER libgen_new.editions_before_upd_tr1; DROP TRIGGER libgen_new.editions_before_del_tr1;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job2pid=$!
echo 'DROP TRIGGER libgen_new.editions_add_descr_before_ins_tr; DROP TRIGGER libgen_new.editions_add_descr_after_ins_tr; DROP TRIGGER libgen_new.editions_add_descr_before_upd_tr; DROP TRIGGER libgen_new.editions_add_descr_after_upd_tr; DROP TRIGGER libgen_new.editions_add_descr_before_del_tr; DROP TRIGGER libgen_new.editions_add_descr_after_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job3pid=$!
echo 'DROP TRIGGER libgen_new.editions_to_files_before_ins_tr; DROP TRIGGER libgen_new.editions_to_files_before_upd_tr; DROP TRIGGER libgen_new.editions_to_files_before_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job4pid=$!
echo 'DROP TRIGGER libgen_new.files_before_ins_tr; DROP TRIGGER libgen_new.files_before_upd_tr; DROP TRIGGER libgen_new.files_before_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job5pid=$!
echo 'DROP TRIGGER libgen_new.files_add_descr_before_ins_tr; DROP TRIGGER libgen_new.files_add_descr_before_upd_tr; DROP TRIGGER libgen_new.files_add_descr_before_del_tr1;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job6pid=$!
echo 'DROP TRIGGER libgen_new.publisher_before_ins_tr; DROP TRIGGER libgen_new.publisher_before_upd_tr; DROP TRIGGER libgen_new.publisher_before_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job7pid=$!
echo 'DROP TRIGGER libgen_new.publisher_add_descr_before_ins_tr; DROP TRIGGER libgen_new.publisher_add_descr_before_upd_tr; DROP TRIGGER libgen_new.publisher_add_descr_before_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job8pid=$!
echo 'DROP TRIGGER libgen_new.series_before_ins_tr; DROP TRIGGER libgen_new.series_before_upd_tr; DROP TRIGGER libgen_new.series_before_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job9pid=$!
echo 'DROP TRIGGER libgen_new.series_add_descr_before_ins_tr; DROP TRIGGER libgen_new.series_add_descr_after_ins_tr; DROP TRIGGER libgen_new.series_add_descr_before_upd_tr; DROP TRIGGER libgen_new.series_add_descr_after_upd_tr; DROP TRIGGER libgen_new.series_add_descr_before_del_tr; DROP TRIGGER libgen_new.series_add_descr_after_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job10pid=$!
echo 'DROP TRIGGER libgen_new.works_before_ins_tr; DROP TRIGGER libgen_new.works_before_upd_tr; DROP TRIGGER libgen_new.works_before_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job11pid=$!
echo 'DROP TRIGGER libgen_new.works_add_descr_before_ins_tr; DROP TRIGGER libgen_new.works_add_descr_before_upd_tr; DROP TRIGGER libgen_new.works_add_descr_before_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job12pid=$!
echo 'DROP TRIGGER libgen_new.works_to_editions_before_ins_tr; DROP TRIGGER libgen_new.works_to_editions_before_upd_tr; DROP TRIGGER libgen_new.works_to_editions_before_del_tr;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
job13pid=$!
wait $job1pid
wait $job2pid
wait $job3pid
wait $job4pid
wait $job5pid
wait $job6pid
wait $job7pid
wait $job8pid
wait $job9pid
wait $job10pid
wait $job11pid
wait $job12pid
wait $job13pid

mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv < /scripts/helpers/libgenli_renames.sql

# Not really necessary; skip to save time.
# echo 'ALTER TABLE libgen_new.libgenli_editions DROP INDEX `YEAR`, DROP INDEX `N_YEAR`, DROP INDEX `MONTH`, DROP INDEX `MONTH_END`, DROP INDEX `VISIBLE`, DROP INDEX `LG_TOP`, DROP INDEX `TYPE`, DROP INDEX `COMMENT`, DROP INDEX `S_ID`, DROP INDEX `DOI`, DROP INDEX `ISSUE`, DROP INDEX `DAY`, DROP INDEX `TIME`, DROP INDEX `TIMELM`;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
# job1pid=$!
# echo 'ALTER TABLE libgen_new.libgenli_editions_add_descr DROP INDEX `TIME`, DROP INDEX `VAL3`, DROP INDEX `VAL`, DROP INDEX `VAL2`, DROP INDEX `VAL1`, DROP INDEX `VAL_ID`, DROP INDEX `VAL_UNIQ`, DROP INDEX `KEY`;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
# job2pid=$!
# echo 'ALTER TABLE libgen_new.libgenli_editions_to_files DROP INDEX `TIME`, DROP INDEX `FID`; -- f_id is already covered by `IDS`.' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
# job3pid=$!
# echo 'ALTER TABLE libgen_new.libgenli_elem_descr DROP INDEX `key`;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
# job4pid=$!
# echo 'ALTER TABLE libgen_new.libgenli_files DROP INDEX `md5_2`, DROP INDEX `MAGZID`, DROP INDEX `COMICSID`, DROP INDEX `LGTOPIC`, DROP INDEX `FICID`, DROP INDEX `FICTRID`, DROP INDEX `SMID`, DROP INDEX `STDID`, DROP INDEX `LGID`, DROP INDEX `FSIZE`, DROP INDEX `TIME`, DROP INDEX `TIMELM`;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
# job5pid=$!
# echo 'ALTER TABLE libgen_new.libgenli_files_add_descr DROP INDEX `TIME`, DROP INDEX `VAL`, DROP INDEX `KEY`;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
# job6pid=$!
# echo 'ALTER TABLE libgen_new.libgenli_publishers DROP INDEX `TIME`, DROP INDEX `COM`, DROP INDEX `FULLTEXT`;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
# job7pid=$!
# echo 'ALTER TABLE libgen_new.libgenli_series DROP INDEX `LG_TOP`, DROP INDEX `TIME`, DROP INDEX `TYPE`, DROP INDEX `VISIBLE`, DROP INDEX `COMMENT`, DROP INDEX `VAL_FULLTEXT`;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
# job8pid=$!
# echo 'ALTER TABLE libgen_new.libgenli_series_add_descr DROP INDEX `TIME`, DROP INDEX `VAL`, DROP INDEX `VAL1`, DROP INDEX `VAL2`, DROP INDEX `VAL3`;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv &
# job9pid=$!
# wait $job1pid
# wait $job2pid
# wait $job3pid
# wait $job4pid
# wait $job5pid
# wait $job6pid
# wait $job7pid
# wait $job8pid
# wait $job9pid

# Split into multiple lines for easier resuming if one fails.
mysqldump -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword libgen_new libgenli_elem_descr         | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} --default-character-set=utf8mb4 -u root -ppassword allthethings &
job1pid=$!
mysqldump -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword libgen_new libgenli_files              | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} --default-character-set=utf8mb4 -u root -ppassword allthethings &
job2pid=$!
mysqldump -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword libgen_new libgenli_editions           | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} --default-character-set=utf8mb4 -u root -ppassword allthethings &
job3pid=$!
mysqldump -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword libgen_new libgenli_editions_to_files  | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} --default-character-set=utf8mb4 -u root -ppassword allthethings &
job4pid=$!
mysqldump -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword libgen_new libgenli_editions_add_descr | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} --default-character-set=utf8mb4 -u root -ppassword allthethings &
job5pid=$!
mysqldump -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword libgen_new libgenli_files_add_descr    | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} --default-character-set=utf8mb4 -u root -ppassword allthethings &
job6pid=$!
mysqldump -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword libgen_new libgenli_series             | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} --default-character-set=utf8mb4 -u root -ppassword allthethings &
job7pid=$!
mysqldump -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword libgen_new libgenli_series_add_descr   | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} --default-character-set=utf8mb4 -u root -ppassword allthethings &
job8pid=$!
mysqldump -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword libgen_new libgenli_publishers         | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} --default-character-set=utf8mb4 -u root -ppassword allthethings &
job9pid=$!
wait $job1pid
wait $job2pid
wait $job3pid
wait $job4pid
wait $job5pid
wait $job6pid
wait $job7pid
wait $job8pid
wait $job9pid

echo 'DROP DATABASE libgen_new;' | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword --show-warnings -vv
