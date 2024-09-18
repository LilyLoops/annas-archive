import os
import orjson
import re
import isbnlib
import collections
import tqdm
import concurrent
import multiprocessing
import elasticsearch.helpers
import time
import pathlib
import traceback
import flask_mail
import click
import pymysql.cursors
import more_itertools
import indexed_zstd
import hashlib
import zstandard

import allthethings.utils

from flask import Blueprint
from allthethings.extensions import engine, mariadb_url_no_timeout, Reflected, mail, mariapersist_url
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from pymysql.constants import CLIENT
from config.settings import SLOW_DATA_IMPORTS

from allthethings.page.views import get_aarecords_mysql, get_isbndb_dicts

cli = Blueprint("cli", __name__, template_folder="templates")

#################################################################################################
# ./run flask cli dbreset
@cli.cli.command('dbreset')
def dbreset():
    print("Erasing entire database (2 MariaDB databases servers + 1 ElasticSearch)! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    mariapersist_reset_internal()
    nonpersistent_dbreset_internal()
    done_message()

def done_message():
    print("Done!")
    print("Search for example for 'Rhythms of the brain': http://localtest.me:8000/search?q=Rhythms+of+the+brain")
    print("To test SciDB: http://localtest.me:8000/scidb/10.5822/978-1-61091-843-5_15")
    print("See mariadb_dump.sql for various other records you can look at.")

#################################################################################################
# ./run flask cli nonpersistent_dbreset
@cli.cli.command('nonpersistent_dbreset')
def nonpersistent_dbreset():
    print("Erasing nonpersistent databases (1 MariaDB databases servers + 1 ElasticSearch)! Did you double-check that any production/large databases are offline/inaccessible from here?")
    nonpersistent_dbreset_internal()
    done_message()


def nonpersistent_dbreset_internal():
    # Per https://stackoverflow.com/a/4060259
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    engine_multi = create_engine(mariadb_url_no_timeout, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = engine_multi.raw_connection().cursor()

    # Generated with `docker compose exec mariadb mysqldump -u allthethings -ppassword --opt --where="1 limit 100" --skip-comments --ignore-table=computed_all_md5s allthethings > mariadb_dump.sql`
    mariadb_dump = pathlib.Path(os.path.join(__location__, 'mariadb_dump.sql')).read_text()
    for sql in mariadb_dump.split('# DELIMITER FOR cli/views.py'):
        cursor.execute(sql)

    openlib_final_sql = pathlib.Path(os.path.join(__location__, '../../data-imports/scripts/helpers/openlib_final.sql')).read_text()
    for sql in openlib_final_sql.split('# DELIMITER FOR cli/views.py'):
        cursor.execute(sql.replace('delimiter //', '').replace('delimiter ;', '').replace('END //', 'END'))

    torrents_json = pathlib.Path(os.path.join(__location__, 'torrents.json')).read_text()
    cursor.execute('DROP TABLE IF EXISTS torrents_json; CREATE TABLE torrents_json (json JSON NOT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin; INSERT INTO torrents_json (json) VALUES (%(json)s); COMMIT', {'json': torrents_json})
    cursor.close()

    mysql_reset_aac_tables_internal()
    mysql_build_aac_tables_internal()

    mysql_build_computed_all_md5s_internal()

    time.sleep(1)
    Reflected.prepare(engine_multi)
    elastic_reset_aarecords_internal()
    elastic_build_aarecords_all_internal()
    mysql_build_aarecords_codes_numbers_internal()

def query_yield_batches(conn, qry, pk_attr, maxrq):
    """specialized windowed query generator (using LIMIT/OFFSET)

    This recipe is to select through a large number of rows thats too
    large to fetch at once. The technique depends on the primary key
    of the FROM clause being an integer value, and selects items
    using LIMIT."""

    firstid = None
    while True:
        q = qry
        if firstid is not None:
            q = qry.where(pk_attr > firstid)
        batch = conn.execute(q.order_by(pk_attr).limit(maxrq)).all()
        if len(batch) == 0:
            break
        yield batch
        firstid = batch[-1][0]

#################################################################################################
# Reset "annas_archive_meta_*" tables so they are built from scratch.
# ./run flask cli mysql_reset_aac_tables
#
# To dump computed_all_md5s to txt: 
#   docker exec mariadb mariadb -uallthethings -ppassword allthethings --skip-column-names -e 'SELECT LOWER(HEX(md5)) from computed_all_md5s;' > md5.txt
@cli.cli.command('mysql_reset_aac_tables')
def mysql_reset_aac_tables():
    mysql_reset_aac_tables_internal()

def mysql_reset_aac_tables_internal():
    print("Resetting aac tables...")
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('DROP TABLE IF EXISTS annas_archive_meta_aac_filenames')
    print("Done!")

#################################################################################################
# Rebuild "annas_archive_meta_*" tables, if they have changed.
# ./run flask cli mysql_build_aac_tables
@cli.cli.command('mysql_build_aac_tables')
def mysql_build_aac_tables():
    mysql_build_aac_tables_internal()

def mysql_build_aac_tables_internal():
    print("Building aac tables...")
    file_data_files_by_collection = collections.defaultdict(list)

    COLLECTIONS_WITH_MULTIPLE_MD5 = ['magzdb_records', 'nexusstc_records']

    for filename in os.listdir(allthethings.utils.aac_path_prefix()):
        if not (filename.startswith('annas_archive_meta__aacid__') and filename.endswith('.jsonl.seekable.zst')):
            continue
        # if 'worldcat' in filename:
        #     continue
        collection = filename.split('__')[2]
        file_data_files_by_collection[collection].append(filename)

    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('CREATE TABLE IF NOT EXISTS annas_archive_meta_aac_filenames (`collection` VARCHAR(250) NOT NULL, `filename` VARCHAR(250) NOT NULL, PRIMARY KEY (`collection`)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
        cursor.execute('SELECT * FROM annas_archive_meta_aac_filenames')
        existing_filenames_by_collection = { row['collection']: row['filename'] for row in cursor.fetchall() }

        collections_need_indexing = {}
        for collection, filenames in file_data_files_by_collection.items():
            filenames.sort()
            previous_filename = existing_filenames_by_collection.get(collection) or ''
            collection_needs_indexing = filenames[-1] != previous_filename
            if collection_needs_indexing:
                collections_need_indexing[collection] = filenames[-1]
            print(f"{collection:20}   files found: {len(filenames):02}    latest: {filenames[-1].split('__')[3].split('.')[0]}    {'previous filename: ' + previous_filename if collection_needs_indexing else '(no change)'}")

        for collection, filename in collections_need_indexing.items():
            print(f"[{collection}] Starting indexing...")

            extra_index_fields = {}
            if collection == 'duxiu_records':
                extra_index_fields['filename_decoded_basename'] = 'VARCHAR(250) NULL'

            def build_insert_data(line, byte_offset):
                # Parse "canonical AAC" more efficiently than parsing all the JSON
                matches = re.match(rb'\{"aacid":"([^"]+)",("data_folder":"([^"]+)",)?"metadata":\{"[^"]+":([^,]+),("md5":"([^"]+)")?', line)
                if matches is None:
                    raise Exception(f"Line is not in canonical AAC format: '{line}'")
                aacid = matches[1]
                # data_folder = matches[3]
                primary_id = matches[4].replace(b'"', b'')

                if collection == 'worldcat':
                    if (b'not_found_title_json' in line) or (b'redirect_title_json' in line):
                        return None
                elif collection == 'nexusstc_records':
                    if b'"type":["wiki"]' in line:
                        return None
                    if line.startswith(b'{"aacid":"aacid__nexusstc_records__20240516T181305Z__78xFBbXdi1dSBZxyoVNAdn","metadata":{"nexus_id":"6etg0wq0q8nsoufh9gtj4n9s5","record":{"abstract":[],"authors":[{"family":"Fu","given":"Ke-Ang","sequence":"first"},{"family":"Wang","given":"Jiangfeng","sequence":"additional"}],"ctr":[0.1],"custom_score":[1.0],"embeddings":[],"id":[{"dois":["10.1080/03610926.2022.2027451"],"nexus_id":"6etg0wq0q8nsoufh9gtj4n9s5"}],"issued_at":[1642982400],"languages":["en"],"links":[],"metadata":[{"container_title":"Communications in Statistics - Theory and Methods","first_page":6266,"issns":["0361-0926","1532-415X"],"issue":"17","last_page":6274,"publisher":"Informa UK Limited","volume":"52"}],"navigational_facets":[],"page_rank":[0.15],"reference_texts":[],"referenced_by_count":[0],"references":[{"doi":"10.1080/03461230802700897","type":"reference"},{"doi":"10.1239/jap/1238592120","type":"reference"},{"doi":"10.1016/j.insmatheco.2012.06.010","type":"reference"},{"doi":"10.1016/j.insmatheco.2020.12.003","type":"reference"},{"doi":"10.1007/s11009-019-09722-8","type":"reference"},{"doi":"10.1016/0304-4149(94)90113-9","type":"reference"},{"doi":"10.1016/j.insmatheco.2008.08.009","type":"reference"},{"doi":"10.1080/03610926.2015.1060338","type":"reference"},{"doi":"10.3150/17-bej948","type":"reference"},{"doi":"10.1093/biomet/58.1.83"("type":"reference"},{"doi":"10.1239/aap/1293113154","type":"reference"},{"doi":"10.1016/j.spl.2020.108857","type":"reference"},{"doi":"10.1007/s11424-019-8159-3","type":"reference"},{"doi":"10.1007/s11425-010-4012-9","type":"reference"},{"doi":"10.1007/s10114-017-6433-7","type":"reference"},{"doi":"10.1016/j.spl.2011.08.024","type":"reference"},{"doi":"10.1007/s11009-008-9110-6","type":"reference"},{"doi":"10.1016/j.insmatheco.2020.12.005","type":"reference"},{"doi":"10.1016/j.spa.2003.07.001","type":"reference"},{"doi":"10.1016/j.insmatheco.2013.08.008","type":"reference"}],"signature":[],"tags":["Statistics and Probability"],"title":["Moderate deviations for a Hawkes-type risk model with arbitrary dependence between claim sizes and waiting times"],"type":["journal-article"],"updated_at":[1715883185]}}}'):
                        # Bad record
                        return None
                elif collection == 'ebscohost_records':
                    ebscohost_matches = re.search(rb'"plink":"https://search\.ebscohost\.com/login\.aspx\?direct=true\\u0026db=edsebk\\u0026AN=([0-9]+)\\u0026site=ehost-live"', line)
                    if ebscohost_matches is None:
                        raise Exception(f"Incorrect ebscohost line: '{line}'")
                    primary_id = ebscohost_matches[1]

                md5 = matches[6]
                if ('duxiu_files' in collection and b'"original_md5"' in line):
                    # For duxiu_files, md5 is the primary id, so we stick original_md5 in the md5 column so we can query that as well.
                    original_md5_matches = re.search(rb'"original_md5":"([^"]+)"', line)
                    if original_md5_matches is None:
                        raise Exception(f"'original_md5' found, but not in an expected format! '{line}'")
                    md5 = original_md5_matches[1]
                elif md5 is None:
                    if b'"md5_reported"' in line:
                        md5_reported_matches = re.search(rb'"md5_reported":"([^"]+)"', line)
                        if md5_reported_matches is None:
                            raise Exception(f"'md5_reported' found, but not in an expected format! '{line}'")
                        md5 = md5_reported_matches[1]
                if (md5 is not None) and (not bool(re.match(rb"^[a-f\d]{32}$", md5))):
                    # Remove if it's not md5.
                    md5 = None

                multiple_md5s = []
                if collection in COLLECTIONS_WITH_MULTIPLE_MD5:
                    multiple_md5s = [md5 for md5 in set([md5.decode().lower() for md5 in re.findall(rb'"md5":"([^"]+)"', line)]) if allthethings.utils.validate_canonical_md5s([md5])]

                return_data = { 
                    'aacid': aacid.decode(), 
                    'primary_id': primary_id.decode(), 
                    'md5': md5.decode().lower() if md5 is not None else None,
                    'multiple_md5s': multiple_md5s,
                    'byte_offset': byte_offset,
                    'byte_length': len(line),
                }

                if collection == 'duxiu_records':
                    return_data['filename_decoded_basename'] = None
                    if b'"filename_decoded"' in line:
                        json = orjson.loads(line)
                        filename_decoded = json['metadata']['record']['filename_decoded']
                        return_data['filename_decoded_basename'] = filename_decoded.rsplit('.', 1)[0]
                return return_data

            CHUNK_SIZE = 100000

            filepath = f'{allthethings.utils.aac_path_prefix()}{filename}'
            table_name = f'annas_archive_meta__aacid__{collection}'
            print(f"[{collection}] Reading from {filepath} to {table_name}")

            filepath_decompressed = filepath.replace('.seekable.zst', '')
            file = None
            uncompressed_size = None
            if os.path.exists(filepath_decompressed):
                print(f"[{collection}] Found decompressed version, using that for performance: {filepath_decompressed}")
                print("Note that using the compressed version for linear operations is sometimes faster than running into drive read limits (even with NVMe), so be sure to performance-test this on your machine if the files are large, and commenting out these lines if necessary.")
                file = open(filepath_decompressed, 'rb')
                uncompressed_size = os.path.getsize(filepath_decompressed)
            else:
                file = indexed_zstd.IndexedZstdFile(filepath)
                uncompressed_size = file.size()
            print(f"[{collection}] {uncompressed_size=}")

            table_extra_fields = ''.join([f', {index_name} {index_type}' for index_name, index_type in extra_index_fields.items()])
            table_extra_index = ''.join([f', INDEX({index_name})' for index_name, index_type in extra_index_fields.items()])
            insert_extra_names = ''.join([f', {index_name}' for index_name, index_type in extra_index_fields.items()])
            insert_extra_values = ''.join([f', %({index_name})s' for index_name, index_type in extra_index_fields.items()])

            tables = []

            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.execute(f"CREATE TABLE {table_name} (`aacid` VARCHAR(250) CHARACTER SET ascii NOT NULL, `primary_id` VARCHAR(250) NULL, `md5` CHAR(32) CHARACTER SET ascii NULL, `byte_offset` BIGINT NOT NULL, `byte_length` BIGINT NOT NULL {table_extra_fields}, PRIMARY KEY (`aacid`), INDEX `primary_id` (`primary_id`), INDEX `md5` (`md5`) {table_extra_index}) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
            tables.append(table_name)

            if collection in COLLECTIONS_WITH_MULTIPLE_MD5:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}__multiple_md5")
                cursor.execute(f"CREATE TABLE {table_name}__multiple_md5 (`md5` CHAR(32) CHARACTER SET ascii NOT NULL, `aacid` VARCHAR(250) CHARACTER SET ascii NOT NULL, PRIMARY KEY (`md5`, `aacid`), INDEX `aacid_md5` (`aacid`, `md5`)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
                tables.append(f"{table_name}__multiple_md5")

            cursor.execute(f"LOCK TABLES {' WRITE, '.join(tables)} WRITE")
            # From https://github.com/indygreg/python-zstandard/issues/13#issuecomment-1544313739
            with tqdm.tqdm(total=uncompressed_size, bar_format='{l_bar}{bar}{r_bar} {eta}', unit='B', unit_scale=True) as pbar:
                byte_offset = 0
                for lines in more_itertools.ichunked(file, CHUNK_SIZE):
                    bytes_in_batch = 0
                    insert_data = []
                    insert_data_multiple_md5s = []
                    for line in lines:
                        allthethings.utils.aac_spot_check_line_bytes(line, {})
                        insert_data_line = build_insert_data(line, byte_offset)
                        if insert_data_line is not None:
                            for md5 in insert_data_line['multiple_md5s']:
                                insert_data_multiple_md5s.append({ "md5": md5, "aacid": insert_data_line['aacid'] })
                            del insert_data_line['multiple_md5s']
                            insert_data.append(insert_data_line)
                        line_len = len(line)
                        byte_offset += line_len
                        bytes_in_batch += line_len
                    action = 'INSERT'
                    if collection == 'duxiu_records':
                        # This collection inadvertently has a bunch of exact duplicate lines.
                        action = 'REPLACE'
                    if len(insert_data) > 0:
                        connection.connection.ping(reconnect=True)
                        cursor.executemany(f'{action} INTO {table_name} (aacid, primary_id, md5, byte_offset, byte_length {insert_extra_names}) VALUES (%(aacid)s, %(primary_id)s, %(md5)s, %(byte_offset)s, %(byte_length)s {insert_extra_values})', insert_data)
                    if len(insert_data_multiple_md5s) > 0:
                        connection.connection.ping(reconnect=True)
                        cursor.executemany(f'{action} INTO {table_name}__multiple_md5 (md5, aacid) VALUES (%(md5)s, %(aacid)s)', insert_data_multiple_md5s)
                    pbar.update(bytes_in_batch)
            connection.connection.ping(reconnect=True)
            cursor.execute("UNLOCK TABLES")
            cursor.execute("REPLACE INTO annas_archive_meta_aac_filenames (collection, filename) VALUES (%(collection)s, %(filename)s)", { "collection": collection, "filename": filepath.rsplit('/', 1)[-1] })
            cursor.execute("COMMIT")
            print(f"[{collection}] Done!")


#################################################################################################
# Rebuild "computed_all_md5s" table in MySQL. At the time of writing, this isn't
# used in the app, but it is used for `./run flask cli elastic_build_aarecords_main`.
# ./run flask cli mysql_build_computed_all_md5s
#
# To dump computed_all_md5s to txt: 
#   docker exec mariadb mariadb -uallthethings -ppassword allthethings --skip-column-names -e 'SELECT LOWER(HEX(md5)) from computed_all_md5s;' > md5.txt
@cli.cli.command('mysql_build_computed_all_md5s')
def mysql_build_computed_all_md5s():
    print("Erasing entire MySQL 'computed_all_md5s' table! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    mysql_build_computed_all_md5s_internal()

def mysql_build_computed_all_md5s_internal():
    engine_multi = create_engine(mariadb_url_no_timeout, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = engine_multi.raw_connection().cursor()
    print("Removing table computed_all_md5s (if exists)")
    cursor.execute('DROP TABLE IF EXISTS computed_all_md5s')
    print("Load indexes of libgenli_files")
    cursor.execute('LOAD INDEX INTO CACHE libgenli_files')
    print("Creating table computed_all_md5s and load with libgenli_files")
    # NOTE: first_source is currently purely for debugging!
    cursor.execute('CREATE TABLE computed_all_md5s (md5 BINARY(16) NOT NULL, first_source TINYINT NOT NULL, PRIMARY KEY (md5)) ENGINE=MyISAM ROW_FORMAT=FIXED SELECT UNHEX(md5) AS md5, 1 AS first_source FROM libgenli_files WHERE md5 IS NOT NULL')
    print("Load indexes of computed_all_md5s")
    cursor.execute('LOAD INDEX INTO CACHE computed_all_md5s')
    print("Load indexes of zlib_book")
    cursor.execute('LOAD INDEX INTO CACHE zlib_book')
    print("Inserting from 'zlib_book' (md5_reported)")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5_reported), 2 FROM zlib_book WHERE md5_reported != "" AND md5_reported IS NOT NULL')
    print("Inserting from 'zlib_book' (md5)")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5), 3 FROM zlib_book WHERE zlib_book.md5 != "" AND md5 IS NOT NULL')
    print("Load indexes of libgenrs_fiction")
    cursor.execute('LOAD INDEX INTO CACHE libgenrs_fiction')
    print("Inserting from 'libgenrs_fiction'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5), 4 FROM libgenrs_fiction WHERE md5 IS NOT NULL')
    print("Load indexes of libgenrs_updated")
    cursor.execute('LOAD INDEX INTO CACHE libgenrs_updated')
    print("Inserting from 'libgenrs_updated'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5), 5 FROM libgenrs_updated WHERE md5 IS NOT NULL')
    print("Load indexes of aa_ia_2023_06_files and aa_ia_2023_06_metadata")
    cursor.execute('LOAD INDEX INTO CACHE aa_ia_2023_06_files, aa_ia_2023_06_metadata')
    print("Inserting from 'aa_ia_2023_06_files'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5), 6 FROM aa_ia_2023_06_metadata USE INDEX (libgen_md5) JOIN aa_ia_2023_06_files USING (ia_id) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NULL')
    print("Load indexes of annas_archive_meta__aacid__ia2_acsmpdf_files and aa_ia_2023_06_metadata")
    cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__ia2_acsmpdf_files, aa_ia_2023_06_metadata')
    print("Inserting from 'annas_archive_meta__aacid__ia2_acsmpdf_files'")
    # Note: annas_archive_meta__aacid__ia2_records / files are all after 2023, so no need to filter out the old libgen ones!
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(annas_archive_meta__aacid__ia2_acsmpdf_files.md5), 7 FROM aa_ia_2023_06_metadata USE INDEX (libgen_md5) JOIN annas_archive_meta__aacid__ia2_acsmpdf_files ON (ia_id=primary_id) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NULL')
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(annas_archive_meta__aacid__ia2_acsmpdf_files.md5), 8 FROM annas_archive_meta__aacid__ia2_records JOIN annas_archive_meta__aacid__ia2_acsmpdf_files USING (primary_id)')
    print("Load indexes of annas_archive_meta__aacid__zlib3_records")
    cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__zlib3_records')
    print("Inserting from 'annas_archive_meta__aacid__zlib3_records'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5), 9 FROM annas_archive_meta__aacid__zlib3_records WHERE md5 IS NOT NULL')
    # We currently don't support loading a zlib3_file without a corresponding zlib3_record. Should we ever?
    # print("Load indexes of annas_archive_meta__aacid__zlib3_files")
    # cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__zlib3_files')
    # print("Inserting from 'annas_archive_meta__aacid__zlib3_files'")
    # cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5), 10 FROM annas_archive_meta__aacid__zlib3_files WHERE md5 IS NOT NULL')
    print("Load indexes of annas_archive_meta__aacid__duxiu_files")
    cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__duxiu_files')
    print("Inserting from 'annas_archive_meta__aacid__duxiu_files'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(primary_id), 11 FROM annas_archive_meta__aacid__duxiu_files WHERE primary_id IS NOT NULL')
    print("Load indexes of annas_archive_meta__aacid__upload_records and annas_archive_meta__aacid__upload_files")
    cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__upload_records, annas_archive_meta__aacid__upload_files')
    print("Inserting from 'annas_archive_meta__aacid__upload_files'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(annas_archive_meta__aacid__upload_files.primary_id), 12 FROM annas_archive_meta__aacid__upload_files JOIN annas_archive_meta__aacid__upload_records ON (annas_archive_meta__aacid__upload_records.md5 = annas_archive_meta__aacid__upload_files.primary_id) WHERE annas_archive_meta__aacid__upload_files.primary_id IS NOT NULL')
    print("Load indexes of annas_archive_meta__aacid__upload_records and annas_archive_meta__aacid__magzdb_records__multiple_md5")
    cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__upload_records, annas_archive_meta__aacid__magzdb_records__multiple_md5')
    print("Inserting from 'annas_archive_meta__aacid__magzdb_records__multiple_md5'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5), 13 FROM annas_archive_meta__aacid__magzdb_records__multiple_md5 WHERE UNHEX(md5) IS NOT NULL')
    print("Load indexes of annas_archive_meta__aacid__upload_records and annas_archive_meta__aacid__nexusstc_records__multiple_md5")
    cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__upload_records, annas_archive_meta__aacid__nexusstc_records__multiple_md5')
    print("Inserting from 'annas_archive_meta__aacid__nexusstc_records__multiple_md5'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5), 14 FROM annas_archive_meta__aacid__nexusstc_records__multiple_md5 WHERE UNHEX(md5) IS NOT NULL')
    cursor.close()
    print("Done mysql_build_computed_all_md5s_internal!")
    # engine_multi = create_engine(mariadb_url_no_timeout, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    # cursor = engine_multi.raw_connection().cursor()
    # print("Removing table computed_all_md5s (if exists)")
    # cursor.execute('DROP TABLE IF EXISTS computed_all_md5s')
    # print("Load indexes of libgenli_files")
    # cursor.execute('LOAD INDEX INTO CACHE libgenli_files')
    # # print("Creating table computed_all_md5s and load with libgenli_files")
    # # cursor.execute('CREATE TABLE computed_all_md5s (md5 CHAR(32) NOT NULL, PRIMARY KEY (md5)) ENGINE=MyISAM DEFAULT CHARSET=ascii COLLATE ascii_bin ROW_FORMAT=FIXED SELECT md5 FROM libgenli_files')

    # # print("Load indexes of computed_all_md5s")
    # # cursor.execute('LOAD INDEX INTO CACHE computed_all_md5s')
    # print("Load indexes of zlib_book")
    # cursor.execute('LOAD INDEX INTO CACHE zlib_book')
    # # print("Inserting from 'zlib_book' (md5_reported)")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT md5_reported FROM zlib_book LEFT JOIN computed_all_md5s ON (computed_all_md5s.md5 = zlib_book.md5_reported) WHERE md5_reported != "" AND computed_all_md5s.md5 IS NULL')
    # # print("Inserting from 'zlib_book' (md5)")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT md5 FROM zlib_book LEFT JOIN computed_all_md5s USING (md5) WHERE zlib_book.md5 != "" AND computed_all_md5s.md5 IS NULL')
    # print("Load indexes of libgenrs_fiction")
    # cursor.execute('LOAD INDEX INTO CACHE libgenrs_fiction')
    # # print("Inserting from 'libgenrs_fiction'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT LOWER(libgenrs_fiction.MD5) FROM libgenrs_fiction LEFT JOIN computed_all_md5s ON (computed_all_md5s.md5 = LOWER(libgenrs_fiction.MD5)) WHERE computed_all_md5s.md5 IS NULL')
    # print("Load indexes of libgenrs_updated")
    # cursor.execute('LOAD INDEX INTO CACHE libgenrs_updated')
    # # print("Inserting from 'libgenrs_updated'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT MD5 FROM libgenrs_updated LEFT JOIN computed_all_md5s USING (md5) WHERE computed_all_md5s.md5 IS NULL')
    # print("Load indexes of aa_ia_2023_06_files")
    # cursor.execute('LOAD INDEX INTO CACHE aa_ia_2023_06_files')
    # # print("Inserting from 'aa_ia_2023_06_files'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT MD5 FROM aa_ia_2023_06_files LEFT JOIN aa_ia_2023_06_metadata USING (ia_id) LEFT JOIN computed_all_md5s USING (md5) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NULL AND computed_all_md5s.md5 IS NULL')
    # print("Load indexes of annas_archive_meta__aacid__zlib3_records")
    # cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__zlib3_records')
    # # print("Inserting from 'annas_archive_meta__aacid__zlib3_records'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT md5 FROM annas_archive_meta__aacid__zlib3_records LEFT JOIN computed_all_md5s USING (md5) WHERE md5 IS NOT NULL AND computed_all_md5s.md5 IS NULL')
    # print("Load indexes of annas_archive_meta__aacid__zlib3_files")
    # cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__zlib3_files')
    # # print("Inserting from 'annas_archive_meta__aacid__zlib3_files'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT md5 FROM annas_archive_meta__aacid__zlib3_files LEFT JOIN computed_all_md5s USING (md5) WHERE md5 IS NOT NULL AND computed_all_md5s.md5 IS NULL')
    # print("Creating table computed_all_md5s")
    # cursor.execute('CREATE TABLE computed_all_md5s (md5 CHAR(32) NOT NULL, PRIMARY KEY (md5)) ENGINE=MyISAM DEFAULT CHARSET=ascii COLLATE ascii_bin ROW_FORMAT=FIXED IGNORE SELECT DISTINCT md5 AS md5 FROM libgenli_files UNION DISTINCT (SELECT DISTINCT md5_reported AS md5 FROM zlib_book WHERE md5_reported != "") UNION DISTINCT (SELECT DISTINCT md5 AS md5 FROM zlib_book WHERE md5 != "") UNION DISTINCT (SELECT DISTINCT LOWER(libgenrs_fiction.MD5) AS md5 FROM libgenrs_fiction) UNION DISTINCT (SELECT DISTINCT MD5 AS md5 FROM libgenrs_updated) UNION DISTINCT (SELECT DISTINCT md5 AS md5 FROM aa_ia_2023_06_files LEFT JOIN aa_ia_2023_06_metadata USING (ia_id) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NULL) UNION DISTINCT (SELECT DISTINCT md5 AS md5 FROM annas_archive_meta__aacid__zlib3_records WHERE md5 IS NOT NULL) UNION DISTINCT (SELECT DISTINCT md5 AS md5 FROM annas_archive_meta__aacid__zlib3_files WHERE md5 IS NOT NULL)')
    # cursor.close()

es_create_index_body = {
    "mappings": {
        "dynamic": False,
        "properties": {
            "search_only_fields": {
                "properties": {
                    "search_filesize": { "type": "long", "index": False, "doc_values": True },
                    "search_year": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                    "search_extension": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                    "search_content_type": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                    "search_most_likely_language_code": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                    "search_isbn13": { "type": "keyword", "index": True, "doc_values": True },
                    "search_doi": { "type": "keyword", "index": True, "doc_values": True },
                    "search_title": { "type": "text", "index": True, "index_phrases": True, "analyzer": "custom_icu_analyzer" },
                    "search_author": { "type": "text", "index": True, "index_phrases": True, "analyzer": "custom_icu_analyzer" },
                    "search_publisher": { "type": "text", "index": True, "index_phrases": True, "analyzer": "custom_icu_analyzer" },
                    "search_edition_varia": { "type": "text", "index": True, "index_phrases": True, "analyzer": "custom_icu_analyzer" },
                    "search_original_filename": { "type": "text", "index": True, "index_phrases": True, "analyzer": "custom_icu_analyzer" },
                    "search_description_comments": { "type": "text", "index": True, "index_phrases": True, "analyzer": "custom_icu_analyzer" },
                    "search_text": { "type": "text", "index": True, "index_phrases": True, "analyzer": "custom_icu_analyzer" },
                    "search_score_base_rank": { "type": "rank_feature" },
                    "search_access_types": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                    "search_record_sources": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                    "search_bulk_torrents": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                    # ES limit https://github.com/langchain-ai/langchain/issues/10218#issuecomment-1706481539
                    # dot_product because embeddings are already normalized. We run on an old version of ES so we shouldn't rely on the
                    # default behavior of normalization.
                    # "search_text_embedding_3_small_100_tokens_1024_dims": {"type": "dense_vector", "dims": 1024, "index": True, "similarity": "cosine"},
                    "search_added_date": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                },
            },
        },
    },
    "settings": {
        "index": {
            "number_of_replicas": 0,
            "search.slowlog.threshold.query.warn": "4s",
            "store.preload": ["nvd", "dvd", "tim", "doc", "dim"],
            "codec": "best_compression",
            "analysis": {
                "analyzer": {
                    "custom_icu_analyzer": {
                        "tokenizer": "icu_tokenizer",
                        "char_filter": ["icu_normalizer"],
                        "filter": ["t2s", "icu_folding"],
                    },
                },
                "filter": { "t2s": { "type": "icu_transform", "id": "Traditional-Simplified" } },
            },
        },
    },
}

#################################################################################################
# Recreate "aarecords" index in ElasticSearch, without filling it with data yet.
# (That is done with `./run flask cli elastic_build_aarecords_*`)
# ./run flask cli elastic_reset_aarecords
@cli.cli.command('elastic_reset_aarecords')
def elastic_reset_aarecords():
    print("Erasing entire ElasticSearch 'aarecords' index! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    elastic_reset_aarecords_internal()

def elastic_reset_aarecords_internal():
    print("Deleting ES indices")
    for index_name, es_handle in allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING.items():
        es_handle.options(ignore_status=[400,404]).indices.delete(index=index_name) # Old
        for virtshard in range(0, 100): # Out of abundance, delete up to a large number
            es_handle.options(ignore_status=[400,404]).indices.delete(index=f'{index_name}__{virtshard}')
    print("Creating ES indices")
    for index_name, es_handle in allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING.items():
        for full_index_name in allthethings.utils.all_virtshards_for_index(index_name):
            es_handle.indices.create(wait_for_active_shards=1,index=full_index_name, body=es_create_index_body)

    print("Creating MySQL aarecords tables")
    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('DROP TABLE IF EXISTS aarecords_all') # Old
        cursor.execute('DROP TABLE IF EXISTS aarecords_isbn13') # Old
        cursor.execute(f'CREATE TABLE IF NOT EXISTS aarecords_codes (code VARBINARY({allthethings.utils.AARECORDS_CODES_CODE_LENGTH}) NOT NULL, aarecord_id VARBINARY({allthethings.utils.AARECORDS_CODES_AARECORD_ID_LENGTH}) NOT NULL, aarecord_id_prefix VARBINARY({allthethings.utils.AARECORDS_CODES_AARECORD_ID_PREFIX_LENGTH}) NOT NULL, row_number_order_by_code BIGINT NOT NULL, dense_rank_order_by_code BIGINT NOT NULL, row_number_partition_by_aarecord_id_prefix_order_by_code BIGINT NOT NULL, dense_rank_partition_by_aarecord_id_prefix_order_by_code BIGINT NOT NULL, PRIMARY KEY (code, aarecord_id), INDEX aarecord_id_prefix (aarecord_id_prefix)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
        cursor.execute(f'CREATE TABLE IF NOT EXISTS aarecords_codes_prefixes (code_prefix VARBINARY({allthethings.utils.AARECORDS_CODES_CODE_LENGTH}) NOT NULL, PRIMARY KEY (code_prefix)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
        # cursor.execute('CREATE TABLE IF NOT EXISTS model_cache_text_embedding_3_small_100_tokens (hashed_aarecord_id BINARY(16) NOT NULL, aarecord_id VARCHAR(1000) NOT NULL, embedding_text LONGTEXT, embedding LONGBLOB, PRIMARY KEY (hashed_aarecord_id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
        cursor.execute('COMMIT')

# These tables always need to be created new if they don't exist yet.
# They should only be used when doing a full refresh, but things will
# crash if they don't exist.
def new_tables_internal(codes_table_name):
    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        print(f"Creating fresh table {codes_table_name}")
        cursor.execute(f'DROP TABLE IF EXISTS {codes_table_name}')
        cursor.execute(f'CREATE TABLE {codes_table_name} (id BIGINT NOT NULL AUTO_INCREMENT, code VARBINARY({allthethings.utils.AARECORDS_CODES_CODE_LENGTH}) NOT NULL, aarecord_id VARBINARY({allthethings.utils.AARECORDS_CODES_AARECORD_ID_LENGTH}) NOT NULL, PRIMARY KEY (id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
        cursor.execute('COMMIT')

#################################################################################################
# ./run flask cli update_aarecords_index_mappings
@cli.cli.command('update_aarecords_index_mappings')
def update_aarecords_index_mappings():
    print("Updating ES indices")
    for index_name, es_handle in allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING.items():
        for full_index_name in allthethings.utils.all_virtshards_for_index(index_name):
            es_handle.indices.put_mapping(body=es_create_index_body['mappings'], index=full_index_name)
    print("Done!")

def elastic_build_aarecords_job_init_pool():
    global elastic_build_aarecords_job_app
    global elastic_build_aarecords_compressor
    print("Initializing pool worker (elastic_build_aarecords_job_init_pool)")
    from allthethings.app import create_app
    elastic_build_aarecords_job_app = create_app()

    # Per https://stackoverflow.com/a/4060259
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    elastic_build_aarecords_compressor = zstandard.ZstdCompressor(level=3, dict_data=zstandard.ZstdCompressionDict(pathlib.Path(os.path.join(__location__, 'aarecords_dump_for_dictionary.bin')).read_bytes()))

AARECORD_ID_PREFIX_TO_CODES_TABLE_NAME = {
    'edsebk': 'aarecords_codes_edsebk',
    'ia': 'aarecords_codes_ia',
    'isbn': 'aarecords_codes_isbndb',
    'ol': 'aarecords_codes_ol',
    'duxiu_ssid': 'aarecords_codes_duxiu',
    'cadal_ssno': 'aarecords_codes_duxiu',
    'oclc': 'aarecords_codes_oclc',
    'magzdb': 'aarecords_codes_magzdb',
    'nexusstc': 'aarecords_codes_nexusstc',
    'md5': 'aarecords_codes_main',
    'doi': 'aarecords_codes_main',
    'nexusstc_download': 'aarecords_codes_main',
}

def elastic_build_aarecords_job(aarecord_ids):
    global elastic_build_aarecords_job_app
    global elastic_build_aarecords_compressor

    with elastic_build_aarecords_job_app.app_context():
        try:
            aarecord_ids = list(aarecord_ids)
            # print(f"[{os.getpid()}] elastic_build_aarecords_job start {len(aarecord_ids)}")
            with Session(engine) as session:
                operations_by_es_handle = collections.defaultdict(list)
                session.connection().connection.ping(reconnect=True)
                cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
                cursor.execute('SELECT 1')
                list(cursor.fetchall())

                # Filter out records that are filtered in get_isbndb_dicts, because there are some bad records there.
                canonical_isbn13s = [aarecord_id[len('isbn:'):] for aarecord_id in aarecord_ids if aarecord_id.startswith('isbn:')]
                bad_isbn13_aarecord_ids = set([f"isbn:{isbndb_dict['ean13']}" for isbndb_dict in get_isbndb_dicts(session, canonical_isbn13s) if len(isbndb_dict['isbndb']) == 0])

                # Filter out "doi:" records that already have an md5. We don't need standalone records for those.
                dois_from_ids = [aarecord_id[4:].encode() for aarecord_id in aarecord_ids if aarecord_id.startswith('doi:')]
                doi_codes_with_md5 = set()
                if len(dois_from_ids) > 0:
                    session.connection().connection.ping(reconnect=True)
                    cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
                    cursor.execute('SELECT doi FROM temp_md5_with_doi_seen WHERE doi IN %(dois_from_ids)s', { "dois_from_ids": dois_from_ids })
                    doi_codes_with_md5 = set([f"doi:{row['doi'].decode(errors='replace')}" for row in cursor.fetchall()])

                aarecord_ids = [aarecord_id for aarecord_id in aarecord_ids if (aarecord_id not in bad_isbn13_aarecord_ids) and (aarecord_id not in doi_codes_with_md5) and (aarecord_id not in allthethings.utils.SEARCH_FILTERED_BAD_AARECORD_IDS)]
                if len(aarecord_ids) == 0:
                    return False

                # print(f"[{os.getpid()}] elastic_build_aarecords_job set up aa_records_all")
                aarecords = get_aarecords_mysql(session, aarecord_ids)
                # print(f"[{os.getpid()}] elastic_build_aarecords_job got aarecords {len(aarecords)}")
                aarecords_all_md5_insert_data = []
                isbn13_oclc_insert_data = []
                isbn13_edsebk_insert_data = []
                nexusstc_cid_only_insert_data = []
                temp_md5_with_doi_seen_insert_data = []
                aarecords_codes_insert_data_by_codes_table_name = collections.defaultdict(list)
                for aarecord in aarecords:
                    aarecord_id_split = aarecord['id'].split(':', 1)
                    hashed_aarecord_id = hashlib.md5(aarecord['id'].encode()).digest()
                    if aarecord_id_split[0] == 'md5':
                        # TODO: bring back for other records if necessary, but keep it possible to rerun
                        # only _main with recreating the table, and not needing INSERT .. ON DUPLICATE KEY UPDATE (deadlocks).
                        aarecords_all_md5_insert_data.append({
                            # 'hashed_aarecord_id': hashed_aarecord_id,
                            # 'aarecord_id': aarecord['id'],
                            'md5': bytes.fromhex(aarecord_id_split[1]) if aarecord['id'].startswith('md5:') else None,
                            'json_compressed': elastic_build_aarecords_compressor.compress(orjson.dumps({
                                # Note: used in external code.
                                'search_only_fields': {
                                    'search_access_types': aarecord['search_only_fields']['search_access_types'],
                                    'search_record_sources': aarecord['search_only_fields']['search_record_sources'],
                                    'search_bulk_torrents': aarecord['search_only_fields']['search_bulk_torrents'],
                                }
                            })),
                        })
                        for doi in aarecord['file_unified_data']['identifiers_unified'].get('doi') or []:
                            temp_md5_with_doi_seen_insert_data.append({ "doi": doi.encode() })
                    elif aarecord_id_split[0] == 'oclc':
                        isbn13s = aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []
                        if len(isbn13s) < 10: # Remove excessive lists.
                            for isbn13 in isbn13s:
                                isbn13_oclc_insert_data.append({
                                    'isbn13': isbn13,
                                    'oclc_id': int(aarecord_id_split[1]),
                                })
                    elif aarecord_id_split[0] == 'edsebk':
                        isbn13s = aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []
                        if len(isbn13s) < 10: # Remove excessive lists.
                            for isbn13 in isbn13s:
                                isbn13_edsebk_insert_data.append({
                                    'isbn13': isbn13,
                                    'edsebk_id': int(aarecord_id_split[1]),
                                })
                    elif aarecord_id_split[0] == 'nexusstc':
                        if len(aarecord['aac_nexusstc']['aa_nexusstc_derived']['cid_only_links']) > 0:
                            nexusstc_cid_only_insert_data.append({ "nexusstc_id": aarecord['aac_nexusstc']['id'] })

                    for index in aarecord['indexes']:
                        virtshard = allthethings.utils.virtshard_for_hashed_aarecord_id(hashed_aarecord_id)
                        operations_by_es_handle[allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[index]].append({ **aarecord, '_op_type': 'index', '_index': f'{index}__{virtshard}', '_id': aarecord['id'] })

                    codes = []
                    for code_name in aarecord['file_unified_data']['identifiers_unified'].keys():
                        for code_value in aarecord['file_unified_data']['identifiers_unified'][code_name]:
                            codes.append(f"{code_name}:{code_value}")
                    for code_name in aarecord['file_unified_data']['classifications_unified'].keys():
                        for code_value in aarecord['file_unified_data']['classifications_unified'][code_name]:
                            codes.append(f"{code_name}:{code_value}")
                    for code in codes:
                        codes_table_name = AARECORD_ID_PREFIX_TO_CODES_TABLE_NAME[aarecord_id_split[0]]
                        aarecords_codes_insert_data_by_codes_table_name[codes_table_name].append({ 'code': code.encode(), 'aarecord_id': aarecord['id'].encode() })

                # print(f"[{os.getpid()}] elastic_build_aarecords_job finished for loop")
                    
                try:
                    for es_handle, operations in operations_by_es_handle.items():
                        elasticsearch.helpers.bulk(es_handle, operations, request_timeout=30)
                except Exception as err:
                    if hasattr(err, 'errors'):
                        print(err.errors)
                    print(repr(err))
                    print("Got the above error; retrying..")
                    try:
                        for es_handle, operations in operations_by_es_handle.items():
                            elasticsearch.helpers.bulk(es_handle, operations, request_timeout=30)
                    except Exception as err:
                        if hasattr(err, 'errors'):
                            print(err.errors)
                        print(repr(err))
                        print("Got the above error; retrying one more time..")
                        for es_handle, operations in operations_by_es_handle.items():
                            elasticsearch.helpers.bulk(es_handle, operations, request_timeout=30)

                # print(f"[{os.getpid()}] elastic_build_aarecords_job inserted into ES")

                if len(aarecords_all_md5_insert_data) > 0:
                    session.connection().connection.ping(reconnect=True)
                    # Avoiding IGNORE / ON DUPLICATE KEY here because of locking.
                    # WARNING: when trying to optimize this (e.g. if you see this in SHOW PROCESSLIST) know that this is a bit of a bottleneck, but
                    # not a huge one. Commenting out all these inserts doesn't speed up the job by that much.
                    cursor.executemany('INSERT DELAYED INTO aarecords_all_md5 (md5, json_compressed) VALUES (%(md5)s, %(json_compressed)s)', aarecords_all_md5_insert_data)
                    cursor.execute('COMMIT')

                if len(isbn13_oclc_insert_data) > 0:
                    session.connection().connection.ping(reconnect=True)
                    # Avoiding IGNORE / ON DUPLICATE KEY here because of locking.
                    # WARNING: when trying to optimize this (e.g. if you see this in SHOW PROCESSLIST) know that this is a bit of a bottleneck, but
                    # not a huge one. Commenting out all these inserts doesn't speed up the job by that much.
                    cursor.executemany('INSERT DELAYED INTO isbn13_oclc (isbn13, oclc_id) VALUES (%(isbn13)s, %(oclc_id)s)', isbn13_oclc_insert_data)
                    cursor.execute('COMMIT')

                if len(isbn13_edsebk_insert_data) > 0:
                    session.connection().connection.ping(reconnect=True)
                    # Avoiding IGNORE / ON DUPLICATE KEY here because of locking.
                    # WARNING: when trying to optimize this (e.g. if you see this in SHOW PROCESSLIST) know that this is a bit of a bottleneck, but
                    # not a huge one. Commenting out all these inserts doesn't speed up the job by that much.
                    cursor.executemany('INSERT DELAYED INTO isbn13_edsebk (isbn13, edsebk_id) VALUES (%(isbn13)s, %(edsebk_id)s)', isbn13_edsebk_insert_data)
                    cursor.execute('COMMIT')

                if len(nexusstc_cid_only_insert_data) > 0:
                    session.connection().connection.ping(reconnect=True)
                    # Avoiding IGNORE / ON DUPLICATE KEY here because of locking.
                    # WARNING: when trying to optimize this (e.g. if you see this in SHOW PROCESSLIST) know that this is a bit of a bottleneck, but
                    # not a huge one. Commenting out all these inserts doesn't speed up the job by that much.
                    cursor.executemany('INSERT DELAYED INTO nexusstc_cid_only (nexusstc_id) VALUES (%(nexusstc_id)s)', nexusstc_cid_only_insert_data)
                    cursor.execute('COMMIT')

                if len(temp_md5_with_doi_seen_insert_data) > 0:
                    session.connection().connection.ping(reconnect=True)
                    # Avoiding IGNORE / ON DUPLICATE KEY here because of locking.
                    # WARNING: when trying to optimize this (e.g. if you see this in SHOW PROCESSLIST) know that this is a bit of a bottleneck, but
                    # not a huge one. Commenting out all these inserts doesn't speed up the job by that much.
                    cursor.executemany('INSERT DELAYED INTO temp_md5_with_doi_seen (doi) VALUES (%(doi)s)', temp_md5_with_doi_seen_insert_data)
                    cursor.execute('COMMIT')

                for codes_table_name, aarecords_codes_insert_data in aarecords_codes_insert_data_by_codes_table_name.items():
                    if len(aarecords_codes_insert_data) > 0:
                        for insert_item in aarecords_codes_insert_data:
                            if len(insert_item['code']) > allthethings.utils.AARECORDS_CODES_CODE_LENGTH:
                                raise Exception(f"Code length exceeds allthethings.utils.AARECORDS_CODES_CODE_LENGTH for {insert_item=}")
                            if len(insert_item['aarecord_id']) > allthethings.utils.AARECORDS_CODES_AARECORD_ID_LENGTH:
                                raise Exception(f"Code length exceeds allthethings.utils.AARECORDS_CODES_AARECORD_ID_LENGTH for {insert_item=}")

                        session.connection().connection.ping(reconnect=True)
                        # Avoiding IGNORE / ON DUPLICATE KEY here because of locking.
                        # WARNING: when trying to optimize this (e.g. if you see this in SHOW PROCESSLIST) know that this is a bit of a bottleneck, but
                        # not a huge one. Commenting out all these inserts doesn't speed up the job by that much.
                        cursor.executemany(f"INSERT DELAYED INTO {codes_table_name} (code, aarecord_id) VALUES (%(code)s, %(aarecord_id)s)", aarecords_codes_insert_data)
                        cursor.execute('COMMIT')

                # print(f"[{os.getpid()}] elastic_build_aarecords_job inserted into aarecords_all")
                # print(f"[{os.getpid()}] Processed {len(aarecords)} md5s")

                return False

        except Exception as err:
            print(repr(err))
            traceback.print_tb(err.__traceback__)
            return True

THREADS = 200
CHUNK_SIZE = 200
BATCH_SIZE = 100000

# Locally
if SLOW_DATA_IMPORTS:
    THREADS = 1
    CHUNK_SIZE = 10
    BATCH_SIZE = 1000

# Uncomment to do them one by one
# THREADS = 1
# CHUNK_SIZE = 1
# BATCH_SIZE = 1

#################################################################################################
# ./run flask cli elastic_build_aarecords_all
@cli.cli.command('elastic_build_aarecords_all')
def elastic_build_aarecords_all():
    elastic_build_aarecords_all_internal()

def elastic_build_aarecords_all_internal():
    elastic_build_aarecords_oclc_internal() # OCLC first since we use `isbn13_oclc` table in later steps.
    elastic_build_aarecords_edsebk_internal() # First since we use `isbn13_edsebk` table in later steps.
    elastic_build_aarecords_magzdb_internal()
    elastic_build_aarecords_nexusstc_internal() # Nexus before 'main' since we use `nexusstc_cid_only` table in 'main'.
    elastic_build_aarecords_ia_internal()
    elastic_build_aarecords_isbndb_internal()
    elastic_build_aarecords_ol_internal()
    elastic_build_aarecords_duxiu_internal()
    elastic_build_aarecords_main_internal()
    elastic_build_aarecords_forcemerge_internal()


#################################################################################################
# ./run flask cli elastic_build_aarecords_ia
@cli.cli.command('elastic_build_aarecords_ia')
def elastic_build_aarecords_ia():
    elastic_build_aarecords_ia_internal()

def elastic_build_aarecords_ia_internal():
    # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
    new_tables_internal('aarecords_codes_ia')

    before_first_ia_id = ''

    if len(before_first_ia_id) > 0:
        print(f'WARNING!!!!! before_first_ia_id is set to {before_first_ia_id}')
        print(f'WARNING!!!!! before_first_ia_id is set to {before_first_ia_id}')
        print(f'WARNING!!!!! before_first_ia_id is set to {before_first_ia_id}')

    with engine.connect() as connection:
        print("Processing from aa_ia_2023_06_metadata+annas_archive_meta__aacid__ia2_records")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)

        # Sanity check: we assume that in annas_archive_meta__aacid__ia2_records we have no libgen-imported records.
        print("Running sanity check on aa_ia_2023_06_metadata")
        cursor.execute('SELECT ia_id FROM aa_ia_2023_06_metadata JOIN annas_archive_meta__aacid__ia2_records ON (aa_ia_2023_06_metadata.ia_id = annas_archive_meta__aacid__ia2_records.primary_id) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NOT NULL LIMIT 500')
        sanity_check_result = list(cursor.fetchall())
        if len(sanity_check_result) > 0:
            raise Exception(f"Sanity check failed: libgen records found in annas_archive_meta__aacid__ia2_records {sanity_check_result=}")

        print("Generating table temp_ia_ids")
        cursor.execute('DROP TABLE IF EXISTS temp_ia_ids')
        cursor.execute('CREATE TABLE temp_ia_ids (ia_id VARCHAR(250) NOT NULL, PRIMARY KEY(ia_id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SELECT ia_id FROM (SELECT ia_id, libgen_md5 FROM aa_ia_2023_06_metadata UNION SELECT primary_id AS ia_id, NULL AS libgen_md5 FROM annas_archive_meta__aacid__ia2_records) combined LEFT JOIN aa_ia_2023_06_files USING (ia_id) LEFT JOIN annas_archive_meta__aacid__ia2_acsmpdf_files ON (combined.ia_id = annas_archive_meta__aacid__ia2_acsmpdf_files.primary_id) WHERE aa_ia_2023_06_files.md5 IS NULL AND annas_archive_meta__aacid__ia2_acsmpdf_files.md5 IS NULL AND combined.libgen_md5 IS NULL')

        cursor.execute('SELECT COUNT(ia_id) AS count FROM temp_ia_ids WHERE ia_id > %(from)s ORDER BY ia_id LIMIT 1', { "from": before_first_ia_id })
        total = cursor.fetchone()['count']
        current_ia_id = before_first_ia_id
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT ia_id FROM temp_ia_ids WHERE ia_id > %(from)s ORDER BY ia_id LIMIT %(limit)s', { "from": current_ia_id, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from aa_ia_2023_06_metadata+annas_archive_meta__aacid__ia2_records ( starting ia_id: {batch[0]['ia_id']} , ia_id: {batch[-1]['ia_id']} )...")
                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked([f"ia:{item['ia_id']}" for item in batch], CHUNK_SIZE))
                    pbar.update(len(batch))
                    current_ia_id = batch[-1]['ia_id']

        print("Removing table temp_ia_ids")
        cursor.execute('DROP TABLE IF EXISTS temp_ia_ids')
        print("Done with IA!")


#################################################################################################
# ./run flask cli elastic_build_aarecords_isbndb
@cli.cli.command('elastic_build_aarecords_isbndb')
def elastic_build_aarecords_isbndb():
    elastic_build_aarecords_isbndb_internal()

def elastic_build_aarecords_isbndb_internal():
    # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
    new_tables_internal('aarecords_codes_isbndb')

    before_first_isbn13 = ''

    if len(before_first_isbn13) > 0:
        print(f'WARNING!!!!! before_first_isbn13 is set to {before_first_isbn13}')
        print(f'WARNING!!!!! before_first_isbn13 is set to {before_first_isbn13}')
        print(f'WARNING!!!!! before_first_isbn13 is set to {before_first_isbn13}')

    with engine.connect() as connection:
        print("Processing from isbndb_isbns")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(isbn13) AS count FROM isbndb_isbns WHERE isbn13 > %(from)s ORDER BY isbn13 LIMIT 1', { "from": before_first_isbn13 })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                current_isbn13 = before_first_isbn13
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    # Note that with `isbn13 >` we might be skipping some, because isbn13 is not unique, but oh well..
                    cursor.execute('SELECT isbn13, isbn10 FROM isbndb_isbns WHERE isbn13 > %(from)s ORDER BY isbn13 LIMIT %(limit)s', { "from": current_isbn13, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from isbndb_isbns ( starting isbn13: {batch[0]['isbn13']} , ending isbn13: {batch[-1]['isbn13']} )...")
                    isbn13s = set()
                    for item in batch:
                        if item['isbn10'] != "0000000000":
                            isbn13s.add(f"isbn:{item['isbn13']}")
                            isbn13s.add(f"isbn:{isbnlib.ean13(item['isbn10'])}")
                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked(list(isbn13s), CHUNK_SIZE))
                    pbar.update(len(batch))
                    current_isbn13 = batch[-1]['isbn13']
        print("Done with ISBNdb!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_ol
@cli.cli.command('elastic_build_aarecords_ol')
def elastic_build_aarecords_ol():
    elastic_build_aarecords_ol_internal()

def elastic_build_aarecords_ol_internal():
    # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
    new_tables_internal('aarecords_codes_ol')

    before_first_ol_key = ''
    # before_first_ol_key = '/books/OL5624024M'
    with engine.connect() as connection:
        print("Processing from ol_base")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(ol_key) AS count FROM ol_base WHERE ol_key LIKE "/books/OL%%" AND ol_key > %(from)s ORDER BY ol_key LIMIT 1', { "from": before_first_ol_key })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                current_ol_key = before_first_ol_key
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT ol_key FROM ol_base WHERE ol_key LIKE "/books/OL%%" AND ol_key > %(from)s ORDER BY ol_key LIMIT %(limit)s', { "from": current_ol_key, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from ol_base ( starting ol_key: {batch[0]['ol_key']} , ending ol_key: {batch[-1]['ol_key']} )...")
                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked([f"ol:{item['ol_key'].replace('/books/','')}" for item in batch if allthethings.utils.validate_ol_editions([item['ol_key'].replace('/books/','')])], CHUNK_SIZE))
                    pbar.update(len(batch))
                    current_ol_key = batch[-1]['ol_key']
        print("Done with OpenLib!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_duxiu
@cli.cli.command('elastic_build_aarecords_duxiu')
def elastic_build_aarecords_duxiu():
    elastic_build_aarecords_duxiu_internal()

def elastic_build_aarecords_duxiu_internal():
    # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
    new_tables_internal('aarecords_codes_duxiu')

    before_first_primary_id = ''
    # before_first_primary_id = 'duxiu_ssid_10000431'
    with engine.connect() as connection:
        print("Processing from annas_archive_meta__aacid__duxiu_records")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(primary_id) AS count FROM annas_archive_meta__aacid__duxiu_records WHERE (primary_id LIKE "duxiu_ssid_%%" OR primary_id LIKE "cadal_ssno_%%") AND primary_id > %(from)s ORDER BY primary_id LIMIT 1', { "from": before_first_primary_id })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                current_primary_id = before_first_primary_id
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT primary_id, byte_offset, byte_length FROM annas_archive_meta__aacid__duxiu_records WHERE (primary_id LIKE "duxiu_ssid_%%" OR primary_id LIKE "cadal_ssno_%%") AND primary_id > %(from)s ORDER BY primary_id LIMIT %(limit)s', { "from": current_primary_id, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from annas_archive_meta__aacid__duxiu_records ( starting primary_id: {batch[0]['primary_id']} , ending primary_id: {batch[-1]['primary_id']} )...")

                    lines_bytes = allthethings.utils.get_lines_from_aac_file(cursor, 'duxiu_records', [(row['byte_offset'], row['byte_length']) for row in batch])

                    ids = []
                    for item_index, item in enumerate(batch):
                        line_bytes = lines_bytes[item_index]

                        if item['primary_id'] == 'duxiu_ssid_-1':
                            continue
                        if item['primary_id'].startswith('cadal_ssno_hj'):
                            # These are collections.
                            continue
                        # TODO: pull these things out into the table?
                        if b'dx_20240122__books' in line_bytes:
                            # Skip, because 512w_final_csv is the authority on these records, and has a bunch of records from dx_20240122__books deleted.
                            continue
                        if (b'dx_toc_db__dx_toc' in line_bytes) and (b'"toc_xml":null' in line_bytes):
                            # Skip empty TOC records.
                            continue
                        if b'dx_20240122__remote_files' in line_bytes:
                            # Skip for now because a lot of the DuXiu SSIDs are actual CADAL SSNOs, and stand-alone records from
                            # remote_files are not useful anyway since they lack metadata like title, author, etc.
                            continue
                        ids.append(item['primary_id'].replace('duxiu_ssid_','duxiu_ssid:').replace('cadal_ssno_','cadal_ssno:'))
                    # Deduping at this level leads to some duplicates at the edges, but thats okay because aarecord
                    # generation is idempotent.
                    ids = list(set(ids))

                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked(ids, CHUNK_SIZE))
                    pbar.update(len(batch))
                    current_primary_id = batch[-1]['primary_id']
        print("Done with annas_archive_meta__aacid__duxiu_records!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_oclc
@cli.cli.command('elastic_build_aarecords_oclc')
def elastic_build_aarecords_oclc():
    elastic_build_aarecords_oclc_internal()

def elastic_build_aarecords_oclc_internal():
    # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
    new_tables_internal('aarecords_codes_oclc')

    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('DROP TABLE IF EXISTS isbn13_oclc')
        cursor.execute('CREATE TABLE isbn13_oclc (isbn13 CHAR(13) NOT NULL, oclc_id BIGINT NOT NULL, PRIMARY KEY (isbn13, oclc_id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ROW_FORMAT=FIXED')

    before_first_primary_id = ''
    # before_first_primary_id = '123'
    oclc_done_already = 0 # To get a proper total count. A real query with primary_id>before_first_primary_id would take too long.
    # oclc_done_already = 456

    with engine.connect() as connection:
        print("Processing from annas_archive_meta__aacid__worldcat")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(*) AS count FROM annas_archive_meta__aacid__worldcat LIMIT 1')
        total = list(cursor.fetchall())[0]['count'] - oclc_done_already
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                current_primary_id = before_first_primary_id
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT primary_id, COUNT(*) AS count FROM annas_archive_meta__aacid__worldcat WHERE primary_id > %(from)s GROUP BY primary_id ORDER BY primary_id LIMIT %(limit)s', { "from": current_primary_id, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from annas_archive_meta__aacid__worldcat ( starting primary_id: {batch[0]['primary_id']} , ending primary_id: {batch[-1]['primary_id']} )...")
                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked([f"oclc:{row['primary_id']}" for row in batch], CHUNK_SIZE))
                    pbar.update(sum([row['count'] for row in batch]))
                    current_primary_id = batch[-1]['primary_id']
        print("Done with annas_archive_meta__aacid__worldcat!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_edsebk
@cli.cli.command('elastic_build_aarecords_edsebk')
def elastic_build_aarecords_edsebk():
    elastic_build_aarecords_edsebk_internal()

def elastic_build_aarecords_edsebk_internal():
    # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
    new_tables_internal('aarecords_codes_edsebk')

    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('DROP TABLE IF EXISTS isbn13_edsebk')
        cursor.execute('CREATE TABLE isbn13_edsebk (isbn13 CHAR(13) NOT NULL, edsebk_id BIGINT NOT NULL, PRIMARY KEY (isbn13, edsebk_id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ROW_FORMAT=FIXED')

    before_first_primary_id = ''
    # before_first_primary_id = '123'

    with engine.connect() as connection:
        print("Processing from annas_archive_meta__aacid__ebscohost_records")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(DISTINCT primary_id) AS count FROM annas_archive_meta__aacid__ebscohost_records WHERE primary_id > %(from)s ORDER BY primary_id LIMIT 1', { "from": before_first_primary_id })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                current_primary_id = before_first_primary_id
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT primary_id FROM annas_archive_meta__aacid__ebscohost_records WHERE primary_id > %(from)s ORDER BY primary_id LIMIT %(limit)s', { "from": current_primary_id, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from annas_archive_meta__aacid__ebscohost_records ( starting primary_id: {batch[0]['primary_id']} , ending primary_id: {batch[-1]['primary_id']} )...")
                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked([f"edsebk:{row['primary_id']}" for row in batch], CHUNK_SIZE))
                    pbar.update(len(batch))
                    current_primary_id = batch[-1]['primary_id']
        print(f"Done with annas_archive_meta__aacid__ebscohost_records!")


#################################################################################################
# ./run flask cli elastic_build_aarecords_magzdb
@cli.cli.command('elastic_build_aarecords_magzdb')
def elastic_build_aarecords_magzdb():
    elastic_build_aarecords_magzdb_internal()

def elastic_build_aarecords_magzdb_internal():
    # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
    new_tables_internal('aarecords_codes_magzdb')

    before_first_primary_id = ''
    # before_first_primary_id = '123'

    with engine.connect() as connection:
        print("Processing from annas_archive_meta__aacid__magzdb_records")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(primary_id) AS count FROM annas_archive_meta__aacid__magzdb_records WHERE primary_id LIKE "record%%" AND primary_id > %(from)s ORDER BY primary_id LIMIT 1', { "from": before_first_primary_id })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                current_primary_id = before_first_primary_id
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT primary_id FROM annas_archive_meta__aacid__magzdb_records WHERE primary_id LIKE "record%%" AND primary_id > %(from)s ORDER BY primary_id LIMIT %(limit)s', { "from": current_primary_id, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from annas_archive_meta__aacid__magzdb_records ( starting primary_id: {batch[0]['primary_id']} , ending primary_id: {batch[-1]['primary_id']} )...")
                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked([f"magzdb:{row['primary_id'][len('record_'):]}" for row in batch], CHUNK_SIZE))
                    pbar.update(len(batch))
                    current_primary_id = batch[-1]['primary_id']
        print(f"Done with annas_archive_meta__aacid__magzdb_records!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_nexusstc
@cli.cli.command('elastic_build_aarecords_nexusstc')
def elastic_build_aarecords_nexusstc():
    elastic_build_aarecords_nexusstc_internal()

def elastic_build_aarecords_nexusstc_internal():
    # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
    new_tables_internal('aarecords_codes_nexusstc')

    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('DROP TABLE IF EXISTS nexusstc_cid_only')
        cursor.execute('CREATE TABLE nexusstc_cid_only (nexusstc_id VARCHAR(200) NOT NULL, PRIMARY KEY (nexusstc_id)) ENGINE=MyISAM DEFAULT CHARSET=ascii COLLATE=ascii_bin ROW_FORMAT=FIXED')

    before_first_primary_id = ''
    # before_first_primary_id = '123'

    with engine.connect() as connection:
        print("Processing from annas_archive_meta__aacid__nexusstc_records")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(primary_id) AS count FROM annas_archive_meta__aacid__nexusstc_records WHERE primary_id > %(from)s ORDER BY primary_id LIMIT 1', { "from": before_first_primary_id })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                current_primary_id = before_first_primary_id
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT primary_id FROM annas_archive_meta__aacid__nexusstc_records WHERE primary_id > %(from)s ORDER BY primary_id LIMIT %(limit)s', { "from": current_primary_id, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from annas_archive_meta__aacid__nexusstc_records ( starting primary_id: {batch[0]['primary_id']} , ending primary_id: {batch[-1]['primary_id']} )...")
                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked([f"nexusstc:{row['primary_id']}" for row in batch], CHUNK_SIZE))
                    pbar.update(len(batch))
                    current_primary_id = batch[-1]['primary_id']
        print(f"Done with annas_archive_meta__aacid__nexusstc_records!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_main
@cli.cli.command('elastic_build_aarecords_main')
def elastic_build_aarecords_main():
    elastic_build_aarecords_main_internal()

def elastic_build_aarecords_main_internal():
    # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
    new_tables_internal('aarecords_codes_main')

    before_first_md5 = ''
    # before_first_md5 = 'aaa5a4759e87b0192c1ecde213535ba1'
    before_first_doi = ''
    # before_first_doi = ''
    before_first_nexusstc_id = ''
    # before_first_nexusstc_id = ''

    if before_first_md5 != '':
        print(f'WARNING!!!!! before_first_md5 is set to {before_first_md5}')
        print(f'WARNING!!!!! before_first_md5 is set to {before_first_md5}')
        print(f'WARNING!!!!! before_first_md5 is set to {before_first_md5}')
    if before_first_doi != '':
        print(f'WARNING!!!!! before_first_doi is set to {before_first_doi}')
        print(f'WARNING!!!!! before_first_doi is set to {before_first_doi}')
        print(f'WARNING!!!!! before_first_doi is set to {before_first_doi}')

    with engine.connect() as connection:
        if before_first_md5 == '' and before_first_doi == '':
            print("Deleting main ES indices")
            for index_name, es_handle in allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING.items():
                if index_name in allthethings.utils.MAIN_SEARCH_INDEXES:
                    es_handle.options(ignore_status=[400,404]).indices.delete(index=index_name) # Old
                    for virtshard in range(0, 100): # Out of abundance, delete up to a large number
                        es_handle.options(ignore_status=[400,404]).indices.delete(index=f'{index_name}__{virtshard}')

            connection.connection.ping(reconnect=True)
            cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
            cursor.execute('DROP TABLE IF EXISTS aarecords_all_md5')
            cursor.execute('CREATE TABLE aarecords_all_md5 (md5 BINARY(16) NOT NULL, json_compressed LONGBLOB NOT NULL, PRIMARY KEY (md5)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
            cursor.execute('DROP TABLE IF EXISTS temp_md5_with_doi_seen')
            cursor.execute('CREATE TABLE temp_md5_with_doi_seen (doi VARBINARY(1000), PRIMARY KEY (doi)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')

        print("Counting computed_all_md5s")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(md5) AS count FROM computed_all_md5s WHERE md5 > %(from)s ORDER BY md5 LIMIT 1', { "from": bytes.fromhex(before_first_md5) })
        total = list(cursor.fetchall())[0]['count']

        if before_first_md5 == '' and before_first_doi == '':
            if not SLOW_DATA_IMPORTS:
                print("Sleeping 3 minutes (no point in making this less)")
                time.sleep(60*3)
            print("Creating main ES indices")
            for index_name, es_handle in allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING.items():
                if index_name in allthethings.utils.MAIN_SEARCH_INDEXES:
                    for full_index_name in allthethings.utils.all_virtshards_for_index(index_name):
                        es_handle.indices.create(wait_for_active_shards=1,index=full_index_name, body=es_create_index_body)

        if before_first_doi == '':
            with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}', smoothing=0.01) as pbar:
                with concurrent.futures.ProcessPoolExecutor(max_workers=THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                    futures = set()
                    def process_future():
                        # print(f"Futures waiting: {len(futures)}")
                        (done, not_done) = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                        # print(f"Done!")
                        for future_done in done:
                            futures.remove(future_done)
                            pbar.update(CHUNK_SIZE)
                            err = future_done.exception()
                            if err:
                                print(f"ERROR IN FUTURE RESOLUTION!!!!! {repr(err)}\n\n/////\n\n{traceback.format_exc()}")
                                raise err
                            result = future_done.result()
                            if result:
                                print("Error detected; exiting")
                                os._exit(1)

                    current_md5 = bytes.fromhex(before_first_md5)
                    last_map = None
                    while True:
                        connection.connection.ping(reconnect=True)
                        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                        cursor.execute('SELECT md5 FROM computed_all_md5s WHERE md5 > %(from)s ORDER BY md5 LIMIT %(limit)s', { "from": current_md5, "limit": BATCH_SIZE })
                        batch = list(cursor.fetchall())
                        if last_map is not None:
                            if any(last_map.get()):
                                print("Error detected; exiting")
                                os._exit(1)
                        if len(batch) == 0:
                            break
                        print(f"Processing (ahead!) with {THREADS=} {len(batch)=} aarecords from computed_all_md5s ( starting md5: {batch[0]['md5'].hex()} , ending md5: {batch[-1]['md5'].hex()} )...")
                        for chunk in more_itertools.chunked([f"md5:{item['md5'].hex()}" for item in batch], CHUNK_SIZE):
                            futures.add(executor.submit(elastic_build_aarecords_job, chunk))
                            if len(futures) > THREADS*2:
                                process_future()
                        # last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked([f"md5:{item['md5'].hex()}" for item in batch], CHUNK_SIZE))
                        # pbar.update(len(batch))
                        current_md5 = batch[-1]['md5']
                    while len(futures) > 0:
                        process_future()

        print("Processing from scihub_dois")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(*) AS count FROM scihub_dois WHERE doi > %(from)s ORDER BY doi LIMIT 1', { "from": before_first_doi })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                current_doi = before_first_doi
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT doi FROM scihub_dois WHERE doi > %(from)s ORDER BY doi LIMIT %(limit)s', { "from": current_doi, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from scihub_dois ( starting doi: {batch[0]['doi']}, ending doi: {batch[-1]['doi']} )...")
                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked([f"doi:{item['doi']}" for item in batch], CHUNK_SIZE))
                    pbar.update(len(batch))
                    current_doi = batch[-1]['doi']

        print("Processing from nexusstc_cid_only")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(*) AS count FROM nexusstc_cid_only WHERE nexusstc_id > %(from)s ORDER BY nexusstc_id LIMIT 1', { "from": before_first_nexusstc_id })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with multiprocessing.Pool(THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                current_nexusstc_id = before_first_nexusstc_id
                last_map = None
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT nexusstc_id FROM nexusstc_cid_only WHERE nexusstc_id > %(from)s ORDER BY nexusstc_id LIMIT %(limit)s', { "from": current_nexusstc_id, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if last_map is not None:
                        if any(last_map.get()):
                            print("Error detected; exiting")
                            os._exit(1)
                    if len(batch) == 0:
                        break
                    print(f"Processing with {THREADS=} {len(batch)=} aarecords from nexusstc_cid_only ( starting nexusstc_id: {batch[0]['nexusstc_id']}, ending nexusstc_id: {batch[-1]['nexusstc_id']} )...")
                    last_map = executor.map_async(elastic_build_aarecords_job, more_itertools.ichunked([f"nexusstc_download:{item['nexusstc_id']}" for item in batch], CHUNK_SIZE))
                    pbar.update(len(batch))
                    current_nexusstc_id = batch[-1]['nexusstc_id']

    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('DROP TABLE temp_md5_with_doi_seen')

    print("Done with main!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_forcemerge
@cli.cli.command('elastic_build_aarecords_forcemerge')
def elastic_build_aarecords_forcemerge():
    elastic_build_aarecords_forcemerge_internal()

def elastic_build_aarecords_forcemerge_internal():
    for index_name, es_handle in allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING.items():
        for full_index_name in allthethings.utils.all_virtshards_for_index(index_name):
            print(f'Calling forcemerge on {full_index_name=}')
            es_handle.options(ignore_status=[400,404]).indices.forcemerge(index=full_index_name, wait_for_completion=True, request_timeout=300)

#################################################################################################
# Fill make aarecords_codes with numbers based off ROW_NUMBER and
# DENSE_RANK MySQL functions, but precomupted because they're expensive.
#
# ./run flask cli mysql_build_aarecords_codes_numbers
@cli.cli.command('mysql_build_aarecords_codes_numbers')
def mysql_build_aarecords_codes_numbers():
    mysql_build_aarecords_codes_numbers_internal()

def mysql_build_aarecords_codes_numbers_internal():
    processed_rows = 0
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)

        if SLOW_DATA_IMPORTS:
            cursor.execute('DROP TABLE IF EXISTS aarecords_codes_new')
            cursor.execute('DROP TABLE IF EXISTS aarecords_codes_prefixes_new')

        # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt, when changing aarecords_codes_* temp tables.
        print("Creating fresh table aarecords_codes_new")
        cursor.execute(f'CREATE TABLE aarecords_codes_new (code VARBINARY({allthethings.utils.AARECORDS_CODES_CODE_LENGTH}) NOT NULL, aarecord_id VARBINARY({allthethings.utils.AARECORDS_CODES_AARECORD_ID_LENGTH}) NOT NULL, aarecord_id_prefix VARBINARY({allthethings.utils.AARECORDS_CODES_AARECORD_ID_PREFIX_LENGTH}) NOT NULL, row_number_order_by_code BIGINT NOT NULL, dense_rank_order_by_code BIGINT NOT NULL, row_number_partition_by_aarecord_id_prefix_order_by_code BIGINT NOT NULL, dense_rank_partition_by_aarecord_id_prefix_order_by_code BIGINT NOT NULL, PRIMARY KEY (code, aarecord_id), INDEX aarecord_id_prefix (aarecord_id_prefix, code, aarecord_id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SELECT code, aarecord_id, SUBSTRING_INDEX(aarecord_id, ":", 1) AS aarecord_id_prefix, (ROW_NUMBER() OVER (ORDER BY code)) AS row_number_order_by_code, (DENSE_RANK() OVER (ORDER BY code)) AS dense_rank_order_by_code, (ROW_NUMBER() OVER (PARTITION BY aarecord_id_prefix ORDER BY code)) AS row_number_partition_by_aarecord_id_prefix_order_by_code, (DENSE_RANK() OVER (PARTITION BY aarecord_id_prefix ORDER BY code)) AS dense_rank_partition_by_aarecord_id_prefix_order_by_code FROM (SELECT code, aarecord_id FROM aarecords_codes_ia UNION ALL SELECT code, aarecord_id FROM aarecords_codes_isbndb UNION ALL SELECT code, aarecord_id FROM aarecords_codes_ol UNION ALL SELECT code, aarecord_id FROM aarecords_codes_duxiu UNION ALL SELECT code, aarecord_id FROM aarecords_codes_oclc UNION ALL SELECT code, aarecord_id FROM aarecords_codes_magzdb UNION ALL SELECT code, aarecord_id FROM aarecords_codes_edsebk UNION ALL SELECT code, aarecord_id FROM aarecords_codes_nexusstc UNION ALL SELECT code, aarecord_id FROM aarecords_codes_main) x')
        cursor.execute(f'CREATE TABLE aarecords_codes_prefixes_new (code_prefix VARBINARY({allthethings.utils.AARECORDS_CODES_CODE_LENGTH}) NOT NULL, PRIMARY KEY (code_prefix)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SELECT DISTINCT SUBSTRING_INDEX(code, ":", 1) AS code_prefix FROM aarecords_codes_new')

        cursor.execute('SELECT table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "allthethings" and TABLE_NAME = "aarecords_codes_new" LIMIT 1')
        total = cursor.fetchone()['table_rows']
        print(f"Found {total=} codes (approximately)")

        if SLOW_DATA_IMPORTS:
            connection.connection.ping(reconnect=True)
            cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
            cursor.execute('SELECT MIN(correct) AS min_correct FROM (SELECT ((row_number_order_by_code = ROW_NUMBER() OVER (ORDER BY code)) AND (dense_rank_order_by_code = DENSE_RANK() OVER (ORDER BY code)) AND (row_number_partition_by_aarecord_id_prefix_order_by_code = ROW_NUMBER() OVER (PARTITION BY aarecord_id_prefix ORDER BY code)) AND (dense_rank_partition_by_aarecord_id_prefix_order_by_code = DENSE_RANK() OVER (PARTITION BY aarecord_id_prefix ORDER BY code))) AS correct FROM aarecords_codes_new ORDER BY code DESC LIMIT 10) x')
            if str(cursor.fetchone()['min_correct']) != '1':
                raise Exception('mysql_build_aarecords_codes_numbers_internal final sanity check failed!')

        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('DROP TABLE IF EXISTS aarecords_codes')
        cursor.execute('COMMIT')
        cursor.execute('ALTER TABLE aarecords_codes_new RENAME aarecords_codes')
        cursor.execute('COMMIT')
        cursor.execute('DROP TABLE IF EXISTS aarecords_codes_prefixes')
        cursor.execute('COMMIT')
        cursor.execute('ALTER TABLE aarecords_codes_prefixes_new RENAME aarecords_codes_prefixes')
        cursor.execute('COMMIT')
    print(f"Done! {processed_rows=}")


#################################################################################################
# ./run flask cli mariapersist_reset
@cli.cli.command('mariapersist_reset')
def mariapersist_reset():
    print("Erasing entire persistent database ('mariapersist')! Did you double-check that any production databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)
    mariapersist_reset_internal()

def mariapersist_reset_internal():
    # Per https://stackoverflow.com/a/4060259
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    mariapersist_engine_multi = create_engine(mariapersist_url, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = mariapersist_engine_multi.raw_connection().cursor()

    # From https://stackoverflow.com/a/8248281
    cursor.execute("SELECT concat('DROP TABLE IF EXISTS `', table_name, '`;') FROM information_schema.tables WHERE table_schema = 'mariapersist' AND table_name LIKE 'mariapersist_%';")
    delete_all_query = "\n".join([item[0] for item in cursor.fetchall()])
    if len(delete_all_query) > 0:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute(delete_all_query)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1; COMMIT;")

    cursor.execute(pathlib.Path(os.path.join(__location__, 'mariapersist_migration.sql')).read_text())
    cursor.close()

#################################################################################################
# Send test email
# ./run flask cli send_test_email <email_addr>
@cli.cli.command('send_test_email')
@click.argument("email_addr")
def send_test_email(email_addr):
    email_msg = flask_mail.Message(subject="Hello", body="Hi there, this is a test!", recipients=[email_addr])
    mail.send(email_msg)
