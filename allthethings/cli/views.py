import os
import orjson
import re
import isbnlib
import collections
import tqdm
import concurrent
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
from allthethings.extensions import engine, mariadb_url_no_timeout, mail, mariapersist_url
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
    print("Giving you 2 seconds to abort..")
    time.sleep(2)

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

    # From https://stackoverflow.com/a/8248281
    cursor.execute("SELECT concat('DROP TABLE IF EXISTS `', table_name, '`;') FROM information_schema.tables WHERE table_schema = 'allthethings';")
    delete_all_query = "\n".join([item[0] for item in cursor.fetchall()])
    if len(delete_all_query) > 0:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute(delete_all_query)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1; COMMIT;")

    # Generated with `docker compose exec mariadb mysqldump -u allthethings -ppassword --opt --where="1 limit 100" --skip-comments --ignore-table=computed_all_md5s allthethings > mariadb_dump.sql`
    mariadb_dump = pathlib.Path(os.path.join(__location__, 'mariadb_dump.sql')).read_text()
    cursor.execute(mariadb_dump)

    torrents_json = pathlib.Path(os.path.join(__location__, 'torrents.json')).read_text()
    cursor.execute('DROP TABLE IF EXISTS torrents_json; CREATE TABLE torrents_json (json JSON NOT NULL, PRIMARY KEY(json(100))) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin; INSERT INTO torrents_json (json) VALUES (%(json)s); COMMIT', {'json': torrents_json})

    mysql_reset_aac_tables_internal()
    mysql_build_aac_tables_internal()

    engine_multi.raw_connection().ping(reconnect=True)
    cursor.execute(mariadb_dump)
    cursor.close()

    mysql_build_computed_all_md5s_internal()

    time.sleep(1)
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
                if SLOW_DATA_IMPORTS:
                    try:
                        orjson.loads(line)
                    except Exception as err:
                        raise Exception(f"Error parsing AAC JSON: {collection=} {filename=} {line=} {err=}")

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
                elif collection == 'goodreads_records':
                    if line.endswith(b',"record":""}}\n'):
                        # Bad record
                        return None

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
    print("Giving you 2 seconds to abort..")
    time.sleep(2)

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
    # Fully superseded by aac_zlib3
    # print("Load indexes of zlib_book")
    # cursor.execute('LOAD INDEX INTO CACHE zlib_book')
    # print("Inserting from 'zlib_book' (md5_reported)")
    # cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5_reported), 2 FROM zlib_book WHERE md5_reported != "" AND md5_reported IS NOT NULL')
    # print("Inserting from 'zlib_book' (md5)")
    # cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5, first_source) SELECT UNHEX(md5), 3 FROM zlib_book WHERE zlib_book.md5 != "" AND md5 IS NOT NULL')
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
    print("Giving you 2 seconds to abort..")
    time.sleep(2)

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
def new_tables_internal(codes_table_name, codes_for_lookup_table_name=None):
    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        print(f"Creating fresh table {codes_table_name}")
        cursor.execute(f'DROP TABLE IF EXISTS {codes_table_name}')
        cursor.execute(f'CREATE TABLE {codes_table_name} (id BIGINT NOT NULL AUTO_INCREMENT, code VARBINARY({allthethings.utils.AARECORDS_CODES_CODE_LENGTH}) NOT NULL, aarecord_id VARBINARY({allthethings.utils.AARECORDS_CODES_AARECORD_ID_LENGTH}) NOT NULL, PRIMARY KEY (id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
        cursor.execute('COMMIT')
        if codes_for_lookup_table_name is not None:
            print(f"Creating fresh table {codes_for_lookup_table_name}")
            cursor.execute(f'DROP TABLE IF EXISTS {codes_for_lookup_table_name}')
            cursor.execute(f'CREATE TABLE {codes_for_lookup_table_name} (code VARBINARY({allthethings.utils.AARECORDS_CODES_CODE_LENGTH}) NOT NULL, aarecord_id VARBINARY({allthethings.utils.AARECORDS_CODES_AARECORD_ID_LENGTH}) NOT NULL, PRIMARY KEY (code, aarecord_id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
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
    'isbndb': 'aarecords_codes_isbndb',
    'ol': 'aarecords_codes_ol',
    'duxiu_ssid': 'aarecords_codes_duxiu',
    'cadal_ssno': 'aarecords_codes_duxiu',
    'oclc': 'aarecords_codes_oclc',
    'magzdb': 'aarecords_codes_magzdb',
    'nexusstc': 'aarecords_codes_nexusstc',
    'md5': 'aarecords_codes_main',
    'doi': 'aarecords_codes_main',
    'nexusstc_download': 'aarecords_codes_main',

    'cerlalc': 'aarecords_codes_cerlalc',
    'czech_oo42hcks': 'aarecords_codes_czech_oo42hcks',
    'gbooks': 'aarecords_codes_gbooks',
    'goodreads': 'aarecords_codes_goodreads',
    'isbngrp': 'aarecords_codes_isbngrp',
    'libby': 'aarecords_codes_libby',
    'rgb': 'aarecords_codes_rgb',
    'trantor': 'aarecords_codes_trantor',
}

AARECORD_ID_PREFIX_TO_CODES_FOR_LOOKUP = {
    'isbndb': { 'table_name': 'aarecords_codes_isbndb_for_lookup', 'code_names': ['collection'] }, # TODO: Use aarecord_id code here instead.
    'ol': { 'table_name': 'aarecords_codes_ol_for_lookup', 'code_names': ['isbn13', 'ocaid', 'md5'] },
    'oclc': { 'table_name': 'aarecords_codes_oclc_for_lookup', 'code_names': ['isbn13'] },
    'edsebk': { 'table_name': 'aarecords_codes_edsebk_for_lookup', 'code_names': ['isbn13'] },
    'trantor': { 'table_name': 'aarecords_codes_trantor_for_lookup', 'code_names': ['isbn13', 'sha256'] },
    'gbooks': { 'table_name': 'aarecords_codes_gbooks_for_lookup', 'code_names': ['isbn13', 'oclc'] },
    'goodreads': { 'table_name': 'aarecords_codes_goodreads_for_lookup', 'code_names': ['isbn13'] },
    'libby': { 'table_name': 'aarecords_codes_libby_for_lookup', 'code_names': ['isbn13'] },
    'czech_oo42hcks': { 'table_name': 'aarecords_codes_czech_oo42hcks_for_lookup', 'code_names': ['czech_oo42hcks_filename'] },
    'cerlalc': { 'table_name': 'aarecords_codes_cerlalc_for_lookup', 'code_names': ['isbn13'] },
    'rgb': { 'table_name': 'aarecords_codes_rgb_for_lookup', 'code_names': ['isbn13'] },
    'isbngrp': { 'table_name': 'aarecords_codes_isbngrp_for_lookup', 'code_names': ['isbn13', 'isbn13_prefix'] },
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

                isbndb_canonical_isbn13s = [aarecord_id[len('isbndb:'):] for aarecord_id in aarecord_ids if aarecord_id.startswith('isbndb:')]
                bad_isbn13_aarecord_ids = []
                if len(isbndb_canonical_isbn13s) > 0:
                    # Filter out records that are filtered in get_isbndb_dicts, because there are some bad records there.
                    valid_isbndb_aarecord_ids = set(f"isbndb:{isbndb_dict['ean13']}" for isbndb_dict in get_isbndb_dicts(session, isbndb_canonical_isbn13s))
                    bad_isbn13_aarecord_ids += set([aarecord_id for aarecord_id in aarecord_ids if aarecord_id.startswith('isbndb:') and aarecord_id not in valid_isbndb_aarecord_ids])
                    # Also filter out existing isbndb: aarecord_ids, which we can get since we do two passes (isbn13 and isbn10).
                    cursor = allthethings.utils.get_cursor_ping(session)
                    cursor.execute('SELECT aarecord_id FROM aarecords_codes_isbndb_for_lookup WHERE code="collection:isbndb" AND aarecord_id IN %(aarecord_ids)s', { "aarecord_ids": [aarecord_id for aarecord_id in aarecord_ids if aarecord_id.startswith('isbndb:')]})
                    bad_isbn13_aarecord_ids += set([aarecord_id.decode() for aarecord_id in allthethings.utils.fetch_scalars(cursor)])
                bad_isbn13_aarecord_ids = set(bad_isbn13_aarecord_ids)

                # Filter out "doi:" records that already have an md5. We don't need standalone records for those.
                dois_from_ids = [aarecord_id[4:].encode() for aarecord_id in aarecord_ids if aarecord_id.startswith('doi:')]
                doi_codes_with_md5 = set()
                if len(dois_from_ids) > 0:
                    cursor = allthethings.utils.get_cursor_ping(session)
                    cursor.execute('SELECT doi FROM temp_md5_with_doi_seen WHERE doi IN %(dois_from_ids)s', { "dois_from_ids": dois_from_ids })
                    doi_codes_with_md5 = set([f"doi:{row['doi'].decode(errors='replace')}" for row in cursor.fetchall()])

                aarecord_ids = [aarecord_id for aarecord_id in aarecord_ids if (aarecord_id not in bad_isbn13_aarecord_ids) and (aarecord_id not in doi_codes_with_md5) and (aarecord_id not in allthethings.utils.SEARCH_FILTERED_BAD_AARECORD_IDS)]
                if len(aarecord_ids) == 0:
                    return False

                # print(f"[{os.getpid()}] elastic_build_aarecords_job set up aa_records_all")
                aarecords = get_aarecords_mysql(session, aarecord_ids)
                # print(f"[{os.getpid()}] elastic_build_aarecords_job got aarecords {len(aarecords)}")
                aarecords_all_md5_insert_data = []
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
                    elif aarecord_id_split[0] == 'nexusstc':
                        source_records_by_type = allthethings.utils.groupby(aarecord['source_records'], 'source_type', 'source_record')
                        for source_record in source_records_by_type['aac_nexusstc']:
                            if len(source_record['aa_nexusstc_derived']['cid_only_links']) > 0:
                                nexusstc_cid_only_insert_data.append({ "nexusstc_id": source_record['id'] })

                    for index in aarecord['indexes']:
                        virtshard = allthethings.utils.virtshard_for_hashed_aarecord_id(hashed_aarecord_id)
                        operations_by_es_handle[allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[index]].append({ **aarecord, '_op_type': 'index', '_index': f'{index}__{virtshard}', '_id': aarecord['id'] })

                    codes = []
                    for code_name in aarecord['file_unified_data']['identifiers_unified'].keys():
                        for code_value in aarecord['file_unified_data']['identifiers_unified'][code_name]:
                            codes.append((code_name, code_value))
                    for code_name in aarecord['file_unified_data']['classifications_unified'].keys():
                        for code_value in aarecord['file_unified_data']['classifications_unified'][code_name]:
                            codes.append((code_name, code_value))
                    for code in codes:
                        code_text = f"{code[0]}:{code[1]}".encode()
                        codes_table_name = AARECORD_ID_PREFIX_TO_CODES_TABLE_NAME[aarecord_id_split[0]]
                        aarecords_codes_insert_data_by_codes_table_name[codes_table_name].append({ 'code': code_text, 'aarecord_id': aarecord['id'].encode() })
                        if aarecord_id_split[0] in AARECORD_ID_PREFIX_TO_CODES_FOR_LOOKUP:
                            if code[0] in AARECORD_ID_PREFIX_TO_CODES_FOR_LOOKUP[aarecord_id_split[0]]['code_names']:
                                codes_for_lookup_table_name = AARECORD_ID_PREFIX_TO_CODES_FOR_LOOKUP[aarecord_id_split[0]]['table_name']
                                aarecords_codes_insert_data_by_codes_table_name[codes_for_lookup_table_name].append({ 'code': code_text, 'aarecord_id': aarecord['id'].encode() })

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
CHUNK_SIZE = 50
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
    elastic_build_aarecords_oclc_internal()
    elastic_build_aarecords_edsebk_internal()
    elastic_build_aarecords_cerlalc_internal()
    elastic_build_aarecords_czech_oo42hcks_internal()
    elastic_build_aarecords_gbooks_internal()
    elastic_build_aarecords_goodreads_internal()
    elastic_build_aarecords_isbngrp_internal()
    elastic_build_aarecords_libby_internal()
    elastic_build_aarecords_rgb_internal()
    elastic_build_aarecords_trantor_internal()
    elastic_build_aarecords_magzdb_internal()
    elastic_build_aarecords_nexusstc_internal()
    elastic_build_aarecords_isbndb_internal()
    elastic_build_aarecords_ol_internal()
    elastic_build_aarecords_duxiu_internal()
    elastic_build_aarecords_ia_internal() # IA depends on tables generated above, so we do it last.
    elastic_build_aarecords_main_internal() # Main depends on tables generated above, so we do it last.
    elastic_build_aarecords_forcemerge_internal()

def build_common(table_name, batch_to_aarecord_ids, primary_id_column='primary_id', additional_where='', additional_select_AGGREGATES='', before_first_primary_id_WARNING_WARNING=''):
    before_first_primary_id=before_first_primary_id_WARNING_WARNING
    if before_first_primary_id != '':
        for i in range(5):
            print(f"WARNING! before_first_primary_id set in {table_name} to {before_first_primary_id} (total will be off)!!!!!!!!!!!!")

    with engine.connect() as connection:
        print(f"Processing from {table_name}")
        cursor = allthethings.utils.get_cursor_ping_conn(connection)
        cursor.execute(f'SELECT COUNT(*) AS count FROM {table_name} {"WHERE" if additional_where else ""} {additional_where} LIMIT 1', { "from": before_first_primary_id })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            with concurrent.futures.ProcessPoolExecutor(max_workers=THREADS, initializer=elastic_build_aarecords_job_init_pool) as executor:
                futures = set()
                count_by_future = {}
                def process_future():
                    # print(f"Futures waiting: {len(futures)}")
                    (done, not_done) = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                    # print(f"Done!")
                    for future_done in done:
                        futures.remove(future_done)
                        pbar.update(count_by_future[future_done])
                        del count_by_future[future_done]
                        err = future_done.exception()
                        if err:
                            print(f"ERROR IN FUTURE RESOLUTION!!!!! {repr(err)}\n\n/////\n\n{traceback.format_exc()}")
                            raise err
                        result = future_done.result()
                        if result:
                            print("Error detected; exiting")
                            os._exit(1)

                current_primary_id = before_first_primary_id
                while True:
                    cursor = allthethings.utils.get_cursor_ping_conn(connection)
                    cursor.execute(f'SELECT {primary_id_column} AS primary_id, COUNT(*) AS count {additional_select_AGGREGATES} FROM {table_name} WHERE {additional_where} {"AND" if additional_where else ""} {primary_id_column} > %(from)s GROUP BY {primary_id_column} ORDER BY {primary_id_column} LIMIT %(limit)s', { "from": current_primary_id, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if len(batch) == 0:
                        break
                    print(f"Processing (ahead!) with {THREADS=} {len(batch)=} aarecords from {table_name} ( starting primary_id: {batch[0]['primary_id']} , ending primary_id: {batch[-1]['primary_id']} )...")
                    for subbatch in more_itertools.chunked(batch, CHUNK_SIZE):
                        future = executor.submit(elastic_build_aarecords_job, batch_to_aarecord_ids(subbatch))
                        count_by_future[future] = sum([row['count'] for row in subbatch])
                        futures.add(future)
                        if len(futures) > THREADS*2:
                            process_future()
                    current_primary_id = batch[-1]['primary_id']
                while len(futures) > 0:
                    process_future()
        print(f"Done with {table_name}!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_ia
@cli.cli.command('elastic_build_aarecords_ia')
def elastic_build_aarecords_ia():
    elastic_build_aarecords_ia_internal()
def elastic_build_aarecords_ia_internal():
    new_tables_internal('aarecords_codes_ia') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.

    with engine.connect() as connection:
        print("Processing from aa_ia_2023_06_metadata+annas_archive_meta__aacid__ia2_records")
        cursor = allthethings.utils.get_cursor_ping_conn(connection)

        # Sanity check: we assume that in annas_archive_meta__aacid__ia2_records we have no libgen-imported records.
        print("Running sanity check on aa_ia_2023_06_metadata")
        cursor.execute('SELECT ia_id FROM aa_ia_2023_06_metadata JOIN annas_archive_meta__aacid__ia2_records ON (aa_ia_2023_06_metadata.ia_id = annas_archive_meta__aacid__ia2_records.primary_id) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NOT NULL LIMIT 500')
        sanity_check_result = list(cursor.fetchall())
        if len(sanity_check_result) > 0:
            raise Exception(f"Sanity check failed: libgen records found in annas_archive_meta__aacid__ia2_records {sanity_check_result=}")

        print("Generating table temp_ia_ids")
        cursor.execute('DROP TABLE IF EXISTS temp_ia_ids')
        cursor.execute('CREATE TABLE temp_ia_ids (ia_id VARCHAR(250) NOT NULL, PRIMARY KEY(ia_id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SELECT ia_id FROM (SELECT ia_id, libgen_md5 FROM aa_ia_2023_06_metadata UNION SELECT primary_id AS ia_id, NULL AS libgen_md5 FROM annas_archive_meta__aacid__ia2_records) combined LEFT JOIN aa_ia_2023_06_files USING (ia_id) LEFT JOIN annas_archive_meta__aacid__ia2_acsmpdf_files ON (combined.ia_id = annas_archive_meta__aacid__ia2_acsmpdf_files.primary_id) WHERE aa_ia_2023_06_files.md5 IS NULL AND annas_archive_meta__aacid__ia2_acsmpdf_files.md5 IS NULL AND combined.libgen_md5 IS NULL')

    build_common('temp_ia_ids', lambda batch: [f"ia:{row['primary_id']}" for row in batch], primary_id_column='ia_id')

    with engine.connect() as connection:
        print("Removing table temp_ia_ids")
        cursor = allthethings.utils.get_cursor_ping_conn(connection)
        cursor.execute('DROP TABLE IF EXISTS temp_ia_ids')
        print("Done with IA!")


#################################################################################################
# ./run flask cli elastic_build_aarecords_isbndb
@cli.cli.command('elastic_build_aarecords_isbndb')
def elastic_build_aarecords_isbndb():
    elastic_build_aarecords_isbndb_internal()
def elastic_build_aarecords_isbndb_internal():
    new_tables_internal('aarecords_codes_isbndb', 'aarecords_codes_isbndb_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('isbndb_isbns', lambda batch: [f"isbndb:{row['primary_id']}" for row in batch], primary_id_column='isbn13')
    build_common('isbndb_isbns', lambda batch: [f"isbndb:{isbnlib.ean13(row['primary_id'])}" for row in batch], primary_id_column='isbn10')

#################################################################################################
# ./run flask cli elastic_build_aarecords_ol
@cli.cli.command('elastic_build_aarecords_ol')
def elastic_build_aarecords_ol():
    elastic_build_aarecords_ol_internal()
def elastic_build_aarecords_ol_internal():
    new_tables_internal('aarecords_codes_ol', 'aarecords_codes_ol_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('ol_base', lambda batch: [f"ol:{row['primary_id'].replace('/books/','')}" for row in batch],
        primary_id_column='ol_key', additional_where='ol_key LIKE "/books/OL%%" AND ol_key LIKE "%%M"')

#################################################################################################
# ./run flask cli elastic_build_aarecords_duxiu
@cli.cli.command('elastic_build_aarecords_duxiu')
def elastic_build_aarecords_duxiu():
    elastic_build_aarecords_duxiu_internal()
def elastic_build_aarecords_duxiu_internal():
    new_tables_internal('aarecords_codes_duxiu') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    def duxiu_batch_to_aarecord_ids(batch):
        with engine.connect() as connection:
            cursor = allthethings.utils.get_cursor_ping_conn(connection)
            unrolled_rows = [{"primary_id": row['primary_id'], "byte_offset": int(byte_offset), "byte_length": int(byte_length)} for row in batch for byte_offset, byte_length in zip(row['byte_offsets'].split(','), row['byte_lengths'].split(',')) ]
            lines_bytes = allthethings.utils.get_lines_from_aac_file(cursor, 'duxiu_records', [(row['byte_offset'], row['byte_length']) for row in unrolled_rows])
            ids = []
            for item_index, item in enumerate(unrolled_rows):
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
            return list(set(ids))
    build_common('annas_archive_meta__aacid__duxiu_records', duxiu_batch_to_aarecord_ids,
        additional_where='(primary_id LIKE "duxiu_ssid_%%" OR primary_id LIKE "cadal_ssno_%%")',
        additional_select_AGGREGATES=', GROUP_CONCAT(byte_offset) AS byte_offsets, GROUP_CONCAT(byte_length) AS byte_lengths')

#################################################################################################
# ./run flask cli elastic_build_aarecords_oclc
@cli.cli.command('elastic_build_aarecords_oclc')
def elastic_build_aarecords_oclc():
    elastic_build_aarecords_oclc_internal()
def elastic_build_aarecords_oclc_internal():
    new_tables_internal('aarecords_codes_oclc', 'aarecords_codes_oclc_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__worldcat', lambda batch: [f"oclc:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_edsebk
@cli.cli.command('elastic_build_aarecords_edsebk')
def elastic_build_aarecords_edsebk():
    elastic_build_aarecords_edsebk_internal()
def elastic_build_aarecords_edsebk_internal():
    new_tables_internal('aarecords_codes_edsebk', 'aarecords_codes_edsebk_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__ebscohost_records', lambda batch: [f"edsebk:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_cerlalc
@cli.cli.command('elastic_build_aarecords_cerlalc')
def elastic_build_aarecords_cerlalc():
    elastic_build_aarecords_cerlalc_internal()
def elastic_build_aarecords_cerlalc_internal():
    new_tables_internal('aarecords_codes_cerlalc', 'aarecords_codes_cerlalc_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__cerlalc_records', lambda batch: [f"cerlalc:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_czech_oo42hcks
@cli.cli.command('elastic_build_aarecords_czech_oo42hcks')
def elastic_build_aarecords_czech_oo42hcks():
    elastic_build_aarecords_czech_oo42hcks_internal()
def elastic_build_aarecords_czech_oo42hcks_internal():
    new_tables_internal('aarecords_codes_czech_oo42hcks', 'aarecords_codes_czech_oo42hcks_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__czech_oo42hcks_records', lambda batch: [f"czech_oo42hcks:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_gbooks
@cli.cli.command('elastic_build_aarecords_gbooks')
def elastic_build_aarecords_gbooks():
    elastic_build_aarecords_gbooks_internal()
def elastic_build_aarecords_gbooks_internal():
    new_tables_internal('aarecords_codes_gbooks', 'aarecords_codes_gbooks_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__gbooks_records', lambda batch: [f"gbooks:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_goodreads
@cli.cli.command('elastic_build_aarecords_goodreads')
def elastic_build_aarecords_goodreads():
    elastic_build_aarecords_goodreads_internal()
def elastic_build_aarecords_goodreads_internal():
    new_tables_internal('aarecords_codes_goodreads', 'aarecords_codes_goodreads_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__goodreads_records', lambda batch: [f"goodreads:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_isbngrp
@cli.cli.command('elastic_build_aarecords_isbngrp')
def elastic_build_aarecords_isbngrp():
    elastic_build_aarecords_isbngrp_internal()
def elastic_build_aarecords_isbngrp_internal():
    new_tables_internal('aarecords_codes_isbngrp', 'aarecords_codes_isbngrp_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__isbngrp_records', lambda batch: [f"isbngrp:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_libby
@cli.cli.command('elastic_build_aarecords_libby')
def elastic_build_aarecords_libby():
    elastic_build_aarecords_libby_internal()
def elastic_build_aarecords_libby_internal():
    new_tables_internal('aarecords_codes_libby', 'aarecords_codes_libby_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__libby_records', lambda batch: [f"libby:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_rgb
@cli.cli.command('elastic_build_aarecords_rgb')
def elastic_build_aarecords_rgb():
    elastic_build_aarecords_rgb_internal()
def elastic_build_aarecords_rgb_internal():
    new_tables_internal('aarecords_codes_rgb', 'aarecords_codes_rgb_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__rgb_records', lambda batch: [f"rgb:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_trantor
@cli.cli.command('elastic_build_aarecords_trantor')
def elastic_build_aarecords_trantor():
    elastic_build_aarecords_trantor_internal()
def elastic_build_aarecords_trantor_internal():
    new_tables_internal('aarecords_codes_trantor', 'aarecords_codes_trantor_for_lookup') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__trantor_records', lambda batch: [f"trantor:{row['primary_id']}" for row in batch])


#################################################################################################
# ./run flask cli elastic_build_aarecords_magzdb
@cli.cli.command('elastic_build_aarecords_magzdb')
def elastic_build_aarecords_magzdb():
    elastic_build_aarecords_magzdb_internal()
def elastic_build_aarecords_magzdb_internal():
    new_tables_internal('aarecords_codes_magzdb') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    build_common('annas_archive_meta__aacid__magzdb_records', lambda batch: [f"magzdb:{row['primary_id'][len('record_'):]}" for row in batch],
        additional_where='primary_id LIKE "record%%"')

#################################################################################################
# ./run flask cli elastic_build_aarecords_nexusstc
@cli.cli.command('elastic_build_aarecords_nexusstc')
def elastic_build_aarecords_nexusstc():
    elastic_build_aarecords_nexusstc_internal()
def elastic_build_aarecords_nexusstc_internal():
    new_tables_internal('aarecords_codes_nexusstc') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('DROP TABLE IF EXISTS nexusstc_cid_only')
        cursor.execute('CREATE TABLE nexusstc_cid_only (nexusstc_id VARCHAR(200) NOT NULL, PRIMARY KEY (nexusstc_id)) ENGINE=MyISAM DEFAULT CHARSET=ascii COLLATE=ascii_bin ROW_FORMAT=FIXED')
    build_common('annas_archive_meta__aacid__nexusstc_records', lambda batch: [f"nexusstc:{row['primary_id']}" for row in batch])

#################################################################################################
# ./run flask cli elastic_build_aarecords_main
@cli.cli.command('elastic_build_aarecords_main')
def elastic_build_aarecords_main():
    elastic_build_aarecords_main_internal()
def elastic_build_aarecords_main_internal():
    new_tables_internal('aarecords_codes_main') # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.

    print("Deleting main ES indices")
    for index_name, es_handle in allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING.items():
        if index_name in allthethings.utils.MAIN_SEARCH_INDEXES:
            es_handle.options(ignore_status=[400,404]).indices.delete(index=index_name) # Old
            for virtshard in range(0, 100): # Out of abundance, delete up to a large number
                es_handle.options(ignore_status=[400,404]).indices.delete(index=f'{index_name}__{virtshard}')
    if not SLOW_DATA_IMPORTS:
        print("Sleeping 3 minutes (no point in making this less)")
        time.sleep(60*3)
    print("Creating main ES indices")
    for index_name, es_handle in allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING.items():
        if index_name in allthethings.utils.MAIN_SEARCH_INDEXES:
            for full_index_name in allthethings.utils.all_virtshards_for_index(index_name):
                es_handle.indices.create(wait_for_active_shards=1,index=full_index_name, body=es_create_index_body)

    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('DROP TABLE IF EXISTS aarecords_all_md5')
        cursor.execute('CREATE TABLE aarecords_all_md5 (md5 BINARY(16) NOT NULL, json_compressed LONGBLOB NOT NULL, PRIMARY KEY (md5)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
        cursor.execute('DROP TABLE IF EXISTS temp_md5_with_doi_seen')
        cursor.execute('CREATE TABLE temp_md5_with_doi_seen (id BIGINT NOT NULL AUTO_INCREMENT, doi VARBINARY(1000), PRIMARY KEY (id), INDEX(doi)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')

    build_common('computed_all_md5s', lambda batch: [f"md5:{row['primary_id'].hex()}" for row in batch], primary_id_column='md5')
    build_common('scihub_dois', lambda batch: [f"doi:{row['primary_id']}" for row in batch], primary_id_column='doi')
    build_common('nexusstc_cid_only', lambda batch: [f"nexusstc_download:{row['primary_id']}" for row in batch], primary_id_column='nexusstc_id')

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
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)

        if SLOW_DATA_IMPORTS:
            cursor.execute('DROP TABLE IF EXISTS aarecords_codes_new')
            cursor.execute('DROP TABLE IF EXISTS aarecords_codes_prefixes_new')

        print("Creating fresh table aarecords_codes_new") # WARNING! Update the upload excludes, and dump_mariadb_omit_tables.txt.
        cursor.execute(f'CREATE TABLE aarecords_codes_new (code VARBINARY({allthethings.utils.AARECORDS_CODES_CODE_LENGTH}) NOT NULL, aarecord_id VARBINARY({allthethings.utils.AARECORDS_CODES_AARECORD_ID_LENGTH}) NOT NULL, aarecord_id_prefix VARBINARY({allthethings.utils.AARECORDS_CODES_AARECORD_ID_PREFIX_LENGTH}) NOT NULL, row_number_order_by_code BIGINT NOT NULL, dense_rank_order_by_code BIGINT NOT NULL, row_number_partition_by_aarecord_id_prefix_order_by_code BIGINT NOT NULL, dense_rank_partition_by_aarecord_id_prefix_order_by_code BIGINT NOT NULL, PRIMARY KEY (code, aarecord_id), INDEX aarecord_id_prefix (aarecord_id_prefix, code, aarecord_id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
        cursor.execute(f'ALTER TABLE aarecords_codes_new DISABLE KEYS')
        cursor.execute(f'INSERT INTO aarecords_codes_new SELECT code, aarecord_id, SUBSTRING_INDEX(aarecord_id, ":", 1) AS aarecord_id_prefix, (ROW_NUMBER() OVER (ORDER BY code, aarecord_id)) AS row_number_order_by_code, (DENSE_RANK() OVER (ORDER BY code)) AS dense_rank_order_by_code, (ROW_NUMBER() OVER (PARTITION BY aarecord_id_prefix ORDER BY code, aarecord_id)) AS row_number_partition_by_aarecord_id_prefix_order_by_code, (DENSE_RANK() OVER (PARTITION BY aarecord_id_prefix ORDER BY code)) AS dense_rank_partition_by_aarecord_id_prefix_order_by_code FROM (SELECT code, aarecord_id FROM aarecords_codes_ia UNION ALL SELECT code, aarecord_id FROM aarecords_codes_isbndb UNION ALL SELECT code, aarecord_id FROM aarecords_codes_ol UNION ALL SELECT code, aarecord_id FROM aarecords_codes_duxiu UNION ALL SELECT code, aarecord_id FROM aarecords_codes_oclc UNION ALL SELECT code, aarecord_id FROM aarecords_codes_magzdb UNION ALL SELECT code, aarecord_id FROM aarecords_codes_edsebk UNION ALL SELECT code, aarecord_id FROM aarecords_codes_nexusstc UNION ALL SELECT code, aarecord_id FROM aarecords_codes_cerlalc UNION ALL SELECT code, aarecord_id FROM aarecords_codes_czech_oo42hcks UNION ALL SELECT code, aarecord_id FROM aarecords_codes_gbooks UNION ALL SELECT code, aarecord_id FROM aarecords_codes_goodreads UNION ALL SELECT code, aarecord_id FROM aarecords_codes_isbngrp UNION ALL SELECT code, aarecord_id FROM aarecords_codes_libby UNION ALL SELECT code, aarecord_id FROM aarecords_codes_rgb UNION ALL SELECT code, aarecord_id FROM aarecords_codes_trantor UNION ALL SELECT code, aarecord_id FROM aarecords_codes_main) x ORDER BY code, aarecord_id')
        # Consider running `myisampack aarecords_codes_new.MYI` here? ~1/3rd space savings? Building index also seems faster this way.
        cursor.execute(f'ALTER TABLE aarecords_codes_new ENABLE KEYS')
        cursor.execute(f'CREATE TABLE aarecords_codes_prefixes_new (code_prefix VARBINARY({allthethings.utils.AARECORDS_CODES_CODE_LENGTH}) NOT NULL, PRIMARY KEY (code_prefix)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SELECT DISTINCT SUBSTRING_INDEX(code, ":", 1) AS code_prefix FROM aarecords_codes_new')

        cursor.execute('SELECT table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "allthethings" and TABLE_NAME = "aarecords_codes_new" LIMIT 1')
        total = cursor.fetchone()['table_rows']
        print(f"Found {total=} codes (approximately)")

        if SLOW_DATA_IMPORTS:
            connection.connection.ping(reconnect=True)
            cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
            cursor.execute('SELECT MIN(correct) AS min_correct FROM (SELECT ((row_number_order_by_code = ROW_NUMBER() OVER (ORDER BY code, aarecord_id)) AND (dense_rank_order_by_code = DENSE_RANK() OVER (ORDER BY code)) AND (row_number_partition_by_aarecord_id_prefix_order_by_code = ROW_NUMBER() OVER (PARTITION BY aarecord_id_prefix ORDER BY code, aarecord_id)) AND (dense_rank_partition_by_aarecord_id_prefix_order_by_code = DENSE_RANK() OVER (PARTITION BY aarecord_id_prefix ORDER BY code))) AS correct FROM aarecords_codes_new ORDER BY code DESC LIMIT 10) x')
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
    print(f"Done!")

#################################################################################################
# Add a better primary key to the aarecords_codes_* tables so we get better diffs in bin/check-dumps.
#
# ./run flask cli mysql_change_aarecords_codes_tables_for_check_dumps
@cli.cli.command('mysql_change_aarecords_codes_tables_for_check_dumps')
def mysql_change_aarecords_codes_tables_for_check_dumps():
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        for table_name in list(dict.fromkeys(AARECORD_ID_PREFIX_TO_CODES_TABLE_NAME.values())):
            cursor.execute(f"ALTER TABLE {table_name} DROP PRIMARY KEY, DROP COLUMN id, ADD PRIMARY KEY(code, aarecord_id);")

    print("Done!")


#################################################################################################
# ./run flask cli mariapersist_reset
@cli.cli.command('mariapersist_reset')
def mariapersist_reset():
    print("Erasing entire persistent database ('mariapersist')! Did you double-check that any production databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 2 seconds to abort..")
    time.sleep(2)
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
    cursor.execute("COMMIT")
    cursor.close()

    annatst_secret_key = allthethings.utils.secret_key_from_account_id('ANNATST')
    print(f"Login to ANNTST account with secret key: {annatst_secret_key}")

#################################################################################################
# Send test email
# ./run flask cli send_test_email <email_addr>
@cli.cli.command('send_test_email')
@click.argument("email_addr")
def send_test_email(email_addr):
    email_msg = flask_mail.Message(subject="Hello", body="Hi there, this is a test!", recipients=[email_addr])
    mail.send(email_msg)
