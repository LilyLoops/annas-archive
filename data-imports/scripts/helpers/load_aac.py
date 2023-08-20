#!/bin/python3 

# Run with PYTHONIOENCODING=UTF8:ignore

import os
import io
import sys
import gzip
import tarfile
import orjson
import httpx
import pymysql
import pymysql.cursors
import more_itertools
import zstandard
import multiprocessing
import re

filename = sys.argv[-1]
collection = filename.split('__')[2]

def build_insert_data(line):
    # Parse "canonical AAC" more efficiently than parsing all the JSON
    matches = re.match(r'\{"aacid":"([^"]+)",("data_folder":"([^"]+)",)?"metadata":\{"[^"]+":([^,]+),("md5":"([^"]+)")?', line)
    if matches is None:
        raise Exception(f"Line is not in canonical AAC format: '{line}'")
    aacid = matches[1]
    data_folder = matches[3]
    primary_id = str(matches[4].replace('"', ''))
    md5 = matches[6]
    if md5 is None:
        if '"md5_reported"' in line:
            md5_reported_matches = re.search(r'"md5_reported":"([^"]+)"', line)
            if md5_reported_matches is None:
                raise Exception(f"'md5_reported' found, but not in an expected format! '{line}'")
            md5 = md5_reported_matches[1]
    metadata = line[(line.index('"metadata":')+len('"metadata":')):-2]
    return { 'aacid': aacid, 'primary_id': primary_id, 'md5': md5, 'data_folder': data_folder, 'metadata': metadata }

CHUNK_SIZE = 100000

table_name = f'annas_archive_meta__aacid__{collection}'
print(f"[{collection}] Reading from {filename} to {table_name}")
db = pymysql.connect(host='aa-data-import--mariadb', user='allthethings', password='password', database='allthethings', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, read_timeout=120, write_timeout=120, autocommit=True)
cursor = db.cursor()
cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
cursor.execute(f"CREATE TABLE {table_name} (`aacid` VARCHAR(250) NOT NULL, `primary_id` VARCHAR(250) NULL, `md5` char(32) CHARACTER SET ascii NULL, `data_folder` VARCHAR(250) NULL, `metadata` JSON NOT NULL, PRIMARY KEY (`aacid`)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
cursor.execute(f"LOCK TABLES {table_name} WRITE")
# From https://github.com/indygreg/python-zstandard/issues/13#issuecomment-1544313739
with open(f'/temp-dir/aac/{filename}', 'rb') as fh:
    dctx = zstandard.ZstdDecompressor()
    stream_reader = dctx.stream_reader(fh)
    text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
    total = 0
    for lines in more_itertools.ichunked(text_stream, CHUNK_SIZE):
        insert_data = [build_insert_data(line) for line in lines]
        total += len(insert_data)
        print(f"[{collection}] Processed {len(insert_data)} lines ({total} lines total)")
        cursor.executemany(f'INSERT INTO {table_name} (aacid, primary_id, md5, data_folder, metadata) VALUES (%(aacid)s, %(primary_id)s, %(md5)s, %(data_folder)s, %(metadata)s)', insert_data)
print(f"[{collection}] Building indexes..")
cursor.execute(f"ALTER TABLE {table_name} ADD INDEX `primary_id` (`primary_id`), ADD INDEX `md5` (`md5`)")
db.ping(reconnect=True)
cursor.execute(f"UNLOCK TABLES")
print(f"[{collection}] Done!")


