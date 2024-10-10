import orjson
import shortuuid
import datetime
import pandas
import hashlib

timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def make_entry_id(row):
    return f"{row['registrant_name']}____{row['agency_name']}____{row['country_name']}"

with open(f"annas_archive_meta__aacid__isbngrp_records__{timestamp}--{timestamp}.jsonl", 'wb') as output_file_handle:
    df = pandas.read_csv('isbndata-isbns.csv')
    entries = {}
    for index, row in df.iterrows():
        dict_row = row.to_dict()
        entry_id = make_entry_id(dict_row)
        if entry_id not in entries:
            entries[entry_id] = {
                "registrant_name": dict_row['registrant_name'],
                "agency_name": dict_row['agency_name'],
                "country_name": dict_row['country_name'],
                "isbns": [],
            }
        entries[entry_id]['isbns'].append({ "isbn": dict_row['isbn'], "isbn_type": dict_row['isbn_type'] })

    for entry_id, entry in entries.items():
        md5 = hashlib.md5(entry_id.encode()).hexdigest()
        uuid = shortuuid.uuid()
        aac_record = {
            "aacid": f"aacid__isbngrp_records__{timestamp}__{uuid}",
            "metadata": {
                "id": md5,
                "record": entry,
            },
        }
        output_file_handle.write(orjson.dumps(aac_record, option=orjson.OPT_APPEND_NEWLINE))
        output_file_handle.flush()
