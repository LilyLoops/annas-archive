import orjson
import shortuuid
import datetime
import os
import hashlib

timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

seen_hashes = set()

with open(f"annas_archive_meta__aacid__goodreads_records__{timestamp}--{timestamp}.jsonl", 'wb') as output_file_handle:
    filenames = set()
    for walk_root, walk_dirs, walk_files in os.walk('book_meta/'):
        if walk_root.startswith('book_meta/'):
            walk_root = walk_root[len('book_meta/'):]
        for walk_filename in walk_files:
            if walk_filename.endswith('.xml'):
                if walk_root == '':
                    filenames.add(walk_filename)
                else:
                    filenames.add(walk_root + '/' + walk_filename)

    filenames_sorted = sorted(filenames, key=lambda x: int(x.rsplit('/', 1)[-1].split('.', 1)[0]))

    for partial_filename in filenames:
        filename = f"book_meta/{partial_filename}"
        with open(filename, 'rb') as record_file:
            record_binary = record_file.read()
            record_xml = record_binary.decode()
            # print(f"{record_xml=}")
            # os._exit(0)

            record_id = int(filename.rsplit('/', 1)[-1].replace('.xml', ''))
            uuid = shortuuid.uuid()

            current_hash = hashlib.md5(record_binary).hexdigest()
            if (record_xml != '') and (current_hash in seen_hashes):
                print(f"Already seen: {current_hash=} {filename=} {record_xml=}")
                continue
            seen_hashes.add(current_hash)
            aac_record = {
                "aacid": f"aacid__goodreads_records__{timestamp}__{record_id}__{uuid}",
                "metadata": { 
                    "id": record_id,
                    "record": record_xml,
                },
            }
            output_file_handle.write(orjson.dumps(aac_record, option=orjson.OPT_APPEND_NEWLINE))
            output_file_handle.flush()
