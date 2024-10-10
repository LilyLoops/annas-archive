import orjson
import shortuuid
import datetime

timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

with open(f"annas_archive_meta__aacid__gbooks_records__{timestamp}--{timestamp}.jsonl", 'wb') as output_file_handle:
    with open('dump.jsonl', 'rb') as input_file_handle:
        ids_seen = set()
        for line in input_file_handle:
            if line[0:7] != (b'{"id":"'):
                raise Exception(f'Invalid start: {line=}')
            if line[-2:] != (b'}\n'):
                raise Exception(f'Invalid end: {line=}')
            if line[19:20] != b'"':
                raise Exception(f'Invalid id end: {line=}')
            gbooks_id = line[7:19]
            if gbooks_id in ids_seen:
                print(f"Warning: id seen: {gbooks_id}")
            ids_seen.add(gbooks_id)

            uuid = shortuuid.uuid()
            aac_record = {
                "aacid": f"aacid__gbooks_records__{timestamp}__{uuid}",
                "metadata": orjson.Fragment(line[:-1]),
            }
            output_file_handle.write(orjson.dumps(aac_record, option=orjson.OPT_APPEND_NEWLINE))
            output_file_handle.flush()
