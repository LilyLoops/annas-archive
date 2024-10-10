import orjson
import shortuuid
import datetime

# bsondump TrantorBooks.bson > TrantorBooks.jsonl

timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

with open(f"annas_archive_meta__aacid__trantor_records__{timestamp}--{timestamp}.jsonl", 'wb') as output_file_handle:
    with open('TrantorBooks.jsonl', 'r') as input_file_handle:
        for line in input_file_handle.readlines():
            metadata = orjson.loads(line)
            uuid = shortuuid.uuid()
            aac_record = {
                "aacid": f"aacid__trantor_records__{timestamp}__{uuid}",
                "metadata": metadata,
            }
            output_file_handle.write(orjson.dumps(aac_record, option=orjson.OPT_APPEND_NEWLINE))
            output_file_handle.flush()
