import orjson
import shortuuid
import datetime
from pymarc import MARCReader
from io import BufferedReader

timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

with open(f"annas_archive_meta__aacid__rgb_records__{timestamp}--{timestamp}.jsonl", 'wb') as output_file_handle:
    with open('rgb1.mrc.mrc', 'rb') as file:
        buffered = BufferedReader(file, 1000000)
        reader = MARCReader(buffered, to_unicode=True, permissive=True)
        for r in reader:
            if r is None:
                print(f"Warning: None record. {reader.current_exception=} {reader.current_chunk=}")
                continue
            record = r.as_dict()
            uuid = shortuuid.uuid()
            aac_record = {
                "aacid": f"aacid__rgb_records__{timestamp}__{uuid}",
                "metadata": {
                    "nr": record['fields'][0]['001'],
                    "record": record,
                },
            }
            output_file_handle.write(orjson.dumps(aac_record, option=orjson.OPT_APPEND_NEWLINE))
            output_file_handle.flush()
