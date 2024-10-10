import orjson
import shortuuid
import datetime
import os

timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

seen_ids = set()

with open(f"annas_archive_meta__aacid__libby_records__{timestamp}--{timestamp}.jsonl", 'wb') as output_file_handle:
    for filename in os.listdir('json'):
        with open(f"json/{filename}", 'rb') as input_file_handle:
            input_binary = input_file_handle.read()
            if b'<title>504 Gateway Time-out</title>' in input_binary:
                continue
            try:
                input_dict = orjson.loads(input_binary)
            except:
                raise Exception(f"Unexpected bad JSON: {input_binary=}")
            if 'message' in input_dict:
                if input_dict['message'] in ['Media not found.', 'An unexpected error has occurred.', 'A task was canceled.', 'Response status code does not indicate success: 503 (Service Unavailable).']:
                    continue
                elif input_dict.get('errorCode') in ['InternalError']:
                    continue
                else:
                    raise Exception(f"Unexpected: {input_dict=}")
            for metadata in input_dict:
                if type(metadata) is not dict:
                    print(input_dict)
                uuid = shortuuid.uuid()
                # if metadata['id'] in seen_ids:
                    # print(f"Already seen: {metadata['id']}")
                # seen_ids.add(metadata['id'])
                aac_record = {
                    "aacid": f"aacid__libby_records__{timestamp}__{metadata['id']}__{uuid}",
                    "metadata": { 
                        "id": metadata['id'],
                        **metadata,
                    },
                }
                output_file_handle.write(orjson.dumps(aac_record, option=orjson.OPT_APPEND_NEWLINE))
                output_file_handle.flush()
