#!/bin/python3 

import sys
import orjson

for line in sys.stdin:
    line = line.strip()
    if line == '':
        break

    record = {}
    try:
        record = orjson.loads(line)
    except Exception:
        print("Error parsing JSON.", file=sys.stderr)
        print(line, file=sys.stderr)
        continue

    if 'isbn13' not in record:
        print("Incorrect JSON, missing isbn13.", file=sys.stderr)
        print(line, file=sys.stderr)
        continue

    if len(record['isbn13']) != 13:
        print(f"Incorrect JSON, isbn13 has wrong length: {len(record['isbn13'])}.", file=sys.stderr)
        print(line, file=sys.stderr)
        continue

    if 'isbn' in record and len(record['isbn']) == 0:
        record['isbn'] = ''
    elif 'isbn' in record and len(record['isbn']) != 10:
        print(f"Incorrect JSON, isbn has wrong length: {len(record['isbn'])}.", file=sys.stderr)
        print(line, file=sys.stderr)
        continue

    fields = (record['isbn13'], record.get('isbn', None) or '', orjson.dumps(record).decode('utf-8'))
    print(f"{fields[0]}\t{fields[1]}\t{fields[2]}")
