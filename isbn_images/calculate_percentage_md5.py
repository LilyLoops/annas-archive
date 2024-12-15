import bencodepy
import isbnlib
import struct
import tqdm
import zstandard

# Get the latest from the `codes_benc` directory in `aa_derived_mirror_metadata`:
# https://annas-archive.org/torrents#aa_derived_mirror_metadata
input_filename = 'aa_isbn13_codes_20241204T185335Z.benc.zst'

isbn_data = bencodepy.bread(zstandard.ZstdDecompressor().stream_reader(open(input_filename, 'rb')))

all_isbns = set()
md5_isbns_count = 0

for prefix, packed_isbns_binary in isbn_data.items():
    print(f"Calculating for {prefix=}")
    current_isbn_count = 0
    packed_isbns_ints = struct.unpack(f'{len(packed_isbns_binary) // 4}I', packed_isbns_binary)
    isbn_streak = True # Alternate between reading `isbn_streak` and `gap_size`.
    position = 0 # ISBN (without check digit) is `978000000000 + position`.
    for value in tqdm.tqdm(packed_isbns_ints):
        if isbn_streak:
            for _ in range(0, value):
                isbn13_without_check = 978000000000 + position
                all_isbns.add(isbn13_without_check)
                current_isbn_count += 1
                position += 1
        else: # Reading `gap_size`.
            position += value
        isbn_streak = not isbn_streak
    if prefix == b'md5':
        md5_isbns_count = current_isbn_count

print(f"Total ISBNs: {len(all_isbns)}")
print(f"MD5 ISBNs: {md5_isbns_count} ({round(float(md5_isbns_count)*100.0/float(len(all_isbns)))}%)")
