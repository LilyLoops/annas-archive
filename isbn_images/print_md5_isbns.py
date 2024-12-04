import bencodepy
import isbnlib
import struct
import zstandard

# Get the latest from the `codes_benc` directory in `aa_derived_mirror_metadata`:
# https://annas-archive.org/torrents#aa_derived_mirror_metadata
input_filename = 'aa_isbn13_codes_20241204T185335Z.benc.zst'

isbn_data = bencodepy.bread(zstandard.ZstdDecompressor().stream_reader(open(input_filename, 'rb')))
packed_isbns_binary = isbn_data[b'md5']
packed_isbns_ints = struct.unpack(f'{len(packed_isbns_binary) // 4}I', packed_isbns_binary)

isbn_streak = True # Alternate between reading `isbn_streak` and `gap_size`.
position = 0 # ISBN (without check digit) is `978000000000 + position`.
for value in packed_isbns_ints:
    if isbn_streak:
        for _ in range(0, value):
            isbn13_without_check = str(978000000000 + position)
            check_digit = isbnlib.check_digit13(isbn13_without_check)
            print(f"{isbn13_without_check}{check_digit}")
            position += 1
    else: # Reading `gap_size`.
        position += value
    isbn_streak = not isbn_streak

