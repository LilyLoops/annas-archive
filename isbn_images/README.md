# ISBN images demo program

Demo program for showing how to work with our file format for codes with continuous IDs, like ISBNs.

For a description of the file format see `dump_isbn13_codes_benc` in `allthethings/cli/views.py`.

Prerequisites:

```sh
pip install bencodepy
pip install isbnlib
pip install Pillow
pip install tqdm
pip install zstandard
```

To dump all ISBNs from the "md5" set:

```sh
python3 print_md5_isbns.py
```

To calculate what percentage the "md5" set is of all ISBNs:

```sh
python3 calculate_percentage_md5.py
```

To generate ISBN images:

```sh
python3 make_isbn_images.py
```
