import os
import json
import orjson
import re
import isbnlib
import functools
import collections
import langcodes
import threading
import random
import fast_langdetect
import traceback
import urllib.parse
import urllib.request
import datetime
import base64
import hashlib
import shortuuid
import pymysql.cursors
import cachetools
import time
import natsort
import unicodedata
# import tiktoken
# import openai

from flask import g, Blueprint, render_template, make_response, redirect, request
from allthethings.extensions import engine, es, es_aux, mariapersist_engine
from sqlalchemy import text
from sqlalchemy.orm import Session
from flask_babel import gettext, force_locale, get_locale
from config.settings import AA_EMAIL, DOWNLOADS_SECRET_KEY, AACID_SMALL_DATA_IMPORTS

import allthethings.utils

HASHED_DOWNLOADS_SECRET_KEY = hashlib.sha256(DOWNLOADS_SECRET_KEY.encode()).digest()

page = Blueprint("page", __name__, template_folder="templates")

ES_TIMEOUT_PRIMARY = "200ms"
ES_TIMEOUT_ALL_AGG = "20s"
ES_TIMEOUT = "100ms"

# Taken from https://github.com/internetarchive/openlibrary/blob/e7e8aa5b8c/openlibrary/plugins/openlibrary/pages/languages.page
# because https://openlibrary.org/languages.json doesn't seem to give a complete list? (And ?limit=.. doesn't seem to work.)
ol_languages_json = json.load(open(os.path.dirname(os.path.realpath(__file__)) + '/ol_languages.json'))
ol_languages = {}
for language in ol_languages_json:
    ol_languages[language['key']] = language


# Good pages to test with:
# * http://localhost:8000/zlib/1
# * http://localhost:8000/zlib/100
# * http://localhost:8000/zlib/4698900
# * http://localhost:8000/zlib/19005844
# * http://localhost:8000/zlib/2425562
# * http://localhost:8000/ol/OL100362M
# * http://localhost:8000/ol/OL33897070M
# * http://localhost:8000/ol/OL39479373M
# * http://localhost:8000/ol/OL1016679M
# * http://localhost:8000/ol/OL10045347M
# * http://localhost:8000/ol/OL1183530M
# * http://localhost:8000/ol/OL1002667M
# * http://localhost:8000/ol/OL1000021M
# * http://localhost:8000/ol/OL13573618M
# * http://localhost:8000/ol/OL999950M
# * http://localhost:8000/ol/OL998696M
# * http://localhost:8000/ol/OL22555477M
# * http://localhost:8000/ol/OL15990933M
# * http://localhost:8000/ol/OL6785286M
# * http://localhost:8000/ol/OL3296622M
# * http://localhost:8000/ol/OL2862972M
# * http://localhost:8000/ol/OL24764643M
# * http://localhost:8000/ol/OL7002375M
# * http://localhost:8000/db/lgrsnf/288054.json
# * http://localhost:8000/db/lgrsnf/3175616.json
# * http://localhost:8000/db/lgrsnf/2933905.json
# * http://localhost:8000/db/lgrsnf/1125703.json
# * http://localhost:8000/db/lgrsnf/59.json
# * http://localhost:8000/db/lgrsnf/1195487.json
# * http://localhost:8000/db/lgrsnf/1360257.json
# * http://localhost:8000/db/lgrsnf/357571.json
# * http://localhost:8000/db/lgrsnf/2425562.json
# * http://localhost:8000/db/lgrsnf/3354081.json
# * http://localhost:8000/db/lgrsnf/3357578.json
# * http://localhost:8000/db/lgrsnf/3357145.json
# * http://localhost:8000/db/lgrsnf/2040423.json
# * http://localhost:8000/db/lgrsfic/1314135.json
# * http://localhost:8000/db/lgrsfic/25761.json
# * http://localhost:8000/db/lgrsfic/2443846.json
# * http://localhost:8000/db/lgrsfic/2473252.json
# * http://localhost:8000/db/lgrsfic/2340232.json
# * http://localhost:8000/db/lgrsfic/1122239.json
# * http://localhost:8000/db/lgrsfic/6862.json
# * http://localhost:8000/db/lgli/100.json
# * http://localhost:8000/db/lgli/1635550.json
# * http://localhost:8000/db/lgli/94069002.json
# * http://localhost:8000/db/lgli/40122.json
# * http://localhost:8000/db/lgli/21174.json
# * http://localhost:8000/db/lgli/91051161.json
# * http://localhost:8000/db/lgli/733269.json
# * http://localhost:8000/db/lgli/156965.json
# * http://localhost:8000/db/lgli/10000000.json
# * http://localhost:8000/db/lgli/933304.json
# * http://localhost:8000/db/lgli/97559799.json
# * http://localhost:8000/db/lgli/3756440.json
# * http://localhost:8000/db/lgli/91128129.json
# * http://localhost:8000/db/lgli/44109.json
# * http://localhost:8000/db/lgli/2264591.json
# * http://localhost:8000/db/lgli/151611.json
# * http://localhost:8000/db/lgli/1868248.json
# * http://localhost:8000/db/lgli/1761341.json
# * http://localhost:8000/db/lgli/4031847.json
# * http://localhost:8000/db/lgli/2827612.json
# * http://localhost:8000/db/lgli/2096298.json
# * http://localhost:8000/db/lgli/96751802.json
# * http://localhost:8000/db/lgli/5064830.json
# * http://localhost:8000/db/lgli/1747221.json
# * http://localhost:8000/db/lgli/1833886.json
# * http://localhost:8000/db/lgli/3908879.json
# * http://localhost:8000/db/lgli/41752.json
# * http://localhost:8000/db/lgli/97768237.json
# * http://localhost:8000/db/lgli/4031335.json
# * http://localhost:8000/db/lgli/1842179.json
# * http://localhost:8000/db/lgli/97562793.json
# * http://localhost:8000/db/lgli/4029864.json
# * http://localhost:8000/db/lgli/2834701.json
# * http://localhost:8000/db/lgli/97562143.json
# * http://localhost:8000/isbndb/9789514596933
# * http://localhost:8000/isbndb/9780000000439
# * http://localhost:8000/isbndb/9780001055506
# * http://localhost:8000/isbndb/9780316769174
# * http://localhost:8000/md5/8fcb740b8c13f202e89e05c4937c09ac
# * http://localhost:8000/md5/a50f2e8f2963888a976899e2c4675d70 (sacrificed for OpenLibrary annas_archive tagging testing)

def normalize_doi(string):
    if not (('/' in string) and (' ' not in string)):
        return ''
    if string.startswith('doi:10.'):
        return string[len('doi:'):]
    if string.startswith('10.'):
        return string
    return ''

# Example: zlib2/pilimi-zlib2-0-14679999-extra/11078831
def make_temp_anon_zlib_path(zlibrary_id, pilimi_torrent):
    prefix = "zlib1"
    if "-zlib2-" in pilimi_torrent:
        prefix = "zlib2"
    return f"e/{prefix}/{pilimi_torrent.replace('.torrent', '')}/{zlibrary_id}"

def make_temp_anon_aac_path(prefix, file_aac_id, data_folder):
    date = data_folder.split('__')[3][0:8]
    return f"{prefix}/{date}/{data_folder}/{file_aac_id}"

def strip_description(description):
    first_pass = re.sub(r'<[^<]+?>', r' ', re.sub(r'<a.+?href="([^"]+)"[^>]*>', r'(\1) ', description.replace('</p>', '\n\n').replace('</P>', '\n\n').replace('<br>', '\n').replace('<BR>', '\n').replace('<br/>', '\n').replace('<br />', '\n').replace('<BR/>', '\n').replace('<BR />', '\n')))
    return '\n'.join([row for row in [row.strip() for row in first_pass.split('\n')] if row != ''])


# A mapping of countries to languages, for those countries that have a clear single spoken language.
# Courtesy of a friendly LLM.. beware of hallucinations!
country_lang_mapping = { "Albania": "Albanian", "Algeria": "Arabic", "Andorra": "Catalan", "Argentina": "Spanish", "Armenia": "Armenian", 
"Azerbaijan": "Azerbaijani", "Bahrain": "Arabic", "Bangladesh": "Bangla", "Belarus": "Belorussian", "Benin": "French", 
"Bhutan": "Dzongkha", "Brazil": "Portuguese", "Brunei Darussalam": "Malay", "Bulgaria": "Bulgarian", "Cambodia": "Khmer", 
"Caribbean Community": "English", "Chile": "Spanish", "China": "Mandarin", "Colombia": "Spanish", "Costa Rica": "Spanish", 
"Croatia": "Croatian", "Cuba": "Spanish", "Cur": "Papiamento", "Cyprus": "Greek", "Denmark": "Danish", 
"Dominican Republic": "Spanish", "Ecuador": "Spanish", "Egypt": "Arabic", "El Salvador": "Spanish", "Estonia": "Estonian", 
"Finland": "Finnish", "France": "French", "Gambia": "English", "Georgia": "Georgian", "Ghana": "English", "Greece": "Greek", 
"Guatemala": "Spanish", "Honduras": "Spanish", "Hungary": "Hungarian", "Iceland": "Icelandic", "Indonesia": "Bahasa Indonesia", 
"Iran": "Persian", "Iraq": "Arabic", "Israel": "Hebrew", "Italy": "Italian", "Japan": "Japanese", "Jordan": "Arabic", 
"Kazakhstan": "Kazak", "Kuwait": "Arabic", "Latvia": "Latvian", "Lebanon": "Arabic", "Libya": "Arabic", "Lithuania": "Lithuanian", 
"Malaysia": "Malay", "Maldives": "Dhivehi", "Mexico": "Spanish", "Moldova": "Moldovan", "Mongolia": "Mongolian", 
"Myanmar": "Burmese", "Namibia": "English", "Nepal": "Nepali", "Netherlands": "Dutch", "Nicaragua": "Spanish", 
"North Macedonia": "Macedonian", "Norway": "Norwegian", "Oman": "Arabic", "Pakistan": "Urdu", "Palestine": "Arabic", 
"Panama": "Spanish", "Paraguay": "Spanish", "Peru": "Spanish", "Philippines": "Filipino", "Poland": "Polish", "Portugal": "Portuguese", 
"Qatar": "Arabic", "Romania": "Romanian", "Saudi Arabia": "Arabic", "Slovenia": "Slovenian", "South Pacific": "English", "Spain": "Spanish", 
"Srpska": "Serbian", "Sweden": "Swedish", "Thailand": "Thai", "Turkey": "Turkish", "Ukraine": "Ukrainian", 
"United Arab Emirates": "Arabic", "United States": "English", "Uruguay": "Spanish", "Venezuela": "Spanish", "Vietnam": "Vietnamese" }

# @functools.cache
# def get_e5_small_model():
#     return sentence_transformers.SentenceTransformer("intfloat/multilingual-e5-small")

# @functools.cache
# def get_tiktoken_text_embedding_3_small():
#     for attempt in range(1,100):
#         try:
#             return tiktoken.encoding_for_model("text-embedding-3-small")
#         except:
#             if attempt > 20:
#                 raise

@functools.cache
def get_bcp47_lang_codes_parse_substr(substr):
    lang = ''
    debug_from = []
    try:
        lang = str(langcodes.standardize_tag(langcodes.get(substr), macro=True))
        debug_from.append('langcodes.get')
    except langcodes.tag_parser.LanguageTagError:
        for country_name, language_name in country_lang_mapping.items():
            # Be careful not to use `in` here, or if we do then watch out for overlap, e.g. "Oman" in "Romania".
            if country_name.lower() == substr.lower():
                try:
                    lang = str(langcodes.standardize_tag(langcodes.find(language_name), macro=True))
                    debug_from.append(f"langcodes.find with country_lang_mapping {country_name.lower()=} == {substr.lower()=}")
                except LookupError:
                    pass
                break
        if lang == '':
            try:
                lang = str(langcodes.standardize_tag(langcodes.find(substr), macro=True))
                debug_from.append('langcodes.find WITHOUT country_lang_mapping')
            except LookupError:
                # In rare cases, disambiguate by saying that `substr` is written in English
                try:
                    lang = str(langcodes.standardize_tag(langcodes.find(substr, language='en'), macro=True))
                    debug_from.append('langcodes.find with language=en')
                except LookupError:
                    lang = ''
    # Further specification is unnecessary for most languages, except Traditional Chinese.
    if ('-' in lang) and (lang != 'zh-Hant'):
        lang = lang.split('-', 1)[0]
        debug_from.append('split on dash')
    # We have a bunch of weird data that gets interpreted as "Egyptian Sign Language" when it's
    # clearly all just Spanish..
    if lang == 'esl':
        lang = 'es'
        debug_from.append('esl to es')
    # Seems present within ISBNdb, and just means "en".
    if lang == 'us':
        lang = 'en'
        debug_from.append('us to en')
    # "urdu" not being converted to "ur" seems to be a bug in langcodes?
    if lang == 'urdu':
        lang = 'ur'
        debug_from.append('urdu to ur')
    # Same
    if lang == 'thai':
        lang = 'ur'
        debug_from.append('thai to ur')
    # Same
    if lang == 'esp':
        lang = 'eo'
        debug_from.append('esp to eo')
    # Same
    if lang == 'ndl':
        lang = 'nl'
        debug_from.append('ndl to nl')
    if lang in ['und', 'mul', 'mis']:
        lang = ''
        debug_from.append('delete und/mul/mis')
    # print(f"{debug_from=}")
    return lang

@functools.cache
def get_bcp47_lang_codes(string):
    potential_codes = list()
    potential_codes.append(get_bcp47_lang_codes_parse_substr(string))
    for substr in re.split(r'[-_,;/]', string):
        potential_codes.append(get_bcp47_lang_codes_parse_substr(substr.strip()))
    return list(dict.fromkeys([code for code in potential_codes if code != '']))

# Stable, since we rely on the first remaining the first.
def combine_bcp47_lang_codes(sets_of_codes):
    combined_codes = {}
    for codes in sets_of_codes:
        for code in codes:
            combined_codes[code] = 1
    return list(combined_codes.keys())

@functools.cache
def get_display_name_for_lang(lang_code, display_lang):
    result = langcodes.Language.make(lang_code).display_name(display_lang)
    if '[' not in result:
        result = result + ' [' + lang_code + ']'
    return result.replace(' []', '')

def add_comments_to_dict(before_dict, comments):
    after_dict = {}
    for key, value in before_dict.items():
        if key in comments:
            comment = comments[key]
            comment_content = comment[1][0] if len(comment[1]) == 1 else comment[1]
            if comment[0] == 'before':
                # Triple-slashes means it shouldn't be put on the previous line by nice_json.
                after_dict["///" + key] = comment_content
            after_dict[key] = value
            if comment[0] == 'after':
                after_dict["//" + key] = comment_content
        else:
            after_dict[key] = value
    return after_dict

@page.get("/")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def home_page():
    if allthethings.utils.DOWN_FOR_MAINTENANCE:
        return render_template("page/maintenance.html", header_active="")

    torrents_data = get_torrents_data()
    return render_template("page/home.html", header_active="home/home", torrents_data=torrents_data)

@page.get("/login")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def login_page():
    return redirect("/account", code=301)
    # return render_template("page/login.html", header_active="account")

@page.get("/about")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def about_page():
    return redirect("/faq", code=301)

@page.get("/faq")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def faq_page():
    popular_ids = [
        "md5:8336332bf5877e3adbfb60ac70720cd5", # Against intellectual monopoly
        "md5:61a1797d76fc9a511fb4326f265c957b", # Cryptonomicon
        "md5:0d9b713d0dcda4c9832fcb056f3e4102", # Aaron Swartz
        "md5:6963187473f4f037a28e2fe1153ca793", # How music got free
        "md5:6ed2d768ec1668c73e4fa742e3df78d6", # Physics
    ]
    with Session(engine):
        aarecords = (get_aarecords_elasticsearch(popular_ids) or [])
        aarecords.sort(key=lambda aarecord: popular_ids.index(aarecord['id']))

        return render_template(
            "page/faq.html",
            header_active="home/faq",
            aarecords=aarecords,
        )

@page.get("/security")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def security_page():
    return redirect("/faq#security", code=301)

@page.get("/mobile")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def mobile_page():
    return redirect("/faq#mobile", code=301)

@page.get("/llm")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def llm_page():
    return render_template("page/llm.html", header_active="home/llm")

@page.get("/browser_verification")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def browser_verification_page():
    return render_template("page/browser_verification.html", header_active="home/search")

@cachetools.cached(cache=cachetools.TTLCache(maxsize=30000, ttl=24*60*60), lock=threading.Lock())
def get_stats_data():
    with engine.connect() as connection:
        cursor = allthethings.utils.get_cursor_ping_conn(connection)

        cursor.execute('SELECT TimeLastModified FROM libgenrs_updated ORDER BY ID DESC LIMIT 1')
        libgenrs_time = allthethings.utils.fetch_one_field(cursor)
        libgenrs_date = str(libgenrs_time.date()) if libgenrs_time is not None else ''

        cursor.execute('SELECT time_last_modified FROM libgenli_files ORDER BY f_id DESC LIMIT 1')
        libgenli_time = allthethings.utils.fetch_one_field(cursor)
        libgenli_date = str(libgenli_time.date()) if libgenli_time is not None else ''

        # OpenLibrary author keys seem randomly distributed, so some random prefix is good enough.
        cursor.execute("SELECT last_modified FROM ol_base WHERE ol_key LIKE '/authors/OL111%' ORDER BY last_modified DESC LIMIT 1")
        openlib_time = allthethings.utils.fetch_one_field(cursor)
        openlib_date = str(openlib_time.date()) if openlib_time is not None else ''

        cursor.execute('SELECT aacid FROM annas_archive_meta__aacid__ia2_acsmpdf_files ORDER BY aacid DESC LIMIT 1')
        ia_aacid = allthethings.utils.fetch_one_field(cursor)
        ia_date_raw = ia_aacid.split('__')[2][0:8]
        ia_date = f"{ia_date_raw[0:4]}-{ia_date_raw[4:6]}-{ia_date_raw[6:8]}"

        # WARNING! Sorting by primary ID does a lexical sort, not numerical. Sorting by zlib3_records.aacid gets records from refreshes. zlib3_files.aacid is most reliable.
        cursor.execute('SELECT annas_archive_meta__aacid__zlib3_records.byte_offset, annas_archive_meta__aacid__zlib3_records.byte_length FROM annas_archive_meta__aacid__zlib3_records JOIN annas_archive_meta__aacid__zlib3_files USING (primary_id) ORDER BY annas_archive_meta__aacid__zlib3_files.aacid DESC LIMIT 1')
        zlib3_record = cursor.fetchone()
        zlib_date = ''
        if zlib3_record is not None:
            zlib_aac_lines = allthethings.utils.get_lines_from_aac_file(cursor, 'zlib3_records', [(zlib3_record['byte_offset'], zlib3_record['byte_length'])])
            if len(zlib_aac_lines) > 0:
                zlib_date = orjson.loads(zlib_aac_lines[0])['metadata']['date_modified']

        cursor.execute('SELECT aacid FROM annas_archive_meta__aacid__duxiu_files ORDER BY aacid DESC LIMIT 1')
        duxiu_file_aacid = cursor.fetchone()['aacid']
        duxiu_file_date_raw = duxiu_file_aacid.split('__')[2][0:8]
        duxiu_file_date = f"{duxiu_file_date_raw[0:4]}-{duxiu_file_date_raw[4:6]}-{duxiu_file_date_raw[6:8]}"

        cursor.execute('SELECT aacid FROM annas_archive_meta__aacid__upload_files ORDER BY aacid DESC LIMIT 1')
        upload_file_aacid = cursor.fetchone()['aacid']
        upload_file_date_raw = upload_file_aacid.split('__')[2][0:8]
        upload_file_date = f"{upload_file_date_raw[0:4]}-{upload_file_date_raw[4:6]}-{upload_file_date_raw[6:8]}"

        nexusstc_date = 'Unknown'
        try:
            cursor.execute('SELECT aacid FROM annas_archive_meta__aacid__nexusstc_records ORDER BY aacid DESC LIMIT 1')
            nexusstc_aacid = cursor.fetchone()['aacid']
            nexusstc_date_raw = nexusstc_aacid.split('__')[2][0:8]
            nexusstc_date = f"{nexusstc_date_raw[0:4]}-{nexusstc_date_raw[4:6]}-{nexusstc_date_raw[6:8]}"
        except:
            pass

        edsebk_date = 'Unknown'
        try:
            cursor.execute('SELECT aacid FROM annas_archive_meta__aacid__ebscohost_records ORDER BY aacid DESC LIMIT 1')
            edsebk_aacid = cursor.fetchone()['aacid']
            edsebk_date_raw = edsebk_aacid.split('__')[2][0:8]
            edsebk_date = f"{edsebk_date_raw[0:4]}-{edsebk_date_raw[4:6]}-{edsebk_date_raw[6:8]}"
        except:
            pass

        stats_data_es = dict(es.msearch(
            request_timeout=30,
            max_concurrent_searches=10,
            max_concurrent_shard_requests=10,
            searches=[
                { "index": allthethings.utils.all_virtshards_for_index("aarecords") },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": {
                        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } },
                        "search_bulk_torrents": { "terms": { "field": "search_only_fields.search_bulk_torrents", "include": "has_bulk_torrents" } },
                    },
                },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": {
                        "search_record_sources": {
                            "terms": { "field": "search_only_fields.search_record_sources" },
                            "aggs": {
                                "search_filesize": { "sum": { "field": "search_only_fields.search_filesize" } },
                                "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } },
                                "search_bulk_torrents": { "terms": { "field": "search_only_fields.search_bulk_torrents", "include": "has_bulk_torrents" } },
                            },
                        },
                    },
                },
            ],
        ))
        stats_data_esaux = dict(es_aux.msearch(
            request_timeout=30,
            max_concurrent_searches=10,
            max_concurrent_shard_requests=10,
            searches=[
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": {
                        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } },
                        "search_bulk_torrents": { "terms": { "field": "search_only_fields.search_bulk_torrents", "include": "has_bulk_torrents" } },
                    },
                },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": { "search_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } },
                },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": {
                        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } },
                        "search_bulk_torrents": { "terms": { "field": "search_only_fields.search_bulk_torrents", "include": "has_bulk_torrents" } },
                    },
                },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_digital_lending") },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
            ],
        ))
        responses_without_timed_out = [response for response in (stats_data_es['responses'] + stats_data_esaux['responses']) if 'timed_out' not in response]
        if len(responses_without_timed_out) > 0:
            raise Exception(f"One of the 'get_stats_data' responses didn't have 'timed_out' field in it: {responses_without_timed_out=}")
        if any([response['timed_out'] for response in (stats_data_es['responses'] + stats_data_esaux['responses'])]):
            # WARNING: don't change this message because we match on 'timed out' below
            raise Exception("One of the 'get_stats_data' responses timed out")

        # print(f'{orjson.dumps(stats_data_es)=}')
        print(f'{orjson.dumps(stats_data_esaux)=}')

        stats_by_group = {
            'lgrs': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'journals': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'lgli': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'zlib': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'zlibzh': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'ia': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'duxiu': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'upload': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'magzdb': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'nexusstc': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
            'edsebk': {'count': 0, 'filesize': 0, 'aa_count': 0, 'torrent_count': 0},
        }
        for bucket in stats_data_es['responses'][2]['aggregations']['search_record_sources']['buckets']:
            stats_by_group[bucket['key']] = {
                'count': bucket['doc_count'],
                'filesize': bucket['search_filesize']['value'],
                'aa_count': bucket['search_access_types']['buckets'][0]['doc_count'] if len(bucket['search_access_types']['buckets']) > 0 else 0,
                'torrent_count': bucket['search_bulk_torrents']['buckets'][0]['doc_count'] if len(bucket['search_bulk_torrents']['buckets']) > 0 else 0,
            }
        stats_by_group['journals'] = {
            'count': stats_data_esaux['responses'][2]['hits']['total']['value'],
            'filesize': stats_data_esaux['responses'][2]['aggregations']['search_filesize']['value'],
            'aa_count': stats_data_esaux['responses'][3]['aggregations']['search_access_types']['buckets'][0]['doc_count'] if len(stats_data_esaux['responses'][3]['aggregations']['search_access_types']['buckets']) > 0 else 0,
            'torrent_count': stats_data_esaux['responses'][3]['aggregations']['search_bulk_torrents']['buckets'][0]['doc_count'] if len(stats_data_esaux['responses'][3]['aggregations']['search_bulk_torrents']['buckets']) > 0 else 0,
        }
        stats_by_group['total'] = {
            'count': stats_data_es['responses'][0]['hits']['total']['value']+stats_data_esaux['responses'][0]['hits']['total']['value'],
            'filesize': stats_data_es['responses'][0]['aggregations']['total_filesize']['value']+stats_data_esaux['responses'][0]['aggregations']['total_filesize']['value'],
            'aa_count': (stats_data_es['responses'][1]['aggregations']['search_access_types']['buckets'][0]['doc_count'] if len(stats_data_es['responses'][1]['aggregations']['search_access_types']['buckets']) > 0 else 0)+(stats_data_esaux['responses'][1]['aggregations']['search_access_types']['buckets'][0]['doc_count'] if len(stats_data_esaux['responses'][1]['aggregations']['search_access_types']['buckets']) > 0 else 0),
            'torrent_count': (stats_data_es['responses'][1]['aggregations']['search_bulk_torrents']['buckets'][0]['doc_count'] if len(stats_data_es['responses'][1]['aggregations']['search_bulk_torrents']['buckets']) > 0 else 0)+(stats_data_esaux['responses'][1]['aggregations']['search_bulk_torrents']['buckets'][0]['doc_count'] if len(stats_data_esaux['responses'][1]['aggregations']['search_bulk_torrents']['buckets']) > 0 else 0),
        }
        stats_by_group['ia']['count'] += stats_data_esaux['responses'][4]['hits']['total']['value']
        stats_by_group['total']['count'] += stats_data_esaux['responses'][4]['hits']['total']['value']
        stats_by_group['ia']['filesize'] += stats_data_esaux['responses'][4]['aggregations']['total_filesize']['value']
        stats_by_group['total']['filesize'] += stats_data_esaux['responses'][4]['aggregations']['total_filesize']['value']
        stats_by_group['total']['count'] -= stats_by_group['zlibzh']['count']
        stats_by_group['total']['filesize'] -= stats_by_group['zlibzh']['filesize']
        stats_by_group['total']['aa_count'] -= stats_by_group['zlibzh']['aa_count']
        stats_by_group['total']['torrent_count'] -= stats_by_group['zlibzh']['torrent_count']

    return {
        'stats_by_group': stats_by_group,
        'libgenrs_date': libgenrs_date,
        'libgenli_date': libgenli_date,
        'openlib_date': openlib_date,
        'zlib_date': zlib_date,
        'ia_date': ia_date,
        'upload_file_date': upload_file_date,
        'duxiu_date': duxiu_file_date,
        'isbndb_date': '2022-09-01',
        'isbn_country_date': '2022-02-11',
        'oclc_date': '2023-10-01',
        'magzdb_date': '2024-07-29',
        'nexusstc_date': nexusstc_date,
        'edsebk_date': edsebk_date,
    }

def torrent_group_data_from_file_path(file_path):
    group = file_path.split('/')[2]
    aac_meta_group = None
    aac_meta_prefix = 'torrents/managed_by_aa/annas_archive_meta__aacid/annas_archive_meta__aacid__'
    if file_path.startswith(aac_meta_prefix):
        aac_meta_group = file_path[len(aac_meta_prefix):].split('__', 1)[0]
        group = aac_meta_group
    aac_data_prefix = 'torrents/managed_by_aa/annas_archive_data__aacid/annas_archive_data__aacid__'
    if file_path.startswith(aac_data_prefix):
        group = file_path[len(aac_data_prefix):].split('__', 1)[0]
    if 'zlib3' in file_path:
        group = 'zlib'
    if '_ia2_' in file_path:
        group = 'ia'
    if 'duxiu' in file_path:
        group = 'duxiu'
    if 'upload' in file_path:
        group = 'upload'
    if 'magzdb_records' in file_path: # To not get magzdb from 'upload' collection.
        group = 'magzdb'
    if 'nexusstc' in file_path:
        group = 'nexusstc'
    if 'ebscohost_records' in file_path:
        group = 'other_metadata'

    return { 'group': group, 'aac_meta_group': aac_meta_group }

@cachetools.cached(cache=cachetools.TTLCache(maxsize=1024, ttl=30*60), lock=threading.Lock())
def get_torrents_data():
    with mariapersist_engine.connect() as connection:
        cursor = allthethings.utils.get_cursor_ping_conn(connection)
        # cursor.execute('SELECT mariapersist_small_files.created, mariapersist_small_files.file_path, mariapersist_small_files.metadata, s.metadata AS scrape_metadata, s.created AS scrape_created FROM mariapersist_small_files LEFT JOIN (SELECT mariapersist_torrent_scrapes.* FROM mariapersist_torrent_scrapes INNER JOIN (SELECT file_path, MAX(created) AS max_created FROM mariapersist_torrent_scrapes GROUP BY file_path) s2 ON (mariapersist_torrent_scrapes.file_path = s2.file_path AND mariapersist_torrent_scrapes.created = s2.max_created)) s USING (file_path) WHERE mariapersist_small_files.file_path LIKE "torrents/managed_by_aa/%" GROUP BY mariapersist_small_files.file_path ORDER BY created ASC, scrape_created DESC LIMIT 50000')
        cursor.execute('SELECT created, file_path, metadata FROM mariapersist_small_files WHERE mariapersist_small_files.file_path LIKE "torrents/%" ORDER BY created, file_path LIMIT 50000')
        small_files = list(cursor.fetchall())
        cursor.execute('SELECT * FROM mariapersist_torrent_scrapes INNER JOIN (SELECT file_path, MAX(created) AS max_created FROM mariapersist_torrent_scrapes GROUP BY file_path) s2 ON (mariapersist_torrent_scrapes.file_path = s2.file_path AND mariapersist_torrent_scrapes.created = s2.max_created)')
        scrapes_by_file_path = { row['file_path']: row for row in list(cursor.fetchall()) }

        group_sizes = collections.defaultdict(int)
        group_num_files = collections.defaultdict(int)
        small_file_dicts_grouped_aa = collections.defaultdict(list)
        small_file_dicts_grouped_external = collections.defaultdict(list)
        small_file_dicts_grouped_other_aa = collections.defaultdict(list)
        aac_meta_file_paths_grouped = collections.defaultdict(list)
        seeder_sizes = collections.defaultdict(int)
        for small_file in small_files:
            metadata = orjson.loads(small_file['metadata'])
            toplevel = small_file['file_path'].split('/')[1]

            torrent_group_data = torrent_group_data_from_file_path(small_file['file_path'])
            group = torrent_group_data['group']
            if torrent_group_data['aac_meta_group'] is not None:
                aac_meta_file_paths_grouped[torrent_group_data['aac_meta_group']].append(small_file['file_path'])

            scrape_row = scrapes_by_file_path.get(small_file['file_path'])
            scrape_metadata = {"scrape":{}}
            scrape_created = datetime.datetime.utcnow()
            if scrape_row is not None:
                scrape_created = scrape_row['created']
                scrape_metadata = orjson.loads(scrape_row['metadata'])
                if (metadata.get('embargo') or False) is False:
                    if scrape_metadata['scrape']['seeders'] < 4:
                        seeder_sizes[0] += metadata['data_size']
                    elif scrape_metadata['scrape']['seeders'] < 11:
                        seeder_sizes[1] += metadata['data_size']
                    else:
                        seeder_sizes[2] += metadata['data_size']

            group_sizes[group] += metadata['data_size']
            group_num_files[group] += metadata.get('num_files') or 0
            if toplevel == 'external':
                list_to_add = small_file_dicts_grouped_external[group]
            elif toplevel == 'other_aa':
                list_to_add = small_file_dicts_grouped_other_aa[group]
            else:
                list_to_add = small_file_dicts_grouped_aa[group]
            display_name = small_file['file_path'].split('/')[-1]
            list_to_add.append({
                "created": small_file['created'].strftime("%Y-%m-%d"), # First, so it gets sorted by first. Also, only year-month-day, so it gets secondarily sorted by file path.
                "file_path": small_file['file_path'],
                "metadata": metadata, 
                "aa_currently_seeding": allthethings.utils.aa_currently_seeding(metadata),
                "size_string": format_filesize(metadata['data_size']), 
                "file_path_short": small_file['file_path'].replace('torrents/managed_by_aa/annas_archive_meta__aacid/', '').replace('torrents/managed_by_aa/annas_archive_data__aacid/', '').replace(f'torrents/managed_by_aa/{group}/', '').replace(f'torrents/external/{group}/', '').replace(f'torrents/other_aa/{group}/', ''),
                "display_name": display_name, 
                "scrape_metadata": scrape_metadata, 
                "scrape_created": scrape_created, 
                "is_metadata": (('annas_archive_meta__' in small_file['file_path']) or ('.sql' in small_file['file_path']) or ('-index-' in small_file['file_path']) or ('-derived' in small_file['file_path']) or ('isbndb' in small_file['file_path']) or ('covers-' in small_file['file_path']) or ('-metadata-' in small_file['file_path']) or ('-thumbs' in small_file['file_path']) or ('.csv' in small_file['file_path'])),
                "magnet_link": f"magnet:?xt=urn:btih:{metadata['btih']}&dn={urllib.parse.quote(display_name)}&tr=udp://tracker.opentrackr.org:1337/announce",
                "temp_uuid": shortuuid.uuid(),
                "partially_broken": (small_file['file_path'] in allthethings.utils.TORRENT_PATHS_PARTIALLY_BROKEN),
                "torrent_code": 'torrent:' + small_file['file_path'].replace('torrents/','')
            })

        for key in small_file_dicts_grouped_external:
            small_file_dicts_grouped_external[key] = natsort.natsorted(small_file_dicts_grouped_external[key], key=lambda x: list(x.values()))
        for key in small_file_dicts_grouped_aa:
            small_file_dicts_grouped_aa[key] = natsort.natsorted(small_file_dicts_grouped_aa[key], key=lambda x: list(x.values()))
        for key in small_file_dicts_grouped_other_aa:
            small_file_dicts_grouped_other_aa[key] = natsort.natsorted(small_file_dicts_grouped_other_aa[key], key=lambda x: list(x.values()))

        obsolete_file_paths = [
            'torrents/managed_by_aa/zlib/pilimi-zlib-index-2022-06-28.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics0__shoutout_to_tosec.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics1__adopted_by_yperion.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics2__never_give_up_against_elsevier.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics4__for_science.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics3.0__hone_the_hachette.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics3.1__adopted_by_oskanios.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/c_2022_12_thousand_dirs.torrent',
            'torrents/managed_by_aa/libgenli_comics/c_2022_12_thousand_dirs_magz.torrent',
            'torrents/managed_by_aa/annas_archive_data__aacid/annas_archive_data__aacid__upload_files_duxiu_epub__20240510T045054Z--20240510T045055Z.torrent',
        ]
        for file_path_list in aac_meta_file_paths_grouped.values():
            obsolete_file_paths += file_path_list[0:-1]
        for item in small_file_dicts_grouped_other_aa['aa_derived_mirror_metadata'][0:-1]:
            obsolete_file_paths.append(item['file_path'])

        # Tack on "obsolete" fields, now that we have them
        for group in list(small_file_dicts_grouped_aa.values()) + list(small_file_dicts_grouped_external.values()) + list(small_file_dicts_grouped_other_aa.values()):
            for item in group:
                item['obsolete'] = (item['file_path'] in obsolete_file_paths)

        # TODO: exclude obsolete
        group_size_strings = { group: format_filesize(total) for group, total in group_sizes.items() }
        seeder_size_strings = { index: format_filesize(seeder_sizes[index]) for index in [0,1,2] }

        return {
            'small_file_dicts_grouped': {
                'managed_by_aa': dict(sorted(small_file_dicts_grouped_aa.items())),
                'external': dict(sorted(small_file_dicts_grouped_external.items())),
                'other_aa': dict(sorted(small_file_dicts_grouped_other_aa.items())),
            },
            'group_size_strings': group_size_strings,
            'group_num_files': group_num_files,
            'seeder_size_strings': seeder_size_strings,
            'seeder_sizes': seeder_sizes,
            'seeder_size_total_string': format_filesize(sum(seeder_sizes.values())),
        }

@page.get("/datasets")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/ia")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_ia_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_ia.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/duxiu")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_duxiu_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_duxiu.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/uploads")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_uploads_page():
    return redirect(f"/datasets/upload", code=302)

@page.get("/datasets/upload")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_upload_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_upload.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/zlibzh")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_zlibzh_page():
    return redirect(f"/datasets/zlib", code=302)

@page.get("/datasets/zlib")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_zlib_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_zlib.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/isbndb")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_isbndb_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_isbndb.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/scihub")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_scihub_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_scihub.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/libgen_rs")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_libgen_rs_page():
    return redirect(f"/datasets/lgrs", code=302)

@page.get("/datasets/lgrs")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_lgrs_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_lgrs.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/libgen_li")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_libgen_li_page():
    return redirect(f"/datasets/lgli", code=302)

@page.get("/datasets/lgli")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_lgli_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_lgli.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

    return redirect(f"/datasets/ol", code=302)

@page.get("/datasets/openlib")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_openlib_page():
    return redirect(f"/datasets/ol", code=302)

@page.get("/datasets/ol")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_ol_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_ol.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/worldcat")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_worldcat_page():
    return redirect(f"/datasets/oclc", code=302)

@page.get("/datasets/oclc")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_oclc_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_oclc.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/magzdb")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_magzdb_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_magzdb.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/nexusstc")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_nexusstc_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_nexusstc.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/edsebk")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_edsebk_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_edsebk.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

# @page.get("/datasets/isbn_ranges")
# @allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
# def datasets_isbn_ranges_page():
#     try:
#         stats_data = get_stats_data()
#     except Exception as e:
#         if 'timed out' in str(e):
#             return "Error with datasets page, please try again.", 503
#     return render_template("page/datasets_isbn_ranges.html", header_active="home/datasets", stats_data=stats_data)

@page.get("/copyright")
@allthethings.utils.no_cache()
def copyright_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return render_template("page/login_to_view.html", header_active="")
    return render_template("page/copyright.html", header_active="")

@page.get("/volunteering")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def volunteering_page():
    return render_template("page/volunteering.html", header_active="home/volunteering")

@page.get("/metadata")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def metadata_page():
    return render_template("page/metadata.html", header_active="home/metadata")

@page.get("/contact")
@allthethings.utils.no_cache()
def contact_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return render_template("page/login_to_view.html", header_active="")
    return render_template("page/contact.html", header_active="", AA_EMAIL=AA_EMAIL)

@page.get("/fast_download_no_more")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def fast_download_no_more_page():
    return render_template("page/fast_download_no_more.html", header_active="")

@page.get("/fast_download_not_member")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def fast_download_not_member_page():
    return render_template("page/fast_download_not_member.html", header_active="")

@page.get("/torrents")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def torrents_page():
    torrents_data = get_torrents_data()

    with mariapersist_engine.connect() as connection:
        cursor = allthethings.utils.get_cursor_ping_conn(connection)
        cursor.execute('SELECT * FROM mariapersist_torrent_scrapes_histogram WHERE day > DATE_FORMAT(NOW() - INTERVAL 60 DAY, "%Y-%m-%d") AND day < DATE_FORMAT(NOW() - INTERVAL 1 DAY, "%Y-%m-%d") ORDER BY day, seeder_group LIMIT 500')
        histogram = list(cursor.fetchall())

        return render_template(
            "page/torrents.html",
            header_active="home/torrents",
            torrents_data=torrents_data,
            histogram=histogram,
            detailview=False,
        )

@page.get("/torrents/<string:group>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def torrents_group_page(group):
    torrents_data = get_torrents_data()

    group_found = False
    for top_level in torrents_data['small_file_dicts_grouped'].keys():
        if group in torrents_data['small_file_dicts_grouped'][top_level]:
            torrents_data = {
                **torrents_data,
                'small_file_dicts_grouped': { top_level: { group: torrents_data['small_file_dicts_grouped'][top_level][group] } }
            }
            group_found = True
            break
    if not group_found:
        return "", 404

    return render_template(
        "page/torrents.html",
        header_active="home/torrents",
        torrents_data=torrents_data,
        detailview=True,
    )

@page.get("/member_codes")
@allthethings.utils.no_cache()
def member_codes_page():
    prefix_arg = request.args.get('prefix') or ''
    if len(prefix_arg) > 0:
        prefix_b64_redirect = base64.b64encode(prefix_arg.encode()).decode()
        return redirect(f"/member_codes?prefix_b64={prefix_b64_redirect}", code=301)

    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return render_template("page/login_to_view.html", header_active="")

    with Session(mariapersist_engine) as mariapersist_session:
        account_fast_download_info = allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id)
        if account_fast_download_info is None:
            prefix_b64 = request.args.get('prefix_b64') or ''
            return redirect(f"/codes?prefix_b64={prefix_b64}", code=302)
    return codes_page()

@page.get("/codes")
@page.post("/codes")
@allthethings.utils.no_cache()
def codes_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return render_template("page/login_to_view.html", header_active="")

    with engine.connect() as connection:
        prefix_arg = request.args.get('prefix') or ''
        if len(prefix_arg) > 0:
            prefix_b64_redirect = base64.b64encode(prefix_arg.encode()).decode()
            return redirect(f"/member_codes?prefix_b64={prefix_b64_redirect}", code=301)

        prefix_b64 = request.args.get('prefix_b64') or ''
        try:
            prefix_bytes = base64.b64decode(prefix_b64.replace(' ', '+'))
        except Exception:
            return "Invalid prefix_b64", 404

        cursor = allthethings.utils.get_cursor_ping_conn(connection)

        # TODO: Since 'code' and 'aarecord_id' are binary, this might not work with multi-byte UTF-8 chars. Test (and fix) that!

        cursor.execute("DROP FUNCTION IF EXISTS fn_get_next_codepoint")
        cursor.execute("""
            CREATE FUNCTION fn_get_next_codepoint(initial INT, prefix VARCHAR(200)) RETURNS INT
            NOT DETERMINISTIC
            READS SQL DATA
            BEGIN
                    DECLARE _next VARCHAR(200);
                    DECLARE EXIT HANDLER FOR NOT FOUND RETURN 0;
                    SELECT  ORD(SUBSTRING(code, LENGTH(prefix)+1, 1))
                    INTO    _next
                    FROM    aarecords_codes
                    WHERE   code LIKE CONCAT(REPLACE(REPLACE(prefix, "%%", "\\%%"), "_", "\\_"), "%%") AND code >= CONCAT(prefix, CHAR(initial + 1))
                    ORDER BY
                            code
                    LIMIT 1;
                    RETURN _next;
            END
        """)

        exact_matches_aarecord_ids = []
        new_prefixes = []
        hit_max_exact_matches = False

        if prefix_bytes == b'':
            cursor.execute('SELECT code_prefix FROM aarecords_codes_prefixes')
            new_prefixes = [row['code_prefix'] + b':' for row in list(cursor.fetchall())]
        else:
            max_exact_matches = 100
            cursor.execute('SELECT aarecord_id FROM aarecords_codes WHERE code = %(prefix)s ORDER BY code, aarecord_id LIMIT %(max_exact_matches)s', { "prefix": prefix_bytes, "max_exact_matches": max_exact_matches })
            exact_matches_aarecord_ids = [row['aarecord_id'].decode() for row in cursor.fetchall()]
            if len(exact_matches_aarecord_ids) == max_exact_matches:
                hit_max_exact_matches = True

            # cursor.execute('SELECT CONCAT(%(prefix)s, IF(@r > 0, CHAR(@r USING utf8), "")) AS new_prefix, @r := fn_get_next_codepoint(IF(@r > 0, @r, ORD(" ")), %(prefix)s) AS next_letter FROM (SELECT @r := ORD(SUBSTRING(code, LENGTH(%(prefix)s)+1, 1)) FROM aarecords_codes WHERE code >= %(prefix)s ORDER BY code LIMIT 1) vars, (SELECT 1 FROM aarecords_codes LIMIT 1000) iterator WHERE @r IS NOT NULL', { "prefix": prefix })
            cursor.execute('SELECT CONCAT(%(prefix)s, CHAR(@r USING binary)) AS new_prefix, @r := fn_get_next_codepoint(@r, %(prefix)s) AS next_letter FROM (SELECT @r := ORD(SUBSTRING(code, LENGTH(%(prefix)s)+1, 1)) FROM aarecords_codes WHERE code > %(prefix)s AND code LIKE CONCAT(REPLACE(REPLACE(%(prefix)s, "%%", "\\%%"), "_", "\\_"), "%%") ORDER BY code LIMIT 1) vars, (SELECT 1 FROM aarecords_codes LIMIT 10000) iterator WHERE @r != 0', { "prefix": prefix_bytes })
            new_prefixes_raw = list(cursor.fetchall())
            new_prefixes = [row['new_prefix'] for row in new_prefixes_raw]
            # print(f"{new_prefixes_raw=}")

        prefix_rows = []
        for new_prefix in new_prefixes:
            # TODO: more efficient? Though this is not that bad because we don't typically iterate through that many values.
            cursor.execute('SELECT code, row_number_order_by_code, dense_rank_order_by_code FROM aarecords_codes WHERE code LIKE CONCAT(REPLACE(REPLACE(%(new_prefix)s, "%%", "\\%%"), "_", "\\_"), "%%") ORDER BY code, aarecord_id LIMIT 1', { "new_prefix": new_prefix })
            first_record = cursor.fetchone()
            cursor.execute('SELECT code, row_number_order_by_code, dense_rank_order_by_code FROM aarecords_codes WHERE code LIKE CONCAT(REPLACE(REPLACE(%(new_prefix)s, "%%", "\\%%"), "_", "\\_"), "%%") ORDER BY code DESC, aarecord_id DESC LIMIT 1', { "new_prefix": new_prefix })
            last_record = cursor.fetchone()

            if (first_record['code'] == last_record['code']) and (prefix_bytes != b''):
                code = first_record["code"]
                code_label = code.decode(errors='replace')
                code_b64 = base64.b64encode(code).decode()
                prefix_rows.append({
                    "label": code_label,
                    "records": last_record["row_number_order_by_code"]-first_record["row_number_order_by_code"]+1,
                    "link": f'/member_codes?prefix_b64={code_b64}',
                })
            else:
                longest_prefix = new_prefix
                if prefix_bytes != b'':
                    longest_prefix = os.path.commonprefix([first_record["code"], last_record["code"]])
                longest_prefix_label = longest_prefix.decode(errors='replace')
                longest_prefix_b64 = base64.b64encode(longest_prefix).decode()
                prefix_rows.append({
                    "label": f'{longest_prefix_label}⋯',
                    "codes": last_record["dense_rank_order_by_code"]-first_record["dense_rank_order_by_code"]+1,
                    "records": last_record["row_number_order_by_code"]-first_record["row_number_order_by_code"]+1,
                    "link": f'/member_codes?prefix_b64={longest_prefix_b64}',
                    "code_item": allthethings.utils.make_code_for_display(longest_prefix_label[:-1], '') if prefix_bytes == b'' else None,
                })

        bad_unicode = False
        try:
            prefix_bytes.decode()
        except Exception:
            bad_unicode = True

        prefix_label = prefix_bytes.decode(errors='replace')
        code_item = None
        if ':' in prefix_label:
            key, value = prefix_label.split(':', 1)
            code_item = allthethings.utils.make_code_for_display(key, value)

        return render_template(
            "page/codes.html",
            header_active="home/codes",
            prefix_label=prefix_label,
            prefix_rows=prefix_rows,
            aarecords=get_aarecords_elasticsearch(exact_matches_aarecord_ids),
            hit_max_exact_matches=hit_max_exact_matches,
            bad_unicode=bad_unicode,
            code_item=code_item,
        )

zlib_book_dict_comments = {
    **allthethings.utils.COMMON_DICT_COMMENTS,
    "zlibrary_id": ("before", ["This is a file from the Z-Library collection of Anna's Archive.",
                      "More details at https://annas-archive.se/datasets/zlib",
                      "The source URL is http://bookszlibb74ugqojhzhg2a63w5i2atv5bqarulgczawnbmsb6s6qead.onion/md5/<md5_reported>",
                      allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
    "edition_varia_normalized": ("after", ["Anna's Archive version of the 'series', 'volume', 'edition', and 'year' fields; combining them into a single field for display and search."]),
    "in_libgen": ("after", ["Whether at the time of indexing, the book was also available in Libgen."]),
    "pilimi_torrent": ("after", ["Which torrent by Anna's Archive (formerly the Pirate Library Mirror or 'pilimi') the file belongs to."]),
    "filesize_reported": ("after", ["The file size as reported by the Z-Library metadata. Is sometimes different from the actually observed file size of the file, as determined by Anna's Archive."]),
    "md5_reported": ("after", ["The md5 as reported by the Z-Library metadata. Is sometimes different from the actually observed md5 of the file, as determined by Anna's Archive."]),
    "unavailable": ("after", ["Set when Anna's Archive was unable to download the book."]),
    "filesize": ("after", ["The actual filesize as determined by Anna's Archive. Missing for AAC zlib3 records"]),
    "category_id": ("after", ["Z-Library's own categorization system; currently only present for AAC zlib3 records (and not actually used yet)"]),
    "file_data_folder": ("after", ["The AAC data folder / torrent that contains this file"]),
    "record_aacid": ("after", ["The AACID of the corresponding metadata entry in the zlib3_records collection"]),
    "file_aacid": ("after", ["The AACID of the corresponding metadata entry in the zlib3_files collection (corresponding to the data filename)"]),
    "cover_url_guess": ("after", ["Anna's Archive best guess of the cover URL, based on the MD5."]),
    "removed": ("after", ["Whether the file has been removed from Z-Library. We typically don't know the precise reason."]),
}
def zlib_add_edition_varia_normalized(zlib_book_dict):
    edition_varia_normalized = []
    if len((zlib_book_dict.get('series') or '').strip()) > 0:
        edition_varia_normalized.append(zlib_book_dict['series'].strip())
    if len((zlib_book_dict.get('volume') or '').strip()) > 0:
        edition_varia_normalized.append(zlib_book_dict['volume'].strip())
    if len((zlib_book_dict.get('edition') or '').strip()) > 0:
        edition_varia_normalized.append(zlib_book_dict['edition'].strip())
    if len((zlib_book_dict.get('year') or '').strip()) > 0:
        edition_varia_normalized.append(zlib_book_dict['year'].strip())
    zlib_book_dict['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

def zlib_cover_url_guess(md5):
    # return f"https://static.z-lib.gs/covers/books/{md5[0:2]}/{md5[2:4]}/{md5[4:6]}/{md5}.jpg"
    return ""

def get_zlib_book_dicts(session, key, values):
    if len(values) == 0:
        return []

    cursor = allthethings.utils.get_cursor_ping(session)
    zlib_books = []
    try:
        cursor.execute(f'SELECT * FROM zlib_book WHERE `{key}` IN %(values)s', { 'values': values })
        zlib_books = cursor.fetchall()

        ids = [str(book['zlibrary_id']) for book in zlib_books]
        cursor.execute('SELECT * FROM zlib_isbn WHERE zlibrary_id IN %(ids)s', { 'ids': ids })
        zlib_isbns = cursor.fetchall()

        for book in zlib_books:
            if 'isbns' not in book or book['isbns'] is None:
                book['isbns'] = []

            for isbn in zlib_isbns:
                if isbn['zlibrary_id'] == book['zlibrary_id']:
                    book['isbns'].append(isbn)
    except Exception as err:
        print(f"Error in get_zlib_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    zlib_book_dicts = []
    for zlib_book in zlib_books:
        zlib_book_dict = zlib_book
        zlib_book_dict['stripped_description'] = strip_description(zlib_book_dict['description'])
        zlib_book_dict['language_codes'] = get_bcp47_lang_codes(zlib_book_dict['language'] or '')
        zlib_book_dict['cover_url_guess'] = zlib_cover_url_guess(zlib_book_dict['md5_reported'])
        zlib_book_dict['added_date_unified'] = { "date_zlib_source": zlib_book_dict['date_added'].split('T', 1)[0] }
        zlib_add_edition_varia_normalized(zlib_book_dict)

        allthethings.utils.init_identifiers_and_classification_unified(zlib_book_dict)
        allthethings.utils.add_identifier_unified(zlib_book_dict, 'zlib', zlib_book_dict['zlibrary_id'])
        if zlib_book_dict['md5'] is not None:
            allthethings.utils.add_identifier_unified(zlib_book_dict, 'md5', zlib_book_dict['md5'])
        if zlib_book_dict['md5_reported'] is not None:
            allthethings.utils.add_identifier_unified(zlib_book_dict, 'md5', zlib_book_dict['md5_reported'])
        allthethings.utils.add_isbns_unified(zlib_book_dict, [record['isbn'] for record in zlib_book['isbns']])
        allthethings.utils.add_isbns_unified(zlib_book_dict, allthethings.utils.get_isbnlike(zlib_book_dict['description']))

        zlib_book_dicts.append(add_comments_to_dict(zlib_book_dict, zlib_book_dict_comments))
    return zlib_book_dicts

def get_aac_zlib3_book_dicts(session, key, values):
    if len(values) == 0:
        return []
    if key == 'zlibrary_id':
        aac_key = 'annas_archive_meta__aacid__zlib3_records.primary_id'
    elif key == 'md5':
        aac_key = 'annas_archive_meta__aacid__zlib3_files.md5'
    elif key == 'md5_reported':
        aac_key = 'annas_archive_meta__aacid__zlib3_records.md5'
    else:
        raise Exception(f"Unexpected 'key' in get_aac_zlib3_book_dicts: '{key}'")
    aac_zlib3_books = []
    try:
        cursor = allthethings.utils.get_cursor_ping(session)
        cursor.execute(f'SELECT annas_archive_meta__aacid__zlib3_records.byte_offset AS record_byte_offset, annas_archive_meta__aacid__zlib3_records.byte_length AS record_byte_length, annas_archive_meta__aacid__zlib3_files.byte_offset AS file_byte_offset, annas_archive_meta__aacid__zlib3_files.byte_length AS file_byte_length, annas_archive_meta__aacid__zlib3_records.primary_id AS primary_id FROM annas_archive_meta__aacid__zlib3_records LEFT JOIN annas_archive_meta__aacid__zlib3_files USING (primary_id) WHERE {aac_key} IN %(values)s', { "values": [str(value) for value in values] })
        
        zlib3_rows = []
        zlib3_records_indexes = []
        zlib3_records_offsets_and_lengths = []
        zlib3_files_indexes = []
        zlib3_files_offsets_and_lengths = []
        for row_index, row in enumerate(list(cursor.fetchall())):
            zlib3_records_indexes.append(row_index)
            zlib3_records_offsets_and_lengths.append((row['record_byte_offset'], row['record_byte_length']))
            if row.get('file_byte_offset') is not None:
                zlib3_files_indexes.append(row_index)
                zlib3_files_offsets_and_lengths.append((row['file_byte_offset'], row['file_byte_length']))
            zlib3_rows.append({ "primary_id": row['primary_id'] })
        for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'zlib3_records', zlib3_records_offsets_and_lengths)):
            zlib3_rows[zlib3_records_indexes[index]]['record'] = orjson.loads(line_bytes)
        for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'zlib3_files', zlib3_files_offsets_and_lengths)):
            zlib3_rows[zlib3_files_indexes[index]]['file'] = orjson.loads(line_bytes)

        raw_aac_zlib3_books_by_primary_id = collections.defaultdict(list)
        aac_zlib3_books_by_primary_id = collections.defaultdict(dict)
        # Merge different iterations of books, so even when a book gets "missing":1 later, we still use old
        # metadata where available (note: depends on the sorting below).
        for row in zlib3_rows:
            raw_aac_zlib3_books_by_primary_id[row['primary_id']].append(row),
            new_row = aac_zlib3_books_by_primary_id[row['primary_id']]
            new_row['primary_id'] = row['primary_id']
            if 'file' in row:
                new_row['file'] = row['file']
            new_row['record'] = {
                **(new_row.get('record') or {}),
                **row['record'],
                'metadata': {
                    **((new_row.get('record') or {}).get('metadata') or {}),
                    **row['record']['metadata'],
                }
            }
        aac_zlib3_books = list(aac_zlib3_books_by_primary_id.values())
    except Exception as err:
        print(f"Error in get_aac_zlib3_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    aac_zlib3_book_dicts = []
    for zlib_book in aac_zlib3_books:
        aac_zlib3_book_dict = { **zlib_book['record']['metadata'] }
        if 'file' in zlib_book:
            aac_zlib3_book_dict['md5'] = zlib_book['file']['metadata']['md5']
            if 'filesize' in zlib_book['file']['metadata']:
                aac_zlib3_book_dict['filesize'] = zlib_book['file']['metadata']['filesize']
            aac_zlib3_book_dict['file_aacid'] = zlib_book['file']['aacid']
            aac_zlib3_book_dict['file_data_folder'] = zlib_book['file']['data_folder']
        else:
            aac_zlib3_book_dict['md5'] = None
            aac_zlib3_book_dict['filesize'] = None
            aac_zlib3_book_dict['file_aacid'] = None
            aac_zlib3_book_dict['file_data_folder'] = None
        aac_zlib3_book_dict['record_aacid'] = zlib_book['record']['aacid']

        if 'annabookinfo' in aac_zlib3_book_dict and len(aac_zlib3_book_dict['annabookinfo']['errors']) == 0:
            aac_zlib3_book_dict['ipfs_cid'] = aac_zlib3_book_dict['annabookinfo']['response']['ipfs_cid']
            aac_zlib3_book_dict['ipfs_cid_blake2b'] = aac_zlib3_book_dict['annabookinfo']['response']['ipfs_cid_blake2b']
            aac_zlib3_book_dict['storage'] = aac_zlib3_book_dict['annabookinfo']['response']['storage']
            if (aac_zlib3_book_dict['annabookinfo']['response']['identifier'] is not None) and (aac_zlib3_book_dict['annabookinfo']['response']['identifier'] != ''):
                aac_zlib3_book_dict['isbns'].append(aac_zlib3_book_dict['annabookinfo']['response']['identifier'])
            aac_zlib3_book_dict['deleted_comment'] = aac_zlib3_book_dict['annabookinfo']['response']['deleted_comment']

        if 'description' not in aac_zlib3_book_dict:
            print(f'WARNING WARNING! missing description in aac_zlib3_book_dict: {aac_zlib3_book_dict=} {zlib_book=}')
            print('------------------')
        aac_zlib3_book_dict['stripped_description'] = strip_description(aac_zlib3_book_dict['description'])
        aac_zlib3_book_dict['language_codes'] = get_bcp47_lang_codes(aac_zlib3_book_dict['language'] or '')
        aac_zlib3_book_dict['cover_url_guess'] = zlib_cover_url_guess(aac_zlib3_book_dict['md5_reported'])
        aac_zlib3_book_dict['added_date_unified'] = { "date_zlib_source": aac_zlib3_book_dict['date_added'].split('T', 1)[0] }
        zlib_add_edition_varia_normalized(aac_zlib3_book_dict)

        allthethings.utils.init_identifiers_and_classification_unified(aac_zlib3_book_dict)
        allthethings.utils.add_identifier_unified(aac_zlib3_book_dict, 'aacid', aac_zlib3_book_dict['record_aacid'])
        if aac_zlib3_book_dict['file_aacid'] is not None:
            allthethings.utils.add_identifier_unified(aac_zlib3_book_dict, 'aacid', aac_zlib3_book_dict['file_aacid'])
        allthethings.utils.add_identifier_unified(aac_zlib3_book_dict, 'zlib', aac_zlib3_book_dict['zlibrary_id'])
        if aac_zlib3_book_dict['md5'] is not None:
            allthethings.utils.add_identifier_unified(aac_zlib3_book_dict, 'md5', aac_zlib3_book_dict['md5'])
        if aac_zlib3_book_dict['md5_reported'] is not None:
            allthethings.utils.add_identifier_unified(aac_zlib3_book_dict, 'md5', aac_zlib3_book_dict['md5_reported'])
        allthethings.utils.add_isbns_unified(aac_zlib3_book_dict, aac_zlib3_book_dict['isbns'])
        allthethings.utils.add_isbns_unified(aac_zlib3_book_dict, allthethings.utils.get_isbnlike(aac_zlib3_book_dict['description']))

        aac_zlib3_book_dict['raw_aac'] = raw_aac_zlib3_books_by_primary_id[str(aac_zlib3_book_dict['zlibrary_id'])]

        aac_zlib3_book_dicts.append(add_comments_to_dict(aac_zlib3_book_dict, zlib_book_dict_comments))
    return aac_zlib3_book_dicts

@page.get("/db/zlib/<int:zlib_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def zlib_book_json(zlib_id):
    with Session(engine) as session:
        zlib_book_dicts = get_zlib_book_dicts(session, "zlibrary_id", [zlib_id])
        if len(zlib_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(zlib_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/aac_zlib3/<int:zlib_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac_zlib3_book_json(zlib_id):
    with Session(engine) as session:
        aac_zlib3_book_dicts = get_aac_zlib3_book_dicts(session, "zlibrary_id", [zlib_id])
        if len(aac_zlib3_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(aac_zlib3_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def extract_list_from_ia_json_field(ia_record_dict, key):
    val = ia_record_dict['json'].get('metadata', {}).get(key, [])
    if isinstance(val, str):
        return [val]
    return val

def get_ia_record_dicts(session, key, values):
    if len(values) == 0:
        return []

    seen_ia_ids = set()
    ia_entries = []
    ia_entries2 = []
    cursor = allthethings.utils.get_cursor_ping(session)
    try:
        base_query = ('SELECT m.*, f.*, ia2f.* FROM aa_ia_2023_06_metadata m '
                          'LEFT JOIN aa_ia_2023_06_files f USING(ia_id) '
                          'LEFT JOIN annas_archive_meta__aacid__ia2_acsmpdf_files ia2f ON m.ia_id = ia2f.primary_id ')
        base_query2 = ('SELECT ia2r.*, f.*, ia2f.* FROM annas_archive_meta__aacid__ia2_records ia2r '
                           'LEFT JOIN aa_ia_2023_06_files f ON f.ia_id = ia2r.primary_id '
                           'LEFT JOIN annas_archive_meta__aacid__ia2_acsmpdf_files ia2f USING (primary_id) ')
        column_count_query1 = [4, 4, 5] # aa_ia_2023_06_metadata, aa_ia_2023_06_files, annas_archive_meta__aacid__ia2_acsmpdf_files
        column_count_query2 = [5, 4, 5] # annas_archive_meta__aacid__ia2_records, aa_ia_2023_06_files, annas_archive_meta__aacid__ia2_acsmpdf_files

        if key == 'md5':
            # TODO: we should also consider matching on libgen_md5, but we used to do that before and it had bad SQL performance,
            # when combined in a single query, so we'd have to split it up.
            # TODO: We get extra records this way, because we might include files from both AaIa202306Files and
            # Ia2AcsmpdfFiles if they both exist. It might be better to split this up here so we don't have to filter later.

            cursor.execute(base_query + 'WHERE f.md5 IN %(values)s', { 'values': values })
            ia_entries = list(cursor.fetchall())

            cursor.execute(base_query + 'WHERE ia2f.md5 IN %(values)s', { 'values': values })
            ia_entries += list(cursor.fetchall())

            cursor.execute(base_query2 + 'WHERE f.md5 IN %(values)s', { 'values': values })
            ia_entries2 = list(cursor.fetchall())

            cursor.execute(base_query2 + 'WHERE ia2f.md5 IN %(values)s', { 'values': values })
            ia_entries2 += list(cursor.fetchall())

            ia_entries = allthethings.utils.split_columns(ia_entries, column_count_query1)
            ia_entries2 = allthethings.utils.split_columns(ia_entries2, column_count_query2)
        elif key == 'ia_id':
            cursor.execute(base_query + f'WHERE m.`{key}` IN %(values)s', { 'values': values })
            ia_entries = allthethings.utils.split_columns(list(cursor.fetchall()), column_count_query1)

            ia2r_key_column = key.replace('ia_id', 'primary_id')
            cursor.execute(base_query2 + f'WHERE ia2r.`{ia2r_key_column}` IN %(values)s', { 'values': values })
            ia_entries2 = allthethings.utils.split_columns(list(cursor.fetchall()), column_count_query2)
        else:
            raise Exception(f"Unexpected 'key' in get_ia_record_dicts: '{key}'")
    except Exception as err:
        print(f"Error in get_ia_record_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    ia_entries_combined = []
    ia2_records_indexes = []
    ia2_records_offsets_and_lengths = []
    ia2_acsmpdf_files_indexes = []
    ia2_acsmpdf_files_offsets_and_lengths = []
    # Prioritize ia_entries2 first, because their records are newer. This order matters
    # futher below.
    for ia_record_dict, ia_file_dict, ia2_acsmpdf_file_dict in ia_entries2 + ia_entries:
        # There are some rare cases where ia_file AND ia2_acsmpdf_file are set, so make
        # sure we create an entry for each.
        # TODO: We get extra records this way, because we might include files from both AaIa202306Files and
        # Ia2AcsmpdfFiles if they both exist. It might be better to split this up here so we don't have to filter later.
        if ia_file_dict is not None:
            if ia_record_dict.get('byte_offset') is not None:
                ia2_records_indexes.append(len(ia_entries_combined))
                ia2_records_offsets_and_lengths.append((ia_record_dict['byte_offset'], ia_record_dict['byte_length']))
            ia_entries_combined.append([ia_record_dict, ia_file_dict, None])
        if ia2_acsmpdf_file_dict is not None:
            if ia_record_dict.get('byte_offset') is not None:
                ia2_records_indexes.append(len(ia_entries_combined))
                ia2_records_offsets_and_lengths.append((ia_record_dict['byte_offset'], ia_record_dict['byte_length']))
            ia2_acsmpdf_files_indexes.append(len(ia_entries_combined))
            ia2_acsmpdf_files_offsets_and_lengths.append((ia2_acsmpdf_file_dict['byte_offset'], ia2_acsmpdf_file_dict['byte_length']))
            ia_entries_combined.append([ia_record_dict, None, ia2_acsmpdf_file_dict])
        if ia_file_dict is None and ia2_acsmpdf_file_dict is None:
            if ia_record_dict.get('byte_offset') is not None:
                ia2_records_indexes.append(len(ia_entries_combined))
                ia2_records_offsets_and_lengths.append((ia_record_dict['byte_offset'], ia_record_dict['byte_length']))
            ia_entries_combined.append([ia_record_dict, None, None])

    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'ia2_records', ia2_records_offsets_and_lengths)):
        ia_entries_combined[ia2_records_indexes[index]][0] = orjson.loads(line_bytes)
    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'ia2_acsmpdf_files', ia2_acsmpdf_files_offsets_and_lengths)):
        ia_entries_combined[ia2_acsmpdf_files_indexes[index]][2] = orjson.loads(line_bytes)

    # print(f"{ia_entries_combined=}")
    # print(orjson.dumps(ia_entries_combined, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS, default=str).decode('utf-8'))

    ia_record_dicts = []
    for ia_record_dict, ia_file_dict, ia2_acsmpdf_file_dict in ia_entries_combined:
        if 'aacid' in ia_record_dict:
            # Convert from AAC.
            ia_record_dict = {
                "ia_id": ia_record_dict["metadata"]["ia_id"],
                "aacid": ia_record_dict["aacid"],
                # "has_thumb" # We'd need to look at both ia_entries2 and ia_entries to get this, but not worth it.
                "libgen_md5": None,
                "json": ia_record_dict["metadata"]['metadata_json'],
            }
            for external_id in extract_list_from_ia_json_field(ia_record_dict, 'external-identifier'):
                if 'urn:libgen:' in external_id:
                    ia_record_dict['libgen_md5'] = external_id.split('/')[-1]
                    break
        else:
            ia_record_dict = {
                "ia_id": ia_record_dict["ia_id"],
                # "has_thumb": ia_record_dict["has_thumb"],
                "libgen_md5": ia_record_dict["libgen_md5"],
                "json": orjson.loads(ia_record_dict["json"]),
            }

        # TODO: When querying by ia_id we can match multiple files. For now we just pick the first one.
        if key == 'ia_id':
            if ia_record_dict['ia_id'] in seen_ia_ids:
                continue
            seen_ia_ids.add(ia_record_dict['ia_id'])

        ia_record_dict['aa_ia_file'] = None
        added_date_unified_file = {}
        if ia_record_dict['libgen_md5'] is None: # If there's a Libgen MD5, then we do NOT serve our IA file.
            if ia_file_dict is not None:
                ia_record_dict['aa_ia_file'] = ia_file_dict
                ia_record_dict['aa_ia_file']['extension'] = 'pdf'
                added_date_unified_file = { "date_ia_file_scrape": "2023-06-28" }
            elif ia2_acsmpdf_file_dict is not None:
                ia_record_dict['aa_ia_file'] = {
                    'md5': ia2_acsmpdf_file_dict['metadata']['md5'].lower(),
                    'type': 'ia2_acsmpdf',
                    'filesize': ia2_acsmpdf_file_dict['metadata']['filesize'],
                    'ia_id': ia2_acsmpdf_file_dict['metadata']['ia_id'],
                    'extension': 'pdf',
                    'aacid': ia2_acsmpdf_file_dict['aacid'],
                    'data_folder': ia2_acsmpdf_file_dict['data_folder'],
                }
                added_date_unified_file = { "date_ia_file_scrape": datetime.datetime.strptime(ia2_acsmpdf_file_dict['aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat().split('T', 1)[0] }

        # TODO: It might be nice to filter this earlier?
        if key == 'md5':
            if ia_record_dict['aa_ia_file'] is None or ia_record_dict['aa_ia_file']['md5'] not in values:
                continue

        ia_collections = ((ia_record_dict['json'].get('metadata') or {}).get('collection') or [])

        ia_record_dict['aa_ia_derived'] = {}
        ia_record_dict['aa_ia_derived']['printdisabled_only'] = 'inlibrary' not in ia_collections
        ia_record_dict['aa_ia_derived']['original_filename'] = (ia_record_dict['ia_id'] + '.pdf') if ia_record_dict['aa_ia_file'] is not None else None
        ia_record_dict['aa_ia_derived']['cover_url'] = f"https://archive.org/download/{ia_record_dict['ia_id']}/__ia_thumb.jpg"
        ia_record_dict['aa_ia_derived']['title'] = (' '.join(extract_list_from_ia_json_field(ia_record_dict, 'title'))).replace(' : ', ': ')
        ia_record_dict['aa_ia_derived']['author'] = ('; '.join(extract_list_from_ia_json_field(ia_record_dict, 'creator') + extract_list_from_ia_json_field(ia_record_dict, 'associated-names'))).replace(' : ', ': ')
        ia_record_dict['aa_ia_derived']['publisher'] = ('; '.join(extract_list_from_ia_json_field(ia_record_dict, 'publisher'))).replace(' : ', ': ')
        ia_record_dict['aa_ia_derived']['combined_comments'] = [strip_description(comment) for comment in extract_list_from_ia_json_field(ia_record_dict, 'notes') + extract_list_from_ia_json_field(ia_record_dict, 'comment') + extract_list_from_ia_json_field(ia_record_dict, 'curation')]
        ia_record_dict['aa_ia_derived']['subjects'] = '\n\n'.join(extract_list_from_ia_json_field(ia_record_dict, 'subject') + extract_list_from_ia_json_field(ia_record_dict, 'level_subject'))
        ia_record_dict['aa_ia_derived']['stripped_description_and_references'] = strip_description('\n\n'.join(extract_list_from_ia_json_field(ia_record_dict, 'description') + extract_list_from_ia_json_field(ia_record_dict, 'references')))
        ia_record_dict['aa_ia_derived']['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes(lang) for lang in (extract_list_from_ia_json_field(ia_record_dict, 'language') + extract_list_from_ia_json_field(ia_record_dict, 'ocr_detected_lang'))])
        ia_record_dict['aa_ia_derived']['all_dates'] = list(dict.fromkeys(extract_list_from_ia_json_field(ia_record_dict, 'year') + extract_list_from_ia_json_field(ia_record_dict, 'date') + extract_list_from_ia_json_field(ia_record_dict, 'range')))
        ia_record_dict['aa_ia_derived']['longest_date_field'] = max([''] + ia_record_dict['aa_ia_derived']['all_dates'])
        ia_record_dict['aa_ia_derived']['year'] = ''
        for date in ([ia_record_dict['aa_ia_derived']['longest_date_field']] + ia_record_dict['aa_ia_derived']['all_dates']):
            potential_year = re.search(r"(\d\d\d\d)", date)
            if potential_year is not None:
                ia_record_dict['aa_ia_derived']['year'] = potential_year[0]
                break

        ia_record_dict['aa_ia_derived']['added_date_unified'] = {}
        publicdate = extract_list_from_ia_json_field(ia_record_dict, 'publicdate')
        if len(publicdate) > 0:
            if publicdate[0].encode('ascii', 'ignore').decode() != publicdate[0]:
                print(f"Warning: {publicdate[0]=} is not ASCII; skipping!")
            else:
                ia_record_dict['aa_ia_derived']['added_date_unified'] = { **added_date_unified_file, "date_ia_source": datetime.datetime.strptime(publicdate[0], "%Y-%m-%d %H:%M:%S").isoformat().split('T', 1)[0] }

        ia_record_dict['aa_ia_derived']['content_type'] = 'book_unknown'
        if ia_record_dict['ia_id'].split('_', 1)[0] in ['sim', 'per'] or extract_list_from_ia_json_field(ia_record_dict, 'pub_type') in ["Government Documents", "Historical Journals", "Law Journals", "Magazine", "Magazines", "Newspaper", "Scholarly Journals", "Trade Journals"]:
            ia_record_dict['aa_ia_derived']['content_type'] = 'magazine'

        ia_record_dict['aa_ia_derived']['edition_varia_normalized'] = ', '.join([
            *extract_list_from_ia_json_field(ia_record_dict, 'series'),
            *extract_list_from_ia_json_field(ia_record_dict, 'series_name'),
            *[f"Volume {volume}" for volume in extract_list_from_ia_json_field(ia_record_dict, 'volume')],
            *[f"Issue {issue}" for issue in extract_list_from_ia_json_field(ia_record_dict, 'issue')],
            *extract_list_from_ia_json_field(ia_record_dict, 'edition'),
            *extract_list_from_ia_json_field(ia_record_dict, 'city'),
            ia_record_dict['aa_ia_derived']['longest_date_field']
        ])

        if ia_record_dict.get('aacid') is not None:
            added_date_unified_file["date_ia_record_scrape"] = datetime.datetime.strptime(ia_record_dict['aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat().split('T', 1)[0]
        else:
            added_date_unified_file["date_ia_record_scrape"] = '2023-06-28'

        allthethings.utils.init_identifiers_and_classification_unified(ia_record_dict['aa_ia_derived'])
        allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'ocaid', ia_record_dict['ia_id'])
        if ia_record_dict.get('aacid') is not None:
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'aacid', ia_record_dict['aacid'])
        if ia_record_dict['libgen_md5'] is not None:
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'md5', ia_record_dict['libgen_md5'])
        if ia_record_dict['aa_ia_file'] is not None:
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'md5', ia_record_dict['aa_ia_file']['md5'])
            if ia_record_dict['aa_ia_file'].get('aacid') is not None:
                allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'aacid', ia_record_dict['aa_ia_file']['aacid'])
        for item in (extract_list_from_ia_json_field(ia_record_dict, 'openlibrary_edition') + extract_list_from_ia_json_field(ia_record_dict, 'openlibrary_work')):
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'ol', item)
        for item in extract_list_from_ia_json_field(ia_record_dict, 'item'):
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'lccn', item)
        for item in ia_collections:
            allthethings.utils.add_classification_unified(ia_record_dict['aa_ia_derived'], 'ia_collection', item)

        for urn in extract_list_from_ia_json_field(ia_record_dict, 'external-identifier'):
            if urn.startswith('urn:oclc:record:'):
                allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'oclc', urn[len('urn:oclc:record:'):])
            elif urn.startswith('urn:oclc:'):
                allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'oclc', urn[len('urn:oclc:'):])

        # Items in this collection have an insane number of ISBNs, unclear what for exactly. E.g. https://archive.org/details/240524-CL-aa
        if 'specialproject_exclude_list' not in ia_collections:
            isbns = extract_list_from_ia_json_field(ia_record_dict, 'isbn')
            for urn in extract_list_from_ia_json_field(ia_record_dict, 'external-identifier'):
                if urn.startswith('urn:isbn:'):
                    isbns.append(urn[len('urn:isbn:'):])
            allthethings.utils.add_isbns_unified(ia_record_dict['aa_ia_derived'], isbns)
            allthethings.utils.add_isbns_unified(ia_record_dict['aa_ia_derived'], allthethings.utils.get_isbnlike('\n'.join([ia_record_dict['ia_id'], ia_record_dict['aa_ia_derived']['title'], ia_record_dict['aa_ia_derived']['stripped_description_and_references']] + ia_record_dict['aa_ia_derived']['combined_comments'])))

        # Clear out title if it only contains the ISBN, but only *after* extracting ISBN from it.
        if ia_record_dict['aa_ia_derived']['title'].strip().lower() == ia_record_dict['ia_id'].strip().lower():
            ia_record_dict['aa_ia_derived']['title'] = ''
        condensed_title = ia_record_dict['aa_ia_derived']['title'].strip().lower().replace(' ', '').replace('_', '')
        if condensed_title.startswith('isbn') or condensed_title.startswith('bookisbn'):
            ia_record_dict['aa_ia_derived']['title'] = ''

        # TODO: add "reviews" array info as comments.

        aa_ia_derived_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "ia_id": ("before", ["This is an IA record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.se/datasets/ia",
                              "A lot of these fields are explained at https://archive.org/developers/metadata-schema/index.html",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "cover_url": ("before", "Constructed directly from ia_id."),
            "author": ("after", "From `metadata.creator` and `metadata.associated-names`."),
            "combined_comments": ("after", "From `metadata.notes`, `metadata.comment`, and `metadata.curation`."),
            "subjects": ("after", "From `metadata.subject` and `metadata.level_subject`."),
            "stripped_description_and_references": ("after", "From `metadata.description` and `metadata.references`, stripped from HTML tags."),
            "all_dates": ("after", "All potential dates, combined from `metadata.year`, `metadata.date`, and `metadata.range`."),
            "longest_date_field": ("after", "The longest field in `all_dates`."),
            "year": ("after", "Found by applying a \d{4} regex to `longest_date_field`."),
            "content_type": ("after", "Magazines determined by ia_id prefix (like 'sim_' and 'per_') and `metadata.pub_type` field."),
            "edition_varia_normalized": ("after", "From `metadata.series`, `metadata.series_name`, `metadata.volume`, `metadata.issue`, `metadata.edition`, `metadata.city`, and `longest_date_field`."),
        }
        ia_record_dict['aa_ia_derived'] = add_comments_to_dict(ia_record_dict['aa_ia_derived'], aa_ia_derived_comments)


        ia_record_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "ia_id": ("before", ["This is an IA record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.se/datasets/ia",
                              "A lot of these fields are explained at https://archive.org/developers/metadata-schema/index.html",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "libgen_md5": ("after", "If the metadata refers to a Libgen MD5 from which IA imported, it will be filled in here."),
            # "has_thumb": ("after", "Whether Anna's Archive has stored a thumbnail (scraped from __ia_thumb.jpg)."),
            "json": ("before", "The original metadata JSON, scraped from https://archive.org/metadata/<ia_id>.",
                               "We did strip out the full file list, since it's a bit long, and replaced it with a shorter `aa_shorter_files`."),
            "aa_ia_file": ("before", "File metadata, if we have it."),
            "aa_ia_derived": ("before", "Derived metadata."),
        }
        ia_record_dicts.append(add_comments_to_dict(ia_record_dict, ia_record_dict_comments))

    return ia_record_dicts

@page.get("/db/ia/<string:ia_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def ia_record_json(ia_id):
    with Session(engine) as session:
        ia_record_dicts = get_ia_record_dicts(session, "ia_id", [ia_id])
        if len(ia_record_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(ia_record_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def extract_ol_str_field(field):
    if field is None:
        return ""
    if type(field) in [str, float, int]:
        return field
    return str(field.get('value')) or ""

def extract_ol_author_field(field):
    if type(field) is str:
        return field
    elif 'author' in field:
        if type(field['author']) is str:
            return field['author']
        elif 'key' in field['author']:
            return field['author']['key']
    elif 'key' in field:
        return field['key']
    return ""

def get_ol_book_dicts(session, key, values):
    if key != 'ol_edition':
        raise Exception(f"Unsupported get_ol_dicts key: {key}")
    if not allthethings.utils.validate_ol_editions(values):
        raise Exception(f"Unsupported get_ol_dicts ol_edition value: {values}")
    if len(values) == 0:
        return []

    with engine.connect() as conn:
        cursor = allthethings.utils.get_cursor_ping_conn(conn)

        cursor.execute('SELECT * FROM ol_base WHERE ol_key IN %(ol_key)s', { 'ol_key': [f"/books/{ol_edition}" for ol_edition in values] })
        ol_books = cursor.fetchall()

        ol_book_dicts = []
        for ol_book in ol_books:
            ol_book_dict = {
                'ol_edition': ol_book['ol_key'].replace('/books/', ''),
                'edition': dict(ol_book),
            }
            ol_book_dict['edition']['json'] = orjson.loads(ol_book_dict['edition']['json'])
            ol_book_dicts.append(ol_book_dict)

        # Load works
        works_ol_keys = []
        for ol_book_dict in ol_book_dicts:
            ol_book_dict['work'] = None
            if 'works' in ol_book_dict['edition']['json'] and len(ol_book_dict['edition']['json']['works']) > 0:
                key = ol_book_dict['edition']['json']['works'][0]['key']
                works_ol_keys.append(key)
        if len(works_ol_keys) > 0:
            cursor.execute('SELECT * FROM ol_base WHERE ol_key IN %(ol_key)s', { 'ol_key': list(dict.fromkeys(works_ol_keys)) })
            ol_works_by_key = {ol_work['ol_key']: ol_work for ol_work in cursor.fetchall()}
            for ol_book_dict in ol_book_dicts:
                ol_book_dict['work'] = None
                if 'works' in ol_book_dict['edition']['json'] and len(ol_book_dict['edition']['json']['works']) > 0:
                    key = ol_book_dict['edition']['json']['works'][0]['key']
                    if key in ol_works_by_key:
                        ol_book_dict['work'] = dict(ol_works_by_key[key])
                        ol_book_dict['work']['json'] = orjson.loads(ol_book_dict['work']['json'])

        # Load authors
        author_keys = []
        author_keys_by_ol_edition = collections.defaultdict(list)
        for ol_book_dict in ol_book_dicts:
            if 'authors' in ol_book_dict['edition']['json'] and len(ol_book_dict['edition']['json']['authors']) > 0:
                for author in ol_book_dict['edition']['json']['authors']:
                    author_str = extract_ol_author_field(author)
                    if author_str != '' and author_str not in author_keys_by_ol_edition[ol_book_dict['ol_edition']]:
                        author_keys.append(author_str)
                        author_keys_by_ol_edition[ol_book_dict['ol_edition']].append(author_str)
            if ol_book_dict['work'] and 'authors' in ol_book_dict['work']['json']:
                for author in ol_book_dict['work']['json']['authors']:
                    author_str = extract_ol_author_field(author)
                    if author_str != '' and author_str not in author_keys_by_ol_edition[ol_book_dict['ol_edition']]:
                        author_keys.append(author_str)
                        author_keys_by_ol_edition[ol_book_dict['ol_edition']].append(author_str)
            ol_book_dict['authors'] = []

        if len(author_keys) > 0:
            author_keys = list(dict.fromkeys(author_keys))
            cursor.execute('SELECT * FROM ol_base WHERE ol_key IN %(ol_key)s', { 'ol_key': author_keys })
            unredirected_ol_authors = {ol_author['ol_key']: ol_author for ol_author in cursor.fetchall()}
            author_redirect_mapping = {}
            for unredirected_ol_author in list(unredirected_ol_authors.values()):
                if unredirected_ol_author['type'] == '/type/redirect':
                    json = orjson.loads(unredirected_ol_author.json)
                    if 'location' not in json:
                        continue
                    author_redirect_mapping[unredirected_ol_author['ol_key']] = json['location']
            redirected_ol_authors = []
            if len(author_redirect_mapping) > 0:
                cursor.execute('SELECT * FROM ol_base WHERE ol_key IN %(ol_key)s', { 'ol_key': [ol_key for ol_key in author_redirect_mapping.values() if ol_key not in author_keys] })
                redirected_ol_authors = {ol_author['ol_key']: ol_author for ol_author in cursor.fetchall()}
            for ol_book_dict in ol_book_dicts:
                ol_authors = []
                for author_ol_key in author_keys_by_ol_edition[ol_book_dict['ol_edition']]:
                    if author_ol_key in author_redirect_mapping:
                        remapped_author_ol_key = author_redirect_mapping[author_ol_key]
                        if remapped_author_ol_key in redirected_ol_authors:
                            ol_authors.append(redirected_ol_authors[remapped_author_ol_key])
                        elif remapped_author_ol_key in unredirected_ol_authors:
                            ol_authors.append(unredirected_ol_authors[remapped_author_ol_key])
                    elif author_ol_key in unredirected_ol_authors:
                        ol_authors.append(unredirected_ol_authors[author_ol_key])
                for author in ol_authors:
                    if author['type'] == '/type/redirect':
                        # Yet another redirect.. this is too much for now, skipping.
                        continue
                    if author['type'] == '/type/delete':
                        # Deleted, not sure how to handle this, skipping.
                        continue
                    if author['type'] != '/type/author':
                        print(f"Warning: found author without /type/author: {author}")
                        continue
                    author_dict = dict(author)
                    author_dict['json'] = orjson.loads(author_dict['json'])
                    ol_book_dict['authors'].append(author_dict)

        # Everything else
        for ol_book_dict in ol_book_dicts:
            allthethings.utils.init_identifiers_and_classification_unified(ol_book_dict['edition'])
            allthethings.utils.add_identifier_unified(ol_book_dict['edition'], 'ol', ol_book_dict['ol_edition'])
            allthethings.utils.add_isbns_unified(ol_book_dict['edition'], (ol_book_dict['edition']['json'].get('isbn_10') or []) + (ol_book_dict['edition']['json'].get('isbn_13') or []))
            for item in (ol_book_dict['edition']['json'].get('lc_classifications') or []):
                # https://openlibrary.org/books/OL52784454M
                if len(item) > 50:
                    continue
                allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['lc_classifications'], item)
            for item in (ol_book_dict['edition']['json'].get('dewey_decimal_class') or []):
                allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_decimal_class'], item)
            for item in (ol_book_dict['edition']['json'].get('dewey_number') or []):
                allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_number'], item)
            for classification_type, items in (ol_book_dict['edition']['json'].get('classifications') or {}).items():
                if classification_type in allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING:
                    # Sometimes identifiers are incorrectly in the classifications list
                    for item in items:
                        allthethings.utils.add_identifier_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[classification_type], item)
                    continue
                if classification_type not in allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
                    # TODO: Do a scrape / review of all classification types in OL.
                    print(f"Warning: missing classification_type: {classification_type}")
                    continue
                for item in items:
                    allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[classification_type], item)
            if ol_book_dict['work']:
                allthethings.utils.init_identifiers_and_classification_unified(ol_book_dict['work'])
                allthethings.utils.add_identifier_unified(ol_book_dict['work'], 'ol', ol_book_dict['work']['ol_key'].replace('/works/', ''))
                for item in (ol_book_dict['work']['json'].get('lc_classifications') or []):
                    allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['lc_classifications'], item)
                for item in (ol_book_dict['work']['json'].get('dewey_decimal_class') or []):
                    allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_decimal_class'], item)
                for item in (ol_book_dict['work']['json'].get('dewey_number') or []):
                    allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_number'], item)
                for classification_type, items in (ol_book_dict['work']['json'].get('classifications') or {}).items():
                    if classification_type == 'annas_archive':
                        print(f"Warning: annas_archive field mistakenly put in 'classifications' on work {ol_book_dict['work']['ol_key']=}")
                    if classification_type in allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING:
                        # Sometimes identifiers are incorrectly in the classifications list
                        for item in items:
                            allthethings.utils.add_identifier_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[classification_type], item)
                        continue
                    if classification_type not in allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
                        # TODO: Do a scrape / review of all classification types in OL.
                        print(f"Warning: missing classification_type: {classification_type}")
                        continue
                    for item in items:
                        allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[classification_type], item)
            for item in (ol_book_dict['edition']['json'].get('lccn') or []):
                if item is not None:
                    # For some reason there's a bunch of nulls in the raw data here.
                    allthethings.utils.add_identifier_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING['lccn'], item)
            for item in (ol_book_dict['edition']['json'].get('oclc_numbers') or []):
                allthethings.utils.add_identifier_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING['oclc_numbers'], item)
            if 'ocaid' in ol_book_dict['edition']['json']:
                allthethings.utils.add_identifier_unified(ol_book_dict['edition'], 'ocaid', ol_book_dict['edition']['json']['ocaid'])
            for identifier_type, items in (ol_book_dict['edition']['json'].get('identifiers') or {}).items():
                if 'isbn' in identifier_type or identifier_type == 'ean':
                    allthethings.utils.add_isbns_unified(ol_book_dict['edition'], items)
                    continue
                if identifier_type in allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
                    # Sometimes classifications are incorrectly in the identifiers list
                    for item in items:
                        allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[identifier_type], item)
                    continue
                if identifier_type not in allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING:
                    # TODO: Do a scrape / review of all identifier types in OL.
                    print(f"Warning: missing identifier_type: {identifier_type}")
                    continue
                for item in items:
                    allthethings.utils.add_identifier_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[identifier_type], item)

            ol_book_dict['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes((ol_languages.get(lang['key']) or {'name':lang['key']})['name']) for lang in (ol_book_dict['edition']['json'].get('languages') or [])])
            ol_book_dict['translated_from_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes((ol_languages.get(lang['key']) or {'name':lang['key']})['name']) for lang in (ol_book_dict['edition']['json'].get('translated_from') or [])])

            ol_book_dict['identifiers_unified'] = allthethings.utils.merge_unified_fields([ol_book_dict['edition']['identifiers_unified'], (ol_book_dict.get('work') or {'identifiers_unified': {}})['identifiers_unified']])
            ol_book_dict['classifications_unified'] = allthethings.utils.merge_unified_fields([ol_book_dict['edition']['classifications_unified'], (ol_book_dict.get('work') or {'classifications_unified': {}})['classifications_unified']])

            ol_book_dict['cover_url_normalized'] = ''
            if len(ol_book_dict['edition']['json'].get('covers') or []) > 0:
                ol_book_dict['cover_url_normalized'] = f"https://covers.openlibrary.org/b/id/{extract_ol_str_field(ol_book_dict['edition']['json']['covers'][0])}-L.jpg"
            elif ol_book_dict['work'] and len(ol_book_dict['work']['json'].get('covers') or []) > 0:
                ol_book_dict['cover_url_normalized'] = f"https://covers.openlibrary.org/b/id/{extract_ol_str_field(ol_book_dict['work']['json']['covers'][0])}-L.jpg"

            ol_book_dict['title_normalized'] = ''
            if len(ol_book_dict['title_normalized'].strip()) == 0 and 'title' in ol_book_dict['edition']['json']:
                if 'title_prefix' in ol_book_dict['edition']['json']:
                    ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['title_prefix']) + " " + extract_ol_str_field(ol_book_dict['edition']['json']['title'])
                else:
                    ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['title'])
            if len(ol_book_dict['title_normalized'].strip()) == 0 and ol_book_dict['work'] and 'title' in ol_book_dict['work']['json']:
                ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['work']['json']['title'])
            if len(ol_book_dict['title_normalized'].strip()) == 0 and len(ol_book_dict['edition']['json'].get('work_titles') or []) > 0:
                ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['work_titles'][0])
            if len(ol_book_dict['title_normalized'].strip()) == 0 and len(ol_book_dict['edition']['json'].get('work_titles') or []) > 0:
                ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['work_titles'][0])
            ol_book_dict['title_normalized'] = ol_book_dict['title_normalized'].replace(' : ', ': ')

            ol_book_dict['authors_normalized'] = ''
            if len(ol_book_dict['authors_normalized'].strip()) == 0 and 'by_statement' in ol_book_dict['edition']['json']:
                ol_book_dict['authors_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['by_statement']).strip()
            if len(ol_book_dict['authors_normalized'].strip()) == 0:
                ol_book_dict['authors_normalized'] = ", ".join([extract_ol_str_field(author['json']['name']) for author in ol_book_dict['authors'] if 'name' in author['json']])

            ol_book_dict['authors_normalized'] = ol_book_dict['authors_normalized'].replace(' ; ', '; ').replace(' , ', ', ')
            if ol_book_dict['authors_normalized'].endswith('.'):
                ol_book_dict['authors_normalized'] = ol_book_dict['authors_normalized'][0:-1]

            ol_book_dict['publishers_normalized'] = (", ".join([extract_ol_str_field(field) for field in ol_book_dict['edition']['json'].get('publishers') or []])).strip()
            if len(ol_book_dict['publishers_normalized']) == 0:
                ol_book_dict['publishers_normalized'] = (", ".join([extract_ol_str_field(field) for field in ol_book_dict['edition']['json'].get('distributors') or []])).strip()

            ol_book_dict['all_dates'] = [item.strip() for item in [
                extract_ol_str_field(ol_book_dict['edition']['json'].get('publish_date')),
                extract_ol_str_field(ol_book_dict['edition']['json'].get('copyright_date')),
                extract_ol_str_field(((ol_book_dict.get('work') or {}).get('json') or {}).get('first_publish_date')),
            ] if item and item.strip() != '']
            ol_book_dict['longest_date_field'] = max([''] + ol_book_dict['all_dates'])

            ol_book_dict['edition_varia_normalized'] = ", ".join([item.strip() for item in [
                *([extract_ol_str_field(field) for field in ol_book_dict['edition']['json'].get('series') or []]),
                extract_ol_str_field(ol_book_dict['edition']['json'].get('edition_name') or ''),
                *([extract_ol_str_field(field) for field in ol_book_dict['edition']['json'].get('publish_places') or []]),
                # TODO: translate?
                allthethings.utils.marc_country_code_to_english(extract_ol_str_field(ol_book_dict['edition']['json'].get('publish_country') or '')),
                ol_book_dict['longest_date_field'],
            ] if item and item.strip() != ''])

            for date in ([ol_book_dict['longest_date_field']] + ol_book_dict['all_dates']):
                potential_year = re.search(r"(\d\d\d\d)", date)
                if potential_year is not None:
                    ol_book_dict['year_normalized'] = potential_year[0]
                    break

            ol_book_dict['stripped_description'] = ''
            if len(ol_book_dict['stripped_description']) == 0 and 'description' in ol_book_dict['edition']['json']:
                ol_book_dict['stripped_description'] = strip_description(extract_ol_str_field(ol_book_dict['edition']['json']['description']))
            if len(ol_book_dict['stripped_description']) == 0 and ol_book_dict['work'] and 'description' in ol_book_dict['work']['json']:
                ol_book_dict['stripped_description'] = strip_description(extract_ol_str_field(ol_book_dict['work']['json']['description']))
            if len(ol_book_dict['stripped_description']) == 0 and 'first_sentence' in ol_book_dict['edition']['json']:
                ol_book_dict['stripped_description'] = strip_description(extract_ol_str_field(ol_book_dict['edition']['json']['first_sentence']))
            if len(ol_book_dict['stripped_description']) == 0 and ol_book_dict['work'] and 'first_sentence' in ol_book_dict['work']['json']:
                ol_book_dict['stripped_description'] = strip_description(extract_ol_str_field(ol_book_dict['work']['json']['first_sentence']))

            ol_book_dict['comments_normalized'] = [item.strip() for item in [
                extract_ol_str_field(ol_book_dict['edition']['json'].get('notes') or ''),
                extract_ol_str_field(((ol_book_dict.get('work') or {}).get('json') or {}).get('notes') or ''),
            ] if item and item.strip() != '']

            created_normalized = ''
            if len(created_normalized) == 0 and 'created' in ol_book_dict['edition']['json']:
                created_normalized = extract_ol_str_field(ol_book_dict['edition']['json']['created']).strip()
            if len(created_normalized) == 0 and ol_book_dict['work'] and 'created' in ol_book_dict['work']['json']:
                created_normalized = extract_ol_str_field(ol_book_dict['work']['json']['created']).strip()
            ol_book_dict['added_date_unified'] = {}
            if len(created_normalized) > 0:
                if '.' in created_normalized:
                    ol_book_dict['added_date_unified'] = { 'date_ol_source': datetime.datetime.strptime(created_normalized, '%Y-%m-%dT%H:%M:%S.%f').isoformat().split('T', 1)[0] }
                else:
                    ol_book_dict['added_date_unified'] = { 'date_ol_source': datetime.datetime.strptime(created_normalized, '%Y-%m-%dT%H:%M:%S').isoformat().split('T', 1)[0] }

            # {% for source_record in ol_book_dict.json.source_records %}
            #   <div class="flex odd:bg-black/5 hover:bg-black/64">
            #     <div class="flex-none w-[150] px-2 py-1">{{ 'Source records' if loop.index0 == 0 else ' ' }}&nbsp;</div>
            #     <div class="px-2 py-1 grow break-words line-clamp-[8]">{{source_record}}</div>
            #     <div class="px-2 py-1 whitespace-nowrap text-right">
            #       <!-- Logic roughly based on https://github.com/internetarchive/openlibrary/blob/e7e8aa5b/openlibrary/templates/history/sources.html#L27 -->
            #       {% if '/' not in source_record and '_meta.mrc:' in source_record %}
            #         <a href="https://openlibrary.org/show-records/ia:{{source_record | split('_') | first}}">url</a></div>
            #       {% else %}
            #         <a href="https://openlibrary.org/show-records/{{source_record | replace('marc:','')}}">url</a></div>
            #       {% endif %}
            #   </div>
            # {% endfor %}

        return ol_book_dicts

def get_ol_book_dicts_by_isbn13(session, isbn13s):
    if len(isbn13s) == 0:
        return {}
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT ol_key, isbn FROM ol_isbn13 WHERE isbn IN %(isbn13s)s', { "isbn13s": isbn13s })
        rows = list(cursor.fetchall())
        if len(rows) == 0:
            return {}
        isbn13s_by_ol_edition = collections.defaultdict(list)
        for row in rows:
            if row['ol_key'].startswith('/books/OL') and row['ol_key'].endswith('M'):
                ol_edition = row['ol_key'][len('/books/'):]
                isbn13s_by_ol_edition[ol_edition].append(row['isbn'])
        ol_book_dicts = get_ol_book_dicts(session, 'ol_edition', list(isbn13s_by_ol_edition.keys()))
        retval = collections.defaultdict(list)
        for ol_book_dict in ol_book_dicts:
            for isbn13 in isbn13s_by_ol_edition[ol_book_dict['ol_edition']]: 
                retval[isbn13].append(ol_book_dict)
        return dict(retval)

def get_ol_book_dicts_by_ia_id(session, ia_ids):
    if len(ia_ids) == 0:
        return {}
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT ol_key, ocaid FROM ol_ocaid WHERE ocaid IN %(ia_ids)s', { "ia_ids": [ia_id for ia_id in ia_ids if ia_id.isascii()] })
        rows = list(cursor.fetchall())
        if len(rows) == 0:
            return {}
        ia_ids_by_ol_edition = collections.defaultdict(list)
        for row in rows:
            if row['ol_key'].startswith('/books/OL') and row['ol_key'].endswith('M'):
                ol_edition = row['ol_key'][len('/books/'):]
                ia_ids_by_ol_edition[ol_edition].append(row['ocaid'])
        ol_book_dicts = get_ol_book_dicts(session, 'ol_edition', list(ia_ids_by_ol_edition.keys()))
        retval = collections.defaultdict(list)
        for ol_book_dict in ol_book_dicts:
            for ia_id in ia_ids_by_ol_edition[ol_book_dict['ol_edition']]: 
                retval[ia_id].append(ol_book_dict)
        return dict(retval)

def get_ol_book_dicts_by_annas_archive_md5(session, annas_archive_md5s):
    if len(annas_archive_md5s) == 0:
        return {}
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT ol_key, annas_archive_md5 FROM ol_annas_archive WHERE annas_archive_md5 IN %(annas_archive_md5s)s', { "annas_archive_md5s": annas_archive_md5s })
        rows = list(cursor.fetchall())
        if len(rows) == 0:
            return {}
        annas_archive_md5s_by_ol_edition = collections.defaultdict(list)
        for row in rows:
            if row['ol_key'].startswith('/books/OL') and row['ol_key'].endswith('M'):
                ol_edition = row['ol_key'][len('/books/'):]
                annas_archive_md5s_by_ol_edition[ol_edition].append(row['annas_archive_md5'])
        ol_book_dicts = get_ol_book_dicts(session, 'ol_edition', list(annas_archive_md5s_by_ol_edition.keys()))
        retval = collections.defaultdict(list)
        for ol_book_dict in ol_book_dicts:
            for annas_archive_md5 in annas_archive_md5s_by_ol_edition[ol_book_dict['ol_edition']]: 
                retval[annas_archive_md5].append(ol_book_dict)
        return dict(retval)

@page.get("/db/ol/<string:ol_edition>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def ol_book_json(ol_edition):
    with Session(engine) as session:
        ol_book_dicts = get_ol_book_dicts(session, "ol_edition", [ol_edition])
        if len(ol_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(ol_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_lgrsnf_book_dicts(session, key, values):
    if len(values) == 0:
        return []

    lgrsnf_books = []
    try:
        cursor = allthethings.utils.get_cursor_ping(session)

        # Hack: we explicitly name all the fields, because otherwise some get overwritten below due to lowercasing the column names.
        cursor.execute("SELECT lu.*, ld.descr, ld.toc, lh.crc32, lh.edonkey, lh.aich, lh.sha1, lh.tth, lh.torrent, lh.btih, lh.sha256, lh.ipfs_cid, lt.topic_descr "
                       "FROM libgenrs_updated lu "
                       "LEFT JOIN libgenrs_description ld ON lu.MD5 = ld.md5 "
                       "LEFT JOIN libgenrs_hashes lh ON lu.MD5 = lh.md5 "
                       "LEFT JOIN libgenrs_topics lt ON lu.Topic = lt.topic_id "
                       f"WHERE lt.lang = 'en' AND lu.`{key}` IN %(ids)s", { 'ids': values })
        lgrsnf_books = cursor.fetchall()
    except Exception as err:
        print(f"Error in get_lgrsnf_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    lgrs_book_dicts = []
    for lgrsnf_book in lgrsnf_books:
        lgrs_book_dict = dict((k.lower(), v) for k,v in dict(lgrsnf_book).items())
        lgrs_book_dict['stripped_description'] = strip_description('\n\n'.join(filter(len, list(dict.fromkeys([lgrs_book_dict.get('descr') or '', lgrs_book_dict.get('toc') or ''])))))
        lgrs_book_dict['language_codes'] = get_bcp47_lang_codes(lgrs_book_dict.get('language') or '')
        lgrs_book_dict['cover_url_normalized'] = f"https://libgen.rs/covers/{lgrs_book_dict['coverurl']}" if len(lgrs_book_dict.get('coverurl') or '') > 0 else ''

        lgrs_book_dict['added_date_unified'] = {}
        if lgrs_book_dict['timeadded'] != '0000-00-00 00:00:00':
            if not isinstance(lgrs_book_dict['timeadded'], datetime.datetime):
                raise Exception(f"Unexpected {lgrs_book_dict['timeadded']=} for {lgrs_book_dict=}")
            lgrs_book_dict['added_date_unified'] = { 'date_lgrsnf_source': lgrs_book_dict['timeadded'].isoformat().split('T', 1)[0] }

        edition_varia_normalized = []
        if len((lgrs_book_dict.get('series') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['series'].strip())
        if len((lgrs_book_dict.get('volume') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['volume'].strip())
        if len((lgrs_book_dict.get('edition') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['edition'].strip())
        if len((lgrs_book_dict.get('periodical') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['periodical'].strip())
        if len((lgrs_book_dict.get('year') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['year'].strip())
        lgrs_book_dict['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

        allthethings.utils.init_identifiers_and_classification_unified(lgrs_book_dict)
        allthethings.utils.add_identifier_unified(lgrs_book_dict, 'lgrsnf', lgrs_book_dict['id'])
        # .lower() on md5 is okay here, we won't miss any fetches since collation is _ci.
        allthethings.utils.add_identifier_unified(lgrs_book_dict, 'md5', lgrs_book_dict['md5'].lower())
        allthethings.utils.add_isbns_unified(lgrs_book_dict, lgrsnf_book['Identifier'].split(",") + lgrsnf_book['IdentifierWODash'].split(","))
        allthethings.utils.add_isbns_unified(lgrs_book_dict, allthethings.utils.get_isbnlike('\n'.join([lgrs_book_dict.get('descr') or '', lgrs_book_dict.get('locator') or '', lgrs_book_dict.get('toc') or ''])))
        allthethings.utils.add_classification_unified(lgrs_book_dict, 'lgrsnf_topic', lgrs_book_dict.get('topic_descr') or '')
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_IDENTIFIERS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_identifier_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_CLASSIFICATIONS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_classification_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])

        lgrs_book_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "id": ("before", ["This is a Libgen.rs Non-Fiction record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.se/datasets/lgrs",
                              "Most of these fields are explained at https://wiki.mhut.org/content:bibliographic_data",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        }
        lgrs_book_dicts.append(add_comments_to_dict(lgrs_book_dict, lgrs_book_dict_comments))

    return lgrs_book_dicts


def get_lgrsfic_book_dicts(session, key, values):
    if len(values) == 0:
        return []

    lgrsfic_books = []
    try:
        cursor = allthethings.utils.get_cursor_ping(session)

        # Hack: we explicitly name all the fields, because otherwise some get overwritten below due to lowercasing the column names.
        cursor.execute('SELECT lf.*, lfd.Descr, lfh.crc32, lfh.edonkey, lfh.aich, lfh.sha1, lfh.tth, lfh.btih, lfh.sha256, lfh.ipfs_cid '
                       'FROM libgenrs_fiction lf '
                       'LEFT JOIN libgenrs_fiction_description lfd ON lf.MD5 = lfd.MD5 '
                       'LEFT JOIN libgenrs_fiction_hashes lfh ON lf.MD5 = lfh.md5 '
                       f'WHERE lf.`{key}` IN %(ids)s',
                       { 'ids': values })
        lgrsfic_books = cursor.fetchall()
    except Exception as err:
        print(f"Error in get_lgrsfic_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    lgrs_book_dicts = []

    for lgrsfic_book in lgrsfic_books:
        lgrs_book_dict = dict((k.lower(), v) for k,v in dict(lgrsfic_book).items())
        lgrs_book_dict['stripped_description'] = strip_description(lgrs_book_dict.get('descr') or '')
        lgrs_book_dict['language_codes'] = get_bcp47_lang_codes(lgrs_book_dict.get('language') or '')
        lgrs_book_dict['cover_url_normalized'] = f"https://libgen.rs/fictioncovers/{lgrs_book_dict['coverurl']}" if len(lgrs_book_dict.get('coverurl') or '') > 0 else ''
        
        lgrs_book_dict['added_date_unified'] = {}
        if lgrs_book_dict['timeadded'] != '0000-00-00 00:00:00':
            if not isinstance(lgrs_book_dict['timeadded'], datetime.datetime):
                raise Exception(f"Unexpected {lgrs_book_dict['timeadded']=} for {lgrs_book_dict=}")
            lgrs_book_dict['added_date_unified'] = { 'date_lgrsfic_source': lgrs_book_dict['timeadded'].isoformat().split('T', 1)[0] }

        edition_varia_normalized = []
        if len((lgrs_book_dict.get('series') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['series'].strip())
        if len((lgrs_book_dict.get('edition') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['edition'].strip())
        if len((lgrs_book_dict.get('year') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['year'].strip())
        lgrs_book_dict['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

        allthethings.utils.init_identifiers_and_classification_unified(lgrs_book_dict)
        allthethings.utils.add_identifier_unified(lgrs_book_dict, 'lgrsfic', lgrs_book_dict['id'])
        # .lower() on md5 is okay here, we won't miss any fetches since collation is _ci.
        allthethings.utils.add_identifier_unified(lgrs_book_dict, 'md5', lgrs_book_dict['md5'].lower())
        allthethings.utils.add_isbns_unified(lgrs_book_dict, lgrsfic_book['Identifier'].split(","))
        allthethings.utils.add_isbns_unified(lgrs_book_dict, allthethings.utils.get_isbnlike('\n'.join([lgrs_book_dict.get('descr') or '', lgrs_book_dict.get('locator') or ''])))
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_IDENTIFIERS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_identifier_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_CLASSIFICATIONS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_classification_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])


        lgrs_book_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "id": ("before", ["This is a Libgen.rs Fiction record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.se/datasets/lgrs",
                              "Most of these fields are explained at https://wiki.mhut.org/content:bibliographic_data",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        }
        lgrs_book_dicts.append(add_comments_to_dict(lgrs_book_dict, lgrs_book_dict_comments))

    return lgrs_book_dicts

@page.get("/db/lgrs/nf/<int:lgrsnf_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgrsnf_book_json_redirect(lgrsnf_book_id):
    return redirect(f"/db/lgrsnf/{lgrsnf_book_id}.json", code=301)
@page.get("/db/lgrs/fic/<int:lgrsfic_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgrsfic_book_json_redirect(lgrsfic_book_id):
    return redirect(f"/db/lgrsfic/{lgrsfic_book_id}.json", code=301)

@page.get("/db/lgrsnf/<int:lgrsnf_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgrsnf_book_json(lgrsnf_book_id):
    with Session(engine) as session:
        lgrs_book_dicts = get_lgrsnf_book_dicts(session, "ID", [lgrsnf_book_id])
        if len(lgrs_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(lgrs_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}
@page.get("/db/lgrsfic/<int:lgrsfic_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgrsfic_book_json(lgrsfic_book_id):
    with Session(engine) as session:
        lgrs_book_dicts = get_lgrsfic_book_dicts(session, "ID", [lgrsfic_book_id])
        if len(lgrs_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(lgrs_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

libgenli_elem_descr_output = None
def libgenli_elem_descr(conn):
    global libgenli_elem_descr_output
    if libgenli_elem_descr_output is None:
        cursor = allthethings.utils.get_cursor_ping_conn(conn)
        cursor.execute('SELECT * FROM libgenli_elem_descr LIMIT 10000')
        all_descr = cursor.fetchall()

        output = {}
        for descr in all_descr:
            output[descr['key']] = dict(descr)
        libgenli_elem_descr_output = output
    return libgenli_elem_descr_output

def lgli_normalize_meta_field(field_name):
    return field_name.lower().replace(' ', '').replace('-', '').replace('.', '').replace('/', '').replace('(','').replace(')', '')

def lgli_map_descriptions(descriptions):
    descrs_mapped = {}
    for descr in descriptions:
        normalized_base_field = lgli_normalize_meta_field(descr['meta']['name_en'])
        normalized_base_field_meta = '///' + normalized_base_field
        if normalized_base_field_meta not in descrs_mapped:
            meta_dict_comments = {
                "link_pattern": ("after", ["Relative links are relative to the Libgen.li domains, e.g. https://libgen.li"]),
            }
            descrs_mapped[normalized_base_field_meta] = {
                "libgenli": add_comments_to_dict({k: v for k, v in descr['meta'].items() if v and v != "" and v != 0}, meta_dict_comments),
            }
            if normalized_base_field in allthethings.utils.LGLI_IDENTIFIERS:
                descrs_mapped[normalized_base_field_meta]["annas_archive"] = allthethings.utils.LGLI_IDENTIFIERS[normalized_base_field]
            # LGLI_IDENTIFIERS and LGLI_CLASSIFICATIONS are non-overlapping
            if normalized_base_field in allthethings.utils.LGLI_CLASSIFICATIONS:
                descrs_mapped[normalized_base_field_meta]["annas_archive"] = allthethings.utils.LGLI_CLASSIFICATIONS[normalized_base_field]
        if normalized_base_field in descrs_mapped:
            descrs_mapped[normalized_base_field].append(descr['value'])
        else:
            descrs_mapped[normalized_base_field] = [descr['value']]
        for i in [1,2,3]:
            add_field_name = f"name_add{i}_en"
            add_field_value = f"value_add{i}"
            if len(descr['meta'][add_field_name]) > 0:
                normalized_add_field = normalized_base_field + "_" + lgli_normalize_meta_field(descr['meta'][add_field_name])
                if normalized_add_field in descrs_mapped:
                    descrs_mapped[normalized_add_field].append(descr[add_field_value])
                else:
                    descrs_mapped[normalized_add_field] = [descr[add_field_value]]
        if len(descr.get('publisher_title') or '') > 0:
            normalized_base_field = 'publisher_title'
            normalized_base_field_meta = '///' + normalized_base_field
            if normalized_base_field_meta not in descrs_mapped:
                descrs_mapped[normalized_base_field_meta] = "Publisher title is a virtual field added by Anna's Archive based on the `publishers` table and the value of `publisherid`."
            if normalized_base_field in descrs_mapped:
                descrs_mapped[normalized_base_field].append(descr['publisher_title'])
            else:
                descrs_mapped[normalized_base_field] = [descr['publisher_title']]

    return descrs_mapped


def get_lgli_file_dicts_fetch_data(session, key, values):
    """ Fetches all the needed data from the DB and emulates the SQLAlchemy normalized format
    """

    cursor = allthethings.utils.get_cursor_ping(session)
    cursor.execute('SELECT * FROM libgenli_files ls '
                   f'WHERE `{key}` IN %(values)s', # key is not controlled by the user, so it's fine to use fstrings here
                   { 'values': values })
    lgli_files_c = cursor.fetchall()
    if len(lgli_files_c) > 0:
        file_ids = [file['f_id'] for file in lgli_files_c]

        # libgenli_files_add_descr 'selectin' join
        cursor.execute('SELECT `key`, value, value_add1, value_add2, value_add3, f_id FROM libgenli_files_add_descr '
                       'WHERE f_id IN %(file_ids)s',
                       { 'file_ids': file_ids })
        file_add_descr_rows = cursor.fetchall()
        for file in lgli_files_c:
            file['add_descrs'] = []
            for add_descr in file_add_descr_rows:
                if file['f_id'] == add_descr['f_id']:
                    file['add_descrs'].append(add_descr)

        # libgenli_editions 'selectin' join
        # series.issn_add_descrs: (LibgenliSeries.s_id == LibgenliSeriesAddDescr.s_id) & (LibgenliSeriesAddDescr.key == 501)
        cursor.execute(
            'SELECT le.*, ls.title AS ls__title, ls.publisher AS ls__publisher, ls.volume AS ls__volume, ls.volume_name AS ls__volume_name, lsad.value AS lsad_value, lef.f_id AS editions_to_file_id '
            'FROM libgenli_editions le '
            'INNER JOIN libgenli_editions_to_files lef ON le.e_id = lef.e_id '
            'LEFT JOIN libgenli_series ls ON ls.s_id = le.issue_s_id '
            'LEFT JOIN libgenli_series_add_descr lsad ON ls.s_id = lsad.s_id '
            'WHERE lef.f_id IN %(file_ids)s AND (lsad.`key` IS NULL OR lsad.`key` = 501)',
            { 'file_ids': file_ids })
        editions_rows = cursor.fetchall()
        editions_ids = [edition['e_id'] for edition in editions_rows]

        # no need to fetch editions' add_descr if no 'editions' were found
        if len(editions_rows) <= 0:
            editions_add_descr_rows = []
        else:
            # ligenli_editions_add_descr 'selectin' join
            # relationship.primaryjoin: (remote(LibgenliEditionsAddDescr.value) == foreign(LibgenliPublishers.p_id)) & (LibgenliEditionsAddDescr.key == 308)
            cursor.execute(
                'SELECT `lead`.`key`, `lead`.value, `lead`.value_add1, `lead`.value_add2, `lead`.value_add3, lp.title as publisher_title, e_id '
                'FROM libgenli_editions_add_descr `lead` '
                'LEFT JOIN libgenli_publishers lp ON lp.p_id = `lead`.value '
                'WHERE e_id IN %(editions_ids)s AND `lead`.key = 308',
                { 'editions_ids': editions_ids })
            editions_add_descr_rows = cursor.fetchall()
        for edition in editions_rows:
            edition['add_descrs'] = []
            for e_add_descr in editions_add_descr_rows:
                if edition['e_id'] == e_add_descr['e_id']:
                    if len(e_add_descr['publisher_title']) > 0:
                        e_add_descr['publisher'] = [
                            {
                                'title': e_add_descr['publisher_title']
                            }
                        ]
                        del e_add_descr['publisher_title']
                    edition['add_descrs'].append(e_add_descr)

        # normalize all rows into dicts
        for file_row in lgli_files_c:
            for add_descr in file_row['add_descrs']:
                # remove helper f_id field
                add_descr.pop('f_id')

            file_row['editions'] = []
            for edition_row in editions_rows:
                if edition_row['editions_to_file_id'] == file_row['f_id']:
                    edition_row_copy = edition_row.copy()

                    # make series into dict (assume one) if exists
                    construct_series = False
                    if edition_row_copy['ls__title'] is not None:
                        edition_row_copy['series'] = {}
                        construct_series = True
                    else:
                        edition_row_copy['series'] = None

                    print(edition_row_copy)

                    # looping through the original edition_row instance allows deleting keys from the copy during iteration
                    for key in edition_row.keys():
                        if key.startswith('ls__'):
                            if construct_series:
                                edition_row_copy['series'][key.replace('ls__', '')] = edition_row_copy[key]
                            del edition_row_copy[key]
                        elif key == 'lsad_value' and construct_series:
                            edition_row_copy['series']['issn_add_descrs'] = [
                                { 'value': edition_row_copy[key] }
                            ]
                            del edition_row_copy[key]

                    file_row['editions'].append(edition_row_copy)
    return lgli_files_c


# See https://libgen.li/community/app.php/article/new-database-structure-published-o%CF%80y6%D0%BB%D0%B8%C4%B8o%D0%B2a%D0%BDa-%D0%BDo%D0%B2a%D1%8F-c%D1%82py%C4%B8%D1%82ypa-6a%D0%B7%C6%85i-%D0%B4a%D0%BD%D0%BD%C6%85ix
def get_lgli_file_dicts(session, key, values):
    if len(values) == 0:
        return []

    description_metadata = libgenli_elem_descr(session.connection())
    lgli_files = get_lgli_file_dicts_fetch_data(session, key, values)

    lgli_file_dicts = []
    for lgli_file in lgli_files:
        lgli_file_dict = lgli_file.copy() # originally: **lgli_file.to_dict()

        # These would not be included in the SQLAlchemy to_dict()
        # these fields were used to build the normalized (nested) dicts
        del lgli_file_dict['add_descrs']
        del lgli_file_dict['editions']

        lgli_file_descriptions_dict = [{**descr, 'meta': description_metadata[descr['key']]} for descr in lgli_file['add_descrs']]
        lgli_file_dict['descriptions_mapped'] = lgli_map_descriptions(lgli_file_descriptions_dict)
        lgli_file_dict['editions'] = []

        for edition in lgli_file['editions']:
            edition_dict = {
                **edition, # originally: **edition.to_dict()
                'issue_series_title': edition['series']['title'] if edition['series'] else '',
                'issue_series_publisher': edition['series']['publisher'] if edition['series'] else '',
                'issue_series_volume_number': edition['series']['volume'] if edition['series'] else '',
                'issue_series_volume_name': edition['series']['volume_name'] if edition['series'] else '',
                'issue_series_issn': edition['series']['issn_add_descrs'][0]['value'] if edition['series'] and edition['series']['issn_add_descrs'] else '',
            }

            # for some reason issue_s_id was the first key when using SQLAlchemy
            # this makes the 'issue_s_id' the first key - then the result can be tested simply by diffing two .json files/responses
            edition_dict = { 'issue_s_id': edition_dict.pop('issue_s_id'), **edition_dict }

            # These would not be included in the SQLAlchemy to_dict()
            # these fields were used to build the normalized (nested) dicts
            del edition_dict['add_descrs']
            del edition_dict['series']
            del edition_dict['editions_to_file_id']
            del edition_dict['lsad_value']

            edition_dict['descriptions_mapped'] = lgli_map_descriptions({
                **descr,
                'meta': description_metadata[descr['key']],
                'publisher_title': descr['publisher'][0]['title'] if len(descr['publisher']) > 0 else '',
            } for descr in edition['add_descrs'])
            edition_dict['authors_normalized'] = edition_dict['author'].strip()
            if len(edition_dict['authors_normalized']) == 0 and len(edition_dict['descriptions_mapped'].get('author') or []) > 0:
                edition_dict['authors_normalized'] = ", ".join(author.strip() for author in edition_dict['descriptions_mapped']['author'])

            edition_dict['cover_url_guess'] = edition_dict['cover_url']
            coverurls = edition_dict['descriptions_mapped'].get('coverurl') or []
            if (len(coverurls) > 0) and (len(coverurls[0]) > 0):
                edition_dict['cover_url_guess'] = coverurls[0]
            if edition_dict['cover_exists'] > 0:
                edition_dict['cover_url_guess'] = f"https://libgen.li/editioncovers/{(edition_dict['e_id'] // 1000) * 1000}/{edition_dict['e_id']}.jpg"

            issue_other_fields = dict((key, edition_dict[key]) for key in allthethings.utils.LGLI_ISSUE_OTHER_FIELDS if edition_dict[key] not in ['', '0', 0, None])
            if len(issue_other_fields) > 0:
                edition_dict['issue_other_fields_json'] = allthethings.utils.nice_json(issue_other_fields)
            standard_info_fields = dict((key, edition_dict['descriptions_mapped'][key]) for key in allthethings.utils.LGLI_STANDARD_INFO_FIELDS if edition_dict['descriptions_mapped'].get(key) not in ['', '0', 0, None])
            if len(standard_info_fields) > 0:
                edition_dict['standard_info_fields_json'] = allthethings.utils.nice_json(standard_info_fields)
            date_info_fields = dict((key, edition_dict['descriptions_mapped'][key]) for key in allthethings.utils.LGLI_DATE_INFO_FIELDS if edition_dict['descriptions_mapped'].get(key) not in ['', '0', 0, None])
            if len(date_info_fields) > 0:
                edition_dict['date_info_fields_json'] = allthethings.utils.nice_json(date_info_fields)

            issue_series_title_normalized = []
            if len((edition_dict['issue_series_title'] or '').strip()) > 0:
                issue_series_title_normalized.append(edition_dict['issue_series_title'].strip())
            if len((edition_dict['issue_series_volume_name'] or '').strip()) > 0:
                issue_series_title_normalized.append(edition_dict['issue_series_volume_name'].strip())
            if len((edition_dict['issue_series_volume_number'] or '').strip()) > 0:
                issue_series_title_normalized.append('Volume ' + edition_dict['issue_series_volume_number'].strip())
            elif len((issue_other_fields.get('issue_year_number') or '').strip()) > 0:
                issue_series_title_normalized.append('#' + issue_other_fields['issue_year_number'].strip())
            edition_dict['issue_series_title_normalized'] = ", ".join(issue_series_title_normalized) if len(issue_series_title_normalized) > 0 else ''

            publisher_titles = (edition_dict['descriptions_mapped'].get('publisher_title') or [])
            edition_dict['publisher_normalized'] = ''
            if len((edition_dict['publisher'] or '').strip()) > 0:
                edition_dict['publisher_normalized'] = edition_dict['publisher'].strip()
            elif len(publisher_titles) > 0 and len(publisher_titles[0].strip()) > 0:
                edition_dict['publisher_normalized'] = publisher_titles[0].strip()
            elif len((edition_dict['issue_series_publisher'] or '').strip()) > 0:
                edition_dict['publisher_normalized'] = edition_dict['issue_series_publisher'].strip()
                if len((edition_dict['issue_series_issn'] or '').strip()) > 0:
                    edition_dict['publisher_normalized'] += ' (ISSN ' + edition_dict['issue_series_issn'].strip() + ')'

            date_normalized = []
            if len((edition_dict['year'] or '').strip()) > 0:
                date_normalized.append(edition_dict['year'].strip())
            if len((edition_dict['month'] or '').strip()) > 0:
                date_normalized.append(edition_dict['month'].strip())
            if len((edition_dict['day'] or '').strip()) > 0:
                date_normalized.append(edition_dict['day'].strip())
            edition_dict['date_normalized'] = " ".join(date_normalized)

            edition_varia_normalized = []
            if len((edition_dict['issue_series_title_normalized'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['issue_series_title_normalized'].strip())
            if len((edition_dict['issue_number'] or '').strip()) > 0:
                edition_varia_normalized.append('#' + edition_dict['issue_number'].strip())
            if len((edition_dict['issue_year_number'] or '').strip()) > 0:
                edition_varia_normalized.append('#' + edition_dict['issue_year_number'].strip())
            if len((edition_dict['issue_volume'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['issue_volume'].strip())
            if (len((edition_dict['issue_first_page'] or '').strip()) > 0) or (len((edition_dict['issue_last_page'] or '').strip()) > 0):
                edition_varia_normalized.append('pages ' + (edition_dict['issue_first_page'] or '').strip() + '-' + (edition_dict['issue_last_page'] or '').strip())
            if len((edition_dict['series_name'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['series_name'].strip())
            if len((edition_dict['edition'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['edition'].strip())
            if len((edition_dict['date_normalized'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['date_normalized'].strip())
            edition_dict['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

            language_codes = [get_bcp47_lang_codes(language_code) for language_code in (edition_dict['descriptions_mapped'].get('language') or [])]
            edition_dict['language_codes'] = combine_bcp47_lang_codes(language_codes)
            languageoriginal_codes = [get_bcp47_lang_codes(language_code) for language_code in (edition_dict['descriptions_mapped'].get('languageoriginal') or [])]
            edition_dict['languageoriginal_codes'] = combine_bcp47_lang_codes(languageoriginal_codes)

            allthethings.utils.init_identifiers_and_classification_unified(edition_dict)
            allthethings.utils.add_identifier_unified(edition_dict, 'doi', edition_dict['doi'])
            for key, values in edition_dict['descriptions_mapped'].items():
                if key in allthethings.utils.LGLI_IDENTIFIERS:
                    for value in values:
                        allthethings.utils.add_identifier_unified(edition_dict, allthethings.utils.LGLI_IDENTIFIERS_MAPPING.get(key, key), value)
            for key, values in edition_dict['descriptions_mapped'].items():
                if key in allthethings.utils.LGLI_CLASSIFICATIONS:
                    for value in values:
                        allthethings.utils.add_classification_unified(edition_dict, allthethings.utils.LGLI_CLASSIFICATIONS_MAPPING.get(key, key), value)
            allthethings.utils.add_isbns_unified(edition_dict, edition_dict['descriptions_mapped'].get('isbn') or [])
            allthethings.utils.add_isbns_unified(edition_dict, allthethings.utils.get_isbnlike('\n'.join(edition_dict['descriptions_mapped'].get('description') or [])))
            if len((edition_dict['issue_series_issn'] or '').strip()) > 0:
                allthethings.utils.add_issn_unified(edition_dict, edition_dict['issue_series_issn'].strip())

            edition_dict['stripped_description'] = ''
            if len(edition_dict['descriptions_mapped'].get('description') or []) > 0:
                edition_dict['stripped_description'] = strip_description("\n\n".join(edition_dict['descriptions_mapped']['description']))

            edition_dict['edition_type_full'] = allthethings.utils.LGLI_EDITION_TYPE_MAPPING.get(edition_dict['type'], '')

            edition_dict_comments = {
                **allthethings.utils.COMMON_DICT_COMMENTS,
                "editions": ("before", ["Files can be associated with zero or more editions."
                                        "Sometimes it corresponds to a particular physical version of a book (similar to ISBN records, or 'editions' in Open Library), but it may also represent a chapter in a periodical (more specific than a single book), or a collection of multiple books (more general than a single book). However, in practice, in most cases files only have a single edition.",
                                        "Note that while usually there is only one 'edition' associated with a file, it is common to have multiple files associated with an edition. For example, different people might have scanned a book."]),
                "issue_series_title": ("before", ["The `issue_series_*` fields were loaded from the `series` table using `issue_s_id`."]),
                "authors_normalized": ("before", ["Anna's Archive best guess at the authors, based on the regular `author` field and `author` from `descriptions_mapped`."]),
                "cover_url_guess": ("before", ["Anna's Archive best guess at the full URL to the cover image on libgen.li, for this specific edition."]),
                "issue_series_title_normalized": ("before", ["Anna's Archive version of the 'issue_series_title', 'issue_series_volume_name', 'issue_series_volume_number', and 'issue_year_number' fields; combining them into a single field for display and search."]),
                "publisher_normalized": ("before", ["Anna's Archive version of the 'publisher', 'publisher_title_first', 'issue_series_publisher', and 'issue_series_issn' fields; combining them into a single field for display and search."]),
                "date_normalized": ("before", ["Anna's Archive combined version of the 'year', 'month', and 'day' fields."]),
                "edition_varia_normalized": ("before", ["Anna's Archive version of the 'issue_series_title_normalized', 'issue_number', 'issue_year_number', 'issue_volume', 'issue_first_page', 'issue_last_page', 'series_name', 'edition', and 'date_normalized' fields; combining them into a single field for display and search."]),
                "language_codes": ("before", ["Anna's Archive version of the 'language' field, where we attempted to parse them into BCP 47 tags."]),
                "languageoriginal_codes": ("before", ["Same as 'language_codes' but for the 'languageoriginal' field, which contains the original language if the work is a translation."]),
                "edition_type_full": ("after", ["Anna's Archive expansion of the `type` field in the edition, based on the `descr_elems` table."]),
            }
            lgli_file_dict['editions'].append(add_comments_to_dict(edition_dict, edition_dict_comments))

        lgli_file_dict['cover_url_guess'] = ''
        if lgli_file_dict['cover_exists'] > 0:
            lgli_file_dict['cover_url_guess'] = f"https://libgen.li/comicscovers/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['libgen_id'] and lgli_file_dict['libgen_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/covers/{(lgli_file_dict['libgen_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['comics_id'] and lgli_file_dict['comics_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/comicscovers_repository/{(lgli_file_dict['comics_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['fiction_id'] and lgli_file_dict['fiction_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/fictioncovers/{(lgli_file_dict['fiction_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['fiction_rus_id'] and lgli_file_dict['fiction_rus_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/fictionruscovers/{(lgli_file_dict['fiction_rus_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['magz_id'] and lgli_file_dict['magz_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/magzcovers/{(lgli_file_dict['magz_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"

        lgli_file_dict['cover_url_guess_normalized'] = ''
        if len(lgli_file_dict['cover_url_guess']) > 0:
            lgli_file_dict['cover_url_guess_normalized'] = lgli_file_dict['cover_url_guess']
        else:
            for edition_dict in lgli_file_dict['editions']:
                if len(edition_dict['cover_url_guess']) > 0:
                    lgli_file_dict['cover_url_guess_normalized'] = edition_dict['cover_url_guess']

        lgli_file_dict['scimag_url_guess'] = ''
        if len(lgli_file_dict['scimag_archive_path']) > 0:
            lgli_file_dict['scimag_url_guess'] = lgli_file_dict['scimag_archive_path'].replace('\\', '/')
            if lgli_file_dict['scimag_url_guess'].endswith('.' + lgli_file_dict['extension']):
                lgli_file_dict['scimag_url_guess'] = lgli_file_dict['scimag_url_guess'][0:-len('.' + lgli_file_dict['extension'])]
            if lgli_file_dict['scimag_url_guess'].startswith('10.0000/') and '%2F' in lgli_file_dict['scimag_url_guess']:
                lgli_file_dict['scimag_url_guess'] = 'http://' + lgli_file_dict['scimag_url_guess'][len('10.0000/'):].replace('%2F', '/')
            else:
                lgli_file_dict['scimag_url_guess'] = 'https://doi.org/' + lgli_file_dict['scimag_url_guess']

        allthethings.utils.init_identifiers_and_classification_unified(lgli_file_dict)
        allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli', lgli_file_dict['f_id'])
        allthethings.utils.add_identifier_unified(lgli_file_dict, 'md5', lgli_file_dict['md5'].lower())
        allthethings.utils.add_isbns_unified(lgli_file_dict, allthethings.utils.get_isbnlike(lgli_file_dict['locator']))
        lgli_file_dict['scimag_archive_path_decoded'] = urllib.parse.unquote(lgli_file_dict['scimag_archive_path'].replace('\\', '/'))
        potential_doi_scimag_archive_path = lgli_file_dict['scimag_archive_path_decoded']
        if potential_doi_scimag_archive_path.endswith('.pdf'):
            potential_doi_scimag_archive_path = potential_doi_scimag_archive_path[:-len('.pdf')]
        potential_doi_scimag_archive_path = normalize_doi(potential_doi_scimag_archive_path)
        if potential_doi_scimag_archive_path != '':
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'doi', potential_doi_scimag_archive_path)

        if lgli_file_dict['libgen_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_libgen_id', lgli_file_dict['libgen_id'])
        if lgli_file_dict['fiction_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_fiction_id', lgli_file_dict['fiction_id'])
        if lgli_file_dict['fiction_rus_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_fiction_rus_id', lgli_file_dict['fiction_rus_id'])
        if lgli_file_dict['comics_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_comics_id', lgli_file_dict['comics_id'])
        if lgli_file_dict['scimag_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_scimag_id', lgli_file_dict['scimag_id'])
        if lgli_file_dict['standarts_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_standarts_id', lgli_file_dict['standarts_id'])
        if lgli_file_dict['magz_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_magz_id', lgli_file_dict['magz_id'])

        lgli_file_dict['added_date_unified'] = {}
        if lgli_file_dict['time_added'] != '0000-00-00 00:00:00':
            if not isinstance(lgli_file_dict['time_added'], datetime.datetime):
                raise Exception(f"Unexpected {lgli_file_dict['time_added']=} for {lgli_file_dict=}")
            lgli_file_dict['added_date_unified'] = { 'date_lgli_source': lgli_file_dict['time_added'].isoformat().split('T', 1)[0] }

        lgli_file_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "f_id": ("before", ["This is a Libgen.li file record, augmented by Anna's Archive.",
                     "More details at https://annas-archive.se/datasets/lgli",
                     "Most of these fields are explained at https://libgen.li/community/app.php/article/new-database-structure-published-o%CF%80y6%D0%BB%D0%B8%C4%B8o%D0%B2a%D0%BDa-%D0%BDo%D0%B2a%D1%8F-c%D1%82py%C4%B8%D1%82ypa-6a%D0%B7%C6%85i-%D0%B4a%D0%BD%D0%BD%C6%85ix",
                     "The source URL is https://libgen.li/file.php?id=<f_id>",
                     allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "cover_url_guess": ("after", ["Anna's Archive best guess at the full URL to the cover image on libgen.li, for this specific file (not taking into account editions)."]),
            "cover_url_guess_normalized": ("after", ["Anna's Archive best guess at the full URL to the cover image on libgen.li, using the guess from the first edition that has a non-empty guess, if the file-specific guess is empty."]),
            "scimag_url_guess": ("after", ["Anna's Archive best guess at the canonical URL for journal articles."]),
            "scimag_archive_path_decoded": ("after", ["scimag_archive_path but with URL decoded"]),
            "libgen_topic": ("after", ["The primary subcollection this file belongs to: l=Non-fiction ('libgen'), s=Standards document, m=Magazine, c=Comic, f=Fiction, r=Russian Fiction, a=Journal article (Sci-Hub/scimag)"]),
        }
        lgli_file_dicts.append(add_comments_to_dict(lgli_file_dict, lgli_file_dict_comments))

    return lgli_file_dicts

@page.get("/db/lgli/file/<int:lgli_file_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgli_file_json(lgli_file_id):
    return redirect(f"/db/lgli/{lgli_file_id}.json", code=301)

@page.get("/db/lgli/<int:lgli_file_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgli_json(lgli_file_id):
    with Session(engine) as session:
        lgli_file_dicts = get_lgli_file_dicts(session, "f_id", [lgli_file_id])
        if len(lgli_file_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(lgli_file_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_isbndb_dicts(session, canonical_isbn13s):
    if len(canonical_isbn13s) == 0:
        return []

    isbndb13_grouped = collections.defaultdict(list)
    cursor = allthethings.utils.get_cursor_ping(session)
    cursor.execute('SELECT * FROM isbndb_isbns WHERE isbn13 IN %(canonical_isbn13s)s', { 'canonical_isbn13s': canonical_isbn13s })
    for row in cursor.fetchall():
        isbndb13_grouped[row['isbn13']].append(row)
    isbndb10_grouped = collections.defaultdict(list)
    isbn10s = list(filter(lambda x: x is not None, [isbnlib.to_isbn10(isbn13) for isbn13 in canonical_isbn13s]))
    if len(isbn10s) > 0:
        cursor.execute('SELECT * FROM isbndb_isbns WHERE isbn10 IN %(isbn10s)s', { 'isbn10s': isbn10s })
        for row in cursor.fetchall():
            # ISBNdb has a bug where they just chop off the prefix of ISBN-13, which is incorrect if the prefix is anything
            # besides "978"; so we double-check on this.
            if row['isbn13'][0:3] == '978':
                isbndb10_grouped[row['isbn10']].append(row)

    isbn_dicts = []
    for canonical_isbn13 in canonical_isbn13s:
        isbn_dict = {
            "ean13": isbnlib.ean13(canonical_isbn13),
            "isbn10": isbnlib.to_isbn10(canonical_isbn13),
            "added_date_unified": { "date_isbndb_scrape": "2022-09-01" },
        }

        isbndb_books = {}
        if isbn_dict['isbn10']:
            isbndb10_all = isbndb10_grouped[isbn_dict['isbn10']]
            for isbndb10 in isbndb10_all:
                isbndb_books[isbndb10['isbn13'] + '-' + isbndb10['isbn10']] = { **isbndb10, 'source_isbn': isbn_dict['isbn10'], 'matchtype': 'ISBN-10' }
        isbndb13_all = isbndb13_grouped[canonical_isbn13]
        for isbndb13 in isbndb13_all:
            key = isbndb13['isbn13'] + '-' + isbndb13['isbn10']
            if key in isbndb_books:
                isbndb_books[key]['matchtype'] = 'ISBN-10 and ISBN-13'
            else:
                isbndb_books[key] = { **isbndb13, 'source_isbn': canonical_isbn13, 'matchtype': 'ISBN-13' }

        for isbndb_book in isbndb_books.values():
            isbndb_book['json'] = orjson.loads(isbndb_book['json'])
            isbndb_book['json']['subjects'] = isbndb_book['json'].get('subjects', None) or []

        # There seem to be a bunch of ISBNdb books with only a language, which is not very useful.
        isbn_dict['isbndb'] = [isbndb_book for isbndb_book in isbndb_books.values() if len(isbndb_book['json'].get('title') or '') > 0 or len(isbndb_book['json'].get('title_long') or '') > 0 or len(isbndb_book['json'].get('authors') or []) > 0 or len(isbndb_book['json'].get('synopsis') or '') > 0 or len(isbndb_book['json'].get('overview') or '') > 0]

        for index, isbndb_dict in enumerate(isbn_dict['isbndb']):
            isbndb_dict['language_codes'] = get_bcp47_lang_codes(isbndb_dict['json'].get('language') or '')
            isbndb_dict['edition_varia_normalized'] = ", ".join(list(dict.fromkeys([item for item in [
                str(isbndb_dict['json'].get('edition') or '').strip(),
                str(isbndb_dict['json'].get('date_published') or '').split('T')[0].strip(),
            ] if item != ''])))
            isbndb_dict['title_normalized'] = max([isbndb_dict['json'].get('title') or '', isbndb_dict['json'].get('title_long') or ''], key=len)
            isbndb_dict['year_normalized'] = ''
            potential_year = re.search(r"(\d\d\d\d)", str(isbndb_dict['json'].get('date_published') or '').split('T')[0])
            if potential_year is not None:
                isbndb_dict['year_normalized'] = potential_year[0]
            # There is often also isbndb_dict['json']['image'], but sometimes images get added later, so we can make a guess ourselves.
            isbndb_dict['cover_url_guess'] = f"https://images.isbndb.com/covers/{isbndb_dict['isbn13'][-4:-2]}/{isbndb_dict['isbn13'][-2:]}/{isbndb_dict['isbn13']}.jpg"
            isbndb_dict['added_date_unified'] = { "date_isbndb_scrape": "2022-09-01" }

            allthethings.utils.init_identifiers_and_classification_unified(isbndb_dict)
            allthethings.utils.add_isbns_unified(isbndb_dict, [canonical_isbn13])

            isbndb_inner_comments = {
                "edition_varia_normalized": ("after", ["Anna's Archive version of the 'edition', and 'date_published' fields; combining them into a single field for display and search."]),
                "title_normalized": ("after", ["Anna's Archive version of the 'title', and 'title_long' fields; we take the longest of the two."]),
                "json": ("before", ["Raw JSON straight from the ISBNdb API."]),
                "cover_url_guess": ("after", ["Anna's Archive best guess of the cover URL, since sometimes the 'image' field is missing from the JSON."]),
                "year_normalized": ("after", ["Anna's Archive version of the year of publication, by extracting it from the 'date_published' field."]),
                "language_codes": ("before", ["Anna's Archive version of the 'language' field, where we attempted to parse them into BCP 47 tags."]),
                "matchtype": ("after", ["Whether the canonical ISBN-13 matched the API's ISBN-13, ISBN-10, or both."]),
            }
            isbn_dict['isbndb'][index] = add_comments_to_dict(isbn_dict['isbndb'][index], isbndb_inner_comments)

        isbndb_wrapper_comments = {
            "ean13": ("before", ["Metadata from our ISBNdb collection, augmented by Anna's Archive.",
                               "More details at https://annas-archive.se/datasets",
                               allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "isbndb": ("before", ["All matching records from the ISBNdb database."]),
        }
        isbn_dicts.append(add_comments_to_dict(isbn_dict, isbndb_wrapper_comments))

    return isbn_dicts

@page.get("/db/isbndb/<string:isbn>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def isbndb_json(isbn):
    with Session(engine) as session:
        isbndb_dicts = get_isbndb_dicts(session, [isbn])
        if len(isbndb_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(isbndb_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}


def get_scihub_doi_dicts(session, key, values):
    if len(values) == 0:
        return []
    if key != 'doi':
        raise Exception(f"Unexpected 'key' in get_scihub_doi_dicts: '{key}'")

    scihub_dois = []
    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT doi FROM scihub_dois WHERE doi IN %(values)s', { "values": [str(value) for value in values] })
        scihub_dois = list(cursor.fetchall())
    except Exception as err:
        print(f"Error in get_scihub_doi_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    scihub_doi_dicts = []
    for scihub_doi in scihub_dois:
        scihub_doi_dict = { "doi": scihub_doi["doi"] }
        allthethings.utils.init_identifiers_and_classification_unified(scihub_doi_dict)
        allthethings.utils.add_identifier_unified(scihub_doi_dict, "doi", scihub_doi_dict["doi"])
        scihub_doi_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "doi": ("before", ["This is a file from Sci-Hub's dois-2022-02-12.7z dataset.",
                              "More details at https://annas-archive.se/datasets/scihub",
                              "The source URL is https://sci-hub.ru/datasets/dois-2022-02-12.7z",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        }
        scihub_doi_dicts.append(add_comments_to_dict(scihub_doi_dict, scihub_doi_dict_comments))
    return scihub_doi_dicts

@page.get("/db/scihub_doi/<path:doi>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def scihub_doi_json(doi):
    with Session(engine) as session:
        scihub_doi_dicts = get_scihub_doi_dicts(session, 'doi', [doi])
        if len(scihub_doi_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(scihub_doi_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}


def oclc_get_authors_from_contributors(contributors):
    has_primary = any(contributor['isPrimary'] for contributor in contributors)
    has_author_relator = any('aut' in (contributor.get('relatorCodes') or []) for contributor in contributors)
    authors = []
    for contributor in contributors:
        author = []
        if has_primary and (not contributor['isPrimary']):
            continue
        if has_author_relator and ('aut' not in (contributor.get('relatorCodes') or [])):
            continue
        if 'nonPersonName' in contributor:
            author = [contributor['nonPersonName'].get('text') or '']
        else:
            author = [((contributor.get('firstName') or {}).get('text') or ''), ((contributor.get('secondName') or {}).get('text') or '')]

        author_full = ' '.join(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in author]))
        if len(author_full) > 0:
            authors.append(author_full)
    return "; ".join(authors)

def oclc_get_authors_from_authors(authors):
    contributors = []
    for author in authors:
        contributors.append({
            'firstName': {'text': (author['firstNameObject'].get('data') or '')},
            'secondName': {'text': ', '.join(filter(len, [(author['lastNameObject'].get('data') or ''), (author.get('notes') or '')]))},
            'isPrimary': author['primary'],
            'relatorCodes': [(relator.get('code') or '') for relator in (author.get('relatorList') or {'relators':[]})['relators']],
        })
    return oclc_get_authors_from_contributors(contributors)

def get_oclc_dicts(session, key, values):
    if len(values) == 0:
        return []
    if key != 'oclc':
        raise Exception(f"Unexpected 'key' in get_oclc_dicts: '{key}'")

    session.connection().connection.ping(reconnect=True)
    cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute('SELECT primary_id, byte_offset, byte_length FROM annas_archive_meta__aacid__worldcat WHERE primary_id IN %(values)s ORDER BY byte_offset', { "values": [str(val) for val in values] })

    worldcat_oclc_ids = []
    worldcat_offsets_and_lengths = []
    for row in list(cursor.fetchall()):
        worldcat_oclc_ids.append(str(row['primary_id']))
        worldcat_offsets_and_lengths.append((row['byte_offset'], row['byte_length']))

    aac_records_by_oclc_id = collections.defaultdict(list)
    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'worldcat', worldcat_offsets_and_lengths)):
        aac_records_by_oclc_id[worldcat_oclc_ids[index]].append(orjson.loads(line_bytes))

    oclc_dicts = []
    for oclc_id in values:
        oclc_id = str(oclc_id)
        aac_records = aac_records_by_oclc_id[oclc_id]

        oclc_dict = {}
        oclc_dict["oclc_id"] = oclc_id
        oclc_dict["aa_oclc_derived"] = {}
        oclc_dict["aa_oclc_derived"]["title_multiple"] = []
        oclc_dict["aa_oclc_derived"]["author_multiple"] = []
        oclc_dict["aa_oclc_derived"]["publisher_multiple"] = []
        oclc_dict["aa_oclc_derived"]["edition_multiple"] = []
        oclc_dict["aa_oclc_derived"]["place_multiple"] = []
        oclc_dict["aa_oclc_derived"]["date_multiple"] = []
        oclc_dict["aa_oclc_derived"]["year_multiple"] = []
        oclc_dict["aa_oclc_derived"]["series_multiple"] = []
        oclc_dict["aa_oclc_derived"]["volume_multiple"] = []
        oclc_dict["aa_oclc_derived"]["description_multiple"] = []
        oclc_dict["aa_oclc_derived"]["languages_multiple"] = []
        oclc_dict["aa_oclc_derived"]["isbn_multiple"] = []
        oclc_dict["aa_oclc_derived"]["issn_multiple"] = []
        oclc_dict["aa_oclc_derived"]["doi_multiple"] = []
        oclc_dict["aa_oclc_derived"]["general_format_multiple"] = []
        oclc_dict["aa_oclc_derived"]["specific_format_multiple"] = []
        oclc_dict["aa_oclc_derived"]["content_type"] = "other"
        oclc_dict["aa_oclc_derived"]["rft_multiple"] = []
        oclc_dict["aac_records"] = aac_records

        for aac_record in aac_records:
            aac_metadata = aac_record['metadata']
            if aac_metadata['type'] in 'title_json':
                oclc_dict["aa_oclc_derived"]["title_multiple"].append((aac_metadata['record'].get('title') or ''))
                oclc_dict["aa_oclc_derived"]["author_multiple"].append(oclc_get_authors_from_contributors(aac_metadata['record'].get('contributors') or []))
                oclc_dict["aa_oclc_derived"]["publisher_multiple"].append((aac_metadata['record'].get('publisher') or ''))
                oclc_dict["aa_oclc_derived"]["edition_multiple"].append((aac_metadata['record'].get('edition') or ''))
                oclc_dict["aa_oclc_derived"]["place_multiple"].append((aac_metadata['record'].get('publicationPlace') or ''))
                oclc_dict["aa_oclc_derived"]["date_multiple"].append((aac_metadata['record'].get('publicationDate') or ''))
                oclc_dict["aa_oclc_derived"]["series_multiple"].append((aac_metadata['record'].get('series') or ''))
                oclc_dict["aa_oclc_derived"]["volume_multiple"] += (aac_metadata['record'].get('seriesVolumes') or [])
                oclc_dict["aa_oclc_derived"]["description_multiple"].append((aac_metadata['record'].get('summary') or ''))
                oclc_dict["aa_oclc_derived"]["languages_multiple"].append((aac_metadata['record'].get('catalogingLanguage') or ''))
                oclc_dict["aa_oclc_derived"]["isbn_multiple"].append((aac_metadata['record'].get('isbn13') or ''))
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (aac_metadata['record'].get('isbns') or [])
                oclc_dict["aa_oclc_derived"]["issn_multiple"].append((aac_metadata['record'].get('sourceIssn') or ''))
                oclc_dict["aa_oclc_derived"]["issn_multiple"] += (aac_metadata['record'].get('issns') or [])
                oclc_dict["aa_oclc_derived"]["doi_multiple"].append((aac_metadata['record'].get('doi') or ''))
                oclc_dict["aa_oclc_derived"]["general_format_multiple"].append((aac_metadata['record'].get('generalFormat') or ''))
                oclc_dict["aa_oclc_derived"]["specific_format_multiple"].append((aac_metadata['record'].get('specificFormat') or ''))
            elif aac_metadata['type'] == 'briefrecords_json':
                oclc_dict["aa_oclc_derived"]["title_multiple"].append((aac_metadata['record'].get('title') or ''))
                oclc_dict["aa_oclc_derived"]["author_multiple"].append(oclc_get_authors_from_contributors(aac_metadata['record'].get('contributors') or []))
                oclc_dict["aa_oclc_derived"]["publisher_multiple"].append((aac_metadata['record'].get('publisher') or ''))
                oclc_dict["aa_oclc_derived"]["edition_multiple"].append((aac_metadata['record'].get('edition') or ''))
                oclc_dict["aa_oclc_derived"]["place_multiple"].append((aac_metadata['record'].get('publicationPlace') or ''))
                oclc_dict["aa_oclc_derived"]["date_multiple"].append((aac_metadata['record'].get('publicationDate') or ''))
                oclc_dict["aa_oclc_derived"]["description_multiple"].append((aac_metadata['record'].get('summary') or ''))
                oclc_dict["aa_oclc_derived"]["description_multiple"] += (aac_metadata['record'].get('summaries') or [])
                oclc_dict["aa_oclc_derived"]["languages_multiple"].append((aac_metadata['record'].get('catalogingLanguage') or ''))
                oclc_dict["aa_oclc_derived"]["isbn_multiple"].append((aac_metadata['record'].get('isbn13') or ''))
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (aac_metadata['record'].get('isbns') or [])
                oclc_dict["aa_oclc_derived"]["general_format_multiple"].append((aac_metadata['record'].get('generalFormat') or ''))
                oclc_dict["aa_oclc_derived"]["specific_format_multiple"].append((aac_metadata['record'].get('specificFormat') or ''))
                # TODO: unverified:
                oclc_dict["aa_oclc_derived"]["issn_multiple"].append((aac_metadata['record'].get('sourceIssn') or ''))
                oclc_dict["aa_oclc_derived"]["issn_multiple"] += (aac_metadata['record'].get('issns') or [])
                oclc_dict["aa_oclc_derived"]["doi_multiple"].append((aac_metadata['record'].get('doi') or ''))
                # TODO: series/volume?
            elif aac_metadata['type'] == 'providersearchrequest_json':
                rft = urllib.parse.parse_qs((aac_metadata['record'].get('openUrlContextObject') or ''))
                oclc_dict["aa_oclc_derived"]["rft_multiple"].append(rft)

                oclc_dict["aa_oclc_derived"]["title_multiple"].append((aac_metadata['record'].get('titleObject') or {}).get('data') or '')
                oclc_dict["aa_oclc_derived"]["author_multiple"].append(oclc_get_authors_from_authors(aac_metadata['record'].get('authors') or []))
                oclc_dict["aa_oclc_derived"]["publisher_multiple"] += (rft.get('rft.pub') or [])
                oclc_dict["aa_oclc_derived"]["edition_multiple"].append((aac_metadata['record'].get('edition') or ''))
                oclc_dict["aa_oclc_derived"]["place_multiple"] += (rft.get('rft.place') or [])
                oclc_dict["aa_oclc_derived"]["date_multiple"] += (rft.get('rft.date') or [])
                oclc_dict["aa_oclc_derived"]["date_multiple"].append((aac_metadata['record'].get('date') or ''))
                oclc_dict["aa_oclc_derived"]["description_multiple"] += [(summary.get('data') or '') for summary in (aac_metadata['record'].get('summariesObjectList') or [])]
                oclc_dict["aa_oclc_derived"]["languages_multiple"].append((aac_metadata['record'].get('language') or ''))
                oclc_dict["aa_oclc_derived"]["general_format_multiple"] += [orjson.loads(dat)['stdrt1'] for dat in (rft.get('rft_dat') or [])]
                oclc_dict["aa_oclc_derived"]["specific_format_multiple"] += [orjson.loads(dat)['stdrt2'] for dat in (rft.get('rft_dat') or [])]
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (aac_metadata['record'].get('isbns') or [])
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (rft.get('rft.isbn') or [])

                # TODO: series/volume?
                # lcNumber, masterCallNumber
            elif aac_metadata['type'] == 'legacysearch_html':
                rft = {}
                rft_match = re.search('url_ver=Z39.88-2004[^"]+', aac_metadata['html'])
                if rft_match is not None:
                    rft = urllib.parse.parse_qs(rft_match.group())
                oclc_dict["aa_oclc_derived"]["rft_multiple"].append(rft)

                oclc_dict["aa_oclc_derived"]["title_multiple"] += (rft.get('rft.title') or [])
                legacy_author_match = re.search('<div class="author">([^<]+)</div>', aac_metadata['html'])
                if legacy_author_match:
                    legacy_authors = legacy_author_match.group(1)
                    if legacy_authors.startswith('by '):
                        legacy_authors = legacy_authors[len('by '):]
                    oclc_dict["aa_oclc_derived"]["author_multiple"].append(legacy_authors)
                oclc_dict["aa_oclc_derived"]["publisher_multiple"] += (rft.get('rft.pub') or [])
                oclc_dict["aa_oclc_derived"]["edition_multiple"] += (rft.get('rft.edition') or [])
                oclc_dict["aa_oclc_derived"]["place_multiple"] += (rft.get('rft.place') or [])
                oclc_dict["aa_oclc_derived"]["date_multiple"] += (rft.get('rft.date') or [])
                legacy_language_match = re.search('<span class="itemLanguage">([^<]+)</span>', aac_metadata['html'])
                if legacy_language_match:
                    legacy_language = legacy_language_match.group(1)
                    oclc_dict["aa_oclc_derived"]["languages_multiple"].append(legacy_language)
                oclc_dict["aa_oclc_derived"]["general_format_multiple"] += [orjson.loads(dat)['stdrt1'] for dat in (rft.get('rft_dat') or [])]
                oclc_dict["aa_oclc_derived"]["specific_format_multiple"] += [orjson.loads(dat)['stdrt2'] for dat in (rft.get('rft_dat') or [])]
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (rft.get('rft.isbn') or [])
                # TODO: series/volume?
            elif aac_metadata['type'] in ['not_found_title_json', 'redirect_title_json']:
                pass
            else:
                raise Exception(f"Unexpected aac_metadata.type: {aac_metadata['type']}")

        oclc_dict["aa_oclc_derived"]["title_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["title_multiple"]])))
        oclc_dict["aa_oclc_derived"]["author_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["author_multiple"]])))
        oclc_dict["aa_oclc_derived"]["publisher_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["publisher_multiple"]])))
        oclc_dict["aa_oclc_derived"]["edition_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["edition_multiple"]])))
        oclc_dict["aa_oclc_derived"]["place_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["place_multiple"]])))
        oclc_dict["aa_oclc_derived"]["date_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["date_multiple"]])))
        oclc_dict["aa_oclc_derived"]["series_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["series_multiple"]])))
        oclc_dict["aa_oclc_derived"]["volume_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["volume_multiple"]])))
        oclc_dict["aa_oclc_derived"]["description_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["description_multiple"])))
        oclc_dict["aa_oclc_derived"]["languages_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["languages_multiple"])))
        oclc_dict["aa_oclc_derived"]["isbn_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["isbn_multiple"])))
        oclc_dict["aa_oclc_derived"]["issn_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["issn_multiple"])))
        oclc_dict["aa_oclc_derived"]["doi_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["doi_multiple"])))
        oclc_dict["aa_oclc_derived"]["general_format_multiple"] = list(dict.fromkeys(filter(len, [s.lower() for s in oclc_dict["aa_oclc_derived"]["general_format_multiple"]])))
        oclc_dict["aa_oclc_derived"]["specific_format_multiple"] = list(dict.fromkeys(filter(len, [s.lower() for s in oclc_dict["aa_oclc_derived"]["specific_format_multiple"]])))

        for s in oclc_dict["aa_oclc_derived"]["date_multiple"]:
            potential_year = re.search(r"(\d\d\d\d)", s)
            if potential_year is not None:
                oclc_dict["aa_oclc_derived"]["year_multiple"].append(potential_year[0])

        if "thsis" in oclc_dict["aa_oclc_derived"]["specific_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'journal_article'
        elif "mss" in oclc_dict["aa_oclc_derived"]["specific_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'journal_article'
        elif "book" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'book_unknown'
        elif "artchap" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'journal_article'
        elif "artcl" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'journal_article'
        elif "news" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'magazine'
        elif "jrnl" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'magazine'
        elif "msscr" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'musical_score'

        oclc_dict["aa_oclc_derived"]['edition_varia_normalized'] = ', '.join(list(dict.fromkeys(filter(len, [
            max(['', *oclc_dict["aa_oclc_derived"]["series_multiple"]], key=len),
            max(['', *oclc_dict["aa_oclc_derived"]["volume_multiple"]], key=len),
            max(['', *oclc_dict["aa_oclc_derived"]["edition_multiple"]], key=len),
            max(['', *oclc_dict["aa_oclc_derived"]["place_multiple"]], key=len),
            max(['', *oclc_dict["aa_oclc_derived"]["date_multiple"]], key=len),
        ]))))

        oclc_dict['aa_oclc_derived']['stripped_description_multiple'] = [strip_description(description) for description in oclc_dict['aa_oclc_derived']['description_multiple']]
        oclc_dict['aa_oclc_derived']['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes(language) for language in oclc_dict['aa_oclc_derived']['languages_multiple']])

        allthethings.utils.init_identifiers_and_classification_unified(oclc_dict['aa_oclc_derived'])
        allthethings.utils.add_identifier_unified(oclc_dict['aa_oclc_derived'], 'oclc', oclc_id)
        allthethings.utils.add_isbns_unified(oclc_dict['aa_oclc_derived'], oclc_dict['aa_oclc_derived']['isbn_multiple'])
        for issn in oclc_dict['aa_oclc_derived']['issn_multiple']:
            allthethings.utils.add_issn_unified(oclc_dict['aa_oclc_derived'], issn)
        for doi in oclc_dict['aa_oclc_derived']['doi_multiple']:
            allthethings.utils.add_identifier_unified(oclc_dict['aa_oclc_derived'], 'doi', doi)
        for aac_record in aac_records:
            allthethings.utils.add_identifier_unified(oclc_dict['aa_oclc_derived'], 'aacid', aac_record['aacid'])

        oclc_dict['aa_oclc_derived']["added_date_unified"] = { "date_oclc_scrape": "2023-10-01" }

        # TODO:
        # * cover_url
        # * comments
        # * other/related OCLC numbers
        # * redirects
        # * Genre for fiction detection
        # * Full audit of all fields
        # * dict comments

        oclc_dicts.append(oclc_dict)
    return oclc_dicts

# SIMILAR to get_edsebk_dicts_by_isbn13
def get_oclc_dicts_by_isbn13(session, isbn13s):
    if len(isbn13s) == 0:
        return {}
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT isbn13, oclc_id FROM isbn13_oclc WHERE isbn13 IN %(isbn13s)s', { "isbn13s": isbn13s })
        rows = list(cursor.fetchall())
        if len(rows) == 0:
            return {}
        isbn13s_by_oclc_id = collections.defaultdict(list)
        for row in rows:
            isbn13s_by_oclc_id[row['oclc_id']].append(str(row['isbn13']))
        oclc_dicts = get_oclc_dicts(session, 'oclc', list(isbn13s_by_oclc_id.keys()))
        retval = collections.defaultdict(list)
        for oclc_dict in oclc_dicts:
            for isbn13 in isbn13s_by_oclc_id[oclc_dict['oclc_id']]:
                retval[isbn13].append(oclc_dict)
        return dict(retval)

@page.get("/db/oclc/<path:oclc>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def oclc_oclc_json(oclc):
    with Session(engine) as session:
        oclc_dicts = get_oclc_dicts(session, 'oclc', [oclc])
        if len(oclc_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(oclc_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_duxiu_dicts(session, key, values, include_deep_transitive_md5s_size_path):
    if len(values) == 0:
        return []
    if key not in ['duxiu_ssid', 'cadal_ssno', 'md5', 'filename_decoded_basename']:
        raise Exception(f"Unexpected 'key' in get_duxiu_dicts: '{key}'")

    primary_id_prefix = f"{key}_"

    aac_records_by_primary_id = collections.defaultdict(dict)
    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        if key == 'md5':
            cursor.execute('SELECT annas_archive_meta__aacid__duxiu_records.byte_offset, annas_archive_meta__aacid__duxiu_records.byte_length, annas_archive_meta__aacid__duxiu_files.primary_id, annas_archive_meta__aacid__duxiu_files.byte_offset AS generated_file_byte_offset, annas_archive_meta__aacid__duxiu_files.byte_length AS generated_file_byte_length FROM annas_archive_meta__aacid__duxiu_records JOIN annas_archive_meta__aacid__duxiu_files ON (CONCAT("md5_", annas_archive_meta__aacid__duxiu_files.md5) = annas_archive_meta__aacid__duxiu_records.primary_id) WHERE annas_archive_meta__aacid__duxiu_files.primary_id IN %(values)s', { "values": values })
        elif key == 'filename_decoded_basename':
            cursor.execute('SELECT byte_offset, byte_length, filename_decoded_basename AS primary_id FROM annas_archive_meta__aacid__duxiu_records WHERE filename_decoded_basename IN %(values)s', { "values": values })
        else:
            cursor.execute('SELECT primary_id, byte_offset, byte_length FROM annas_archive_meta__aacid__duxiu_records WHERE primary_id IN %(values)s', { "values": [f'{primary_id_prefix}{value}' for value in values] })
    except Exception as err:
        print(f"Error in get_duxiu_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    top_level_records = []
    duxiu_records_indexes = []
    duxiu_records_offsets_and_lengths = []
    duxiu_files_indexes = []
    duxiu_files_offsets_and_lengths = []
    for row_index, row in enumerate(list(cursor.fetchall())):
        duxiu_records_indexes.append(row_index)
        duxiu_records_offsets_and_lengths.append((row['byte_offset'], row['byte_length']))
        if row.get('generated_file_byte_offset') is not None:
            duxiu_files_indexes.append(row_index)
            duxiu_files_offsets_and_lengths.append((row['generated_file_byte_offset'], row['generated_file_byte_length']))
        top_level_records.append([{ "primary_id": row['primary_id'] }, None])

    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'duxiu_records', duxiu_records_offsets_and_lengths)):
        top_level_records[duxiu_records_indexes[index]][0]["aac"] = orjson.loads(line_bytes)
    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'duxiu_files', duxiu_files_offsets_and_lengths)):
        top_level_records[duxiu_files_indexes[index]][1] = { "aac": orjson.loads(line_bytes) }

    for duxiu_record_dict, duxiu_file_dict in top_level_records:
        new_aac_record = {
            **duxiu_record_dict["aac"],
            "primary_id": duxiu_record_dict["primary_id"],
        }
        if duxiu_file_dict is not None:
            new_aac_record["generated_file_aacid"] = duxiu_file_dict["aac"]["aacid"]
            new_aac_record["generated_file_data_folder"] = duxiu_file_dict["aac"]["data_folder"]
            new_aac_record["generated_file_metadata"] = duxiu_file_dict["aac"]["metadata"]
        if "serialized_files" in new_aac_record["metadata"]["record"]:
            for serialized_file in new_aac_record["metadata"]["record"]["serialized_files"]:
                serialized_file['aa_derived_deserialized_gbk'] = ''
                try:
                    serialized_file['aa_derived_deserialized_gbk'] = base64.b64decode(serialized_file['data_base64']).decode('gbk')
                except Exception:
                    pass

            new_aac_record["metadata"]["record"]["aa_derived_ini_values"] = {}
            for serialized_file in new_aac_record['metadata']['record']['serialized_files']:
                if 'bkmk.txt' in serialized_file['filename'].lower():
                    continue
                if 'downpdg.log' in serialized_file['filename'].lower():
                    continue
                for line in serialized_file['aa_derived_deserialized_gbk'].split('\n'):
                    line = line.strip()
                    if '=' in line:
                        line_key, line_value = line.split('=', 1)
                        if line_value.strip() != '':
                            if line_key not in new_aac_record["metadata"]["record"]["aa_derived_ini_values"]:
                                new_aac_record["metadata"]["record"]["aa_derived_ini_values"][line_key] = []
                            new_aac_record["metadata"]["record"]["aa_derived_ini_values"][line_key].append({ 
                                "aacid": new_aac_record["aacid"],
                                "filename": serialized_file["filename"], 
                                "key": line_key, 
                                "value": line_value,
                            })

            if 'SS号' in new_aac_record["metadata"]["record"]["aa_derived_ini_values"]:
                new_aac_record["metadata"]["record"]["aa_derived_duxiu_ssid"] = new_aac_record["metadata"]["record"]["aa_derived_ini_values"]["SS号"][0]["value"]
            else:
                # TODO: Only duxiu_ssid here? Or also CADAL?
                ssid_dir = allthethings.utils.extract_ssid_or_ssno_from_filepath(new_aac_record['metadata']['record']['pdg_dir_name'])
                if ssid_dir is not None:
                    new_aac_record["metadata"]["record"]["aa_derived_duxiu_ssid"] = ssid_dir
                else:
                    ssid_filename = allthethings.utils.extract_ssid_or_ssno_from_filepath(new_aac_record['metadata']['record']['filename_decoded'])
                    if ssid_filename is not None:
                        new_aac_record["metadata"]["record"]["aa_derived_duxiu_ssid"] = ssid_filename

        aac_records_by_primary_id[new_aac_record['primary_id']][new_aac_record['aacid']] = new_aac_record

    if key != 'filename_decoded_basename':
        aa_derived_duxiu_ssids_to_primary_ids = collections.defaultdict(list)
        for primary_id, aac_records in aac_records_by_primary_id.items():
            for aac_record in aac_records.values():
                if "aa_derived_duxiu_ssid" in aac_record["metadata"]["record"]:
                    aa_derived_duxiu_ssids_to_primary_ids[aac_record["metadata"]["record"]["aa_derived_duxiu_ssid"]].append(primary_id)
        if len(aa_derived_duxiu_ssids_to_primary_ids) > 0:
            # Careful! Make sure this recursion doesn't loop infinitely.
            for record in get_duxiu_dicts(session, 'duxiu_ssid', list(aa_derived_duxiu_ssids_to_primary_ids.keys()), include_deep_transitive_md5s_size_path=include_deep_transitive_md5s_size_path):
                for primary_id in aa_derived_duxiu_ssids_to_primary_ids[record['duxiu_ssid']]:
                    for aac_record in record['aac_records']:
                        # NOTE: It's important that we append these aac_records at the end, since we select the "best" records
                        # first, and any data we get directly from the fields associated with the file itself should take precedence.
                        if aac_record['aacid'] not in aac_records_by_primary_id[primary_id]:
                            aac_records_by_primary_id[primary_id][aac_record['aacid']] = {
                                "aac_record_added_because": "duxiu_ssid",
                                **aac_record
                            }

        filename_decoded_basename_to_primary_ids = collections.defaultdict(list)
        for primary_id, aac_records in aac_records_by_primary_id.items():
            for aac_record in aac_records.values():
                if "filename_decoded" in aac_record["metadata"]["record"]:
                    basename = aac_record["metadata"]["record"]["filename_decoded"].rsplit('.', 1)[0][0:250] # Same logic as in MySQL query.
                    if len(basename) >= 5: # Skip very short basenames as they might have too many hits.
                        filename_decoded_basename_to_primary_ids[basename].append(primary_id)
        if len(filename_decoded_basename_to_primary_ids) > 0:
            # Careful! Make sure this recursion doesn't loop infinitely.
            for record in get_duxiu_dicts(session, 'filename_decoded_basename', list(filename_decoded_basename_to_primary_ids.keys()), include_deep_transitive_md5s_size_path=include_deep_transitive_md5s_size_path):
                for primary_id in filename_decoded_basename_to_primary_ids[record['filename_decoded_basename']]:
                    for aac_record in record['aac_records']:
                        # NOTE: It's important that we append these aac_records at the end, since we select the "best" records
                        # first, and any data we get directly from the fields associated with the file itself should take precedence.
                        if aac_record['aacid'] not in aac_records_by_primary_id[primary_id]:
                            aac_records_by_primary_id[primary_id][aac_record['aacid']] = {
                                "aac_record_added_because": "filename_decoded_basename",
                                **aac_record
                            }

    duxiu_dicts = []
    for primary_id, aac_records in aac_records_by_primary_id.items():
        # print(f"{primary_id=}, {aac_records=}")

        duxiu_dict = {}

        if key == 'duxiu_ssid':
            duxiu_dict['duxiu_ssid'] = primary_id.replace('duxiu_ssid_', '')
        elif key == 'cadal_ssno':
            duxiu_dict['cadal_ssno'] = primary_id.replace('cadal_ssno_', '')
        elif key == 'md5':
            duxiu_dict['md5'] = primary_id
        elif key == 'filename_decoded_basename':
            duxiu_dict['filename_decoded_basename'] = primary_id
        else:
            raise Exception(f"Unexpected 'key' in get_duxiu_dicts: '{key}'")
        duxiu_dict['duxiu_file'] = None
        duxiu_dict['aa_duxiu_derived'] = {}
        duxiu_dict['aa_duxiu_derived']['source_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['title_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['author_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['publisher_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['year_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['series_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['pages_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['cadal_ssno_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['isbn_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['issn_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['ean13_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['dxid_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['md5_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['aacid_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['filesize_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['filepath_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['ini_values_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['description_cumulative'] = []
        duxiu_dict['aa_duxiu_derived']['comments_cumulative'] = []
        duxiu_dict['aa_duxiu_derived']['debug_language_codes'] = {}
        duxiu_dict['aa_duxiu_derived']['language_codes'] = []
        duxiu_dict['aa_duxiu_derived']['added_date_unified'] = {}
        duxiu_dict['aa_duxiu_derived']['problems_infos'] = []
        duxiu_dict['aa_duxiu_derived']['related_files'] = []
        duxiu_dict['aac_records'] = list(aac_records.values())

        if key == 'duxiu_ssid':
            duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple'].append(duxiu_dict['duxiu_ssid'])
        elif key == 'cadal_ssno':
            duxiu_dict['aa_duxiu_derived']['cadal_ssno_multiple'].append(duxiu_dict['cadal_ssno'])
        elif key == 'md5':
            duxiu_dict['aa_duxiu_derived']['md5_multiple'].append(duxiu_dict['md5'])

        for aac_record in aac_records.values():
            duxiu_dict['aa_duxiu_derived']['aacid_multiple'].append(aac_record['aacid'])
            duxiu_dict['aa_duxiu_derived']['added_date_unified']['date_duxiu_meta_scrape'] = max(duxiu_dict['aa_duxiu_derived']['added_date_unified'].get('date_duxiu_meta_scrape') or '', datetime.datetime.strptime(aac_record['aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat().split('T', 1)[0])

            if aac_record['metadata']['type'] == 'dx_20240122__books':
                # 512w_final_csv has a bunch of incorrect records from dx_20240122__books deleted, so skip these entirely.
                # if len(aac_record['metadata']['record'].get('source') or '') > 0:
                #     duxiu_dict['aa_duxiu_derived']['source_multiple'].append(f"dx_20240122__books: {aac_record['metadata']['record']['source']} {aac_record['aacid']}")
                pass
            elif aac_record['metadata']['type'] in ['512w_final_csv', 'DX_corrections240209_csv']:
                if aac_record['metadata']['type'] == '512w_final_csv' and any([record['metadata']['type'] == 'DX_corrections240209_csv' for record in aac_records.values()]):
                    # Skip if there is also a correction.
                    pass

                duxiu_dict['aa_duxiu_derived']['source_multiple'].append(f"{aac_record['metadata']['type']}: {aac_record['aacid']}")

                if len(aac_record['metadata']['record'].get('title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['title_multiple'].append(aac_record['metadata']['record']['title'])
                if len(aac_record['metadata']['record'].get('author') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['author_multiple'].append(aac_record['metadata']['record']['author'])
                if len(aac_record['metadata']['record'].get('publisher') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['publisher_multiple'].append(aac_record['metadata']['record']['publisher'])
                if len(aac_record['metadata']['record'].get('year') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['year'])
                if len(aac_record['metadata']['record'].get('pages') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['pages_multiple'].append(aac_record['metadata']['record']['pages'])
                if len(aac_record['metadata']['record'].get('dx_id') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['dxid_multiple'].append(aac_record['metadata']['record']['dx_id'])

                if len(aac_record['metadata']['record'].get('isbn') or '') > 0:
                    identifiers = []
                    if aac_record['metadata']['record']['isbn_type'].startswith('multiple('):
                        identifier_values = aac_record['metadata']['record']['isbn'].split('_')
                        for index, identifier_type in enumerate(aac_record['metadata']['record']['isbn_type'][len('multiple('):-len(')')].split(',')):
                            identifiers.append({ 'type': identifier_type, 'value': identifier_values[index] })
                    elif aac_record['metadata']['record']['isbn_type'] != 'none':
                        identifiers.append({ 'type': aac_record['metadata']['record']['isbn_type'], 'value': aac_record['metadata']['record']['isbn'] })

                    for identifier in identifiers:
                        if identifier['type'] in ['ISBN-13', 'ISBN-10', 'CSBN']:
                            duxiu_dict['aa_duxiu_derived']['isbn_multiple'].append(identifier['value'])
                        elif identifier['type'] in ['ISSN-13', 'ISSN-8']:
                            duxiu_dict['aa_duxiu_derived']['issn_multiple'].append(identifier['value'])
                        elif identifier['type'] == 'EAN-13':
                            duxiu_dict['aa_duxiu_derived']['ean13_multiple'].append(identifier['value'])
                        elif identifier['type'] in ['unknown', 'unknow']:
                            pass
                        else:
                            raise Exception(f"Unknown type of duxiu 512w_final_csv isbn_type {identifier_type=}")
            elif aac_record['metadata']['type'] == 'dx_20240122__remote_files':
                if len(aac_record['metadata']['record'].get('source') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['source_multiple'].append(f"dx_20240122__remote_files: {aac_record['metadata']['record']['source']} {aac_record['aacid']}")
                else:
                    duxiu_dict['aa_duxiu_derived']['source_multiple'].append(f"dx_20240122__remote_files: {aac_record['aacid']}")
                if len(aac_record['metadata']['record'].get('dx_id') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['dxid_multiple'].append(aac_record['metadata']['record']['dx_id'])

                related_file = {
                    "filepath": None,
                    "md5": None,
                    "filesize": None,
                    "from": "dx_20240122__remote_files",
                    "aacid": aac_record['aacid'],
                }
                if len(aac_record['metadata']['record'].get('md5') or '') > 0:
                    related_file['md5'] = aac_record['metadata']['record']['md5']
                if (aac_record['metadata']['record'].get('size') or 0) > 0:
                    related_file['filesize'] = aac_record['metadata']['record']['size']    
                filepath_components = []
                if len(aac_record['metadata']['record'].get('path') or '') > 0:
                    filepath_components.append(aac_record['metadata']['record']['path'])
                    if not aac_record['metadata']['record']['path'].endswith('/'):
                        filepath_components.append('/')
                if len(aac_record['metadata']['record'].get('filename') or '') > 0:
                    filepath_components.append(aac_record['metadata']['record']['filename'])
                if len(filepath_components) > 0:
                    related_file['filepath'] = ''.join(filepath_components)

                duxiu_dict['aa_duxiu_derived']['related_files'].append(related_file)

            elif aac_record['metadata']['type'] == 'dx_toc_db__dx_toc':
                duxiu_dict['aa_duxiu_derived']['source_multiple'].append(f"dx_toc_db__dx_toc: {aac_record['aacid']}")
                # TODO: Better parsing; maintain tree structure.
                toc_xml = (aac_record['metadata']['record'].get('toc_xml') or '')
                toc_matches = re.findall(r'id="([^"]+)" Caption="([^"]+)" PageNumber="([^"]+)"', toc_xml)
                if len(toc_matches) > 0:
                    duxiu_dict['aa_duxiu_derived']['description_cumulative'].append('\n'.join([f"{match[2]} ({match[0]}): {match[1]}" for match in toc_matches]))
            elif aac_record['metadata']['type'] == 'cadal_table__books_detail':
                duxiu_dict['aa_duxiu_derived']['source_multiple'].append(f"cadal_table__books_detail: {aac_record['aacid']}")
                if len(aac_record['metadata']['record'].get('title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['title_multiple'].append(aac_record['metadata']['record']['title'])
                if len(aac_record['metadata']['record'].get('creator') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['author_multiple'].append(aac_record['metadata']['record']['creator'])
                if len(aac_record['metadata']['record'].get('publisher') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['publisher_multiple'].append(aac_record['metadata']['record']['publisher'])
                if len(aac_record['metadata']['record'].get('isbn') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['isbn_multiple'].append(aac_record['metadata']['record']['isbn'])
                if len(aac_record['metadata']['record'].get('date') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['date'])
                if len(aac_record['metadata']['record'].get('page_num') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['pages_multiple'].append(aac_record['metadata']['record']['page_num'])
                if len(aac_record['metadata']['record'].get('common_title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['common_title'])
                if len(aac_record['metadata']['record'].get('topic') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['topic'])
                if len(aac_record['metadata']['record'].get('tags') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['tags'])
                if len(aac_record['metadata']['record'].get('period') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['period'])
                if len(aac_record['metadata']['record'].get('period_year') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['period_year'])
                if len(aac_record['metadata']['record'].get('publication_place') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['publication_place'])
                if len(aac_record['metadata']['record'].get('common_title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['common_title'])
                if len(aac_record['metadata']['record'].get('type') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['type'])
            elif aac_record['metadata']['type'] == 'cadal_table__books_solr':
                duxiu_dict['aa_duxiu_derived']['source_multiple'].append(f"cadal_table__books_solr: {aac_record['aacid']}")
                if len(aac_record['metadata']['record'].get('Title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['title_multiple'].append(aac_record['metadata']['record']['Title'])
                if len(aac_record['metadata']['record'].get('CreateDate') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['CreateDate'])
                if len(aac_record['metadata']['record'].get('ISBN') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['isbn_multiple'].append(aac_record['metadata']['record']['ISBN'])
                if len(aac_record['metadata']['record'].get('Creator') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['author_multiple'].append(aac_record['metadata']['record']['Creator'])
                if len(aac_record['metadata']['record'].get('Publisher') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['publisher_multiple'].append(aac_record['metadata']['record']['Publisher'])
                if len(aac_record['metadata']['record'].get('Page') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['pages_multiple'].append(aac_record['metadata']['record']['Page'])
                if len(aac_record['metadata']['record'].get('Description') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['description_cumulative'].append(aac_record['metadata']['record']['Description'])
                if len(aac_record['metadata']['record'].get('Subject') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Subject'])
                if len(aac_record['metadata']['record'].get('theme') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['theme'])
                if len(aac_record['metadata']['record'].get('label') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['label'])
                if len(aac_record['metadata']['record'].get('HostID') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['HostID'])
                if len(aac_record['metadata']['record'].get('Contributor') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Contributor'])
                if len(aac_record['metadata']['record'].get('Relation') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Relation'])
                if len(aac_record['metadata']['record'].get('Rights') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Rights'])
                if len(aac_record['metadata']['record'].get('Format') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Format'])
                if len(aac_record['metadata']['record'].get('Type') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Type'])
                if len(aac_record['metadata']['record'].get('BookType') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['BookType'])
                if len(aac_record['metadata']['record'].get('Coverage') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Coverage'])
            elif aac_record['metadata']['type'] == 'cadal_table__site_journal_items':
                if len(aac_record['metadata']['record'].get('date_year') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['date_year'])
                # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__sa_newspaper_items':
                if len(aac_record['metadata']['record'].get('date_year') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['date_year'])
                # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__books_search':
                pass # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__site_book_collection_items':
                pass # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__sa_collection_items':
                pass # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__books_aggregation':
                pass # TODO
            elif aac_record['metadata']['type'] == 'aa_catalog_files':
                if len(aac_record.get('generated_file_aacid') or '') > 0:
                    duxiu_dict['duxiu_file'] = {
                        "aacid": aac_record['generated_file_aacid'],
                        "data_folder": aac_record['generated_file_data_folder'],
                        "filesize": aac_record['generated_file_metadata']['filesize'],
                        "extension": 'pdf',
                    }
                    # Make sure to prepend these, in case there is another 'aa_catalog_files' entry without a generated_file.
                    # No need to check for include_deep_transitive_md5s_size_path here, because generated_file_aacid only exists
                    # for the primary (non-transitive) md5 record.
                    duxiu_dict['aa_duxiu_derived']['md5_multiple'] = [aac_record['generated_file_metadata']['md5'], aac_record['generated_file_metadata']['original_md5']] + duxiu_dict['aa_duxiu_derived']['md5_multiple']
                    duxiu_dict['aa_duxiu_derived']['filesize_multiple'] = [int(aac_record['generated_file_metadata']['filesize'])] + duxiu_dict['aa_duxiu_derived']['filesize_multiple']
                    duxiu_dict['aa_duxiu_derived']['filepath_multiple'] = [aac_record['metadata']['record']['filename_decoded']] + duxiu_dict['aa_duxiu_derived']['filepath_multiple']

                    duxiu_dict['aa_duxiu_derived']['added_date_unified']['date_duxiu_filegen'] = datetime.datetime.strptime(aac_record['generated_file_aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat().split('T', 1)[0]

                    # Only check for problems when we have generated_file_aacid, since that indicates this is the main file record.
                    if len(aac_record['metadata']['record']['pdg_broken_files']) > 3:
                        duxiu_dict['aa_duxiu_derived']['problems_infos'].append({
                            'duxiu_problem_type': 'pdg_broken_files',
                            'pdg_broken_files_len': len(aac_record['metadata']['record']['pdg_broken_files']),
                        })
                else:
                    related_file = {
                        "filepath": aac_record['metadata']['record']['filename_decoded'],
                        "md5": aac_record['metadata']['record']['md5'],
                        "filesize": int(aac_record['metadata']['record']['filesize']),
                        "from": "aa_catalog_files",
                        "aacid": aac_record['aacid'],
                    }
                    duxiu_dict['aa_duxiu_derived']['related_files'].append(related_file)

                duxiu_dict['aa_duxiu_derived']['source_multiple'].append(f"aa_catalog_files: {aac_record['aacid']}")

                aa_derived_ini_values = aac_record['metadata']['record']['aa_derived_ini_values']
                for aa_derived_ini_values_list in aa_derived_ini_values.values():
                    duxiu_dict['aa_duxiu_derived']['ini_values_multiple'] += aa_derived_ini_values_list
                for ini_value in ((aa_derived_ini_values.get('Title') or []) + (aa_derived_ini_values.get('书名') or [])):
                    duxiu_dict['aa_duxiu_derived']['title_multiple'].append(ini_value['value'])
                for ini_value in ((aa_derived_ini_values.get('Author') or []) + (aa_derived_ini_values.get('作者') or [])):
                    duxiu_dict['aa_duxiu_derived']['author_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('出版社') or []):
                    duxiu_dict['aa_duxiu_derived']['publisher_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('丛书名') or []):
                    duxiu_dict['aa_duxiu_derived']['series_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('出版日期') or []):
                    potential_year = re.search(r"(\d\d\d\d)", ini_value['value'])
                    if potential_year is not None:
                        duxiu_dict['aa_duxiu_derived']['year_multiple'].append(potential_year[0])
                for ini_value in (aa_derived_ini_values.get('页数') or []):
                    duxiu_dict['aa_duxiu_derived']['pages_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('ISBN号') or []):
                    duxiu_dict['aa_duxiu_derived']['isbn_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('DX号') or []):
                    duxiu_dict['aa_duxiu_derived']['dxid_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('SS号') or []):
                    duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple'].append(ini_value['value'])

                for ini_value in (aa_derived_ini_values.get('参考文献格式') or []): # Reference format
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('原书定价') or []): # Original Book Pricing
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('中图法分类号') or []): # CLC Classification Number # TODO: more proper handling than throwing in description
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('主题词') or []): # Keywords
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('Subject') or []):
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('Keywords') or []):
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])

                if 'aa_derived_duxiu_ssid' in aac_record['metadata']['record']:
                    duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple'].append(aac_record['metadata']['record']['aa_derived_duxiu_ssid'])
            else:
                raise Exception(f"Unknown type of duxiu metadata type {aac_record['metadata']['type']=}")

        allthethings.utils.init_identifiers_and_classification_unified(duxiu_dict['aa_duxiu_derived'])
        allthethings.utils.add_isbns_unified(duxiu_dict['aa_duxiu_derived'], duxiu_dict['aa_duxiu_derived']['isbn_multiple'])
        allthethings.utils.add_isbns_unified(duxiu_dict['aa_duxiu_derived'], allthethings.utils.get_isbnlike('\n'.join(duxiu_dict['aa_duxiu_derived']['filepath_multiple'] + duxiu_dict['aa_duxiu_derived']['description_cumulative'] + duxiu_dict['aa_duxiu_derived']['comments_cumulative'])))
        for duxiu_ssid in duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'duxiu_ssid', duxiu_ssid)
        for cadal_ssno in duxiu_dict['aa_duxiu_derived']['cadal_ssno_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'cadal_ssno', cadal_ssno)
        for issn in duxiu_dict['aa_duxiu_derived']['issn_multiple']:
            allthethings.utils.add_issn_unified(duxiu_dict['aa_duxiu_derived'], issn)
        for ean13 in duxiu_dict['aa_duxiu_derived']['ean13_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'ean13', ean13)
        for dxid in duxiu_dict['aa_duxiu_derived']['dxid_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'duxiu_dxid', dxid)
        for md5 in duxiu_dict['aa_duxiu_derived']['md5_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'md5', md5)
        for aacid in duxiu_dict['aa_duxiu_derived']['aacid_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'aacid', aacid)

        if include_deep_transitive_md5s_size_path:
            for related_file in duxiu_dict['aa_duxiu_derived']['related_files']:
                if related_file['md5'] is not None:
                    duxiu_dict['aa_duxiu_derived']['md5_multiple'].append(related_file['md5'])
                if related_file['filesize'] is not None:
                    duxiu_dict['aa_duxiu_derived']['filesize_multiple'].append(related_file['filesize'])
                if related_file['filepath'] is not None:
                    duxiu_dict['aa_duxiu_derived']['filepath_multiple'].append(related_file['filepath'])
                if related_file['aacid'] is not None:
                    duxiu_dict['aa_duxiu_derived']['aacid_multiple'].append(related_file['aacid'])

        # We know this collection is mostly Chinese language, so mark as Chinese if any of these (lightweight) tests pass.
        if 'isbn13' in duxiu_dict['aa_duxiu_derived']['identifiers_unified']:
            isbnlib_info = isbnlib.info(duxiu_dict['aa_duxiu_derived']['identifiers_unified']['isbn13'][0])
            if 'china' in isbnlib_info.lower():
                duxiu_dict['aa_duxiu_derived']['language_codes'] = ['zh']
        else: # If there is an isbn13 and it's not from China, then there's a good chance it's a foreign work, so don't do the language detect in that case.
            language_detect_string = " ".join(list(dict.fromkeys(duxiu_dict['aa_duxiu_derived']['title_multiple'] + duxiu_dict['aa_duxiu_derived']['author_multiple'] + duxiu_dict['aa_duxiu_derived']['publisher_multiple'])))
            langdetect_response = {}
            try:
                langdetect_response = fast_langdetect.detect(language_detect_string)
            except Exception:
                pass
            duxiu_dict['aa_duxiu_derived']['debug_language_codes'] = { 'langdetect_response': langdetect_response }

            if langdetect_response['lang'] in ['zh', 'ja', 'ko'] and langdetect_response['score'] > 0.5: # Somewhat arbitrary cutoff for any CJK lang.
                duxiu_dict['aa_duxiu_derived']['language_codes'] = ['zh']

        duxiu_dict['aa_duxiu_derived']['title_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['title_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['author_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['author_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['publisher_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['publisher_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['year_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['year_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['series_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['series_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['pages_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['pages_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['filesize_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['filesize_multiple']), 0)
        duxiu_dict['aa_duxiu_derived']['filepath_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['filepath_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['description_best'] = '\n\n'.join(list(dict.fromkeys(duxiu_dict['aa_duxiu_derived']['description_cumulative'])))
        _sources_joined = '\n'.join(sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(duxiu_dict['aa_duxiu_derived']['source_multiple']))
        related_files_joined = '\n'.join(sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode([" — ".join([f"{key}:{related_file[key]}" for key in ["filepath", "md5", "filesize"] if related_file[key] is not None]) for related_file in duxiu_dict['aa_duxiu_derived']['related_files']]))
        duxiu_dict['aa_duxiu_derived']['combined_comments'] = list(dict.fromkeys(filter(len, duxiu_dict['aa_duxiu_derived']['comments_cumulative'] + [
            # TODO: pass through comments metadata in a structured way so we can add proper translations.
            # For now remove sources, it's not useful enough and it's still in the JSON.
            # f"sources:\n{sources_joined}" if sources_joined != "" else "",
            f"related_files:\n{related_files_joined}" if related_files_joined != "" else "",
        ])))
        duxiu_dict['aa_duxiu_derived']['edition_varia_normalized'] = ', '.join(list(dict.fromkeys(filter(len, [
            next(iter(duxiu_dict['aa_duxiu_derived']['series_multiple']), ''),
            next(iter(duxiu_dict['aa_duxiu_derived']['year_multiple']), ''),
        ]))))


        duxiu_dict_derived_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "source_multiple": ("before", ["Sources of the metadata."]),
            "md5_multiple": ("before", ["Includes both our generated MD5, and the original file MD5."]),
            "filesize_multiple": ("before", ["Includes both our generated file’s size, and the original filesize.",
                                "Our generated filesize should be the first listed."]),
            "filepath_multiple": ("before", ["Original filenames."]),
            "ini_values_multiple": ("before", ["Extracted .ini-style entries from serialized_files."]),
            "language_codes": ("before", ["Our inferred language codes (BCP 47).",
                                "Gets set to 'zh' if the ISBN is Chinese, or if the language detection finds a CJK lang."]),
            "duxiu_ssid_multiple": ("before", ["Duxiu SSID, often extracted from .ini-style values or filename (8 digits)."
                                "This is then used to bring in more metadata."]),
            "title_best": ("before", ["For the DuXiu collection, these 'best' fields pick the first value from the '_multiple' fields."
                                "The first values are metadata taken directly from the files, followed by metadata from associated DuXiu SSID records."]),
        }
        duxiu_dict['aa_duxiu_derived'] = add_comments_to_dict(duxiu_dict['aa_duxiu_derived'], duxiu_dict_derived_comments)

        duxiu_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "duxiu_ssid": ("before", ["This is a DuXiu metadata record.",
                                "More details at https://annas-archive.se/datasets/duxiu",
                                allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "cadal_ssno": ("before", ["This is a CADAL metadata record.",
                                "More details at https://annas-archive.se/datasets/duxiu",
                                allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "md5": ("before", ["This is a DuXiu/related metadata record.",
                                "More details at https://annas-archive.se/datasets/duxiu",
                                allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "duxiu_file": ("before", ["Information on the actual file in our collection (see torrents)."]),
            "aa_duxiu_derived": ("before", "Derived metadata."),
            "aac_records": ("before", "Metadata records from the 'duxiu_records' file, which is a compilation of metadata from various sources."),
        }
        duxiu_dicts.append(add_comments_to_dict(duxiu_dict, duxiu_dict_comments))

    # TODO: Look at more ways of associating remote files besides SSID.
    # TODO: Parse TOCs.
    # TODO: Book covers.
    # TODO: DuXiu book types mostly (even only?) non-fiction?
    # TODO: Mostly Chinese, detect non-Chinese based on English text or chars in title?
    # TODO: Pull in more CADAL fields.

    return duxiu_dicts

# Good examples:
# select primary_id, count(*) as c, group_concat(json_extract(metadata, '$.type')) as type from annas_archive_meta__aacid__duxiu_records group by primary_id order by c desc limit 100;
# duxiu_ssid_10000431    |        3 | "dx_20240122__books","dx_20240122__remote_files","512w_final_csv"
# cadal_ssno_06G48911    |        2 | "cadal_table__site_journal_items","cadal_table__sa_newspaper_items"
# cadal_ssno_01000257    |        2 | "cadal_table__site_book_collection_items","cadal_table__sa_collection_items"
# cadal_ssno_06G48910    |        2 | "cadal_table__sa_newspaper_items","cadal_table__site_journal_items"
# cadal_ssno_ZY297043388 |        2 | "cadal_table__sa_collection_items","cadal_table__books_aggregation"
# cadal_ssno_01000001    |        2 | "cadal_table__books_solr","cadal_table__books_detail"
# duxiu_ssid_11454502    |        1 | "dx_toc_db__dx_toc"
# duxiu_ssid_10002062    |        1 | "DX_corrections240209_csv"
# 
# duxiu_ssid_14084714 has Miaochuan link.
# cadal_ssno_44517971 has some <font>s.
# 
@page.get("/db/duxiu_ssid/<path:duxiu_ssid>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def duxiu_ssid_json(duxiu_ssid):
    with Session(engine) as session:
        duxiu_dicts = get_duxiu_dicts(session, 'duxiu_ssid', [duxiu_ssid], include_deep_transitive_md5s_size_path=True)
        if len(duxiu_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(duxiu_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/cadal_ssno/<path:cadal_ssno>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def cadal_ssno_json(cadal_ssno):
    with Session(engine) as session:
        duxiu_dicts = get_duxiu_dicts(session, 'cadal_ssno', [cadal_ssno], include_deep_transitive_md5s_size_path=True)
        if len(duxiu_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(duxiu_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/duxiu_md5/<path:md5>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def duxiu_md5_json(md5):
    with Session(engine) as session:
        duxiu_dicts = get_duxiu_dicts(session, 'md5', [md5], include_deep_transitive_md5s_size_path=False)
        if len(duxiu_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(duxiu_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def upload_book_exiftool_append(newlist, record, fieldname):
    field = (record['metadata'].get('exiftool_output') or {}).get(fieldname)
    if field is None:
        pass
    elif isinstance(field, str):
        field = field.strip()
        if len(field) > 0:
            newlist.append(field)
    elif isinstance(field, int) or isinstance(field, float):
        newlist.append(str(field))
    elif isinstance(field, list):
        field = ",".join([str(item).strip() for item in field])
        if len(field) > 0:
            newlist.append(field)
    else:
        raise Exception(f"Unexpected field in upload_book_exiftool_append: {record=} {fieldname=} {field=}")

def get_aac_upload_book_dicts(session, key, values):
    if len(values) == 0:
        return []
    if key == 'md5':
        aac_key = 'annas_archive_meta__aacid__upload_records.md5'
    else:
        raise Exception(f"Unexpected 'key' in get_aac_upload_book_dicts: '{key}'")
    
    aac_upload_book_dicts_raw = []
    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f'SELECT annas_archive_meta__aacid__upload_records.byte_offset AS record_byte_offset, annas_archive_meta__aacid__upload_records.byte_length AS record_byte_length, annas_archive_meta__aacid__upload_files.byte_offset AS file_byte_offset, annas_archive_meta__aacid__upload_files.byte_length AS file_byte_length, annas_archive_meta__aacid__upload_records.md5 AS md5 FROM annas_archive_meta__aacid__upload_records LEFT JOIN annas_archive_meta__aacid__upload_files ON (annas_archive_meta__aacid__upload_records.md5 = annas_archive_meta__aacid__upload_files.primary_id) WHERE {aac_key} IN %(values)s', { "values": [str(value) for value in values] })
    
        upload_records_indexes = []
        upload_records_offsets_and_lengths = []
        upload_files_indexes = []
        upload_files_offsets_and_lengths = []
        records_by_md5 = collections.defaultdict(dict)
        files_by_md5 = collections.defaultdict(dict)
        for row_index, row in enumerate(list(cursor.fetchall())):
            upload_records_indexes.append(row_index)
            upload_records_offsets_and_lengths.append((row['record_byte_offset'], row['record_byte_length']))
            if row.get('file_byte_offset') is not None:
                upload_files_indexes.append(row_index)
                upload_files_offsets_and_lengths.append((row['file_byte_offset'], row['file_byte_length']))
        for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'upload_records', upload_records_offsets_and_lengths)):
            record = orjson.loads(line_bytes)
            records_by_md5[record['metadata']['md5']][record['aacid']] = record
        for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'upload_files', upload_files_offsets_and_lengths)):
            file = orjson.loads(line_bytes)
            files_by_md5[file['metadata']['md5']][file['aacid']] = file
        for md5 in list(dict.fromkeys(list(records_by_md5.keys()) + list(files_by_md5.keys()))):
            aac_upload_book_dicts_raw.append({
                "md5": md5,
                "records": list(records_by_md5[md5].values()),
                "files": list(files_by_md5[md5].values()),
            })
    except Exception as err:
        print(f"Error in get_aac_upload_book_dicts_raw when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    aac_upload_book_dicts = []
    for aac_upload_book_dict_raw in aac_upload_book_dicts_raw:
        aac_upload_book_dict = {
            "md5": aac_upload_book_dict_raw['md5'],
            "aa_upload_derived": {},
            "records": aac_upload_book_dict_raw['records'],
            "files": aac_upload_book_dict_raw['files'],
        }
        aac_upload_book_dict['aa_upload_derived']['subcollection_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['filename_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['filesize_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['extension_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['title_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['author_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['publisher_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['pages_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['source_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['producer_multiple'] = []
        aac_upload_book_dict['aa_upload_derived']['description_cumulative'] = []
        aac_upload_book_dict['aa_upload_derived']['comments_cumulative'] = []
        aac_upload_book_dict['aa_upload_derived']['language_codes'] = []
        aac_upload_book_dict['aa_upload_derived']['problems_infos'] = []
        aac_upload_book_dict['aa_upload_derived']['content_type'] = ''
        aac_upload_book_dict['aa_upload_derived']['added_date_unified'] = {}
        allthethings.utils.init_identifiers_and_classification_unified(aac_upload_book_dict['aa_upload_derived'])

        for record in aac_upload_book_dict['records']:
            if 'filesize' not in record['metadata']:
                print(f"WARNING: filesize missing in aac_upload_record: {record=}")
                continue

            allthethings.utils.add_identifier_unified(aac_upload_book_dict['aa_upload_derived'], 'aacid', record['aacid'])
            subcollection = record['aacid'].split('__')[1].replace('upload_records_', '')
            aac_upload_book_dict['aa_upload_derived']['subcollection_multiple'].append(subcollection)
            aac_upload_book_dict['aa_upload_derived']['filename_multiple'].append(f"{subcollection}/{record['metadata']['filepath']}")
            aac_upload_book_dict['aa_upload_derived']['filesize_multiple'].append(int(record['metadata']['filesize']))

            if '.' in record['metadata']['filepath']:
                extension = record['metadata']['filepath'].rsplit('.', 1)[-1]
                if (len(extension) <= 4) and (extension not in ['bin']):
                    aac_upload_book_dict['aa_upload_derived']['extension_multiple'].append(extension)
            # Note that exiftool detects comic books as zip, so actual filename extension is still preferable in most cases.
            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['extension_multiple'], record, 'FileTypeExtension')

            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['title_multiple'], record, 'Title')
            if len(((record['metadata'].get('pikepdf_docinfo') or {}).get('/Title') or '').strip()) > 0:
                aac_upload_book_dict['aa_upload_derived']['title_multiple'].append(record['metadata']['pikepdf_docinfo']['/Title'].strip())

            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['author_multiple'], record, 'Author')
            if len(((record['metadata'].get('pikepdf_docinfo') or {}).get('/Author') or '').strip()) > 0:
                aac_upload_book_dict['aa_upload_derived']['author_multiple'].append(record['metadata']['pikepdf_docinfo']['/Author'].strip())
            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['author_multiple'], record, 'Creator')

            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['publisher_multiple'], record, 'Publisher')
            if len(((record['metadata'].get('pikepdf_docinfo') or {}).get('/Publisher') or '').strip()) > 0:
                aac_upload_book_dict['aa_upload_derived']['publisher_multiple'].append(record['metadata']['pikepdf_docinfo']['/Publisher'].strip())

            if (record['metadata'].get('total_pages') or 0) > 0:
                aac_upload_book_dict['aa_upload_derived']['pages_multiple'].append(str(record['metadata']['total_pages']))
            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['pages_multiple'], record, 'PageCount')

            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['description_cumulative'], record, 'Description')
            if len(((record['metadata'].get('pikepdf_docinfo') or {}).get('/Description') or '').strip()) > 0:
                aac_upload_book_dict['aa_upload_derived']['description_cumulative'].append(record['metadata']['pikepdf_docinfo']['/Description'].strip())
            if len((record['metadata'].get('pdftoc_output2_stdout') or '')) > 0:
                aac_upload_book_dict['aa_upload_derived']['description_cumulative'].append(record['metadata']['pdftoc_output2_stdout'].strip())
            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['description_cumulative'], record, 'Keywords')
            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['description_cumulative'], record, 'Subject')

            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['source_multiple'], record, 'Source')

            upload_book_exiftool_append(aac_upload_book_dict['aa_upload_derived']['producer_multiple'], record, 'Producer')

            if (record['metadata'].get('exiftool_failed') or False) and ('Wide character in print' not in ((record['metadata'].get('exiftool_output') or {}).get('error') or '')):
                aac_upload_book_dict['aa_upload_derived']['problems_infos'].append({
                    'upload_problem_type': 'exiftool_failed',
                })

            potential_languages = []
            # Sadly metadata doesn’t often have reliable information about languages. Many tools seem to default to tagging with English when writing PDFs.
            # upload_book_exiftool_append(potential_languages, record, 'Language')
            # upload_book_exiftool_append(potential_languages, record, 'Languages')
            # if len(((record['metadata'].get('pikepdf_docinfo') or {}).get('/Language') or '').strip()) > 0:
            #     potential_languages.append(record['metadata']['pikepdf_docinfo']['/Language'] or '')
            # if len(((record['metadata'].get('pikepdf_docinfo') or {}).get('/Languages') or '').strip()) > 0:
            #     potential_languages.append(record['metadata']['pikepdf_docinfo']['/Languages'] or '')
            if 'japanese_manga' in subcollection:
                potential_languages.append('Japanese')
            if 'polish' in subcollection:
                potential_languages.append('Polish')
            if len(potential_languages) > 0:
                aac_upload_book_dict['aa_upload_derived']['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes(language) for language in potential_languages])

            if len(str((record['metadata'].get('exiftool_output') or {}).get('Identifier') or '').strip()) > 0:
                allthethings.utils.add_isbns_unified(aac_upload_book_dict['aa_upload_derived'], allthethings.utils.get_isbnlike(str(record['metadata']['exiftool_output']['Identifier'] or '')))
            allthethings.utils.add_isbns_unified(aac_upload_book_dict['aa_upload_derived'], allthethings.utils.get_isbnlike('\n'.join([record['metadata']['filepath']] + aac_upload_book_dict['aa_upload_derived']['title_multiple'] + aac_upload_book_dict['aa_upload_derived']['description_cumulative'])))

            doi_from_filepath = allthethings.utils.extract_doi_from_filepath(record['metadata']['filepath'])
            if doi_from_filepath is not None:
                allthethings.utils.add_identifier_unified(aac_upload_book_dict['aa_upload_derived'], 'doi', doi_from_filepath)
            doi_from_text = allthethings.utils.find_doi_in_text('\n'.join([record['metadata']['filepath']] + aac_upload_book_dict['aa_upload_derived']['title_multiple'] + aac_upload_book_dict['aa_upload_derived']['description_cumulative']))
            if doi_from_text is not None:
                allthethings.utils.add_identifier_unified(aac_upload_book_dict['aa_upload_derived'], 'doi', doi_from_text)

            if 'bpb9v_cadal' in subcollection:
                cadal_ssno_filename = allthethings.utils.extract_ssid_or_ssno_from_filepath(record['metadata']['filepath'])
                if cadal_ssno_filename is not None:
                    allthethings.utils.add_identifier_unified(aac_upload_book_dict['aa_upload_derived'], 'cadal_ssno', cadal_ssno_filename)
            if ('duxiu' in subcollection) or ('chinese' in subcollection):
                duxiu_ssid_filename = allthethings.utils.extract_ssid_or_ssno_from_filepath(record['metadata']['filepath'])
                if duxiu_ssid_filename is not None:
                    allthethings.utils.add_identifier_unified(aac_upload_book_dict['aa_upload_derived'], 'duxiu_ssid', duxiu_ssid_filename)

            upload_record_date = datetime.datetime.strptime(record['aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat().split('T', 1)[0]
            aac_upload_book_dict['aa_upload_derived']['added_date_unified']['date_upload_record'] = min(upload_record_date, aac_upload_book_dict['aa_upload_derived']['added_date_unified'].get('date_upload_record') or upload_record_date)

            file_created_date = None
            create_date_field = (record['metadata'].get('exiftool_output') or {}).get('CreateDate') or ''
            if create_date_field != '':
                try:
                    file_created_date = datetime.datetime.strptime(create_date_field, "%Y:%m:%d %H:%M:%S%z").astimezone(datetime.timezone.utc).replace(tzinfo=None).isoformat().split('T', 1)[0]
                except Exception:
                    try:
                        file_created_date = datetime.datetime.strptime(create_date_field, "%Y:%m:%d %H:%M:%S").isoformat().split('T', 1)[0]
                    except Exception:
                        pass
            if file_created_date is not None:
                aac_upload_book_dict['aa_upload_derived']['added_date_unified']['date_file_created'] = min(file_created_date, aac_upload_book_dict['aa_upload_derived']['added_date_unified'].get('date_file_created') or file_created_date)

        if any([('duxiu' in subcollection) or ('chinese' in subcollection) for subcollection in aac_upload_book_dict['aa_upload_derived']['subcollection_multiple']]):
            aac_upload_book_dict['aa_upload_derived']['filename_multiple'] = [allthethings.utils.attempt_fix_chinese_filepath(text) for text in aac_upload_book_dict['aa_upload_derived']['filename_multiple']]
            aac_upload_book_dict['aa_upload_derived']['title_multiple'] = [allthethings.utils.attempt_fix_chinese_uninterrupted_text(text) for text in aac_upload_book_dict['aa_upload_derived']['title_multiple']]
            aac_upload_book_dict['aa_upload_derived']['author_multiple'] = [allthethings.utils.attempt_fix_chinese_uninterrupted_text(text) for text in aac_upload_book_dict['aa_upload_derived']['author_multiple']]
            aac_upload_book_dict['aa_upload_derived']['publisher_multiple'] = [allthethings.utils.attempt_fix_chinese_uninterrupted_text(text) for text in aac_upload_book_dict['aa_upload_derived']['publisher_multiple']]
            aac_upload_book_dict['aa_upload_derived']['source_multiple'] = [allthethings.utils.attempt_fix_chinese_uninterrupted_text(text) for text in aac_upload_book_dict['aa_upload_derived']['source_multiple']]
            aac_upload_book_dict['aa_upload_derived']['producer_multiple'] = [allthethings.utils.attempt_fix_chinese_uninterrupted_text(text) for text in aac_upload_book_dict['aa_upload_derived']['producer_multiple']]
            aac_upload_book_dict['aa_upload_derived']['description_cumulative'] = [allthethings.utils.attempt_fix_chinese_uninterrupted_text(text) for text in aac_upload_book_dict['aa_upload_derived']['description_cumulative']]
            aac_upload_book_dict['aa_upload_derived']['comments_cumulative'] = [allthethings.utils.attempt_fix_chinese_uninterrupted_text(text) for text in aac_upload_book_dict['aa_upload_derived']['comments_cumulative']]

        if any(['degruyter' in subcollection for subcollection in aac_upload_book_dict['aa_upload_derived']['subcollection_multiple']]):
            aac_upload_book_dict['aa_upload_derived']['title_multiple'] = [title for title in aac_upload_book_dict['aa_upload_derived']['title_multiple'] if title != 'Page not found']

        aac_upload_book_dict['aa_upload_derived']['filename_best'] = next(iter(aac_upload_book_dict['aa_upload_derived']['filename_multiple']), '')
        aac_upload_book_dict['aa_upload_derived']['filesize_best'] = next(iter(aac_upload_book_dict['aa_upload_derived']['filesize_multiple']), '')
        aac_upload_book_dict['aa_upload_derived']['extension_best'] = next(iter(aac_upload_book_dict['aa_upload_derived']['extension_multiple']), '')
        aac_upload_book_dict['aa_upload_derived']['title_best'] = next(iter(aac_upload_book_dict['aa_upload_derived']['title_multiple']), '')
        aac_upload_book_dict['aa_upload_derived']['author_best'] = next(iter(aac_upload_book_dict['aa_upload_derived']['author_multiple']), '')
        aac_upload_book_dict['aa_upload_derived']['publisher_best'] = next(iter(aac_upload_book_dict['aa_upload_derived']['publisher_multiple']), '')
        aac_upload_book_dict['aa_upload_derived']['pages_best'] = next(iter(aac_upload_book_dict['aa_upload_derived']['pages_multiple']), '')
        aac_upload_book_dict['aa_upload_derived']['description_best'] = '\n\n'.join(list(dict.fromkeys(aac_upload_book_dict['aa_upload_derived']['description_cumulative'])))        
        sources_joined = '\n'.join(sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(aac_upload_book_dict['aa_upload_derived']['source_multiple']))
        producers_joined = '\n'.join(sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(aac_upload_book_dict['aa_upload_derived']['producer_multiple']))
        aac_upload_book_dict['aa_upload_derived']['combined_comments'] = list(dict.fromkeys(filter(len, aac_upload_book_dict['aa_upload_derived']['comments_cumulative'] + [
            # TODO: pass through comments metadata in a structured way so we can add proper translations.
            f"sources:\n{sources_joined}" if sources_joined != "" else "",
            f"producers:\n{producers_joined}" if producers_joined != "" else "",
        ])))

        for ocaid in allthethings.utils.extract_ia_archive_org_from_string(aac_upload_book_dict['aa_upload_derived']['description_best']):
            allthethings.utils.add_identifier_unified(aac_upload_book_dict['aa_upload_derived'], 'ocaid', ocaid)

        if 'acm' in aac_upload_book_dict['aa_upload_derived']['subcollection_multiple']:
            aac_upload_book_dict['aa_upload_derived']['content_type'] = 'journal_article'
        elif 'degruyter' in aac_upload_book_dict['aa_upload_derived']['subcollection_multiple']:
            if 'DeGruyter Journals' in aac_upload_book_dict['aa_upload_derived']['filename_best']:
                aac_upload_book_dict['aa_upload_derived']['content_type'] = 'journal_article'
            else:
                aac_upload_book_dict['aa_upload_derived']['content_type'] = 'book_nonfiction'
        elif 'japanese_manga' in aac_upload_book_dict['aa_upload_derived']['subcollection_multiple']:
            aac_upload_book_dict['aa_upload_derived']['content_type'] = 'book_comic'
        elif 'magzdb' in aac_upload_book_dict['aa_upload_derived']['subcollection_multiple']:
            aac_upload_book_dict['aa_upload_derived']['content_type'] = 'magazine'
        elif 'longquan_archives' in aac_upload_book_dict['aa_upload_derived']['subcollection_multiple']:
            aac_upload_book_dict['aa_upload_derived']['content_type'] = 'book_nonfiction'
        elif any('misc/music_books' in filename for filename in aac_upload_book_dict['aa_upload_derived']['filename_multiple']):
            aac_upload_book_dict['aa_upload_derived']['content_type'] = 'musical_score'

        aac_upload_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "md5": ("before", ["This is a record of a file uploaded directly to Anna's Archive",
                                "More details at https://annas-archive.se/datasets/upload",
                                allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "records": ("before", ["Metadata from inspecting the file."]),
            "files": ("before", ["Short metadata on the file in our torrents."]),
            "aa_upload_derived": ("before", "Derived metadata."),
        }
        aac_upload_book_dicts.append(add_comments_to_dict(aac_upload_book_dict, aac_upload_dict_comments))

    return aac_upload_book_dicts

@page.get("/db/aac_upload/<string:md5>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac_upload_book_json(md5):
    with Session(engine) as session:
        aac_upload_book_dicts = get_aac_upload_book_dicts(session, "md5", [md5])
        if len(aac_upload_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(aac_upload_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}


def get_aac_magzdb_book_dicts(session, key, values):
    if len(values) == 0:
        return []

    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        if key == 'magzdb_id':
            cursor.execute(f'SELECT byte_offset, byte_length, primary_id, SUBSTRING(primary_id, 8) AS requested_value FROM annas_archive_meta__aacid__magzdb_records WHERE primary_id IN %(values)s', { "values": [f"record_{value}" for value in values] })
        elif key == 'md5':
            cursor.execute(f'SELECT byte_offset, byte_length, primary_id, annas_archive_meta__aacid__magzdb_records__multiple_md5.md5 as requested_value FROM annas_archive_meta__aacid__magzdb_records JOIN annas_archive_meta__aacid__magzdb_records__multiple_md5 USING (aacid) WHERE annas_archive_meta__aacid__magzdb_records__multiple_md5.md5 IN %(values)s', { "values": values })
        else:
            raise Exception(f"Unexpected 'key' in get_aac_magzdb_book_dicts: '{key}'")
    except Exception as err:
        print(f"Error in get_aac_magzdb_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    record_offsets_and_lengths = []
    requested_values = []
    for row_index, row in enumerate(list(cursor.fetchall())):
        record_offsets_and_lengths.append((row['byte_offset'], row['byte_length']))
        requested_values.append(row['requested_value'])

    if len(record_offsets_and_lengths) == 0:
        return []

    aac_records_by_requested_value = {}
    publication_ids = set()
    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'magzdb_records', record_offsets_and_lengths)):
        aac_record = orjson.loads(line_bytes)
        aac_records_by_requested_value[requested_values[index]] = aac_record
        publication_ids.add(aac_record['metadata']['record']['publicationId'])

    publication_offsets_and_lengths = []
    if len(publication_ids) > 0:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f'SELECT byte_offset, byte_length FROM annas_archive_meta__aacid__magzdb_records WHERE primary_id IN %(values)s', { "values": [f"publication_{pubid}" for pubid in publication_ids] })
        for row in cursor.fetchall():
            publication_offsets_and_lengths.append((row['byte_offset'], row['byte_length']))
    publication_aac_records_by_id = {}
    for line_bytes in allthethings.utils.get_lines_from_aac_file(cursor, 'magzdb_records', publication_offsets_and_lengths):
        aac_record = orjson.loads(line_bytes)
        publication_aac_records_by_id[aac_record['metadata']['record']['id']] = aac_record
    
    values_set = set(values)
    aac_magzdb_book_dicts = []
    for requested_value, aac_record in aac_records_by_requested_value.items():
        publication_aac_record = publication_aac_records_by_id[aac_record['metadata']['record']['publicationId']]

        aac_magzdb_book_dict = {
            "requested_value": requested_value,
            "id": aac_record['metadata']['record']['id'],
            "aa_magzdb_derived": {
                "filesize": 0,
                "extension": '',
                "title_best": '',
                "title_multiple": [],
                "filepath_best": '',
                "filepath_multiple": [],
                "edition_varia_normalized": '',
                "year": '',
                "stripped_description": '',
                "combined_comments": [],
                "language_codes": [],
                "added_date_unified": { "date_magzdb_meta_scrape": datetime.datetime.strptime(aac_record['aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat().split('T', 1)[0] },
            },
            "aac_record": aac_record,
            "publication_aac_record": publication_aac_record,
        }

        allthethings.utils.init_identifiers_and_classification_unified(aac_magzdb_book_dict['aa_magzdb_derived'])
        allthethings.utils.add_identifier_unified(aac_magzdb_book_dict['aa_magzdb_derived'], 'aacid', aac_record['aacid'])
        allthethings.utils.add_identifier_unified(aac_magzdb_book_dict['aa_magzdb_derived'], 'aacid', publication_aac_record['aacid'])
        allthethings.utils.add_identifier_unified(aac_magzdb_book_dict['aa_magzdb_derived'], 'magzdb', aac_record['metadata']['record']['id'])
        allthethings.utils.add_classification_unified(aac_magzdb_book_dict['aa_magzdb_derived'], 'magzdb_pub', publication_aac_record['metadata']['record']['id'])

        for keyword in (publication_aac_record['metadata']['record']['topic'] or '').split(';'):
            keyword_stripped = keyword.strip()
            if keyword_stripped != '':
                allthethings.utils.add_classification_unified(aac_magzdb_book_dict['aa_magzdb_derived'], 'magzdb_keyword', keyword_stripped)

        issn_stripped = (publication_aac_record['metadata']['record']['issn'] or '').strip()
        if issn_stripped != '':
            allthethings.utils.add_issn_unified(aac_magzdb_book_dict['aa_magzdb_derived'], issn_stripped)
        
        aac_magzdb_book_dict['aa_magzdb_derived']['title_best'] = f"{publication_aac_record['metadata']['record']['title'].strip()} {aac_record['metadata']['record']['year'] or ''} № {(aac_record['metadata']['record']['edition'] or '').strip()}"
        aac_magzdb_book_dict['aa_magzdb_derived']['title_multiple'] = []
        for aka in (publication_aac_record['metadata']['record']['aka'] or '').split(';'):
            aka_stripped = aka.strip()
            if aka_stripped != '':
                aac_magzdb_book_dict['aa_magzdb_derived']['title_multiple'].append(f"{aka_stripped} {aac_record['metadata']['record']['year'] or ''} № {(aac_record['metadata']['record']['edition'] or '').strip()}")

        if (aac_record['metadata']['record']['year'] or 0) != 0:
            aac_magzdb_book_dict['aa_magzdb_derived']['year'] = str(aac_record['metadata']['record']['year'])

        aac_magzdb_book_dict['aa_magzdb_derived']['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes(language.strip()) for language in publication_aac_record['metadata']['record']['language'].split(';')])

        place_of_publication_stripped = (publication_aac_record['metadata']['record']['placeOfPublication'] or '').strip()
        if place_of_publication_stripped != '':
            aac_magzdb_book_dict['aa_magzdb_derived']['edition_varia_normalized'] = place_of_publication_stripped

        stripped_description = strip_description(publication_aac_record['metadata']['record']['description'] or '')
        if stripped_description != '':
            aac_magzdb_book_dict['aa_magzdb_derived']['stripped_description'] = stripped_description

        year_range_stripped = (publication_aac_record['metadata']['record']['yearRange'] or '').strip()
        if year_range_stripped != '':
            aac_magzdb_book_dict['aa_magzdb_derived']['combined_comments'].append(year_range_stripped)

        for previous_edition in (publication_aac_record['metadata']['record']['previousEditions'] or []):
            aac_magzdb_book_dict['aa_magzdb_derived']['combined_comments'].append(f"Previous edition: magzdb_pub:{previous_edition}")
        for subsequent_edition in (publication_aac_record['metadata']['record']['subsequentEditions'] or []):
            aac_magzdb_book_dict['aa_magzdb_derived']['combined_comments'].append(f"Subsequent edition: magzdb_pub:{subsequent_edition}")
        for supplementary_edition in (publication_aac_record['metadata']['record']['supplementaryEditions'] or []):
            aac_magzdb_book_dict['aa_magzdb_derived']['combined_comments'].append(f"Supplementary edition: magzdb_pub:{supplementary_edition}")

        for upload in aac_record['metadata']['record']['uploads']:
            if key == 'md5':
                if (upload['md5'] or '').lower() != requested_value:
                    continue
                aac_magzdb_book_dict['aa_magzdb_derived']['extension'] = upload['format'] or ''
                aac_magzdb_book_dict['aa_magzdb_derived']['filesize'] = upload['sizeB'] or 0
                content_type_stripped = (upload['contentType'] or '').strip()
                if content_type_stripped != '':
                    aac_magzdb_book_dict['aa_magzdb_derived']['combined_comments'].append(content_type_stripped)
                author_stripped = (upload['author'] or '').strip()
                if author_stripped != '':
                    aac_magzdb_book_dict['aa_magzdb_derived']['combined_comments'].append(f"Uploaded by: {author_stripped}")
                note_stripped = (upload['note'] or '').strip()
                if note_stripped != '':
                    aac_magzdb_book_dict['aa_magzdb_derived']['combined_comments'].append(note_stripped)

            extension_with_dot = f".{upload['format']}" if upload['format'] != '' else ''
            aac_magzdb_book_dict['aa_magzdb_derived']['filepath_multiple'].append(f"{publication_aac_record['metadata']['record']['title'].strip()}/{aac_record['metadata']['record']['year']}/{(aac_record['metadata']['record']['edition'] or '').strip()}/{upload['md5'].lower()}{extension_with_dot}")

            if (upload['md5'] or '') != '':
                allthethings.utils.add_identifier_unified(aac_magzdb_book_dict['aa_magzdb_derived'], 'md5', upload['md5'].lower())

        aac_magzdb_book_dict['aa_magzdb_derived']['filepath_best'] = next(iter(aac_magzdb_book_dict['aa_magzdb_derived']['filepath_multiple']), '')
        aac_magzdb_book_dicts.append(aac_magzdb_book_dict)
    return aac_magzdb_book_dicts

@page.get("/db/aac_magzdb/<string:magzdb_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac_magzdb_book_json(magzdb_id):
    with Session(engine) as session:
        aac_magzdb_book_dicts = get_aac_magzdb_book_dicts(session, "magzdb_id", [magzdb_id])
        if len(aac_magzdb_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(aac_magzdb_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/aac_magzdb_md5/<string:md5>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac_magzdb_md5_book_json(md5):
    with Session(engine) as session:
        aac_magzdb_book_dicts = get_aac_magzdb_book_dicts(session, "md5", [md5])
        if len(aac_magzdb_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(aac_magzdb_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_nexusstc_ids(ids, key):
    if type(ids) is not dict:
        raise Exception(f"Unexpected {ids=}")
    if key not in ids:
        return []
    if ids[key] is None:
        return []
    if type(ids[key]) is list:
        return ids[key]
    if type(ids[key]) in [str, float, int]:
        return [str(ids[key])]
    raise Exception(f"Unexpected {key=} in {ids=}")

def get_aac_nexusstc_book_dicts(session, key, values):
    if len(values) == 0:
        return []

    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        if key in ['nexusstc_id', 'nexusstc_download']:
            cursor.execute(f'SELECT byte_offset, byte_length, primary_id, primary_id AS requested_value FROM annas_archive_meta__aacid__nexusstc_records WHERE primary_id IN %(values)s', { "values": values })
        elif key == 'md5':
            cursor.execute(f'SELECT byte_offset, byte_length, primary_id, annas_archive_meta__aacid__nexusstc_records__multiple_md5.md5 as requested_value FROM annas_archive_meta__aacid__nexusstc_records JOIN annas_archive_meta__aacid__nexusstc_records__multiple_md5 USING (aacid) WHERE annas_archive_meta__aacid__nexusstc_records__multiple_md5.md5 IN %(values)s', { "values": values })
        else:
            raise Exception(f"Unexpected 'key' in get_aac_nexusstc_book_dicts: '{key}'")
    except Exception as err:
        print(f"Error in get_aac_nexusstc_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    record_offsets_and_lengths = []
    requested_values = []
    for row_index, row in enumerate(list(cursor.fetchall())):
        record_offsets_and_lengths.append((row['byte_offset'], row['byte_length']))
        requested_values.append(row['requested_value'])

    if len(record_offsets_and_lengths) == 0:
        return []

    aac_records_by_requested_value = {}
    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'nexusstc_records', record_offsets_and_lengths)):
        try:
            aac_record = orjson.loads(line_bytes)
        except:
            raise Exception(f"Invalid JSON in get_aac_nexusstc_book_dicts: {line_bytes=}")
        aac_records_by_requested_value[requested_values[index]] = aac_record

    values_set = set(values)
    aac_nexusstc_book_dicts = []
    for requested_value, aac_record in aac_records_by_requested_value.items():
        aac_nexusstc_book_dict = {
            "requested_value": requested_value,
            "id": aac_record['metadata']['nexus_id'],
            "aa_nexusstc_derived": {
                "filesize": 0,
                "extension": '',
                "ipfs_cids": [],
                "title_best": '',
                "author_best": '',
                "publisher_best": '',
                "filepath_multiple": [],
                "edition_varia_normalized": '',
                "year": '',
                "stripped_description": '',
                "combined_comments": [],
                "language_codes": [],
                "content_type": "",
                "cid_only_links": [],
                "added_date_unified": { 
                    "date_nexusstc_source_update": datetime.datetime.fromtimestamp(aac_record['metadata']['record']['updated_at'][0]).isoformat().split('T', 1)[0],
                },
            },
            "aac_record": aac_record,
        }

        metadata = {}
        if len(aac_record['metadata']['record']['metadata']) == 1:
            metadata = aac_record['metadata']['record']['metadata'][0]
        elif len(aac_record['metadata']['record']['metadata']) > 1:
            raise Exception(f"Unexpected {aac_record['metadata']['record']['metadata'][0]=}")

        allthethings.utils.init_identifiers_and_classification_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'])
        allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'aacid', aac_record['aacid'])
        allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'nexusstc', aac_record['metadata']['nexus_id'])

        for doi in get_nexusstc_ids(aac_record['metadata']['record']['id'][0], 'dois'):
            allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'doi', doi)
        for zlibrary_id in get_nexusstc_ids(aac_record['metadata']['record']['id'][0], 'zlibrary_ids'):
            allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'zlib', zlibrary_id)
        for libgen_id in get_nexusstc_ids(aac_record['metadata']['record']['id'][0], 'libgen_ids'):
            allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'lgrsnf', libgen_id)
        for manualslib_id in get_nexusstc_ids(aac_record['metadata']['record']['id'][0], 'manualslib_id'):
            allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'manualslib', manualslib_id)
        for iso in get_nexusstc_ids(aac_record['metadata']['record']['id'][0], 'internal_iso'):
            allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'iso', iso)
        for british_standard in get_nexusstc_ids(aac_record['metadata']['record']['id'][0], 'internal_bs'):
            allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'british_standard', british_standard)
        for pubmed_id in get_nexusstc_ids(aac_record['metadata']['record']['id'][0], 'pubmed_id'):
            allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'pmid', pubmed_id)
        allthethings.utils.add_isbns_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], get_nexusstc_ids(metadata, 'isbns'))
        allthethings.utils.add_isbns_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], get_nexusstc_ids(metadata, 'parent_isbns'))
        for issn in get_nexusstc_ids(metadata, 'issns'):
            allthethings.utils.add_issn_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], issn)
        for author in aac_record['metadata']['record']['authors']:
            if 'orcid' in author:
                allthethings.utils.add_orcid_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], author['orcid'])
        # `ark_ids` appears to never be present.

        if len(aac_record['metadata']['record']['issued_at']) > 0:
            issued_at = None
            try:
                issued_at = datetime.datetime.fromtimestamp(aac_record['metadata']['record']['issued_at'][0])
            except:
                pass
            if issued_at is not None:
                if allthethings.utils.validate_year(issued_at.year):
                    aac_nexusstc_book_dict["aa_nexusstc_derived"]["added_date_unified"]["date_nexusstc_source_issued_at"] = issued_at.isoformat().split('T', 1)[0]
                    aac_nexusstc_book_dict["aa_nexusstc_derived"]["year"] = str(issued_at.year)
        if len(((metadata.get('event') or {}).get('start') or {}).get('date-parts') or []) > 0:
            potential_year = str(metadata['event']['start']['date-parts'][0])
            if allthethings.utils.validate_year(potential_year):
                aac_nexusstc_book_dict["aa_nexusstc_derived"]["year"] = potential_year

        for tag in (aac_record['metadata']['record']['tags'] or []):
            for sub_tag in tag.split(','):
                sub_tag_stripped = sub_tag.strip()[0:50]
                if sub_tag_stripped != '':
                    allthethings.utils.add_classification_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'nexusstc_tag', sub_tag_stripped)

        title_stripped = aac_record['metadata']['record']['title'][0].strip() if len(aac_record['metadata']['record']['title']) > 0 else ''
        if title_stripped != '':
            aac_nexusstc_book_dict['aa_nexusstc_derived']['title_best'] = title_stripped

        publisher_stripped = (metadata.get('publisher') or '').strip()
        if publisher_stripped != '':
            aac_nexusstc_book_dict['aa_nexusstc_derived']['publisher_best'] = publisher_stripped

        abstract_stripped = strip_description(aac_record['metadata']['record']['abstract'][0]) if len(aac_record['metadata']['record']['abstract']) > 0 else ''
        if abstract_stripped != '':
            aac_nexusstc_book_dict['aa_nexusstc_derived']['stripped_description'] = abstract_stripped

        authors = []
        for author in aac_record['metadata']['record']['authors']:
            if 'name' in author:
                name_stripped = author['name'].strip()
                if name_stripped != '':
                    authors.append(name_stripped)
            elif ('family' in author) and ('given' in author):
                family_stripped = author['family'].strip()
                given_stripped = author['given'].strip()
                name = []
                if given_stripped != '':
                    name.append(given_stripped)
                if family_stripped != '':
                    name.append(family_stripped)
                if len(name) > 0:
                    authors.append(' '.join(name))
            elif 'family' in author:
                family_stripped = author['family'].strip()
                if family_stripped != '':
                    authors.append(family_stripped)
            elif 'given' in author:
                given_stripped = author['given'].strip()
                if given_stripped != '':
                    authors.append(given_stripped)
            elif list(author.keys()) == ['sequence']:
                pass
            elif list(author.keys()) == []:
                pass
            else:
                raise Exception(f"Unexpected {author=}")
        if len(authors) > 0:
            aac_nexusstc_book_dict['aa_nexusstc_derived']['author_best'] = '; '.join(authors)

        edition_varia_normalized = []
        if len(str(metadata.get('container_title') or '').strip()) > 0:
            edition_varia_normalized.append(str(metadata['container_title']).strip())
        if len(str(metadata.get('series') or '').strip()) > 0:
            edition_varia_normalized.append(str(metadata['series']).strip())
        if len(str(metadata.get('volume') or '').strip()) > 0:
            edition_varia_normalized.append(str(metadata['volume']).strip())
        if len(str(metadata.get('edition') or '').strip()) > 0:
            edition_varia_normalized.append(str(metadata['edition']).strip())
        if len(str(metadata.get('brand_name') or '').strip()) > 0:
            edition_varia_normalized.append(str(metadata['brand_name']).strip())
        if len(metadata.get('model_names') or []) > 0:
            for model_name in metadata['model_names']:
                edition_varia_normalized.append(str(model_name).strip())
        if len(str(metadata.get('category') or '').strip()) > 0:
            edition_varia_normalized.append(str(metadata['category']).strip())
        if len(str((metadata.get('event') or {}).get('acronym') or '').strip()) > 0:
            edition_varia_normalized.append(str(metadata['event']['acronym']).strip())
        if len(str((metadata.get('event') or {}).get('name') or '').strip()) > 0:
            edition_varia_normalized.append(str(metadata['event']['name']).strip())
        if len(str((metadata.get('event') or {}).get('location') or '').strip()) > 0:
            edition_varia_normalized.append(str(metadata['event']['location']).strip())
        if aac_nexusstc_book_dict["aa_nexusstc_derived"]["year"] != '':
            edition_varia_normalized.append(aac_nexusstc_book_dict["aa_nexusstc_derived"]["year"])
        aac_nexusstc_book_dict['aa_nexusstc_derived']['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

        if metadata != {}:
            aac_nexusstc_book_dict['aa_nexusstc_derived']['combined_comments'].append(orjson.dumps(metadata).decode())

        aac_nexusstc_book_dict['aa_nexusstc_derived']['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes(language.strip()) for language in aac_record['metadata']['record']['languages']])

        # 10609438 "journal-article"
        # 5741360 "wiki" (we filter this out)
        # 1651305 "book-chapter"
        # 917778 "posted-content"
        # 763539 "proceedings-article"
        # 168344 "book"
        # 95645 "other"
        # 84247 "component"
        # 56201 "monograph"
        # 49194 "edited-book"
        # 43758 "report"
        # 28024 "reference-entry"
        # 12789 "grant"
        # 8284 "report-component"
        # 3706 "book-section"
        # 2818 "book-part"
        # 2675 "reference-book"
        # 2356 "standard"
        # 647 "magazine"
        # 630 "database"
        # 69 null
        if len(aac_record['metadata']['record']['type']) == 1:
            if aac_record['metadata']['record']['type'][0] == 'journal-article':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'journal_article'
            elif aac_record['metadata']['record']['type'][0] == 'journal-issue':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'magazine'
            elif aac_record['metadata']['record']['type'][0] == 'journal-volume':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'magazine'
            elif aac_record['metadata']['record']['type'][0] == 'journal':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'magazine'
            elif aac_record['metadata']['record']['type'][0] == 'proceedings-article':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'journal_article'
            elif aac_record['metadata']['record']['type'][0] == 'proceedings':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'magazine'
            elif aac_record['metadata']['record']['type'][0] == 'proceedings-series':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'magazine'
            elif aac_record['metadata']['record']['type'][0] == 'dataset':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'component':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'report':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'journal_article'
            elif aac_record['metadata']['record']['type'][0] == 'report-component':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'journal_article'
            elif aac_record['metadata']['record']['type'][0] == 'report-series':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'book_nonfiction'
            elif aac_record['metadata']['record']['type'][0] == 'standard':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'standards_document'
            elif aac_record['metadata']['record']['type'][0] == 'standard-series':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'standards_document'
            elif aac_record['metadata']['record']['type'][0] == 'edited-book':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'book_nonfiction'
            elif aac_record['metadata']['record']['type'][0] == 'monograph':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'book_nonfiction'
            elif aac_record['metadata']['record']['type'][0] == 'reference-book':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'book_unknown'
            elif aac_record['metadata']['record']['type'][0] == 'book':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'book_unknown'
            elif aac_record['metadata']['record']['type'][0] == 'book-series':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'book_unknown'
            elif aac_record['metadata']['record']['type'][0] == 'book-set':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'book_unknown'
            elif aac_record['metadata']['record']['type'][0] == 'book-chapter':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'book-section':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'book-part':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'book-track':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'reference-entry':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'dissertation':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'book_nonfiction'
            elif aac_record['metadata']['record']['type'][0] == 'posted-content':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'journal_article'
            elif aac_record['metadata']['record']['type'][0] == 'peer-review':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'other':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'magazine':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'magazine'
            elif aac_record['metadata']['record']['type'][0] == 'chapter':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'manual':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'book_nonfiction'
            elif aac_record['metadata']['record']['type'][0] == 'wiki':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'grant':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] == 'database':
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            elif aac_record['metadata']['record']['type'][0] is None:
                aac_nexusstc_book_dict['aa_nexusstc_derived']['content_type'] = 'other'
            else:
                raise Exception(f"Unexpected {aac_record['metadata']['record']['type'][0]=}")
        elif len(aac_record['metadata']['record']['type']) > 1:
            raise Exception(f"Unexpected {aac_record['metadata']['record']['type']=}")

        for link in aac_record['metadata']['record']['links']:
            # print(f"{key=} {link=}")
            if key == 'md5':
                if (link.get('md5') or '').lower() != requested_value:
                    continue
                if (link.get('cid') or '') != '':
                    aac_nexusstc_book_dict['aa_nexusstc_derived']['ipfs_cids'].append(link['cid'])
                aac_nexusstc_book_dict['aa_nexusstc_derived']['extension'] = link.get('extension') or ''
                aac_nexusstc_book_dict['aa_nexusstc_derived']['filesize'] = link.get('filesize') or 0
            elif key == 'nexusstc_download':
                if (link.get('cid') or '') != '':
                    aac_nexusstc_book_dict['aa_nexusstc_derived']['ipfs_cids'].append(link['cid'])
                # This will overwrite/combine different link records if they exist, but that's okay.
                aac_nexusstc_book_dict['aa_nexusstc_derived']['extension'] = link.get('extension') or ''
                aac_nexusstc_book_dict['aa_nexusstc_derived']['filesize'] = link.get('filesize') or 0

            if (link.get('md5') or '') != '':
                allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'md5', link['md5'].lower())
                extension_with_dot = f".{link['extension']}" if (link.get('extension') or '') != '' else ''
                aac_nexusstc_book_dict['aa_nexusstc_derived']['filepath_multiple'].append(f"{title_stripped + '/' if title_stripped != '' else ''}{link['md5'].lower()}{extension_with_dot}")
            if (link.get('cid') or '') != '':
                allthethings.utils.add_identifier_unified(aac_nexusstc_book_dict['aa_nexusstc_derived'], 'ipfs_cid', link['cid'])

            if ((link.get('cid') or '') != '') and ((link.get('md5') or '') == ''):
                aac_nexusstc_book_dict['aa_nexusstc_derived']['cid_only_links'].append(link['cid'])

            # Do something with link['iroh_hash']?

        if len(aac_record['metadata']['record']['references'] or []) > 0:
            references = ' '.join([f"doi:{ref['doi']}" for ref in aac_record['metadata']['record']['references']])
            aac_nexusstc_book_dict['aa_nexusstc_derived']['combined_comments'].append(f"Referenced by: {references}")

        aac_nexusstc_book_dict['aa_nexusstc_derived']['filepath_best'] = next(iter(aac_nexusstc_book_dict['aa_nexusstc_derived']['filepath_multiple']), '')
        aac_nexusstc_book_dicts.append(aac_nexusstc_book_dict)
    return aac_nexusstc_book_dicts

@page.get("/db/aac_nexusstc/<string:nexusstc_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac_nexusstc_book_json(nexusstc_id):
    with Session(engine) as session:
        aac_nexusstc_book_dicts = get_aac_nexusstc_book_dicts(session, "nexusstc_id", [nexusstc_id])
        if len(aac_nexusstc_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(aac_nexusstc_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/aac_nexusstc_download/<string:nexusstc_download>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac_nexusstc_download_book_json(nexusstc_download):
    with Session(engine) as session:
        aac_nexusstc_book_dicts = get_aac_nexusstc_book_dicts(session, "nexusstc_download", [nexusstc_download])
        if len(aac_nexusstc_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(aac_nexusstc_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/aac_nexusstc_md5/<string:md5>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac_nexusstc_md5_book_json(md5):
    with Session(engine) as session:
        aac_nexusstc_book_dicts = get_aac_nexusstc_book_dicts(session, "md5", [md5])
        if len(aac_nexusstc_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(aac_nexusstc_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_aac_edsebk_book_dicts(session, key, values):
    if len(values) == 0:
        return []

    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        if key == 'edsebk_id':
            cursor.execute(f'SELECT byte_offset, byte_length, primary_id FROM annas_archive_meta__aacid__ebscohost_records WHERE primary_id IN %(values)s GROUP BY primary_id', { "values": values })
        else:
            raise Exception(f"Unexpected 'key' in get_aac_edsebk_book_dicts: '{key}'")
    except Exception as err:
        print(f"Error in get_aac_edsebk_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        return []

    record_offsets_and_lengths = []
    primary_ids = []
    for row_index, row in enumerate(list(cursor.fetchall())):
        record_offsets_and_lengths.append((row['byte_offset'], row['byte_length']))
        primary_ids.append(row['primary_id'])

    if len(record_offsets_and_lengths) == 0:
        return []

    aac_records_by_primary_id = {}
    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'ebscohost_records', record_offsets_and_lengths)):
        aac_record = orjson.loads(line_bytes)
        aac_records_by_primary_id[primary_ids[index]] = aac_record

    aac_edsebk_book_dicts = []
    for primary_id, aac_record in aac_records_by_primary_id.items():
        aac_edsebk_book_dict = {
            "edsebk_id": primary_id,
            "aa_edsebk_derived": {
                "title_best": '',
                "title_multiple": [],
                "author_best": '',
                "publisher_best": '',
                "edition_varia_normalized": '',
                "year": '',
                "stripped_description": '',
                "combined_comments": [],
                "language_codes": [],
                "added_date_unified": { "date_edsebk_meta_scrape": datetime.datetime.strptime(aac_record['aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat().split('T', 1)[0] },
            },
            "aac_record": aac_record,
        }

        allthethings.utils.init_identifiers_and_classification_unified(aac_edsebk_book_dict['aa_edsebk_derived'])
        allthethings.utils.add_identifier_unified(aac_edsebk_book_dict['aa_edsebk_derived'], 'aacid', aac_record['aacid'])
        allthethings.utils.add_identifier_unified(aac_edsebk_book_dict['aa_edsebk_derived'], 'edsebk', primary_id)

        title_stripped = aac_record['metadata']['header']['artinfo']['title'].strip()
        if title_stripped != '':
            aac_edsebk_book_dict['aa_edsebk_derived']['title_best'] = title_stripped

        subtitle_stripped = (aac_record['metadata']['header']['artinfo'].get('subtitle') or '').strip()
        if subtitle_stripped != '':
            aac_edsebk_book_dict['aa_edsebk_derived']['title_multiple'] = [subtitle_stripped]

        aac_edsebk_book_dict['aa_edsebk_derived']['author_best'] = '; '.join([author.strip() for author in (aac_record['metadata']['header']['artinfo'].get('authors') or [])])

        publisher_stripped = (aac_record['metadata']['header']['pubinfo'].get('publisher') or '').strip()
        if publisher_stripped != '':
            aac_edsebk_book_dict['aa_edsebk_derived']['publisher_best'] = publisher_stripped

        edition_varia_normalized = []
        if len((aac_record['metadata']['header']['pubinfo'].get('publisher_contract') or '').strip()) > 0:
            edition_varia_normalized.append(aac_record['metadata']['header']['pubinfo']['publisher_contract'].strip())
        if len((aac_record['metadata']['header']['pubinfo'].get('place') or '').strip()) > 0:
            edition_varia_normalized.append(aac_record['metadata']['header']['pubinfo']['place'].strip())
        edition_varia_normalized.append(aac_record['metadata']['header']['pubinfo']['date']['year'].strip())
        aac_edsebk_book_dict['aa_edsebk_derived']['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

        aac_edsebk_book_dict['aa_edsebk_derived']['year'] = aac_record['metadata']['header']['pubinfo']['date']['year'].strip()

        abstract_stripped = strip_description(aac_record['metadata']['header']['artinfo']['abstract'])
        if abstract_stripped != '':
            aac_edsebk_book_dict['aa_edsebk_derived']['stripped_description'] = abstract_stripped

        allthethings.utils.add_isbns_unified(aac_edsebk_book_dict['aa_edsebk_derived'], aac_record['metadata']['header']['bkinfo']['print_isbns'] + aac_record['metadata']['header']['bkinfo']['electronic_isbns'])

        oclc_stripped = (aac_record['metadata']['header']['artinfo']['uis'].get('oclc') or '').strip()
        if oclc_stripped != '':
            allthethings.utils.add_identifier_unified(aac_edsebk_book_dict['aa_edsebk_derived'], 'oclc', oclc_stripped)

        dewey_stripped = (aac_record['metadata']['header']['pubinfo']['pre_pub_group']['dewey'].get('class') or '').strip()
        if dewey_stripped != '':
            allthethings.utils.add_classification_unified(aac_edsebk_book_dict['aa_edsebk_derived'], 'ddc', dewey_stripped)

        lcc_stripped = (aac_record['metadata']['header']['pubinfo']['pre_pub_group']['lc'].get('class') or '').strip()
        if lcc_stripped != '':
            allthethings.utils.add_classification_unified(aac_edsebk_book_dict['aa_edsebk_derived'], 'lcc', lcc_stripped)

        language_code_stripped = (aac_record['metadata']['header']['language'].get('code') or '').strip()
        if language_code_stripped != '':
            aac_edsebk_book_dict['aa_edsebk_derived']['language_codes'] = get_bcp47_lang_codes(language_code_stripped)

        for subject in (aac_record['metadata']['header']['artinfo'].get('subject_groups') or []):
            allthethings.utils.add_classification_unified(aac_edsebk_book_dict['aa_edsebk_derived'], 'edsebk_subject', f"{subject['Type']}/{subject['Subject']}")

        aac_edsebk_book_dicts.append(aac_edsebk_book_dict)
    return aac_edsebk_book_dicts

# SIMILAR to get_oclc_dicts_by_isbn13
def get_edsebk_dicts_by_isbn13(session, isbn13s):
    if len(isbn13s) == 0:
        return {}
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT isbn13, edsebk_id FROM isbn13_edsebk WHERE isbn13 IN %(isbn13s)s', { "isbn13s": isbn13s })
        rows = list(cursor.fetchall())
        if len(rows) == 0:
            return {}
        isbn13s_by_edsebk_id = collections.defaultdict(list)
        for row in rows:
            isbn13s_by_edsebk_id[row['edsebk_id']].append(str(row['isbn13']))
        edsebk_dicts = get_aac_edsebk_book_dicts(session, 'edsebk', list(isbn13s_by_edsebk_id.keys()))
        retval = collections.defaultdict(list)
        for edsebk_dict in edsebk_dicts:
            for isbn13 in isbn13s_by_edsebk_id[edsebk_dict['edsebk_id']]:
                retval[isbn13].append(edsebk_dict)
        return dict(retval)

@page.get("/db/aac_edsebk/<string:edsebk_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac_edsebk_book_json(edsebk_id):
    with Session(engine) as session:
        aac_edsebk_book_dicts = get_aac_edsebk_book_dicts(session, "edsebk_id", [edsebk_id])
        if len(aac_edsebk_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(aac_edsebk_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}


# def get_embeddings_for_aarecords(session, aarecords):
#     filtered_aarecord_ids = [aarecord['id'] for aarecord in aarecords if aarecord['id'].startswith('md5:')]
#     if len(filtered_aarecord_ids) == 0:
#         return {}

#     embedding_text_text_embedding_3_small_100_tokens_by_aarecord_id = {}
#     tokens_text_embedding_3_small_100_tokens_by_aarecord_id = {}
#     tiktoken_encoder = get_tiktoken_text_embedding_3_small()
#     for aarecord in aarecords:
#         if aarecord['id'] not in filtered_aarecord_ids:
#             continue
#         embedding_text = []
#         if aarecord['file_unified_data']['original_filename_best'] != '':
#             embedding_text.append(f"file:{aarecord['file_unified_data']['original_filename_best'][:300]}")
#         if aarecord['file_unified_data']['title_best'] != '':
#             embedding_text.append(f"title:{aarecord['file_unified_data']['title_best'][:100]}")
#         if aarecord['file_unified_data']['author_best'] != '':
#             embedding_text.append(f"author:{aarecord['file_unified_data']['author_best'][:100]}")
#         if aarecord['file_unified_data']['edition_varia_best'] != '':
#             embedding_text.append(f"edition:{aarecord['file_unified_data']['edition_varia_best'][:100]}")
#         if aarecord['file_unified_data']['publisher_best'] != '':
#             embedding_text.append(f"publisher:{aarecord['file_unified_data']['publisher_best'][:100]}")
#         for item in aarecord['file_unified_data'].get('title_additional') or []:
#             if item != '':
#                 embedding_text.append(f"alt_title:{item[:100]}")
#         for item in aarecord['file_unified_data'].get('author_additional') or []:
#             if item != '':
#                 embedding_text.append(f"alt_author:{item[:100]}")
#         if len(embedding_text) > 0:
#             tokens = tiktoken_encoder.encode('\n'.join(embedding_text))[:100]
#             tokens_text_embedding_3_small_100_tokens_by_aarecord_id[aarecord['id']] = tokens
#             embedding_text_text_embedding_3_small_100_tokens_by_aarecord_id[aarecord['id']] = tiktoken_encoder.decode(tokens)
#     # print(f"{embedding_text_text_embedding_3_small_100_tokens_by_aarecord_id=}")

#     # session.connection().connection.ping(reconnect=True)
#     # cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
#     # cursor.execute(f'SELECT * FROM model_cache WHERE model_name = "e5_small_query" AND hashed_aarecord_id IN %(hashed_aarecord_ids)s', { "hashed_aarecord_ids": hashed_aarecord_ids })
#     # rows_by_aarecord_id = { row['aarecord_id']: row for row in list(cursor.fetchall()) }

#     # embeddings = []
#     # insert_data_e5_small_query = []
#     # for aarecord_id in aarecord_ids:
#     #     embedding_text = embedding_text_by_aarecord_id[aarecord_id]
#     #     if aarecord_id in rows_by_aarecord_id:
#     #         if rows_by_aarecord_id[aarecord_id]['embedding_text'] != embedding_text:
#     #             print(f"WARNING! embedding_text has changed for e5_small_query: {aarecord_id=} {rows_by_aarecord_id[aarecord_id]['embedding_text']=} {embedding_text=}")
#     #         embeddings.append({ 'e5_small_query': list(struct.unpack(f"{len(rows_by_aarecord_id[aarecord_id]['embedding'])//4}f", rows_by_aarecord_id[aarecord_id]['embedding'])) })
#     #     else:
#     #         e5_small_query = list(map(float, get_e5_small_model().encode(f"query: {embedding_text}", normalize_embeddings=True)))
#     #         embeddings.append({ 'e5_small_query': e5_small_query })
#     #         insert_data_e5_small_query.append({
#     #             'hashed_aarecord_id': hashlib.md5(aarecord_id.encode()).digest(),
#     #             'aarecord_id': aarecord_id,
#     #             'model_name': 'e5_small_query',
#     #             'embedding_text': embedding_text,
#     #             'embedding': struct.pack(f'{len(e5_small_query)}f', *e5_small_query),
#     #         })

#     # if len(insert_data_e5_small_query) > 0:
#     #     session.connection().connection.ping(reconnect=True)
#     #     cursor.executemany(f"REPLACE INTO model_cache (hashed_aarecord_id, aarecord_id, model_name, embedding_text, embedding) VALUES (%(hashed_aarecord_id)s, %(aarecord_id)s, %(model_name)s, %(embedding_text)s, %(embedding)s)", insert_data_e5_small_query)
#     #     cursor.execute("COMMIT")

#     session.connection().connection.ping(reconnect=True)
#     cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
#     hashed_aarecord_ids = [hashlib.md5(aarecord_id.encode()).digest() for aarecord_id in filtered_aarecord_ids]
#     cursor.execute('SELECT * FROM model_cache_text_embedding_3_small_100_tokens WHERE hashed_aarecord_id IN %(hashed_aarecord_ids)s', { "hashed_aarecord_ids": hashed_aarecord_ids })
#     rows_by_aarecord_id = { row['aarecord_id']: row for row in list(cursor.fetchall()) }

#     embeddings = {}
#     embeddings_to_fetch_aarecord_id = []
#     embeddings_to_fetch_text = []
#     embeddings_to_fetch_tokens = []
#     for aarecord_id in embedding_text_text_embedding_3_small_100_tokens_by_aarecord_id.keys():
#         embedding_text = embedding_text_text_embedding_3_small_100_tokens_by_aarecord_id[aarecord_id]
#         if aarecord_id in rows_by_aarecord_id:
#             if rows_by_aarecord_id[aarecord_id]['embedding_text'] != embedding_text:
#                 if AACID_SMALL_DATA_IMPORTS or SLOW_DATA_IMPORTS:
#                     raise Exception(f"WARNING! embedding_text has changed for text_embedding_3_small_100_tokens. Only raising this when AACID_SMALL_DATA_IMPORTS or SLOW_DATA_IMPORTS is set, to make sure this is expected. Wipe the database table to remove this error, after carefully checking that this is indeed expected. {aarecord_id=} {rows_by_aarecord_id[aarecord_id]['embedding_text']=} {embedding_text=}")
#             embedding = rows_by_aarecord_id[aarecord_id]['embedding']
#             embeddings[aarecord_id] = { 'text_embedding_3_small_100_tokens': list(struct.unpack(f"{len(embedding)//4}f", embedding)) }
#         else:
#             embeddings_to_fetch_aarecord_id.append(aarecord_id)
#             embeddings_to_fetch_text.append(embedding_text)
#             embeddings_to_fetch_tokens.append(tokens_text_embedding_3_small_100_tokens_by_aarecord_id[aarecord_id])

#     insert_data_text_embedding_3_small_100_tokens = []
#     if len(embeddings_to_fetch_text) > 0:
#         embedding_response = None
#         for attempt in range(1,500):
#             try:
#                 embedding_response = openai.OpenAI().embeddings.create(
#                     model="text-embedding-3-small",
#                     input=embeddings_to_fetch_tokens,
#                 )
#                 break
#             except openai.RateLimitError:
#                 time.sleep(3+random.randint(0,5))
#             except Exception as e:
#                 if attempt > 50:
#                     print(f"Warning! Lots of attempts for OpenAI! {attempt=} {e=}")
#                 if attempt > 400:
#                     raise
#                 time.sleep(3+random.randint(0,5))
#         for index, aarecord_id in enumerate(embeddings_to_fetch_aarecord_id):
#             embedding_text = embeddings_to_fetch_text[index]
#             text_embedding_3_small_100_tokens = embedding_response.data[index].embedding
#             embeddings[aarecord_id] = { 'text_embedding_3_small_100_tokens': text_embedding_3_small_100_tokens }
#             insert_data_text_embedding_3_small_100_tokens.append({
#                 'hashed_aarecord_id': hashlib.md5(aarecord_id.encode()).digest(),
#                 'aarecord_id': aarecord_id,
#                 'embedding_text': embedding_text,
#                 'embedding': struct.pack(f'{len(text_embedding_3_small_100_tokens)}f', *text_embedding_3_small_100_tokens),
#             })

#     if len(insert_data_text_embedding_3_small_100_tokens) > 0:
#         session.connection().connection.ping(reconnect=True)
#         cursor.executemany(f"REPLACE INTO model_cache_text_embedding_3_small_100_tokens (hashed_aarecord_id, aarecord_id, embedding_text, embedding) VALUES (%(hashed_aarecord_id)s, %(aarecord_id)s, %(embedding_text)s, %(embedding)s)", insert_data_text_embedding_3_small_100_tokens)
#         cursor.execute("COMMIT")

#     return embeddings


def is_string_subsequence(needle, haystack):
    i_needle = 0
    i_haystack = 0
    while i_needle < len(needle) and i_haystack < len(haystack):
        if needle[i_needle].lower() == haystack[i_haystack].lower():
            i_needle += 1
        i_haystack += 1
    return i_needle == len(needle)

def sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(strings):
    # WARNING: we depend on this being stable sorted, e.g. when calling max(.., key=len).
    strings = [unicodedata.normalize('NFKC', string) for string in sorted(strings, key=len, reverse=True) if string != '']
    if len(strings) == 0:
        return []
    strings_filtered = []
    for string in strings:
        if any([is_string_subsequence(string, string_filtered) for string_filtered in strings_filtered]):
            continue
        strings_filtered.append(string)
    return strings_filtered

number_of_get_aarecords_elasticsearch_exceptions = 0
def get_aarecords_elasticsearch(aarecord_ids):
    global number_of_get_aarecords_elasticsearch_exceptions

    if not allthethings.utils.validate_aarecord_ids(aarecord_ids):
        raise Exception(f"Invalid aarecord_ids {aarecord_ids=}")

    # Filter out bad data
    aarecord_ids = [val for val in aarecord_ids if val not in allthethings.utils.SEARCH_FILTERED_BAD_AARECORD_IDS]

    if len(aarecord_ids) == 0:
        return []

    # Uncomment the following lines to use MySQL directly; useful for local development.
    # with Session(engine) as session:
    #     return [add_additional_to_aarecord({ '_source': aarecord }) for aarecord in get_aarecords_mysql(session, aarecord_ids)]

    docs_by_es_handle = collections.defaultdict(list)
    for aarecord_id in aarecord_ids:
        indexes = allthethings.utils.get_aarecord_search_indexes_for_id_prefix(aarecord_id.split(':', 1)[0])
        for index in indexes:
            es_handle = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[index]
            docs_by_es_handle[es_handle].append({'_id': aarecord_id, '_index': f'{index}__{allthethings.utils.virtshard_for_aarecord_id(aarecord_id)}' })

    search_results_raw = []
    for es_handle, docs in docs_by_es_handle.items():
        for attempt in range(1, 100):
            try:
                search_results_raw += es_handle.mget(docs=docs)['docs']
                break
            except Exception:
                print(f"Warning: another attempt during get_aarecords_elasticsearch {es_handle=} {aarecord_ids=}")
                if attempt >= 3:
                    number_of_get_aarecords_elasticsearch_exceptions += 1
                    if number_of_get_aarecords_elasticsearch_exceptions > 5:
                        raise
                    else:
                        print("Haven't reached number_of_get_aarecords_elasticsearch_exceptions limit yet, so not raising")
                        return None
        number_of_get_aarecords_elasticsearch_exceptions = 0
    return [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in search_results_raw if aarecord_raw.get('found') and (aarecord_raw['_id'] not in allthethings.utils.SEARCH_FILTERED_BAD_AARECORD_IDS)]


def aarecord_score_base(aarecord):
    if len(aarecord['file_unified_data'].get('problems') or []) > 0:
        return 0.01

    score = 10000.0
    # Filesize of >0.2MB is overriding everything else.
    if (aarecord['file_unified_data'].get('filesize_best') or 0) > 200000:
        score += 1000.0
    if (aarecord['file_unified_data'].get('filesize_best') or 0) > 700000:
        score += 5.0
    if (aarecord['file_unified_data'].get('filesize_best') or 0) > 1200000:
        score += 5.0
    # If we're not confident about the language, demote.
    if len(aarecord['file_unified_data'].get('language_codes') or []) == 0:
        score -= 2.0
    # Bump English a little bit regardless of the user's language
    if ('en' in aarecord['search_only_fields']['search_most_likely_language_code']):
        score += 5.0
    if (aarecord['file_unified_data'].get('extension_best') or '') in ['epub', 'pdf']:
        score += 15.0
    if (aarecord['file_unified_data'].get('extension_best') or '') in ['cbr', 'mobi', 'fb2', 'cbz', 'azw3', 'djvu', 'fb2.zip']:
        score += 5.0
    if len(aarecord['file_unified_data'].get('cover_url_best') or '') > 0:
        score += 3.0
    if (aarecord['file_unified_data'].get('has_aa_downloads') or 0) > 0:
        score += 5.0
    # Don't bump IA too much.
    if (aarecord['file_unified_data'].get('has_aa_exclusive_downloads') or 0) > 0:
        score += 3.0
    if len(aarecord['file_unified_data'].get('title_best') or '') > 0:
        score += 10.0
    if len(aarecord['file_unified_data'].get('author_best') or '') > 0:
        score += 2.0
    if len(aarecord['file_unified_data'].get('publisher_best') or '') > 0:
        score += 2.0
    if len(aarecord['file_unified_data'].get('edition_varia_best') or '') > 0:
        score += 2.0
    score += min(8.0, 2.0*len(aarecord['file_unified_data'].get('identifiers_unified') or []))
    if len(aarecord['file_unified_data'].get('content_type') or '') in ['journal_article', 'standards_document', 'book_comic', 'magazine']:
        # For now demote non-books quite a bit, since they can drown out books.
        # People can filter for them directly.
        score -= 70.0
    record_sources = aarecord_sources(aarecord)
    if (record_sources == ['upload']) or (record_sources == ['zlibzh']) or (record_sources == ['nexusstc']):
        # Demote upload-only results below the demotion above, since there's some garbage in there.
        # Similarly demote zlibzh since we don't have direct download for them, and Zlib downloads are annoying because the require login.
        # And Nexus/STC-only results are often missing downloadable files.
        score -= 100.0
    if len(aarecord['file_unified_data'].get('stripped_description_best') or '') > 0:
        score += 3.0
    return score

def aarecord_sources(aarecord):
    aarecord_id_split = aarecord['id'].split(':', 1)
    return list(dict.fromkeys([
        # Should match /datasets/<aarecord_source>!!
        *(['duxiu']     if aarecord['duxiu'] is not None else []),
        *(['edsebk']    if aarecord.get('aac_edsebk') is not None else []),
        *(['ia']        if aarecord['ia_record'] is not None else []),
        *(['isbndb']    if (aarecord_id_split[0] == 'isbn' and len(aarecord['isbndb'] or []) > 0) else []),
        *(['lgli']      if aarecord['lgli_file'] is not None else []),
        *(['lgrs']      if aarecord['lgrsfic_book'] is not None else []),
        *(['lgrs']      if aarecord['lgrsnf_book'] is not None else []),
        *(['magzdb']    if aarecord.get('aac_magzdb') is not None else []),
        *(['nexusstc']  if aarecord.get('aac_nexusstc') is not None else []),
        *(['oclc']      if (aarecord_id_split[0] == 'oclc' and len(aarecord['oclc'] or []) > 0) else []),
        *(['ol']        if (aarecord_id_split[0] == 'ol' and len(aarecord['ol'] or []) > 0) else []),
        *(['scihub']    if len(aarecord['scihub_doi']) > 0 else []),
        *(['upload']    if aarecord.get('aac_upload') is not None else []),
        *(['zlib']      if (aarecord['aac_zlib3_book'] is not None) and ((aarecord['aac_zlib3_book'].get('storage') or '') != 'chinese') else []),
        *(['zlib']      if aarecord['zlib_book'] is not None else []),
        *(['zlibzh']    if (aarecord['aac_zlib3_book'] is not None) and ((aarecord['aac_zlib3_book'].get('storage') or '') == 'chinese') else []),
    ]))

# Dummy translation to keep this msgid around. TODO: fix see below.
dummy_translation_affected_files = gettext('page.md5.box.download.affected_files')

def get_aarecords_mysql(session, aarecord_ids):
    if not allthethings.utils.validate_aarecord_ids(aarecord_ids):
        raise Exception(f"Invalid aarecord_ids {aarecord_ids=}")

    # Filter out bad data
    aarecord_ids = list(dict.fromkeys([val for val in aarecord_ids if val not in allthethings.utils.SEARCH_FILTERED_BAD_AARECORD_IDS]))

    split_ids = allthethings.utils.split_aarecord_ids(aarecord_ids)
    lgrsnf_book_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_lgrsnf_book_dicts(session, "MD5", split_ids['md5']))
    lgrsfic_book_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_lgrsfic_book_dicts(session, "MD5", split_ids['md5']))
    lgli_file_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_lgli_file_dicts(session, "md5", split_ids['md5']))
    zlib_book_dicts1 = dict(('md5:' + item['md5_reported'].lower(), item) for item in get_zlib_book_dicts(session, "md5_reported", split_ids['md5']))
    zlib_book_dicts2 = dict(('md5:' + item['md5'].lower(), item) for item in get_zlib_book_dicts(session, "md5", split_ids['md5']))
    aac_zlib3_book_dicts1 = dict(('md5:' + item['md5_reported'].lower(), item) for item in get_aac_zlib3_book_dicts(session, "md5_reported", split_ids['md5']))
    aac_zlib3_book_dicts2 = dict(('md5:' + item['md5'].lower(), item) for item in get_aac_zlib3_book_dicts(session, "md5", split_ids['md5']))
    ia_record_dicts = dict(('md5:' + item['aa_ia_file']['md5'].lower(), item) for item in get_ia_record_dicts(session, "md5", split_ids['md5']) if item.get('aa_ia_file') is not None)
    ia_record_dicts2 = dict(('ia:' + item['ia_id'], item) for item in get_ia_record_dicts(session, "ia_id", split_ids['ia']) if item.get('aa_ia_file') is None)
    isbndb_dicts = {('isbn:' + item['ean13']): item['isbndb'] for item in get_isbndb_dicts(session, split_ids['isbn'])}
    ol_book_dicts = {('ol:' + item['ol_edition']): [item] for item in get_ol_book_dicts(session, 'ol_edition', split_ids['ol'])}
    scihub_doi_dicts = {('doi:' + item['doi']): [item] for item in get_scihub_doi_dicts(session, 'doi', split_ids['doi'])}
    oclc_dicts = {('oclc:' + item['oclc_id']): [item] for item in get_oclc_dicts(session, 'oclc', split_ids['oclc'])}
    duxiu_dicts = {('duxiu_ssid:' + item['duxiu_ssid']): item for item in get_duxiu_dicts(session, 'duxiu_ssid', split_ids['duxiu_ssid'], include_deep_transitive_md5s_size_path=True)}
    duxiu_dicts2 = {('cadal_ssno:' + item['cadal_ssno']): item for item in get_duxiu_dicts(session, 'cadal_ssno', split_ids['cadal_ssno'], include_deep_transitive_md5s_size_path=True)}
    duxiu_dicts3 = {('md5:' + item['md5']): item for item in get_duxiu_dicts(session, 'md5', split_ids['md5'], include_deep_transitive_md5s_size_path=False)}
    aac_upload_md5_dicts = {('md5:' + item['md5']): item for item in get_aac_upload_book_dicts(session, 'md5', split_ids['md5'])}
    aac_magzdb_book_dicts = {('md5:' + item['requested_value']): item for item in get_aac_magzdb_book_dicts(session, 'md5', split_ids['md5'])}
    aac_magzdb_book_dicts2 = {('magzdb:' + item['requested_value']): item for item in get_aac_magzdb_book_dicts(session, 'magzdb_id', split_ids['magzdb'])}
    aac_nexusstc_book_dicts = {('md5:' + item['requested_value']): item for item in get_aac_nexusstc_book_dicts(session, 'md5', split_ids['md5'])}
    aac_nexusstc_book_dicts2 = {('nexusstc:' + item['requested_value']): item for item in get_aac_nexusstc_book_dicts(session, 'nexusstc_id', split_ids['nexusstc'])}
    aac_nexusstc_book_dicts3 = {('nexusstc_download:' + item['requested_value']): item for item in get_aac_nexusstc_book_dicts(session, 'nexusstc_download', split_ids['nexusstc_download'])}
    ol_book_dicts_primary_linked = {('md5:' + md5): item for md5, item in get_ol_book_dicts_by_annas_archive_md5(session, split_ids['md5']).items()}
    aac_edsebk_book_dicts = {('edsebk:' + item['edsebk_id']): item for item in get_aac_edsebk_book_dicts(session, 'edsebk_id', split_ids['edsebk'])}

    # First pass, so we can fetch more dependencies.
    aarecords = []
    canonical_isbn13s = []
    ol_editions = []
    dois = []
    oclc_ids = []
    ia_ids = []
    duxiu_ssids = []
    cadal_ssnos = []
    for aarecord_id in aarecord_ids:
        aarecord_id_split = aarecord_id.split(':', 1)
        aarecord = {}
        aarecord['id'] = aarecord_id
        aarecord['lgrsnf_book'] = lgrsnf_book_dicts.get(aarecord_id)
        aarecord['lgrsfic_book'] = lgrsfic_book_dicts.get(aarecord_id)
        aarecord['lgli_file'] = lgli_file_dicts.get(aarecord_id)
        if aarecord.get('lgli_file'):
            aarecord['lgli_file']['editions'] = aarecord['lgli_file']['editions'][0:5]
        aarecord['zlib_book'] = zlib_book_dicts1.get(aarecord_id) or zlib_book_dicts2.get(aarecord_id)
        aarecord['aac_zlib3_book'] = aac_zlib3_book_dicts1.get(aarecord_id) or aac_zlib3_book_dicts2.get(aarecord_id)
        aarecord['ia_record'] = ia_record_dicts.get(aarecord_id) or ia_record_dicts2.get(aarecord_id)
        aarecord['ia_records_meta_only'] = []
        aarecord['isbndb'] = list(isbndb_dicts.get(aarecord_id) or [])
        aarecord['ol'] = list(ol_book_dicts.get(aarecord_id) or [])
        aarecord['scihub_doi'] = list(scihub_doi_dicts.get(aarecord_id) or [])
        aarecord['oclc'] = list(oclc_dicts.get(aarecord_id) or [])
        aarecord['duxiu'] = duxiu_dicts.get(aarecord_id) or duxiu_dicts2.get(aarecord_id) or duxiu_dicts3.get(aarecord_id)
        aarecord['aac_upload'] = aac_upload_md5_dicts.get(aarecord_id)
        aarecord['aac_magzdb'] = aac_magzdb_book_dicts.get(aarecord_id) or aac_magzdb_book_dicts2.get(aarecord_id)
        aarecord['aac_nexusstc'] = aac_nexusstc_book_dicts.get(aarecord_id) or aac_nexusstc_book_dicts2.get(aarecord_id) or aac_nexusstc_book_dicts3.get(aarecord_id)
        aarecord['ol_book_dicts_primary_linked'] = list(ol_book_dicts_primary_linked.get(aarecord_id) or [])
        aarecord['duxius_nontransitive_meta_only'] = []
        aarecord['aac_edsebk'] = aac_edsebk_book_dicts.get(aarecord_id)

        lgli_all_editions = aarecord['lgli_file']['editions'] if aarecord.get('lgli_file') else []

        aarecord['file_unified_data'] = {}
        allthethings.utils.init_identifiers_and_classification_unified(aarecord['file_unified_data'])
        # Duplicated below, with more fields
        aarecord['file_unified_data']['identifiers_unified'] = allthethings.utils.merge_unified_fields([
            aarecord['file_unified_data']['identifiers_unified'],
            ((aarecord['lgrsnf_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('identifiers_unified') or {}),
            *[edition['identifiers_unified'] for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('identifiers_unified') or {}),
            *[ia_record['aa_ia_derived']['identifiers_unified'] for ia_record in aarecord['ia_records_meta_only']],
            *[isbndb['identifiers_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['identifiers_unified'] for ol_book_dict in aarecord['ol']],
            *[ol_book_dict['identifiers_unified'] for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
            *[scihub_doi['identifiers_unified'] for scihub_doi in aarecord['scihub_doi']],
            *[oclc['aa_oclc_derived']['identifiers_unified'] for oclc in aarecord['oclc']],
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('identifiers_unified') or {}),
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('identifiers_unified') or {}),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('identifiers_unified') or {}),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('identifiers_unified') or {}),
            *[duxiu_record['aa_duxiu_derived']['identifiers_unified'] for duxiu_record in aarecord['duxius_nontransitive_meta_only']],
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('identifiers_unified') or {}),
        ])
        # TODO: This `if` is not necessary if we make sure that the fields of the primary records get priority.
        if not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0]):
            current_record_isbn13s = aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []
            if len(current_record_isbn13s) < 10: # Filter out obscenely long ISBN lists, e.g. https://archive.org/details/240524-CL-aa
                for canonical_isbn13 in current_record_isbn13s:
                    canonical_isbn13s.append(canonical_isbn13)
            for potential_ol_edition in (aarecord['file_unified_data']['identifiers_unified'].get('ol') or []):
                if allthethings.utils.validate_ol_editions([potential_ol_edition]):
                    ol_editions.append(potential_ol_edition)
            for code in (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []):
                dois.append(code)
            for code in (aarecord['file_unified_data']['identifiers_unified'].get('oclc') or []):
                oclc_ids.append(code)
            for code in (aarecord['file_unified_data']['identifiers_unified'].get('ocaid') or []):
                ia_ids.append(code)
            for code in (aarecord['file_unified_data']['identifiers_unified'].get('duxiu_ssid') or []):
                duxiu_ssids.append(code)
            for code in (aarecord['file_unified_data']['identifiers_unified'].get('cadal_ssno') or []):
                cadal_ssnos.append(code)

        aarecords.append(aarecord)

    if not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0]):
        isbndb_dicts2 = {item['ean13']: item for item in get_isbndb_dicts(session, list(dict.fromkeys(canonical_isbn13s)))}
        ol_book_dicts2 = {item['ol_edition']: item for item in get_ol_book_dicts(session, 'ol_edition', list(dict.fromkeys(ol_editions)))}
        ol_book_dicts2_for_isbn13 = get_ol_book_dicts_by_isbn13(session, list(dict.fromkeys(canonical_isbn13s)))
        ol_book_dicts2_for_ia_id = get_ol_book_dicts_by_ia_id(session, list(dict.fromkeys(ia_ids)))
        ia_record_dicts3 = {item['ia_id']: item for item in get_ia_record_dicts(session, "ia_id", list(dict.fromkeys(ia_ids))) if item.get('aa_ia_file') is None}
        scihub_doi_dicts2 = {item['doi']: item for item in get_scihub_doi_dicts(session, 'doi', list(dict.fromkeys(dois)))}
        oclc_dicts2 = {item['oclc_id']: item for item in get_oclc_dicts(session, 'oclc', list(dict.fromkeys(oclc_ids)))}
        oclc_dicts2_for_isbn13 = get_oclc_dicts_by_isbn13(session, list(dict.fromkeys(canonical_isbn13s)))
        duxiu_dicts4 = {item['duxiu_ssid']: item for item in get_duxiu_dicts(session, 'duxiu_ssid', list(dict.fromkeys(duxiu_ssids)), include_deep_transitive_md5s_size_path=False)}
        duxiu_dicts5 = {item['cadal_ssno']: item for item in get_duxiu_dicts(session, 'cadal_ssno', list(dict.fromkeys(cadal_ssnos)), include_deep_transitive_md5s_size_path=False)}
        edsebk_dicts2_for_isbn13 = get_edsebk_dicts_by_isbn13(session, list(dict.fromkeys(canonical_isbn13s)))

    # Second pass
    for aarecord in aarecords:
        aarecord_id = aarecord['id']
        aarecord_id_split = aarecord_id.split(':', 1)
        lgli_single_edition = aarecord['lgli_file']['editions'][0] if len((aarecord.get('lgli_file') or {}).get('editions') or []) == 1 else None
        lgli_all_editions = aarecord['lgli_file']['editions'] if aarecord.get('lgli_file') else []

        if not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0]):
            isbndb_all = []
            existing_isbn13s = set([isbndb['isbn13'] for isbndb in aarecord['isbndb']])
            for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                if (canonical_isbn13 in isbndb_dicts2) and (canonical_isbn13 not in existing_isbn13s):
                    for isbndb in isbndb_dicts2[canonical_isbn13]['isbndb']:
                        isbndb_all.append(isbndb)
                    # No need to add to existing_isbn13s here.
            isbndb_all = isbndb_all[0:5]
            aarecord['isbndb'] = (aarecord['isbndb'] + isbndb_all)

            ol_book_dicts_all = []
            existing_ol_editions = set([ol_book_dict['ol_edition'] for ol_book_dict in aarecord['ol']])
            for potential_ol_edition in (aarecord['file_unified_data']['identifiers_unified'].get('ol') or []):
                if (potential_ol_edition in ol_book_dicts2) and (potential_ol_edition not in existing_ol_editions):
                    ol_book_dicts_all.append(ol_book_dicts2[potential_ol_edition])
                    # No need to add to existing_ol_editions here.
            ol_book_dicts_all = ol_book_dicts_all[0:5]
            aarecord['ol'] = (aarecord['ol'] + ol_book_dicts_all)

            ol_book_dicts_all = []
            existing_ol_editions = set([ol_book_dict['ol_edition'] for ol_book_dict in aarecord['ol']])
            for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                for ol_book_dict in (ol_book_dicts2_for_isbn13.get(canonical_isbn13) or []):
                    if ol_book_dict['ol_edition'] not in existing_ol_editions:
                        ol_book_dicts_all.append(ol_book_dict)
                        existing_ol_editions.add(ol_book_dict['ol_edition'])
            ol_book_dicts_all = ol_book_dicts_all[0:5]
            # Since these come from isbn13, we don't have the ol codes yet.
            for ol_book_dict in ol_book_dicts_all:
                allthethings.utils.add_identifier_unified(aarecord['file_unified_data'], 'ol', ol_book_dict['ol_edition'])
            aarecord['ol'] = (aarecord['ol'] + ol_book_dicts_all)

            ol_book_dicts_all = []
            existing_ol_editions = set([ol_book_dict['ol_edition'] for ol_book_dict in aarecord['ol']])
            for ia_id in (aarecord['file_unified_data']['identifiers_unified'].get('ocaid') or []):
                for ol_book_dict in (ol_book_dicts2_for_ia_id.get(ia_id) or []):
                    if ol_book_dict['ol_edition'] not in existing_ol_editions:
                        ol_book_dicts_all.append(ol_book_dict)
                        existing_ol_editions.add(ol_book_dict['ol_edition'])
            ol_book_dicts_all = ol_book_dicts_all[0:5]
            # Since these come from ocaid (ia_id), we don't have the ol codes yet.
            for ol_book_dict in ol_book_dicts_all:
                allthethings.utils.add_identifier_unified(aarecord['file_unified_data'], 'ol', ol_book_dict['ol_edition'])
            aarecord['ol'] = (aarecord['ol'] + ol_book_dicts_all)

            ia_record_dicts_all = []
            existing_ia_ids = set([aarecord['ia_record']['ia_id']] if aarecord['ia_record'] is not None else [])
            for potential_ia_id in (aarecord['file_unified_data']['identifiers_unified'].get('ocaid') or []):
                if (potential_ia_id in ia_record_dicts3) and (potential_ia_id not in existing_ia_ids):
                    ia_record_dicts_all.append(ia_record_dicts3[potential_ia_id])
                    # No need to add to existing_ia_ids here.
            ia_record_dicts_all = ia_record_dicts_all[0:5]
            aarecord['ia_records_meta_only'] = (aarecord['ia_records_meta_only'] + ia_record_dicts_all)

            scihub_doi_all = []
            existing_dois = set([scihub_doi['doi'] for scihub_doi in aarecord['scihub_doi']])
            for doi in (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []):
                if (doi in scihub_doi_dicts2) and (doi not in existing_dois):
                    scihub_doi_all.append(scihub_doi_dicts2[doi])
                    # No need to add to existing_dois here.
            scihub_doi_all = scihub_doi_all[0:5]
            aarecord['scihub_doi'] = (aarecord['scihub_doi'] + scihub_doi_all)

            oclc_all = []
            existing_oclc_ids = set([oclc['oclc_id'] for oclc in aarecord['oclc']])
            for oclc_id in (aarecord['file_unified_data']['identifiers_unified'].get('oclc') or []):
                if (oclc_id in oclc_dicts2) and (oclc_id not in existing_oclc_ids):
                    oclc_all.append(oclc_dicts2[oclc_id])
                    # No need to add to existing_oclc_ids here.
            oclc_all = oclc_all[0:5]
            aarecord['oclc'] = (aarecord['oclc'] + oclc_all)

            oclc_all = []
            existing_oclc_ids = set([oclc['oclc_id'] for oclc in aarecord['oclc']])
            for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                for oclc_dict in (oclc_dicts2_for_isbn13.get(canonical_isbn13) or []):
                    if oclc_dict['oclc_id'] not in existing_oclc_ids:
                        oclc_all.append(oclc_dict)
                        existing_oclc_ids.add(oclc_dict['oclc_id'])
            oclc_all = oclc_all[0:5]
            # Since these come from isbn13, we don't have the oclc codes yet.
            for oclc_dict in oclc_all:
                allthethings.utils.add_identifier_unified(aarecord['file_unified_data'], 'oclc', oclc_dict['oclc_id'])
            aarecord['oclc'] = (aarecord['oclc'] + oclc_all)

            duxiu_all = []
            existing_duxiu_ssids = set([duxiu_record.get('duxiu_ssid') for duxiu_record in (aarecord['duxius_nontransitive_meta_only'] + [aarecord['duxiu']] if aarecord['duxiu'] is not None else [])])
            for duxiu_ssid in (aarecord['file_unified_data']['identifiers_unified'].get('duxiu_ssid') or []):
                if (duxiu_ssid in duxiu_dicts4) and (duxiu_ssid not in existing_duxiu_ssids):
                    duxiu_all.append(duxiu_dicts4[duxiu_ssid])
                    # No need to add to existing_duxiu_ssids here.
            duxiu_all = duxiu_all[0:5]
            aarecord['duxius_nontransitive_meta_only'] = (aarecord['duxius_nontransitive_meta_only'] + duxiu_all)

            duxiu_all = []
            existing_cadal_ssnos = set([duxiu_record.get('cadal_ssno') for duxiu_record in (aarecord['duxius_nontransitive_meta_only'] + [aarecord['duxiu']] if aarecord['duxiu'] is not None else [])])
            for cadal_ssno in (aarecord['file_unified_data']['identifiers_unified'].get('cadal_ssno') or []):
                if (cadal_ssno in duxiu_dicts5) and (cadal_ssno not in existing_cadal_ssnos):
                    duxiu_all.append(duxiu_dicts5[cadal_ssno])
                    # No need to add to existing_cadal_ssnos here.
            duxiu_all = duxiu_all[0:5]
            aarecord['duxius_nontransitive_meta_only'] = (aarecord['duxius_nontransitive_meta_only'] + duxiu_all)

            if aarecord['aac_edsebk'] is None:
                edsebk_all = []
                for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                    for edsebk_dict in (edsebk_dicts2_for_isbn13.get(canonical_isbn13) or []):
                        edsebk_all += edsebk_dict
                if len(edsebk_all) > 0:
                    aarecord['aac_edsebk'] = edsebk_all[0]

        aarecord['ipfs_infos'] = []
        if aarecord['lgrsnf_book'] and ((aarecord['lgrsnf_book'].get('ipfs_cid') or '') != ''):
            aarecord['ipfs_infos'].append({ 'ipfs_cid': aarecord['lgrsnf_book']['ipfs_cid'], 'from': 'lgrsnf' })
        if aarecord['lgrsfic_book'] and ((aarecord['lgrsfic_book'].get('ipfs_cid') or '') != ''):
            aarecord['ipfs_infos'].append({ 'ipfs_cid': aarecord['lgrsfic_book']['ipfs_cid'], 'from': 'lgrsfic' })
        if aarecord['aac_zlib3_book'] and ((aarecord['aac_zlib3_book'].get('ipfs_cid') or '') != ''):
            aarecord['ipfs_infos'].append({ 'ipfs_cid': aarecord['aac_zlib3_book']['ipfs_cid'], 'from': 'zlib_ipfs_cid' })
        if aarecord['aac_zlib3_book'] and ((aarecord['aac_zlib3_book'].get('ipfs_cid_blake2b') or '') != ''):
            aarecord['ipfs_infos'].append({ 'ipfs_cid': aarecord['aac_zlib3_book']['ipfs_cid_blake2b'], 'from': 'zlib_ipfs_cid_blake2b' })
        if aarecord['aac_nexusstc']:
            for index, ipfs_cid in enumerate(aarecord['aac_nexusstc']['aa_nexusstc_derived']['ipfs_cids']):
                aarecord['ipfs_infos'].append({ 'ipfs_cid': ipfs_cid, 'from': f"nexusstc{index+1}" })
        for ipfs_info in aarecord['ipfs_infos']:
            allthethings.utils.add_identifier_unified(aarecord['file_unified_data'], 'ipfs_cid', ipfs_info['ipfs_cid'])

        original_filename_multiple = [
            *[allthethings.utils.prefix_filepath('lgrsnf', filepath) for filepath in filter(len, [((aarecord['lgrsnf_book'] or {}).get('locator') or '').strip()])],
            *[allthethings.utils.prefix_filepath('lgrsfic', filepath) for filepath in filter(len, [((aarecord['lgrsfic_book'] or {}).get('locator') or '').strip()])],
            *[allthethings.utils.prefix_filepath('lgli', filepath) for filepath in filter(len, [((aarecord['lgli_file'] or {}).get('locator') or '').strip()])],
            *[allthethings.utils.prefix_filepath('lgli', filename.strip()) for filename in (((aarecord['lgli_file'] or {}).get('descriptions_mapped') or {}).get('library_filename') or [])],
            *[allthethings.utils.prefix_filepath('ia', filepath) for filepath in filter(len, [(((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('original_filename') or '').strip()])],
            *[allthethings.utils.prefix_filepath('duxiu', filepath) for filepath in filter(len, [(((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('filepath_best') or '').strip()])],
            *[allthethings.utils.prefix_filepath('magzdb', filepath) for filepath in filter(len, [(((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('filepath_best') or '').strip()])],
            *[allthethings.utils.prefix_filepath('upload', filepath) for filepath in filter(len, [(((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('filename_best') or '').strip()])],
            *[allthethings.utils.prefix_filepath('nexusstc', filepath) for filepath in filter(len, [(((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('filepath_best') or '').strip()])],
            *[allthethings.utils.prefix_filepath('scimag', filepath) for filepath in filter(len, [((aarecord['lgli_file'] or {}).get('scimag_archive_path_decoded') or '').strip()])],
        ]
        original_filename_multiple_processed = list(dict.fromkeys(filter(len, original_filename_multiple))) # Before selecting best, since the best might otherwise get filtered.
        aarecord['file_unified_data']['original_filename_best'] = (original_filename_multiple_processed + [''])[0]
        original_filename_multiple += [allthethings.utils.prefix_filepath('ia', filepath) for filepath in filter(len, [(ia_record['aa_ia_derived']['original_filename'] or '').strip() for ia_record in aarecord['ia_records_meta_only']])]
        original_filename_multiple += [allthethings.utils.prefix_filepath('scihub', f"{scihub_doi['doi'].strip()}.pdf") for scihub_doi in aarecord['scihub_doi']]
        original_filename_multiple += [allthethings.utils.prefix_filepath('duxiu', filepath) for filepath in (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('filepath_multiple') or [])]
        original_filename_multiple += [allthethings.utils.prefix_filepath('upload', filepath) for filepath in (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('filename_multiple') or [])]
        original_filename_multiple += [allthethings.utils.prefix_filepath('magzdb', filepath) for filepath in (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('filepath_multiple') or [])]
        original_filename_multiple += [allthethings.utils.prefix_filepath('nexusstc', filepath) for filepath in (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('filepath_multiple') or [])]
        for duxiu_record in aarecord['duxius_nontransitive_meta_only']:
            original_filename_multiple += [allthethings.utils.prefix_filepath('duxiu', filepath) for filepath in duxiu_record['aa_duxiu_derived']['filepath_multiple']]
        if aarecord['file_unified_data']['original_filename_best'] == '':
            original_filename_multiple_processed = list(dict.fromkeys(filter(len, original_filename_multiple))) # Before selecting best, since the best might otherwise get filtered.
            aarecord['file_unified_data']['original_filename_best'] = (original_filename_multiple_processed + [''])[0]
        aarecord['file_unified_data']['original_filename_additional'] = [s for s in original_filename_multiple_processed if s != aarecord['file_unified_data']['original_filename_best']]
        aarecord['file_unified_data']['original_filename_best_name_only'] = re.split(r'[\\/]', aarecord['file_unified_data']['original_filename_best'])[-1] if not aarecord['file_unified_data']['original_filename_best'].startswith('10.') else aarecord['file_unified_data']['original_filename_best']
        for filepath in original_filename_multiple:
            allthethings.utils.add_identifier_unified(aarecord['file_unified_data'], 'filepath', filepath.encode()[0:allthethings.utils.AARECORDS_CODES_CODE_LENGTH-len('filepath:')-5].decode(errors='replace'))

        cover_url_multiple = [
            *[ol_book_dict['cover_url_normalized'] for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
        ]
        cover_url_multiple = list(dict.fromkeys(filter(len, cover_url_multiple)))
        aarecord['file_unified_data']['cover_url_best'] = (cover_url_multiple + [''])[0]
        # Select the cover_url_normalized in order of what is likely to be the best one: ia, lgrsnf, lgrsfic, lgli, zlib.
        cover_url_multiple += [
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('cover_url') or '').strip(),
            *[ia_record['aa_ia_derived']['cover_url'].strip() for ia_record in aarecord['ia_records_meta_only']],
            ((aarecord['lgrsnf_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((aarecord['lgli_file'] or {}).get('cover_url_guess_normalized') or '').strip(),
            ((aarecord['zlib_book'] or {}).get('cover_url_guess') or '').strip(),
            *[ol_book_dict['cover_url_normalized'] for ol_book_dict in aarecord['ol']],
            *[(isbndb['json'].get('image') or '').strip() for isbndb in aarecord['isbndb']],
        ]
        cover_url_multiple = list(dict.fromkeys(filter(len, cover_url_multiple)))
        if aarecord['file_unified_data']['cover_url_best'] == '':
            aarecord['file_unified_data']['cover_url_best'] = (cover_url_multiple + [''])[0]
        aarecord['file_unified_data']['cover_url_additional'] = [s for s in cover_url_multiple if s != aarecord['file_unified_data']['cover_url_best']]
        if aarecord['file_unified_data']['cover_url_best'] == '':
            cover_url_multiple += [isbndb['cover_url_guess'] for isbndb in aarecord['isbndb']]
            # For now, keep out cover urls from zlib entirely, and only add them ad-hoc from aac_zlib3_book.cover_path.
            # cover_url_multiple.append(((aarecord['aac_zlib3_book'] or {}).get('cover_url_guess') or '').strip())
            # cover_url_multiple.append(((aarecord['zlib_book'] or {}).get('cover_url_guess') or '').strip())
            cover_url_multiple = list(dict.fromkeys(filter(len, cover_url_multiple)))
            aarecord['file_unified_data']['cover_url_best'] = (cover_url_multiple + [''])[0]
            aarecord['file_unified_data']['cover_url_additional'] = [s for s in cover_url_multiple if s != aarecord['file_unified_data']['cover_url_best']]

        extension_multiple = [
            (((aarecord['ia_record'] or {}).get('aa_ia_file') or {}).get('extension') or '').strip().lower(),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgrsnf_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgrsfic_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgli_file'] or {}).get('extension') or '').strip().lower(),
            (((aarecord['duxiu'] or {}).get('duxiu_file') or {}).get('extension') or '').strip().lower(),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('extension') or '').strip(),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('extension') or '').strip(),
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('extension_best') or '').strip(),
            ('pdf' if aarecord_id_split[0] == 'doi' else ''),
        ]
        if "epub" in extension_multiple:
            aarecord['file_unified_data']['extension_best'] = "epub"
        elif "pdf" in extension_multiple:
            aarecord['file_unified_data']['extension_best'] = "pdf"
        else:
            aarecord['file_unified_data']['extension_best'] = max(extension_multiple + [''], key=len)
        aarecord['file_unified_data']['extension_additional'] = [s for s in dict.fromkeys(filter(len, extension_multiple)) if s != aarecord['file_unified_data']['extension_best']]

        filesize_multiple = [
            ((aarecord['ia_record'] or {}).get('aa_ia_file') or {}).get('filesize') or 0,
            (aarecord['aac_zlib3_book'] or {}).get('filesize') or 0,
            (aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('filesize_reported') or 0,
            (aarecord['zlib_book'] or {}).get('filesize') or 0,
            (aarecord['lgrsnf_book'] or {}).get('filesize') or 0,
            (aarecord['lgrsfic_book'] or {}).get('filesize') or 0,
            (aarecord['lgli_file'] or {}).get('filesize') or 0,
            ((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('filesize_best') or 0,
            ((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('filesize') or 0,
            ((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('filesize') or 0,
            ((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('filesize_best') or 0,
        ]
        aarecord['file_unified_data']['filesize_best'] = max(filesize_multiple)
        if aarecord['ia_record'] is not None and len(aarecord['ia_record']['json']['aa_shorter_files']) > 0:
            filesize_multiple.append(max(int(file.get('size') or '0') for file in aarecord['ia_record']['json']['aa_shorter_files']))
        for ia_record in aarecord['ia_records_meta_only']:
            if len(ia_record['json']['aa_shorter_files']) > 0:
                filesize_multiple.append(max(int(file.get('size') or '0') for file in ia_record['json']['aa_shorter_files']))
        if aarecord['file_unified_data']['filesize_best'] == 0:
            aarecord['file_unified_data']['filesize_best'] = max(filesize_multiple)
        zlib_book_filesize = (aarecord['zlib_book'] or {}).get('filesize') or 0
        if zlib_book_filesize > 0:
            # If we have a zlib_book with a `filesize`, then that is leading, since we measured it ourselves.
            aarecord['file_unified_data']['filesize_best'] = zlib_book_filesize
        filesize_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('filesize_multiple') or [])
        filesize_multiple += (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('filesize_multiple') or [])
        aarecord['file_unified_data']['filesize_additional'] = [s for s in dict.fromkeys(filter(lambda fz: fz > 0, filesize_multiple)) if s != aarecord['file_unified_data']['filesize_best']]

        title_multiple = [
            *[(ol_book_dict.get('title_normalized') or '').strip() for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
        ]
        title_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(title_multiple) # Before selecting best, since the best might otherwise get filtered.
        aarecord['file_unified_data']['title_best'] = max(title_multiple + [''], key=len)
        title_multiple += [
            ((aarecord['lgrsnf_book'] or {}).get('title') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('title') or '').strip(),
            ((lgli_single_edition or {}).get('title') or '').strip(),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('title') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('title') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('title_best') or '').strip(),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('title_best') or '').strip(),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('title_best') or '').strip(),
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('title_best') or '').strip(),
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('title_best') or '').strip(),
        ]
        title_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(title_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['title_best'] == '':
            aarecord['file_unified_data']['title_best'] = max(title_multiple + [''], key=len)
        title_multiple += [(edition.get('title') or '').strip() for edition in lgli_all_editions]
        title_multiple += [title.strip() for edition in lgli_all_editions for title in (edition['descriptions_mapped'].get('maintitleonoriginallanguage') or [])]
        title_multiple += [title.strip() for edition in lgli_all_editions for title in (edition['descriptions_mapped'].get('maintitleonenglishtranslate') or [])]
        title_multiple += [(ol_book_dict.get('title_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        title_multiple += [(isbndb.get('title_normalized') or '').strip() for isbndb in aarecord['isbndb']]
        title_multiple += [ia_record['aa_ia_derived']['title'].strip() for ia_record in aarecord['ia_records_meta_only']]
        title_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('title_multiple') or [])
        title_multiple += (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('title_multiple') or [])
        title_multiple += (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('title_multiple') or [])
        title_multiple += (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('title_multiple') or [])
        for oclc in aarecord['oclc']:
            title_multiple += oclc['aa_oclc_derived']['title_multiple']
        for duxiu_record in aarecord['duxius_nontransitive_meta_only']:
            title_multiple += duxiu_record['aa_duxiu_derived']['title_multiple']
        title_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(title_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['title_best'] == '':
            aarecord['file_unified_data']['title_best'] = max(title_multiple + [''], key=len)
        aarecord['file_unified_data']['title_additional'] = [s for s in title_multiple if s != aarecord['file_unified_data']['title_best']]

        author_multiple = [
            *[(ol_book_dict.get('authors_normalized') or '').strip() for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
        ]
        author_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(author_multiple) # Before selecting best, since the best might otherwise get filtered.
        aarecord['file_unified_data']['author_best'] = max(author_multiple + [''], key=len)
        author_multiple += [
            (aarecord['lgrsnf_book'] or {}).get('author', '').strip(),
            (aarecord['lgrsfic_book'] or {}).get('author', '').strip(),
            (lgli_single_edition or {}).get('authors_normalized', '').strip(),
            (aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('author', '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('author') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('author_best') or '').strip(),
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('author_best') or '').strip(),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('author_best') or '').strip(),
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('author_best') or '').strip(),
        ]
        author_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(author_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['author_best'] == '':
            aarecord['file_unified_data']['author_best'] = max(author_multiple + [''], key=len)
        author_multiple += [edition.get('authors_normalized', '').strip() for edition in lgli_all_editions]
        author_multiple += [ol_book_dict['authors_normalized'] for ol_book_dict in aarecord['ol']]
        author_multiple += [", ".join(isbndb['json'].get('authors') or []) for isbndb in aarecord['isbndb']]
        author_multiple += [ia_record['aa_ia_derived']['author'].strip() for ia_record in aarecord['ia_records_meta_only']]
        author_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('author_multiple') or [])
        author_multiple += (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('author_multiple') or [])
        for oclc in aarecord['oclc']:
            author_multiple += oclc['aa_oclc_derived']['author_multiple']
        for duxiu_record in aarecord['duxius_nontransitive_meta_only']:
            author_multiple += duxiu_record['aa_duxiu_derived']['author_multiple']
        author_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(author_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['author_best'] == '':
            aarecord['file_unified_data']['author_best'] = max(author_multiple + [''], key=len)
        aarecord['file_unified_data']['author_additional'] = [s for s in author_multiple if s != aarecord['file_unified_data']['author_best']]

        publisher_multiple = [
            *[(ol_book_dict.get('publishers_normalized') or '').strip() for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
        ]
        publisher_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(publisher_multiple) # Before selecting best, since the best might otherwise get filtered.
        aarecord['file_unified_data']['publisher_best'] = max(publisher_multiple + [''], key=len)
        publisher_multiple += [
            ((aarecord['lgrsnf_book'] or {}).get('publisher') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('publisher') or '').strip(),
            ((lgli_single_edition or {}).get('publisher_normalized') or '').strip(),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('publisher') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('publisher') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('publisher_best') or '').strip(),
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('publisher_best') or '').strip(),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('publisher_best') or '').strip(),
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('publisher_best') or '').strip(),
        ]
        publisher_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(publisher_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['publisher_best'] == '':
            aarecord['file_unified_data']['publisher_best'] = max(publisher_multiple + [''], key=len)
        publisher_multiple += [(edition.get('publisher_normalized') or '').strip() for edition in lgli_all_editions]
        publisher_multiple += [(ol_book_dict.get('publishers_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        publisher_multiple += [(isbndb['json'].get('publisher') or '').strip() for isbndb in aarecord['isbndb']]
        publisher_multiple += [ia_record['aa_ia_derived']['publisher'].strip() for ia_record in aarecord['ia_records_meta_only']]
        publisher_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('publisher_multiple') or [])
        publisher_multiple += (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('publisher_multiple') or [])
        for oclc in aarecord['oclc']:
            publisher_multiple += oclc['aa_oclc_derived']['publisher_multiple']
        for duxiu_record in aarecord['duxius_nontransitive_meta_only']:
            publisher_multiple += duxiu_record['aa_duxiu_derived']['publisher_multiple']
        publisher_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(publisher_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['publisher_best'] == '':
            aarecord['file_unified_data']['publisher_best'] = max(publisher_multiple + [''], key=len)
        aarecord['file_unified_data']['publisher_additional'] = [s for s in publisher_multiple if s != aarecord['file_unified_data']['publisher_best']]

        edition_varia_multiple = [
            *[(ol_book_dict.get('edition_varia_normalized') or '').strip() for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
        ]
        edition_varia_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(edition_varia_multiple) # Before selecting best, since the best might otherwise get filtered.
        aarecord['file_unified_data']['edition_varia_best'] = max(edition_varia_multiple + [''], key=len)
        edition_varia_multiple += [
            ((aarecord['lgrsnf_book'] or {}).get('edition_varia_normalized') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('edition_varia_normalized') or '').strip(),
            ((lgli_single_edition or {}).get('edition_varia_normalized') or '').strip(),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('edition_varia_normalized') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('edition_varia_normalized') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('edition_varia_normalized') or '').strip(),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('edition_varia_normalized') or '').strip(),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('edition_varia_normalized') or '').strip(),
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('edition_varia_normalized') or '').strip(),
        ]
        edition_varia_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(edition_varia_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['edition_varia_best'] == '':
            aarecord['file_unified_data']['edition_varia_best'] = max(edition_varia_multiple + [''], key=len)
        edition_varia_multiple += [(edition.get('edition_varia_normalized') or '').strip() for edition in lgli_all_editions]
        edition_varia_multiple += [(ol_book_dict.get('edition_varia_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        edition_varia_multiple += [(isbndb.get('edition_varia_normalized') or '').strip() for isbndb in aarecord['isbndb']]
        edition_varia_multiple += [ia_record['aa_ia_derived']['edition_varia_normalized'].strip() for ia_record in aarecord['ia_records_meta_only']]
        edition_varia_multiple += [oclc['aa_oclc_derived']['edition_varia_normalized'] for oclc in aarecord['oclc']]
        edition_varia_multiple += [duxiu_record['aa_duxiu_derived']['edition_varia_normalized'] for duxiu_record in aarecord['duxius_nontransitive_meta_only']]
        edition_varia_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(edition_varia_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['edition_varia_best'] == '':
            aarecord['file_unified_data']['edition_varia_best'] = max(edition_varia_multiple + [''], key=len)
        aarecord['file_unified_data']['edition_varia_additional'] = [s for s in edition_varia_multiple if s != aarecord['file_unified_data']['edition_varia_best']]

        year_multiple = [
            *[(ol_book_dict.get('year_normalized') or '').strip() for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
        ]
        # Filter out years in for which we surely don't have books (famous last words..)
        # WARNING duplicated below
        year_multiple = [(year if allthethings.utils.validate_year(year) else '') for year in year_multiple]
        year_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(year_multiple) # Before selecting best, since the best might otherwise get filtered.
        aarecord['file_unified_data']['year_best'] = max(year_multiple + [''], key=len)
        year_multiple += [
            ((aarecord['lgrsnf_book'] or {}).get('year') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('year') or '').strip(),
            ((lgli_single_edition or {}).get('year') or '').strip(),
            ((lgli_single_edition or {}).get('issue_year_number') or '').strip(),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('year') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('year') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('year_best') or '').strip(),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('year') or '').strip(),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('year') or '').strip(),
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('year') or '').strip(),
        ]
        # Filter out years in for which we surely don't have books (famous last words..)
        # WARNING duplicated above
        year_multiple = [(year if year.isdigit() and int(year) >= 1600 and int(year) < 2100 else '') for year in year_multiple]
        year_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(year_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['year_best'] == '':
            aarecord['file_unified_data']['year_best'] = max(year_multiple + [''], key=len)
        year_multiple += [(edition.get('year_normalized') or '').strip() for edition in lgli_all_editions]
        year_multiple += [(ol_book_dict.get('year_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        year_multiple += [(isbndb.get('year_normalized') or '').strip() for isbndb in aarecord['isbndb']]
        year_multiple += [ia_record['aa_ia_derived']['year'].strip() for ia_record in aarecord['ia_records_meta_only']]
        year_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('year_multiple') or [])
        for oclc in aarecord['oclc']:
            year_multiple += oclc['aa_oclc_derived']['year_multiple']
        for duxiu_record in aarecord['duxius_nontransitive_meta_only']:
            year_multiple += duxiu_record['aa_duxiu_derived']['year_multiple']
        for year in year_multiple:
            # If a year appears in edition_varia_best, then use that, for consistency.
            if year != '' and year in aarecord['file_unified_data']['edition_varia_best']:
                aarecord['file_unified_data']['year_best'] = year
        year_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(year_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['year_best'] == '':
            aarecord['file_unified_data']['year_best'] = max(year_multiple + [''], key=len)
        aarecord['file_unified_data']['year_additional'] = [s for s in year_multiple if s != aarecord['file_unified_data']['year_best']]

        for year in year_multiple:
            allthethings.utils.add_classification_unified(aarecord['file_unified_data'], 'year', year)

        comments_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('commentary') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('commentary') or '').strip(),
            ' -- '.join(filter(len, [((aarecord['lgrsnf_book'] or {}).get('library') or '').strip(), (aarecord['lgrsnf_book'] or {}).get('issue', '').strip()])),
            ' -- '.join(filter(len, [((aarecord['lgrsfic_book'] or {}).get('library') or '').strip(), (aarecord['lgrsfic_book'] or {}).get('issue', '').strip()])),
            ' -- '.join(filter(len, [*((aarecord['lgli_file'] or {}).get('descriptions_mapped') or {}).get('descriptions_mapped.library', []), *(aarecord['lgli_file'] or {}).get('descriptions_mapped', {}).get('descriptions_mapped.library_issue', [])])),
            ((lgli_single_edition or {}).get('commentary') or '').strip(),
            ((lgli_single_edition or {}).get('editions_add_info') or '').strip(),
            ((lgli_single_edition or {}).get('commentary') or '').strip(),
            *[note.strip() for note in (((lgli_single_edition or {}).get('descriptions_mapped') or {}).get('descriptions_mapped.notes') or [])],
            *(((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('combined_comments') or []),
            *[comment for ia_record in aarecord['ia_records_meta_only'] for comment in ia_record['aa_ia_derived']['combined_comments']],
            *(((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('combined_comments') or []),
            *(((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('combined_comments') or []),
            *(((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('combined_comments') or []),
            *(((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('combined_comments') or []),
            *(((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('combined_comments') or []),
        ]
        comments_multiple += [(edition.get('comments_normalized') or '').strip() for edition in lgli_all_editions]
        for edition in lgli_all_editions:
            comments_multiple.append((edition.get('editions_add_info') or '').strip())
            comments_multiple.append((edition.get('commentary') or '').strip())
            for note in (edition.get('descriptions_mapped') or {}).get('descriptions_mapped.notes', []):
                comments_multiple.append(note.strip())
        for ol_book_dict in aarecord['ol']:
            for comment in ol_book_dict.get('comments_normalized') or []:
                comments_multiple.append(comment.strip())
        for ol_book_dict in aarecord['ol_book_dicts_primary_linked']:
            for comment in ol_book_dict.get('comments_normalized') or []:
                comments_multiple.append(comment.strip())
        for duxiu_record in aarecord['duxius_nontransitive_meta_only']:
            for comment in duxiu_record.get('combined_comments') or []:
                comments_multiple.append(comment.strip())
        aarecord['file_unified_data']['comments_multiple'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(comments_multiple)]

        stripped_description_multiple = [
            *[(ol_book_dict.get('stripped_description') or '').strip() for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
        ]
        stripped_description_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(stripped_description_multiple) # Before selecting best, since the best might otherwise get filtered.
        aarecord['file_unified_data']['stripped_description_best'] = max(stripped_description_multiple + [''], key=len)
        stripped_description_multiple += [
            ((aarecord['lgrsnf_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['lgrsfic_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((lgli_single_edition or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('description_best') or '').strip(),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('stripped_description') or '').strip(),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('stripped_description') or '').strip(),
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('description_best') or '').strip(),
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('description_best') or '').strip(),
        ]
        stripped_description_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(stripped_description_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['stripped_description_best'] == '':
            aarecord['file_unified_data']['stripped_description_best'] = max(stripped_description_multiple + [''], key=len)
        stripped_description_multiple += [(edition.get('stripped_description') or '').strip()[0:5000] for edition in lgli_all_editions]
        stripped_description_multiple += [ol_book_dict['stripped_description'].strip()[0:5000] for ol_book_dict in aarecord['ol']]
        stripped_description_multiple += [(isbndb['json'].get('synopsis') or '').strip()[0:5000] for isbndb in aarecord['isbndb']]
        stripped_description_multiple += [(isbndb['json'].get('overview') or '').strip()[0:5000] for isbndb in aarecord['isbndb']]
        # Don't make ia_record's description a primary choice here, since it's often not very good.
        stripped_description_multiple += [(((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('stripped_description_and_references') or '').strip()[0:5000]]
        stripped_description_multiple += [ia_record['aa_ia_derived']['stripped_description_and_references'].strip()[0:5000] for ia_record in aarecord['ia_records_meta_only']]
        for oclc in aarecord['oclc']:
            stripped_description_multiple += oclc['aa_oclc_derived']['stripped_description_multiple']
        stripped_description_multiple += [duxiu_record['aa_duxiu_derived']['description_best'] for duxiu_record in aarecord['duxius_nontransitive_meta_only']]
        stripped_description_multiple = sort_by_length_and_filter_subsequences_with_longest_string_and_normalize_unicode(stripped_description_multiple) # Before selecting best, since the best might otherwise get filtered.
        if aarecord['file_unified_data']['stripped_description_best'] == '':
            aarecord['file_unified_data']['stripped_description_best'] = max(stripped_description_multiple + [''], key=len)
        aarecord['file_unified_data']['stripped_description_additional'] = [s for s in stripped_description_multiple if s != aarecord['file_unified_data']['stripped_description_best']]

        aarecord['file_unified_data']['most_likely_language_codes'] = []
        aarecord['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([
            # Still lump in other language codes with ol_book_dicts_primary_linked. We use the
            # fact that combine_bcp47_lang_codes is stable (preserves order).
            *[(ol_book_dict.get('language_codes') or []) for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
            ((aarecord['lgrsnf_book'] or {}).get('language_codes') or []),
            ((aarecord['lgrsfic_book'] or {}).get('language_codes') or []),
            ((lgli_single_edition or {}).get('language_codes') or []),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('language_codes') or []),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('language_codes') or []),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('language_codes') or []),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('language_codes') or []),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('language_codes') or []),
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('language_codes') or []),
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('language_codes') or []),
        ])
        if len(aarecord['file_unified_data']['most_likely_language_codes']) == 0:
            aarecord['file_unified_data']['most_likely_language_codes'] = aarecord['file_unified_data']['language_codes']
        aarecord['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([
            aarecord['file_unified_data']['language_codes'],
            *[(edition.get('language_codes') or []) for edition in lgli_all_editions],
            *[(ol_book_dict.get('language_codes') or []) for ol_book_dict in aarecord['ol']],
            *[ia_record['aa_ia_derived']['language_codes'] for ia_record in aarecord['ia_records_meta_only']],
            *[(isbndb.get('language_codes') or []) for isbndb in aarecord['isbndb']],
            *[oclc['aa_oclc_derived']['language_codes'] for oclc in aarecord['oclc']],
            *[duxiu_record['aa_duxiu_derived']['language_codes'] for duxiu_record in aarecord['duxius_nontransitive_meta_only']],
        ])
        if len(aarecord['file_unified_data']['language_codes']) == 0:
            for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                potential_code = get_bcp47_lang_codes_parse_substr(isbnlib.info(canonical_isbn13))
                if potential_code != '':
                    aarecord['file_unified_data']['language_codes'] = [potential_code]
                    break
        if len(aarecord['file_unified_data']['most_likely_language_codes']) == 0:
            aarecord['file_unified_data']['most_likely_language_codes'] = aarecord['file_unified_data']['language_codes']

        aarecord['file_unified_data']['language_codes_detected'] = []
        if len(aarecord['file_unified_data']['most_likely_language_codes']) == 0 and len(aarecord['file_unified_data']['stripped_description_best']) > 20:
            language_detect_string = " ".join(title_multiple) + " ".join(stripped_description_multiple)
            try:
                language_detection_data = fast_langdetect.detect(language_detect_string)
                if language_detection_data['score'] > 0.5: # Somewhat arbitrary cutoff
                    language_detection = language_detection_data['lang']
                    aarecord['file_unified_data']['language_codes_detected'] = [get_bcp47_lang_codes(language_detection)[0]]
                    aarecord['file_unified_data']['language_codes'] = aarecord['file_unified_data']['language_codes_detected']
                    aarecord['file_unified_data']['most_likely_language_codes'] = aarecord['file_unified_data']['language_codes']
            except Exception:
                pass

        for lang_code in aarecord['file_unified_data']['language_codes']:
            allthethings.utils.add_classification_unified(aarecord['file_unified_data'], 'lang', lang_code)

        # detected_language_codes_probs = []
        # for item in language_detection:
        #     for code in get_bcp47_lang_codes(item.lang):
        #         detected_language_codes_probs.append(f"{code}: {item.prob}")
        # aarecord['file_unified_data']['detected_language_codes_probs'] = ", ".join(detected_language_codes_probs)

        aarecord['file_unified_data']['added_date_unified'] = dict(collections.ChainMap(*[
            ((aarecord['lgrsnf_book'] or {}).get('added_date_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('added_date_unified') or {}),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('added_date_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('added_date_unified') or {}),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('added_date_unified') or {}),
            *[ia_record['aa_ia_derived']['added_date_unified'] for ia_record in aarecord['ia_records_meta_only']],
            *[isbndb['added_date_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['added_date_unified'] for ol_book_dict in aarecord['ol']],
            *[ol_book_dict['added_date_unified'] for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
            *[oclc['aa_oclc_derived']['added_date_unified'] for oclc in aarecord['oclc']],
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('added_date_unified') or {}),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('added_date_unified') or {}),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('added_date_unified') or {}),
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('added_date_unified') or {}),
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('added_date_unified') or {}),
        ]))
        for prefix, date in aarecord['file_unified_data']['added_date_unified'].items():
            allthethings.utils.add_classification_unified(aarecord['file_unified_data'], prefix, date)

        # Duplicated from above, but with more fields now.
        aarecord['file_unified_data']['identifiers_unified'] = allthethings.utils.merge_unified_fields([
            aarecord['file_unified_data']['identifiers_unified'],
            ((aarecord['lgrsnf_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('identifiers_unified') or {}),
            *[edition['identifiers_unified'] for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('identifiers_unified') or {}),
            *[ia_record['aa_ia_derived']['identifiers_unified'] for ia_record in aarecord['ia_records_meta_only']],
            *[isbndb['identifiers_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['identifiers_unified'] for ol_book_dict in aarecord['ol']],
            *[ol_book_dict['identifiers_unified'] for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
            *[scihub_doi['identifiers_unified'] for scihub_doi in aarecord['scihub_doi']],
            *[oclc['aa_oclc_derived']['identifiers_unified'] for oclc in aarecord['oclc']],
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('identifiers_unified') or {}),
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('identifiers_unified') or {}),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('identifiers_unified') or {}),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('identifiers_unified') or {}),
            *[duxiu_record['aa_duxiu_derived']['identifiers_unified'] for duxiu_record in aarecord['duxius_nontransitive_meta_only']],
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('identifiers_unified') or {}),
        ])
        aarecord['file_unified_data']['classifications_unified'] = allthethings.utils.merge_unified_fields([
            aarecord['file_unified_data']['classifications_unified'],
            ((aarecord['lgrsnf_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('classifications_unified') or {}),
            *[(edition['classifications_unified'] or {}) for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('classifications_unified') or {}),
            *[ia_record['aa_ia_derived']['classifications_unified'] for ia_record in aarecord['ia_records_meta_only']],
            *[isbndb['classifications_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['classifications_unified'] for ol_book_dict in aarecord['ol']],
            *[ol_book_dict['classifications_unified'] for ol_book_dict in aarecord['ol_book_dicts_primary_linked']],
            *[scihub_doi['classifications_unified'] for scihub_doi in aarecord['scihub_doi']],
            (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('classifications_unified') or {}),
            (((aarecord['aac_magzdb'] or {}).get('aa_magzdb_derived') or {}).get('classifications_unified') or {}),
            (((aarecord['aac_nexusstc'] or {}).get('aa_nexusstc_derived') or {}).get('classifications_unified') or {}),
            *[duxiu_record['aa_duxiu_derived']['classifications_unified'] for duxiu_record in aarecord['duxius_nontransitive_meta_only']],
            (((aarecord['aac_edsebk'] or {}).get('aa_edsebk_derived') or {}).get('classifications_unified') or {}),
        ])

        aarecord['file_unified_data']['added_date_best'] = ''
        if aarecord_id_split[0] == 'md5':
            potential_dates = list(filter(len, [
                (aarecord['file_unified_data']['added_date_unified'].get('date_duxiu_filegen') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('date_ia_file_scrape') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('date_lgli_source') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('date_lgrsfic_source') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('date_lgrsnf_source') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('date_upload_record') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('date_zlib_source') or ''),
            ]))
            if len(potential_dates) > 0:
                aarecord['file_unified_data']['added_date_best'] = min(potential_dates)
        elif aarecord_id_split[0] == 'ia':
            if 'date_ia_source' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_ia_source']
            elif 'date_ia_record_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_ia_record_scrape']
        elif aarecord_id_split[0] == 'isbn':
            if 'date_isbndb_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_isbndb_scrape']
        elif aarecord_id_split[0] == 'ol':
            if 'date_ol_source' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_ol_source']
        elif aarecord_id_split[0] == 'doi':
            pass # We don't have the information of when this was added to scihub sadly.
        elif aarecord_id_split[0] == 'oclc':
            if 'date_oclc_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_oclc_scrape']
        elif aarecord_id_split[0] == 'duxiu_ssid':
            if 'date_duxiu_meta_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_duxiu_meta_scrape']
        elif aarecord_id_split[0] == 'cadal_ssno':
            if 'date_duxiu_meta_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_duxiu_meta_scrape']
        elif aarecord_id_split[0] == 'magzdb':
            if 'date_magzdb_meta_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_magzdb_meta_scrape']
        elif aarecord_id_split[0] == 'edsebk':
            if 'date_edsebk_meta_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_edsebk_meta_scrape']
        elif aarecord_id_split[0] in ['nexusstc', 'nexusstc_download']:
            if 'date_nexusstc_source_update' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['date_nexusstc_source_update']
        else:
            raise Exception(f"Unknown {aarecord_id_split[0]=}")

        aarecord['file_unified_data']['problems'] = []
        if ((aarecord['lgrsnf_book'] or {}).get('visible') or '') != '':
            aarecord['file_unified_data']['problems'].append({ 'type': 'lgrsnf_visible', 'descr': ((aarecord['lgrsnf_book'] or {}).get('visible') or ''), 'better_md5': ((aarecord['lgrsnf_book'] or {}).get('generic') or '').lower() })
        if ((aarecord['lgrsfic_book'] or {}).get('visible') or '') != '':
            aarecord['file_unified_data']['problems'].append({ 'type': 'lgrsfic_visible', 'descr': ((aarecord['lgrsfic_book'] or {}).get('visible') or ''), 'better_md5': ((aarecord['lgrsfic_book'] or {}).get('generic') or '').lower() })
        if ((aarecord['lgli_file'] or {}).get('visible') or '') != '':
            aarecord['file_unified_data']['problems'].append({ 'type': 'lgli_visible', 'descr': ((aarecord['lgli_file'] or {}).get('visible') or ''), 'better_md5': ((aarecord['lgli_file'] or {}).get('generic') or '').lower() })
        if ((aarecord['lgli_file'] or {}).get('broken') or '') in [1, "1", "y", "Y"]:
            aarecord['file_unified_data']['problems'].append({ 'type': 'lgli_broken', 'descr': ((aarecord['lgli_file'] or {}).get('broken') or ''), 'better_md5': ((aarecord['lgli_file'] or {}).get('generic') or '').lower() })
        if len(((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('problems_infos') or []) > 0:
            for duxiu_problem_info in (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('problems_infos') or []):
                if duxiu_problem_info['duxiu_problem_type'] == 'pdg_broken_files':
                    # TODO:TRANSLATE bring back translation: dummy_translation_affected_files = gettext('page.md5.box.download.affected_files')
                    # but later when actually rendering the page.
                    # TODO: not covered by local fixtures.
                    aarecord['file_unified_data']['problems'].append({ 'type': 'duxiu_pdg_broken_files', 'descr': f"{duxiu_problem_info['pdg_broken_files_len']} affected pages", 'better_md5': '' })
                else:
                    raise Exception(f"Unknown duxiu_problem_type: {duxiu_problem_info=}")
        if len(((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('problems_infos') or []) > 0:
            for upload_problem_info in (((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('problems_infos') or []):
                if upload_problem_info['upload_problem_type'] == 'exiftool_failed':
                    aarecord['file_unified_data']['problems'].append({ 'type': 'upload_exiftool_failed', 'descr': '', 'better_md5': '' })
                else:
                    raise Exception(f"Unknown upload_problem_type: {upload_problem_info=}")

        zlib_deleted_comment = ((aarecord['aac_zlib3_book'] or {}).get('deleted_comment') or '').lower()
        if zlib_deleted_comment == '':
            pass
        elif zlib_deleted_comment == 'dmca':
            # Only mark it if we can't serve the file.
            if ((aarecord['aac_zlib3_book'].get('file_aacid') or '') == '') and (len((aarecord['zlib_book'] or {}).get('pilimi_torrent') or '') == '') and (aarecord['lgli_file'] is None) and (aarecord['lgrsfic_book'] is None) and (aarecord['lgrsnf_book'] is None):
                aarecord['file_unified_data']['problems'].append({ 'type': 'zlib_missing', 'descr': '', 'better_md5': '' })
        elif zlib_deleted_comment == 'spam':
            aarecord['file_unified_data']['problems'].append({ 'type': 'zlib_spam', 'descr': '', 'better_md5': '' })
        elif zlib_deleted_comment == 'bad file':
            aarecord['file_unified_data']['problems'].append({ 'type': 'zlib_bad_file', 'descr': '', 'better_md5': '' })
        else:
            raise Exception(f"Unexpected {zlib_deleted_comment=} for {aarecord=}")

        aarecord['file_unified_data']['content_type'] = None
        if (aarecord['file_unified_data']['content_type'] is None) and (aarecord['lgli_file'] is not None):
            if aarecord['lgli_file']['libgen_topic'] == 'l':
                aarecord['file_unified_data']['content_type'] = 'book_nonfiction'
            if aarecord['lgli_file']['libgen_topic'] == 'f':
                aarecord['file_unified_data']['content_type'] = 'book_fiction'
            if aarecord['lgli_file']['libgen_topic'] == 'r':
                aarecord['file_unified_data']['content_type'] = 'book_fiction'
            if aarecord['lgli_file']['libgen_topic'] == 'a':
                aarecord['file_unified_data']['content_type'] = 'journal_article'
            if aarecord['lgli_file']['libgen_topic'] == 's':
                aarecord['file_unified_data']['content_type'] = 'standards_document'
            if aarecord['lgli_file']['libgen_topic'] == 'm':
                aarecord['file_unified_data']['content_type'] = 'magazine'
            if aarecord['lgli_file']['libgen_topic'] == 'c':
                aarecord['file_unified_data']['content_type'] = 'book_comic'
        if (aarecord['file_unified_data']['content_type'] is None) and aarecord['aac_magzdb']:
            aarecord['file_unified_data']['content_type'] = 'magazine'
        if (aarecord['file_unified_data']['content_type'] is None) and aarecord['lgrsnf_book'] and (not aarecord['lgrsfic_book']):
            aarecord['file_unified_data']['content_type'] = 'book_nonfiction'
        if (aarecord['file_unified_data']['content_type'] is None) and (not aarecord['lgrsnf_book']) and aarecord['lgrsfic_book']:
            aarecord['file_unified_data']['content_type'] = 'book_fiction'
        if (aarecord['file_unified_data']['content_type'] is None) and aarecord['aac_nexusstc'] and (aarecord['aac_nexusstc']['aa_nexusstc_derived']['content_type'] != ''):
            aarecord['file_unified_data']['content_type'] = aarecord['aac_nexusstc']['aa_nexusstc_derived']['content_type']
        if aarecord['file_unified_data']['content_type'] is None:
            ia_content_type = (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('content_type') or 'book_unknown')
            for ia_record in aarecord['ia_records_meta_only']:
                if ia_content_type == 'book_unknown':
                    ia_content_type = ia_record['aa_ia_derived']['content_type']
            if (aarecord['file_unified_data']['content_type'] is None) and (ia_content_type != 'book_unknown'):
                aarecord['file_unified_data']['content_type'] = ia_content_type
        # TODO: pull non-fiction vs fiction from "subjects" in ol_book_dicts_primary_linked, and make that more leading?
        if (aarecord['file_unified_data']['content_type'] is None) and (len(aarecord['ol_book_dicts_primary_linked']) > 0):
            aarecord['file_unified_data']['content_type'] = 'book_unknown'
        if (aarecord['file_unified_data']['content_type'] is None) and (len(aarecord['scihub_doi']) > 0):
            aarecord['file_unified_data']['content_type'] = 'journal_article'
        if (aarecord['file_unified_data']['content_type'] is None) and (len(aarecord['oclc']) > 0):
            for oclc in aarecord['oclc']:
                # OCLC has a lot of books mis-tagged as journal article.
                if (aarecord_id_split[0] == 'oclc') or (oclc['aa_oclc_derived']['content_type'] != 'other' and oclc['aa_oclc_derived']['content_type'] != 'journal_article'):
                    aarecord['file_unified_data']['content_type'] = oclc['aa_oclc_derived']['content_type']
                    break
        if (aarecord['file_unified_data']['content_type'] is None) and ((((aarecord['aac_upload'] or {}).get('aa_upload_derived') or {}).get('content_type') or '') != ''):
            aarecord['file_unified_data']['content_type'] = aarecord['aac_upload']['aa_upload_derived']['content_type']
        if aarecord['file_unified_data']['content_type'] is None:
            aarecord['file_unified_data']['content_type'] = 'book_unknown'

        if aarecord['lgrsnf_book'] is not None:
            aarecord['lgrsnf_book'] = {
                'id': aarecord['lgrsnf_book']['id'],
                'md5': aarecord['lgrsnf_book']['md5'],
            }
        if aarecord['lgrsfic_book'] is not None:
            aarecord['lgrsfic_book'] = {
                'id': aarecord['lgrsfic_book']['id'],
                'md5': aarecord['lgrsfic_book']['md5'],
            }
        if aarecord['lgli_file'] is not None:
            aarecord['lgli_file'] = {
                'f_id': aarecord['lgli_file']['f_id'],
                'md5': aarecord['lgli_file']['md5'],
                'libgen_topic': aarecord['lgli_file']['libgen_topic'],
                'libgen_id': aarecord['lgli_file']['libgen_id'],
                'fiction_id': aarecord['lgli_file']['fiction_id'],
                'fiction_rus_id': aarecord['lgli_file']['fiction_rus_id'],
                'comics_id': aarecord['lgli_file']['comics_id'],
                'scimag_id': aarecord['lgli_file']['scimag_id'],
                'standarts_id': aarecord['lgli_file']['standarts_id'],
                'magz_id': aarecord['lgli_file']['magz_id'],
                'scimag_archive_path': aarecord['lgli_file']['scimag_archive_path'],
            }
        if aarecord['zlib_book'] is not None:
            aarecord['zlib_book'] = {
                'zlibrary_id': aarecord['zlib_book']['zlibrary_id'],
                'md5': aarecord['zlib_book']['md5'],
                'md5_reported': aarecord['zlib_book']['md5_reported'],
                'filesize': aarecord['zlib_book']['filesize'],
                'filesize_reported': aarecord['zlib_book']['filesize_reported'],
                'in_libgen': aarecord['zlib_book']['in_libgen'],
                'pilimi_torrent': aarecord['zlib_book']['pilimi_torrent'],
            }
        if aarecord['aac_zlib3_book'] is not None:
            aarecord['aac_zlib3_book'] = {
                'zlibrary_id': aarecord['aac_zlib3_book']['zlibrary_id'],
                'md5': aarecord['aac_zlib3_book']['md5'],
                'md5_reported': aarecord['aac_zlib3_book']['md5_reported'],
                'filesize_reported': aarecord['aac_zlib3_book']['filesize_reported'],
                'file_data_folder': aarecord['aac_zlib3_book']['file_data_folder'],
                'record_aacid': aarecord['aac_zlib3_book']['record_aacid'],
                'file_aacid': aarecord['aac_zlib3_book']['file_aacid'],
                'deleted_comment': (aarecord['aac_zlib3_book'].get('deleted_comment') or 0),
                'cover_path': (aarecord['aac_zlib3_book'].get('cover_path') or ''),
                'storage': (aarecord['aac_zlib3_book'].get('storage') or ''),
            }
        if aarecord['ia_record'] is not None:
            aarecord['ia_record'] = {
                'ia_id': aarecord['ia_record']['ia_id'],
                # 'has_thumb': aarecord['ia_record']['has_thumb'],
                'aa_ia_file': {
                    'type': aarecord['ia_record']['aa_ia_file']['type'],
                    'filesize': aarecord['ia_record']['aa_ia_file']['filesize'],
                    'extension': aarecord['ia_record']['aa_ia_file']['extension'],
                    'ia_id': aarecord['ia_record']['aa_ia_file']['ia_id'],
                    'aacid': aarecord['ia_record']['aa_ia_file'].get('aacid'),
                    'data_folder': aarecord['ia_record']['aa_ia_file'].get('data_folder'),
                } if (aarecord['ia_record'].get('aa_ia_file') is not None) else None,
                'aa_ia_derived': {
                    'printdisabled_only': aarecord['ia_record']['aa_ia_derived']['printdisabled_only'],
                }
            }
        aarecord['ia_records_meta_only'] = aarecord.get('ia_records_meta_only') or []
        for index, item in enumerate(aarecord['ia_records_meta_only']):
            aarecord['ia_records_meta_only'][index] = {
                'ia_id': aarecord['ia_records_meta_only'][index]['ia_id'],
            }
        aarecord['isbndb'] = aarecord.get('isbndb') or []
        for index, item in enumerate(aarecord['isbndb']):
            aarecord['isbndb'][index] = {
                'isbn13': aarecord['isbndb'][index]['isbn13'],
            }
        aarecord['ol_book_dicts_primary_linked'] = aarecord.get('ol_book_dicts_primary_linked') or []
        for index, item in enumerate(aarecord['ol_book_dicts_primary_linked']):
            aarecord['ol_book_dicts_primary_linked'][index] = {
                'ol_edition': aarecord['ol_book_dicts_primary_linked'][index]['ol_edition'],
            }
        aarecord['ol'] = aarecord.get('ol') or []
        for index, item in enumerate(aarecord['ol']):
            aarecord['ol'][index] = {
                'ol_edition': aarecord['ol'][index]['ol_edition'],
            }
        aarecord['scihub_doi'] = aarecord.get('scihub_doi') or []
        for index, item in enumerate(aarecord['scihub_doi']):
            aarecord['scihub_doi'][index] = {
                'doi': aarecord['scihub_doi'][index]['doi'],
            }
        aarecord['oclc'] = aarecord.get('oclc') or []
        for index, item in enumerate(aarecord['oclc']):
            aarecord['oclc'][index] = {
                'oclc_id': aarecord['oclc'][index]['oclc_id'],
            }
        if aarecord['duxiu'] is not None:
            aarecord['duxiu'] = {
                'duxiu_ssid': aarecord['duxiu'].get('duxiu_ssid'),
                'cadal_ssno': aarecord['duxiu'].get('cadal_ssno'),
                'md5': aarecord['duxiu'].get('md5'),
                'duxiu_file': aarecord['duxiu'].get('duxiu_file'),
            }
            if aarecord['duxiu']['duxiu_ssid'] is None:
                del aarecord['duxiu']['duxiu_ssid']
            if aarecord['duxiu']['cadal_ssno'] is None:
                del aarecord['duxiu']['cadal_ssno']
        aarecord['duxius_nontransitive_meta_only'] = aarecord.get('duxius_nontransitive_meta_only') or []
        for index, item in enumerate(aarecord['duxius_nontransitive_meta_only']):
            aarecord['duxius_nontransitive_meta_only'][index] = {
                'duxiu_ssid': aarecord['duxius_nontransitive_meta_only'][index].get('duxiu_ssid'),
                'cadal_ssno': aarecord['duxius_nontransitive_meta_only'][index].get('cadal_ssno'),
                'md5': aarecord['duxius_nontransitive_meta_only'][index].get('md5'),
            }
        if aarecord.get('aac_upload') is not None:
            aarecord['aac_upload'] = {
                'md5': aarecord['aac_upload']['md5'],
                'files': aarecord['aac_upload']['files'],
            }
        if aarecord.get('aac_magzdb') is not None:
            aarecord['aac_magzdb'] = {
                'requested_value': aarecord['aac_magzdb']['requested_value'],
                'id': aarecord['aac_magzdb']['id'],
            }
        if aarecord.get('aac_nexusstc') is not None:
            aarecord['aac_nexusstc'] = {
                'requested_value': aarecord['aac_nexusstc']['requested_value'],
                'id': aarecord['aac_nexusstc']['id'],
                'aa_nexusstc_derived': {
                    'cid_only_links': aarecord['aac_nexusstc']['aa_nexusstc_derived']['cid_only_links'],
                },
            }
        if aarecord.get('aac_edsebk') is not None:
            aarecord['aac_edsebk'] = {
                'edsebk_id': aarecord['aac_edsebk']['edsebk_id'],
            }

        search_content_type = aarecord['file_unified_data']['content_type']
        # Once we have the content type.
        aarecord['indexes'] = [allthethings.utils.get_aarecord_search_index(aarecord_id_split[0], search_content_type)]

        # Even though `additional` is only for computing real-time stuff,
        # we'd like to cache some fields for in the search results.
        with force_locale('en'):
            additional = get_additional_for_aarecord(aarecord)
            aarecord['file_unified_data']['has_aa_downloads'] = additional['has_aa_downloads']
            aarecord['file_unified_data']['has_aa_exclusive_downloads'] = additional['has_aa_exclusive_downloads']
            aarecord['file_unified_data']['has_torrent_paths'] = (1 if (len(additional['torrent_paths']) > 0) else 0)
            aarecord['file_unified_data']['has_scidb'] = additional['has_scidb']
            for torrent_path in additional['torrent_paths']:
                allthethings.utils.add_classification_unified(aarecord['file_unified_data'], 'torrent', torrent_path['torrent_path'])
            for partner_url_path in additional['partner_url_paths']:
                allthethings.utils.add_identifier_unified(aarecord['file_unified_data'], 'server_path', partner_url_path['path'])

        record_sources = aarecord_sources(aarecord)
        for source_name in record_sources:
            allthethings.utils.add_classification_unified(aarecord['file_unified_data'], 'collection', source_name)

        REPLACE_PUNCTUATION = r'[.:_\-/\(\)\\]'
        initial_search_text = "\n".join([
            aarecord['file_unified_data']['title_best'][:2000],
            *[item[:2000] for item in aarecord['file_unified_data'].get('title_additional') or []],
            aarecord['file_unified_data']['author_best'][:2000],
            *[item[:2000] for item in aarecord['file_unified_data'].get('author_additional') or []],
            aarecord['file_unified_data']['edition_varia_best'][:2000],
            *[item[:2000] for item in aarecord['file_unified_data'].get('edition_varia_additional') or []],
            aarecord['file_unified_data']['publisher_best'][:2000],
            *[item[:2000] for item in aarecord['file_unified_data'].get('publisher_additional') or []],
            # Don't truncate filenames, the best is at the end and they're usually not so long.
            aarecord['file_unified_data']['original_filename_best'],
            *[item for item in aarecord['file_unified_data'].get('original_filename_additional') or []],
            aarecord_id,
            aarecord['file_unified_data']['extension_best'],
            *(aarecord['file_unified_data'].get('extension_additional') or []),
            # If we find REPLACE_PUNCTUATION in item, we need a separate standalone one in which punctionation is not replaced.
            # Otherwise we can rely on REPLACE_PUNCTUATION replacing the : and generating the standalone one.
            *[f"{key}:{item} {item}" if re.search(REPLACE_PUNCTUATION, item) else f"{key}:{item}" for key, items in aarecord['file_unified_data']['identifiers_unified'].items() for item in items],
            *[f"{key}:{item} {item}" if re.search(REPLACE_PUNCTUATION, item) else f"{key}:{item}" for key, items in aarecord['file_unified_data']['classifications_unified'].items() for item in items],
        ])
        # Duplicate search terms that contain punctuation, in *addition* to the original search terms (so precise matches still work).
        split_search_text = set(initial_search_text.split())
        normalized_search_terms = re.sub(REPLACE_PUNCTUATION, ' ', initial_search_text)
        filtered_normalized_search_terms = ' '.join([term for term in normalized_search_terms.split() if term not in split_search_text])
        search_text = f"{initial_search_text}\n\n{filtered_normalized_search_terms}"

        aarecord['search_only_fields'] = {
            'search_filesize': aarecord['file_unified_data']['filesize_best'],
            'search_year': aarecord['file_unified_data']['year_best'],
            'search_extension': aarecord['file_unified_data']['extension_best'],
            'search_content_type': search_content_type,
            'search_most_likely_language_code': aarecord['file_unified_data']['most_likely_language_codes'],
            'search_isbn13': (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []),
            'search_doi': (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []),
            'search_title': aarecord['file_unified_data']['title_best'],
            'search_author': aarecord['file_unified_data']['author_best'],
            'search_publisher': aarecord['file_unified_data']['publisher_best'],
            'search_edition_varia': aarecord['file_unified_data']['edition_varia_best'],
            'search_original_filename': aarecord['file_unified_data']['original_filename_best'],
            'search_added_date': aarecord['file_unified_data']['added_date_best'],
            'search_description_comments': ('\n'.join([aarecord['file_unified_data']['stripped_description_best']] + (aarecord['file_unified_data'].get('comments_multiple') or [])))[:10000],
            'search_text': search_text,
            'search_access_types': [
                *(['external_download'] if (not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0])) and any([((aarecord.get(field) is not None) and (type(aarecord[field]) is not list or len(aarecord[field]) > 0)) for field in ['lgrsnf_book', 'lgrsfic_book', 'lgli_file', 'zlib_book', 'aac_zlib3_book', 'scihub_doi', 'aac_magzdb', 'aac_nexusstc']]) else []),
                *(['external_borrow'] if (not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0])) and (aarecord.get('ia_record') and (not aarecord['ia_record']['aa_ia_derived']['printdisabled_only'])) else []),
                *(['external_borrow_printdisabled'] if (not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0])) and (aarecord.get('ia_record') and (aarecord['ia_record']['aa_ia_derived']['printdisabled_only'])) else []),
                *(['aa_download'] if (not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0])) and aarecord['file_unified_data']['has_aa_downloads'] == 1 else []),
                *(['aa_scidb'] if (not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0])) and aarecord['file_unified_data']['has_scidb'] == 1 else []),
                *(['torrents_available'] if (not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0])) and aarecord['file_unified_data']['has_torrent_paths'] == 1 else []),
                *(['meta_explore'] if allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0]) else []),
            ],
            'search_record_sources': record_sources,
            # Used in external system, check before changing.
            'search_bulk_torrents': 'has_bulk_torrents' if aarecord['file_unified_data']['has_torrent_paths'] else 'no_bulk_torrents',
        }

        if len(aarecord['search_only_fields']['search_record_sources']) == 0:
            raise Exception(f"Missing search_record_sources; phantom record? {aarecord=}")
        if len(aarecord['search_only_fields']['search_access_types']) == 0:
            raise Exception(f"Missing search_access_types; phantom record? {aarecord=}")
        
        # At the very end
        aarecord['search_only_fields']['search_score_base_rank'] = float(aarecord_score_base(aarecord))

    # When re-enabling this, consider:
    #   * Actual calculation of size of the cache and ES indexes.
    #   * Out-of-bounds batch processing to prevent accidental external calls.
    # embeddings = get_embeddings_for_aarecords(session, aarecords)
    # for aarecord in aarecords:
    #     if aarecord['id'] not in embeddings:
    #         continue
    #     embedding = embeddings[aarecord['id']]
    #     # ES limit https://github.com/langchain-ai/langchain/issues/10218#issuecomment-1706481539
    #     # We can simply cut the embedding for ES because of Matryoshka: https://openai.com/index/new-embedding-models-and-api-updates/
    #     aarecord['search_only_fields']['search_text_embedding_3_small_100_tokens_1024_dims'] = embedding['text_embedding_3_small_100_tokens'][0:1024]
    
    return aarecords

def get_md5_problem_type_mapping():
    return { 
        "lgrsnf_visible":         gettext("common.md5_problem_type_mapping.lgrsnf_visible"),
        "lgrsfic_visible":        gettext("common.md5_problem_type_mapping.lgrsfic_visible"),
        "lgli_visible":           gettext("common.md5_problem_type_mapping.lgli_visible"),
        "lgli_broken":            gettext("common.md5_problem_type_mapping.lgli_broken"),
        "zlib_missing":           gettext("common.md5_problem_type_mapping.zlib_missing"),
        "zlib_spam":              gettext("common.md5_problem_type_mapping.zlib_spam"),
        "zlib_bad_file":          gettext("common.md5_problem_type_mapping.zlib_bad_file"),
        "duxiu_pdg_broken_files": gettext("common.md5_problem_type_mapping.duxiu_pdg_broken_files"),
        "upload_exiftool_failed": gettext("common.md5_problem_type_mapping.upload_exiftool_failed"),
    }

def get_md5_content_type_mapping(display_lang):
    with force_locale(display_lang):
        return {
            "book_unknown":       "📗 " + gettext("common.md5_content_type_mapping.book_unknown"),
            "book_nonfiction":    "📘 " + gettext("common.md5_content_type_mapping.book_nonfiction"),
            "book_fiction":       "📕 " + gettext("common.md5_content_type_mapping.book_fiction"),
            "journal_article":    "📄 " + gettext("common.md5_content_type_mapping.journal_article"),
            "standards_document": "📝 " + gettext("common.md5_content_type_mapping.standards_document"),
            "magazine":           "📰 " + gettext("common.md5_content_type_mapping.magazine"),
            "book_comic":         "💬 " + gettext("common.md5_content_type_mapping.book_comic"),
            "musical_score":      "🎶 " + gettext("common.md5_content_type_mapping.musical_score"),
            "other":              "🤨 " + gettext("common.md5_content_type_mapping.other"),
        }

def get_access_types_mapping(display_lang):
    with force_locale(display_lang):
        return {
            "aa_download": gettext("common.access_types_mapping.aa_download"),
            "aa_scidb": "🧬 " + gettext("common.access_types_mapping.aa_scidb"),
            "external_download": gettext("common.access_types_mapping.external_download"),
            "external_borrow": gettext("common.access_types_mapping.external_borrow"),
            "external_borrow_printdisabled": gettext("common.access_types_mapping.external_borrow_printdisabled"),
            "meta_explore": gettext("common.access_types_mapping.meta_explore"),
            "torrents_available": gettext("common.access_types_mapping.torrents_available"),
        }

def get_record_sources_mapping(display_lang):
    with force_locale(display_lang):
        return {
            "lgrs": gettext("common.record_sources_mapping.lgrs"),
            "lgli": gettext("common.record_sources_mapping.lgli"),
            "zlib": gettext("common.record_sources_mapping.zlib"),
            "zlibzh": gettext("common.record_sources_mapping.zlibzh"),
            "ia": gettext("common.record_sources_mapping.ia"),
            "isbndb": gettext("common.record_sources_mapping.isbndb"),
            "ol": gettext("common.record_sources_mapping.ol"),
            "scihub": gettext("common.record_sources_mapping.scihub"),
            "oclc": gettext("common.record_sources_mapping.oclc"),
            "duxiu": gettext("common.record_sources_mapping.duxiu"),
            "upload": gettext("common.record_sources_mapping.uploads"),
            "magzdb": gettext("common.record_sources_mapping.magzdb"),
            "nexusstc": gettext("common.record_soruces_mapping.nexusstc"),
            "edsebk": "EBSCOhost", # TODO:TRANSLATE
        }

def get_specific_search_fields_mapping(display_lang):
    with force_locale(display_lang):
        return {
            'title': gettext('common.specific_search_fields.title'),
            'author': gettext('common.specific_search_fields.author'),
            'publisher': gettext('common.specific_search_fields.publisher'),
            'edition_varia': gettext('common.specific_search_fields.edition_varia'),
            'year': gettext('common.specific_search_fields.year'),
            'original_filename': gettext('common.specific_search_fields.original_filename'),
            'description_comments': gettext('common.specific_search_fields.description_comments'),
        }

def format_filesize(num):
    if num < 100000:
        return "0.1MB"
    elif num < 1000000:
        return f"{num/1000000:3.1f}MB"
    else:
        for unit in ["", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"]:
            if abs(num) < 1000.0:
                return f"{num:3.1f}{unit}"
            num /= 1000.0
        return f"{num:.1f}YB"

def add_partner_servers(path, modifier, aarecord, additional):
    additional['has_aa_downloads'] = 1
    targeted_seconds = 200
    if modifier == 'aa_exclusive':
        targeted_seconds = 300
        additional['has_aa_exclusive_downloads'] = 1
    if modifier == 'scimag':
        targeted_seconds = 10
    # When changing the domains, don't forget to change md5_fast_download and md5_slow_download.
    for index in range(len(allthethings.utils.FAST_DOWNLOAD_DOMAINS)):
        additional['fast_partner_urls'].append((gettext("common.md5.servers.fast_partner", number=len(additional['fast_partner_urls'])+1), '/fast_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(index), gettext("common.md5.servers.no_browser_verification_or_waitlists") if len(additional['fast_partner_urls']) == 0 else ''))
    for index in range(len(allthethings.utils.SLOW_DOWNLOAD_DOMAINS)):
        if allthethings.utils.SLOW_DOWNLOAD_DOMAINS_SLIGHTLY_FASTER[index]:
            additional['slow_partner_urls'].append((gettext("common.md5.servers.slow_partner", number=len(additional['slow_partner_urls'])+1), '/slow_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(index), gettext("common.md5.servers.faster_with_waitlist")))
        else:
            additional['slow_partner_urls'].append((gettext("common.md5.servers.slow_partner", number=len(additional['slow_partner_urls'])+1), '/slow_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(index), gettext("common.md5.servers.slow_no_waitlist")))
    additional['partner_url_paths'].append({ 'path': path, 'targeted_seconds': targeted_seconds })

def max_length_with_word_boundary(sentence, max_len):
    str_split = sentence.split(' ')
    output_index = 0
    output_total = 0
    for item in str_split:
        item = item.strip()
        len_item = len(item)+1 # Also count a trailing space
        if output_total+len_item-1 > max_len: # But don't count the very last trailing space here
            break
        output_index += 1
        output_total += len_item
    if output_index == 0:
        return sentence[0:max_len].strip()
    else:
        return ' '.join(str_split[0:output_index]).strip()

def get_additional_for_aarecord(aarecord):
    aarecord_id_split = aarecord['id'].split(':', 1)

    additional = {}
    additional['path'] = allthethings.utils.path_for_aarecord_id(aarecord['id'])

    # TODO: remove backwards compatibility
    most_likely_language_codes = aarecord['file_unified_data'].get('most_likely_language_codes', None) or []
    if len(most_likely_language_codes) == 0:
        most_likely_language_code_backwardscompatibility = aarecord['file_unified_data'].get('most_likely_language_code', None) or ''
        if len(most_likely_language_code_backwardscompatibility) > 0:
            most_likely_language_codes = [most_likely_language_code_backwardscompatibility]

    additional['most_likely_language_names'] = [get_display_name_for_lang(lang_code, allthethings.utils.get_base_lang_code(get_locale())) for lang_code in most_likely_language_codes]

    additional['codes'] = []
    for key, values in aarecord['file_unified_data'].get('identifiers_unified', {}).items():
        for value in values:
            additional['codes'].append(allthethings.utils.make_code_for_display(key, value))
    for key, values in aarecord['file_unified_data'].get('classifications_unified', {}).items():
        for value in values:
            additional['codes'].append(allthethings.utils.make_code_for_display(key, value))
    # CODES_PRIORITY = ['isbn13', 'isbn10', 'csbn', 'doi', 'issn', 'udc', 'oclc', 'ol', 'ocaid', 'asin', 'duxiu_ssid', 'cadal_ssno', 'lang', 'year', 'md5']
    # additional['codes'].sort(key=lambda item: (CODES_PRIORITY.index(item['key']) if item['key'] in CODES_PRIORITY else 100, item['key']))
    additional['codes'].sort(key=lambda item: item['key'])

    md5_content_type_mapping = get_md5_content_type_mapping(allthethings.utils.get_base_lang_code(get_locale()))

    cover_url = (aarecord['file_unified_data'].get('cover_url_best', None) or '')
    zlib3_cover_path = ((aarecord.get('aac_zlib3_book') or {}).get('cover_path') or '')
    if '/collections/' in zlib3_cover_path:
        cover_url = f"https://s3proxy.cdn-zlib.se/{zlib3_cover_path}"
    elif 'zlib' in cover_url or '1lib' in cover_url: # Remove old zlib cover_urls.
        non_zlib_covers = [url for url in (aarecord['file_unified_data'].get('cover_url_additional', None) or []) if ('zlib' not in url and '1lib' not in url)]
        if len(non_zlib_covers) > 0:
            cover_url = non_zlib_covers[0]
        else:
            cover_url = ""

    additional['top_box'] = {
        'meta_information': [item for item in [
                aarecord['file_unified_data'].get('title_best') or '',
                aarecord['file_unified_data'].get('author_best') or '',
                (aarecord['file_unified_data'].get('stripped_description_best') or '')[0:100],
                aarecord['file_unified_data'].get('publisher_best') or '',
                aarecord['file_unified_data'].get('edition_varia_best') or '',
                aarecord['file_unified_data'].get('original_filename_best') or '',
            ] if item != ''],
        'cover_missing_hue_deg': int(hashlib.md5(aarecord['id'].encode()).hexdigest(), 16) % 360,
        'cover_url': cover_url,
        'top_row': ("✅ " if len(aarecord.get('ol_book_dicts_primary_linked') or []) > 0 else "") + ", ".join([item for item in [
                *additional['most_likely_language_names'][0:3],
                f".{aarecord['file_unified_data']['extension_best']}" if len(aarecord['file_unified_data']['extension_best']) > 0 else '',
                "/".join(filter(len,[
                    "🧬" if (aarecord['file_unified_data'].get('has_scidb') == 1) else "", 
                    "🚀" if (aarecord['file_unified_data'].get('has_aa_downloads') == 1) else "", 
                    *aarecord_sources(aarecord)
                ])),
                format_filesize(aarecord['file_unified_data'].get('filesize_best') or 0) if aarecord['file_unified_data'].get('filesize_best') else '',
                md5_content_type_mapping[aarecord['file_unified_data']['content_type']],
                (aarecord['file_unified_data'].get('original_filename_best') or ''),
                aarecord_id_split[1] if aarecord_id_split[0] in ['ia', 'ol'] else '',
                f"ISBNdb {aarecord_id_split[1]}" if aarecord_id_split[0] == 'isbn' else '',
                f"OCLC {aarecord_id_split[1]}" if aarecord_id_split[0] == 'oclc' else '',
                f"DuXiu SSID {aarecord_id_split[1]}" if aarecord_id_split[0] == 'duxiu_ssid' else '',
                f"CADAL SSNO {aarecord_id_split[1]}" if aarecord_id_split[0] == 'cadal_ssno' else '',
            ] if item != '']),
        'title': aarecord['file_unified_data'].get('title_best') or aarecord['file_unified_data'].get('original_filename_best_name_only') or '',
        'publisher_and_edition': ", ".join([item for item in [
                aarecord['file_unified_data'].get('publisher_best') or '',
                aarecord['file_unified_data'].get('edition_varia_best') or '',
            ] if item != '']),
        'author': aarecord['file_unified_data'].get('author_best') or '',
        'freeform_fields': [item for item in [
            (gettext('page.md5.box.descr_title'), strip_description(aarecord['file_unified_data'].get('stripped_description_best') or '')),
            *[(gettext('page.md5.box.metadata_comments_title'), strip_description(comment)) for comment in (aarecord['file_unified_data'].get('comments_multiple') or [])],
            *[(gettext('page.md5.box.alternative_title'), row) for row in (aarecord['file_unified_data'].get('title_additional') or '')],
            *[(gettext('page.md5.box.alternative_author'), row) for row in (aarecord['file_unified_data'].get('author_additional') or '')],
            *[(gettext('page.md5.box.alternative_publisher'), row) for row in (aarecord['file_unified_data'].get('publisher_additional') or '')],
            *[(gettext('page.md5.box.alternative_edition'), row) for row in (aarecord['file_unified_data'].get('edition_varia_additional') or '')],
            *[(gettext('page.md5.box.alternative_description'), row) for row in (aarecord['file_unified_data'].get('stripped_description_additional') or '')],
            *[(gettext('page.md5.box.alternative_filename'), row) for row in (aarecord['file_unified_data'].get('original_filename_additional') or '')],
            *[(gettext('page.md5.box.alternative_extension'), row) for row in (aarecord['file_unified_data'].get('extension_additional') or '')],
            (gettext('page.md5.box.date_open_sourced_title'), aarecord['file_unified_data'].get('added_date_best') or ''),
        ] if item[1] != ''],
    }

    filename_info = [item for item in [
            max_length_with_word_boundary(aarecord['file_unified_data'].get('title_best') or aarecord['file_unified_data'].get('original_filename_best_name_only') or '', 60),
            max_length_with_word_boundary(aarecord['file_unified_data'].get('author_best') or '', 60),
            max_length_with_word_boundary(aarecord['file_unified_data'].get('edition_varia_best') or '', 60),
            max_length_with_word_boundary(aarecord['file_unified_data'].get('publisher_best') or '', 60),
        ] if item != '']
    filename_slug = max_length_with_word_boundary(" -- ".join(filename_info), 150)
    if filename_slug.endswith(' --'):
        filename_slug = filename_slug[0:-len(' --')]
    filename_extension = aarecord['file_unified_data'].get('extension_best', None) or ''    
    filename_code = ''
    for code in additional['codes']:
        if code['key'] in ['isbn13', 'isbn10', 'doi', 'issn', 'duxiu_ssid', 'cadal_ssno']:
            filename_code = f" -- {code['value']}"
            break
    filename_base = f"{filename_slug}{filename_code} -- {aarecord['id'].split(':', 1)[1]}".replace('.', '_')
    additional['filename_without_annas_archive'] = urllib.parse.quote(f"{filename_base}.{filename_extension}", safe='')
    additional['filename'] = urllib.parse.quote(f"{filename_base} -- Anna’s Archive.{filename_extension}", safe='')

    additional['download_urls'] = []
    additional['fast_partner_urls'] = []
    additional['slow_partner_urls'] = []
    additional['partner_url_paths'] = []
    additional['has_aa_downloads'] = 0
    additional['has_aa_exclusive_downloads'] = 0
    additional['torrent_paths'] = []
    additional['ipfs_urls'] = []
    shown_click_get = False
    linked_dois = set()

    torrents_json_aa_currently_seeding_by_torrent_path = allthethings.utils.get_torrents_json_aa_currently_seeding_by_torrent_path()

    _temporarily_unavailable = gettext('page.md5.box.download.temporarily_unavailable') # Keeping translation

    for scihub_doi in aarecord.get('scihub_doi') or []:
        doi = scihub_doi['doi']
        additional['download_urls'].append((gettext('page.md5.box.download.scihub', doi=doi), f"https://sci-hub.ru/{doi}", ""))
        linked_dois.add(doi)
    if (aarecord.get('ia_record') is not None) and (aarecord['ia_record'].get('aa_ia_file') is not None):
        ia_id = aarecord['ia_record']['aa_ia_file']['ia_id']
        extension = aarecord['ia_record']['aa_ia_file']['extension']
        ia_file_type = aarecord['ia_record']['aa_ia_file']['type']
        if ia_file_type == 'acsm':
            directory = 'other'
            if bool(re.match(r"^[a-z]", ia_id)):
                directory = ia_id[0]
            partner_path = f"u/ia/annas-archive-ia-2023-06-acsm/{directory}/{ia_id}.{extension}"
            additional['torrent_paths'].append({ "collection": "ia", "torrent_path": f"managed_by_aa/ia/annas-archive-ia-acsm-{directory}.tar.torrent", "file_level1": f"annas-archive-ia-acsm-{directory}.tar", "file_level2": f"{ia_id}.{extension}" })
        elif ia_file_type == 'lcpdf':
            directory = 'other'
            if ia_id.startswith('per_c'):
                directory = 'per_c'
            elif ia_id.startswith('per_w'):
                directory = 'per_w'
            elif ia_id.startswith('per_'):
                directory = 'per_'
            elif bool(re.match(r"^[a-z]", ia_id)):
                directory = ia_id[0]
            partner_path = f"u/ia/annas-archive-ia-2023-06-lcpdf/{directory}/{ia_id}.{extension}"
            additional['torrent_paths'].append({ "collection": "ia", "torrent_path": f"managed_by_aa/ia/annas-archive-ia-lcpdf-{directory}.tar.torrent", "file_level1": f"annas-archive-ia-lcpdf-{directory}.tar", "file_level2": f"{ia_id}.{extension}" })
        elif ia_file_type == 'ia2_acsmpdf':
            server = 'i'
            date = aarecord['ia_record']['aa_ia_file']['data_folder'].split('__')[3][0:8]
            datetime = aarecord['ia_record']['aa_ia_file']['data_folder'].split('__')[3][0:16]
            if date in ['20240701', '20240702']:
                server = 'o'
            elif date in ['20240823', '20240824']:
                server = 'z'
                if datetime in ['20240823T234037Z', '20240823T234109Z', '20240823T234117Z', '20240823T234126Z', '20240823T234134Z', '20240823T234143Z', '20240823T234153Z', '20240823T234203Z', '20240823T234214Z', '20240823T234515Z', '20240823T234534Z', '20240823T234555Z', '20240823T234615Z', '20240823T234637Z', '20240823T234658Z', '20240823T234720Z']:
                    server = 'i'
                elif datetime in ['20240823T234225Z', '20240823T234238Z', '20240823T234250Z', '20240823T234304Z', '20240823T234318Z', '20240823T234333Z', '20240823T234348Z', '20240823T234404Z', '20240823T234805Z', '20240823T234421Z', '20240823T234438Z']:
                    server = 'w'
            partner_path = make_temp_anon_aac_path(f"{server}/ia2_acsmpdf_files", aarecord['ia_record']['aa_ia_file']['aacid'], aarecord['ia_record']['aa_ia_file']['data_folder'])
            additional['torrent_paths'].append({ "collection": "ia", "torrent_path": f"managed_by_aa/annas_archive_data__aacid/{aarecord['ia_record']['aa_ia_file']['data_folder']}.torrent", "file_level1": aarecord['ia_record']['aa_ia_file']['aacid'], "file_level2": "" })
        else:
            raise Exception(f"Unknown ia_record file type: {ia_file_type}")
        add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
    if (aarecord.get('duxiu') is not None) and (aarecord['duxiu'].get('duxiu_file') is not None):
        data_folder = aarecord['duxiu']['duxiu_file']['data_folder']
        additional['torrent_paths'].append({ "collection": "duxiu", "torrent_path": f"managed_by_aa/annas_archive_data__aacid/{data_folder}.torrent", "file_level1": aarecord['duxiu']['duxiu_file']['aacid'], "file_level2": "" })
        server = None
        if data_folder >= 'annas_archive_data__aacid__duxiu_files__20240613T170516Z--20240613T170517Z' and data_folder <= 'annas_archive_data__aacid__duxiu_files__20240613T171624Z--20240613T171625Z':
            server = 'w'
        elif data_folder >= 'annas_archive_data__aacid__duxiu_files__20240613T171757Z--20240613T171758Z' and data_folder <= 'annas_archive_data__aacid__duxiu_files__20240613T190311Z--20240613T190312Z':
            server = 'v'
        elif data_folder >= 'annas_archive_data__aacid__duxiu_files__20240613T190428Z--20240613T190429Z' and data_folder <= 'annas_archive_data__aacid__duxiu_files__20240613T204954Z--20240613T204955Z':
            server = 'w'
        elif data_folder >= 'annas_archive_data__aacid__duxiu_files__20240613T205835Z--20240613T205836Z' and data_folder <= 'annas_archive_data__aacid__duxiu_files__20240613T223234Z--20240613T223235Z':
            server = 'w'
        else:
            if AACID_SMALL_DATA_IMPORTS:
                server = 'w'
            else:
                raise Exception(f"Warning: Unknown duxiu range: {data_folder=}")
        partner_path = make_temp_anon_aac_path(f"{server}/duxiu_files", aarecord['duxiu']['duxiu_file']['aacid'], data_folder)
        add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
    if (aarecord.get('aac_upload') is not None) and (len(aarecord['aac_upload']['files']) > 0):
        for aac_upload_file in aarecord['aac_upload']['files']:
            additional['torrent_paths'].append({ "collection": "upload", "torrent_path": f"managed_by_aa/annas_archive_data__aacid/{aac_upload_file['data_folder']}.torrent", "file_level1": aac_upload_file['aacid'], "file_level2": "" })
            server = 'v'
            if 'upload_files_misc__20240510' in aac_upload_file['data_folder']:
                server = 'w'
            data_folder_split = aac_upload_file['data_folder'].split('__')
            directory = f"{data_folder_split[2]}_{data_folder_split[3][0:8]}" # Different than make_temp_anon_aac_path!
            partner_path = f"{server}/upload_files/{directory}/{aac_upload_file['data_folder']}/{aac_upload_file['aacid']}"
            add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
    if aarecord.get('lgrsnf_book') is not None:
        lgrsnf_thousands_dir = (aarecord['lgrsnf_book']['id'] // 1000) * 1000
        lgrsnf_torrent_path = f"external/libgen_rs_non_fic/r_{lgrsnf_thousands_dir:03}.torrent"
        lgrsnf_manually_synced = (lgrsnf_thousands_dir <= 4371000)
        lgrsnf_filename = aarecord['lgrsnf_book']['md5'].lower()
        if lgrsnf_manually_synced or (lgrsnf_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path):
            additional['torrent_paths'].append({ "collection": "libgen_rs_non_fic", "torrent_path": lgrsnf_torrent_path, "file_level1": lgrsnf_filename, "file_level2": "" })
        if lgrsnf_manually_synced or ((lgrsnf_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path) and (torrents_json_aa_currently_seeding_by_torrent_path[lgrsnf_torrent_path])):
            lgrsnf_path = f"e/lgrsnf/{lgrsnf_thousands_dir}/{lgrsnf_filename}"
            add_partner_servers(lgrsnf_path, '', aarecord, additional)

        additional['download_urls'].append((gettext('page.md5.box.download.lgrsnf'), f"http://library.lol/main/{aarecord['lgrsnf_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if aarecord.get('lgrsfic_book') is not None:
        lgrsfic_thousands_dir = (aarecord['lgrsfic_book']['id'] // 1000) * 1000
        lgrsfic_torrent_path = f"external/libgen_rs_fic/f_{lgrsfic_thousands_dir}.torrent" # Note: no leading zeroes
        lgrsfic_manually_synced = (lgrsfic_thousands_dir <= 3026000)
        lgrsfic_filename = f"{aarecord['lgrsfic_book']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
        if lgrsfic_manually_synced or (lgrsfic_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path):
            additional['torrent_paths'].append({ "collection": "libgen_rs_fic", "torrent_path": lgrsfic_torrent_path, "file_level1": lgrsfic_filename, "file_level2": "" })
        if lgrsfic_manually_synced or ((lgrsfic_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path) and (torrents_json_aa_currently_seeding_by_torrent_path[lgrsfic_torrent_path])):
            lgrsfic_path = f"e/lgrsfic/{lgrsfic_thousands_dir}/{lgrsfic_filename}"
            add_partner_servers(lgrsfic_path, '', aarecord, additional)

        additional['download_urls'].append((gettext('page.md5.box.download.lgrsfic'), f"http://library.lol/fiction/{aarecord['lgrsfic_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if aarecord.get('lgli_file') is not None:
        lglific_id = aarecord['lgli_file']['fiction_id']
        if lglific_id > 0:
            lglific_thousands_dir = (lglific_id // 1000) * 1000
            lglific_filename = f"{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
            # Don't use torrents_json for this, because we have more files that don't get
            # torrented, because they overlap with our Z-Library torrents.
            # TODO: Verify overlap, and potentially add more torrents for what's missing?
            if lglific_thousands_dir >= 2201000 and lglific_thousands_dir <= 4259000:
                lglific_path = f"e/lglific/{lglific_thousands_dir}/{lglific_filename}"
                add_partner_servers(lglific_path, '', aarecord, additional)

            lglific_torrent_path = f"external/libgen_li_fic/f_{lglific_thousands_dir}.torrent" # Note: no leading zeroes
            if lglific_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path:
                additional['torrent_paths'].append({ "collection": "libgen_li_fic", "torrent_path": lglific_torrent_path, "file_level1": lglific_filename, "file_level2": "" })

        scimag_id = aarecord['lgli_file']['scimag_id']
        if scimag_id > 0 and scimag_id <= 87599999: # 87637042 seems the max now in the libgenli db
            scimag_hundredthousand_dir = (scimag_id // 100000)
            scimag_thousand_dir = (scimag_id // 1000)
            scimag_filename = urllib.parse.quote(aarecord['lgli_file']['scimag_archive_path'].replace('\\', '/'))
            
            scimag_torrent_path = f"external/scihub/sm_{scimag_hundredthousand_dir:03}00000-{scimag_hundredthousand_dir:03}99999.torrent"
            additional['torrent_paths'].append({ "collection": "scihub", "torrent_path": scimag_torrent_path, "file_level1": f"libgen.scimag{scimag_thousand_dir:05}000-{scimag_thousand_dir:05}999.zip", "file_level2": scimag_filename })

            scimag_path = f"i/scimag/{scimag_hundredthousand_dir:03}00000/{scimag_thousand_dir:05}000/{scimag_filename}"
            add_partner_servers(scimag_path, 'scimag', aarecord, additional)

        lglicomics_id = aarecord['lgli_file']['comics_id']
        if lglicomics_id > 0 and lglicomics_id < 2566000:
            lglicomics_thousands_dir = (lglicomics_id // 1000) * 1000
            lglicomics_filename = f"{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
            lglicomics_path = f"a/comics/{lglicomics_thousands_dir}/{lglicomics_filename}"
            add_partner_servers(lglicomics_path, '', aarecord, additional)
            additional['torrent_paths'].append({ "collection": "libgen_li_comics", "torrent_path": f"external/libgen_li_comics/c_{lglicomics_thousands_dir}.torrent", "file_level1": lglicomics_filename, "file_level2": "" }) # Note: no leading zero

        lglimagz_id = aarecord['lgli_file']['magz_id']
        if lglimagz_id > 0 and lglimagz_id < 1363000:
            lglimagz_thousands_dir = (lglimagz_id // 1000) * 1000
            lglimagz_filename = f"{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
            lglimagz_path = f"y/magz/{lglimagz_thousands_dir}/{lglimagz_filename}"
            add_partner_servers(lglimagz_path, '', aarecord, additional)
            if lglimagz_id < 1000000:
                additional['torrent_paths'].append({ "collection": "libgen_li_magazines", "torrent_path": f"external/libgen_li_magazines/m_{lglimagz_thousands_dir}.torrent", "file_level1": lglimagz_filename, "file_level2": "" }) # Note: no leading zero

        additional['download_urls'].append((gettext('page.md5.box.download.lgli'), f"http://libgen.li/ads.php?md5={aarecord['lgli_file']['md5'].lower()}", (gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')) + ' <div style="margin-left: 24px" class="text-sm text-gray-500">' + gettext('page.md5.box.download.libgen_ads') + '</div>'))
        shown_click_get = True
    if (len(aarecord.get('ipfs_infos') or []) > 0) and (aarecord_id_split[0] in ['md5', 'nexusstc_download']):
        # additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=1), f"https://ipfs.eth.aragon.network/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}", gettext('page.md5.box.download.ipfs_gateway_extra')))

        for ipfs_info in aarecord['ipfs_infos']:
            additional['ipfs_urls'].append({ "name": "w3s.link", "url": f"https://w3s.link/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "cf-ipfs.com", "url": f"https://cf-ipfs.com/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "ipfs.eth.aragon.network", "url": f"https://ipfs.eth.aragon.network/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "zerolend.myfilebase.com", "url": f"https://zerolend.myfilebase.com/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "ccgateway.infura-ipfs.io", "url": f"https://ccgateway.infura-ipfs.io/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "knownorigin.mypinata.cloud", "url": f"https://knownorigin.mypinata.cloud/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "storry.tv", "url": f"https://storry.tv/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "ipfs-stg.fleek.co", "url": f"https://ipfs-stg.fleek.co/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "cloudflare-ipfs.com", "url": f"https://cloudflare-ipfs.com/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "ipfs.io", "url": f"https://ipfs.io/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "snapshot.4everland.link", "url": f"https://snapshot.4everland.link/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "gateway.pinata.cloud", "url": f"https://gateway.pinata.cloud/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "dweb.link", "url": f"https://dweb.link/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "gw3.io", "url": f"https://gw3.io/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "public.w3ipfs.aioz.network", "url": f"https://public.w3ipfs.aioz.network/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "ipfsgw.com", "url": f"https://ipfsgw.com/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "magic.decentralized-content.com", "url": f"https://magic.decentralized-content.com/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "ipfs.raribleuserdata.com", "url": f"https://ipfs.raribleuserdata.com/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "www.gstop-content.com", "url": f"https://www.gstop-content.com/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })
            additional['ipfs_urls'].append({ "name": "atomichub-ipfs.com", "url": f"https://atomichub-ipfs.com/ipfs/{ipfs_info['ipfs_cid']}?filename={additional['filename_without_annas_archive']}", "from": ipfs_info['from'] })

        additional['download_urls'].append(("IPFS", f"/ipfs_downloads/{aarecord['id']}", ""))
    
    if aarecord.get('zlib_book') is not None and len(aarecord['zlib_book']['pilimi_torrent'] or '') > 0:
        zlib_path = make_temp_anon_zlib_path(aarecord['zlib_book']['zlibrary_id'], aarecord['zlib_book']['pilimi_torrent'])
        add_partner_servers(zlib_path, 'aa_exclusive' if (len(additional['fast_partner_urls']) == 0) else '', aarecord, additional)
        if "-zlib2-" in aarecord['zlib_book']['pilimi_torrent']:
            additional['torrent_paths'].append({ "collection": "zlib", "torrent_path": f"managed_by_aa/zlib/{aarecord['zlib_book']['pilimi_torrent']}", "file_level1": aarecord['zlib_book']['pilimi_torrent'].replace('.torrent', '.tar'), "file_level2": str(aarecord['zlib_book']['zlibrary_id']) })
        else:
            additional['torrent_paths'].append({ "collection": "zlib", "torrent_path": f"managed_by_aa/zlib/{aarecord['zlib_book']['pilimi_torrent']}", "file_level1": str(aarecord['zlib_book']['zlibrary_id']), "file_level2": "" })
    
    if (aarecord.get('aac_zlib3_book') is not None) and (aarecord['aac_zlib3_book']['file_aacid'] is not None):
        server = 'u'
        date = aarecord['aac_zlib3_book']['file_data_folder'].split('__')[3][0:8]
        if date in ['20240807', '20240823']:
            server = 'o'
        zlib_path = make_temp_anon_aac_path(f"{server}/zlib3_files", aarecord['aac_zlib3_book']['file_aacid'], aarecord['aac_zlib3_book']['file_data_folder'])
        add_partner_servers(zlib_path, 'aa_exclusive' if (len(additional['fast_partner_urls']) == 0) else '', aarecord, additional)
        additional['torrent_paths'].append({ "collection": "zlib", "torrent_path": f"managed_by_aa/annas_archive_data__aacid/{aarecord['aac_zlib3_book']['file_data_folder']}.torrent", "file_level1": aarecord['aac_zlib3_book']['file_aacid'], "file_level2": "" })
    
    if aarecord.get('aac_zlib3_book') is not None:
        additional['download_urls'].append((gettext('page.md5.box.download.zlib'), f"https://z-lib.gs/md5/{aarecord['aac_zlib3_book']['md5_reported'].lower()}", ""))
        additional['download_urls'].append((gettext('page.md5.box.download.zlib_tor'), f"http://bookszlibb74ugqojhzhg2a63w5i2atv5bqarulgczawnbmsb6s6qead.onion/md5/{aarecord['aac_zlib3_book']['md5_reported'].lower()}", gettext('page.md5.box.download.zlib_tor_extra')))
    
    if (aarecord.get('zlib_book') is not None) and (aarecord.get('aac_zlib3_book') is None):
        additional['download_urls'].append((gettext('page.md5.box.download.zlib'), f"https://z-lib.gs/md5/{aarecord['zlib_book']['md5_reported'].lower()}", ""))
        additional['download_urls'].append((gettext('page.md5.box.download.zlib_tor'), f"http://bookszlibb74ugqojhzhg2a63w5i2atv5bqarulgczawnbmsb6s6qead.onion/md5/{aarecord['zlib_book']['md5_reported'].lower()}", gettext('page.md5.box.download.zlib_tor_extra')))
    
    if aarecord.get('aac_magzdb') is not None:
        additional['download_urls'].append((gettext('page.md5.box.download.magzdb'), f"http://magzdb.org/num/{aarecord['aac_magzdb']['id']}", ""))
    
    if aarecord.get('aac_nexusstc') is not None:
        additional['download_urls'].append((gettext('page.md5.box.download.nexusstc'), f"https://libstc.cc/#/stc/nid:{aarecord['aac_nexusstc']['id']}", ""))

    if aarecord.get('aac_edsebk') is not None:
        # TODO:TRANSLATE
        additional['download_urls'].append(("EBSCOhost", f"https://library.macewan.ca/full-record/edsebk/{aarecord['aac_edsebk']['edsebk_id']}", ""))

    if aarecord.get('ia_record') is not None:
        ia_id = aarecord['ia_record']['ia_id']
        printdisabled_only = aarecord['ia_record']['aa_ia_derived']['printdisabled_only']
        additional['download_urls'].append((gettext('page.md5.box.download.ia_borrow'), f"https://archive.org/details/{ia_id}", gettext('page.md5.box.download.print_disabled_only') if printdisabled_only else ''))
    
    for doi in (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []):
        if doi not in linked_dois:
            additional['download_urls'].append((gettext('page.md5.box.download.scihub', doi=doi), f"https://sci-hub.ru/{doi}", gettext('page.md5.box.download.scihub_maybe')))
    
    for manualslib_id in (aarecord['file_unified_data']['identifiers_unified'].get('manualslib') or []):
        additional['download_urls'].append((gettext('page.md5.box.download.manualslib'), f"https://www.manualslib.com/manual/{manualslib_id}/manual.html", ""))

    for pmid in (aarecord['file_unified_data']['identifiers_unified'].get('pmid') or []):
        additional['download_urls'].append((gettext('page.md5.box.download.pubmed'), f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/", ""))
    
    if aarecord_id_split[0] == 'md5':
        for torrent_path in additional['torrent_paths']:
            # path = "/torrents"
            # group = torrent_group_data_from_file_path(f"torrents/{torrent_path}")['group']
            # path += f"#{group}"
            collection_text = gettext("page.md5.box.download.collection") # Separate line
            torrent_text = gettext("page.md5.box.download.torrent") # Separate line
            files_html = f'{collection_text} <a href="/torrents#{torrent_path["collection"]}">“{torrent_path["collection"]}”</a> → {torrent_text} <a href="/dyn/small_file/torrents/{torrent_path["torrent_path"]}">“{torrent_path["torrent_path"].rsplit("/", 1)[-1]}”</a>'
            if len(torrent_path['file_level1']) > 0:
                files_html += f" →&nbsp;file&nbsp;“{torrent_path['file_level1']}”"
            if len(torrent_path['file_level2']) > 0:
                files_html += f"&nbsp;(extract) →&nbsp;file&nbsp;“{torrent_path['file_level2']}”"
            additional['download_urls'].append((gettext('page.md5.box.download.bulk_torrents'), f"/torrents#{torrent_path['collection']}", gettext('page.md5.box.download.experts_only') + f' <div style="margin-left: 24px" class="text-sm text-gray-500">{files_html}</em></div>'))
        if len(additional['torrent_paths']) == 0:
            if additional['has_aa_downloads'] == 0:
                additional['download_urls'].append(("", "", 'Bulk torrents not yet available for this file. If you have this file, help out by <a href="/faq#upload">uploading</a>.'))
            else:
                additional['download_urls'].append(("", "", 'Bulk torrents not yet available for this file.'))
    if aarecord_id_split[0] == 'isbn':
        additional['download_urls'].append((gettext('page.md5.box.download.aa_isbn'), f'/search?q="isbn13:{aarecord_id_split[1]}"', ""))
        additional['download_urls'].append((gettext('page.md5.box.download.other_isbn'), f"https://en.wikipedia.org/wiki/Special:BookSources?isbn={aarecord_id_split[1]}", ""))
        if len(aarecord.get('isbndb') or []) > 0:
            additional['download_urls'].append((gettext('page.md5.box.download.original_isbndb'), f"https://isbndb.com/book/{aarecord_id_split[1]}", ""))
    if aarecord_id_split[0] == 'ol':
        additional['download_urls'].append((gettext('page.md5.box.download.aa_openlib'), f'/search?q="ol:{aarecord_id_split[1]}"', ""))
        if len(aarecord.get('ol') or []) > 0:
            additional['download_urls'].append((gettext('page.md5.box.download.original_openlib'), f"https://openlibrary.org/books/{aarecord_id_split[1]}", ""))
    if aarecord_id_split[0] == 'oclc':
        additional['download_urls'].append((gettext('page.md5.box.download.aa_oclc'), f'/search?q="oclc:{aarecord_id_split[1]}"', ""))
        additional['download_urls'].append((gettext('page.md5.box.download.original_oclc'), f"https://worldcat.org/title/{aarecord_id_split[1]}", ""))
    if aarecord_id_split[0] == 'duxiu_ssid':
        additional['download_urls'].append((gettext('page.md5.box.download.aa_duxiu'), f'/search?q="duxiu_ssid:{aarecord_id_split[1]}"', ""))
        additional['download_urls'].append((gettext('page.md5.box.download.original_duxiu'), 'https://www.duxiu.com/bottom/about.html', ""))
    if aarecord_id_split[0] == 'cadal_ssno':
        additional['download_urls'].append((gettext('page.md5.box.download.aa_cadal'), f'/search?q="cadal_ssno:{aarecord_id_split[1]}"', ""))
        additional['download_urls'].append((gettext('page.md5.box.download.original_cadal'), f'https://cadal.edu.cn/cardpage/bookCardPage?ssno={aarecord_id_split[1]}', ""))
    if aarecord_id_split[0] in ['duxiu_ssid', 'cadal_ssno']:
        if 'duxiu_dxid' in aarecord['file_unified_data']['identifiers_unified']:
            for duxiu_dxid in aarecord['file_unified_data']['identifiers_unified']['duxiu_dxid']:
                additional['download_urls'].append((gettext('page.md5.box.download.aa_dxid'), f'/search?q="duxiu_dxid:{duxiu_dxid}"', ""))

    additional['has_scidb'] = 0
    additional['scidb_info'] = allthethings.utils.scidb_info(aarecord, additional)
    if additional['scidb_info'] is not None:
        additional['fast_partner_urls'] = [(gettext('page.md5.box.download.scidb'), f"/scidb?doi={additional['scidb_info']['doi']}", gettext('common.md5.servers.no_browser_verification'))] + additional['fast_partner_urls']
        additional['slow_partner_urls'] = [(gettext('page.md5.box.download.scidb'), f"/scidb?doi={additional['scidb_info']['doi']}", gettext('common.md5.servers.no_browser_verification'))] + additional['slow_partner_urls']
        additional['has_scidb'] = 1

    return additional

def add_additional_to_aarecord(aarecord):
    return { **aarecord['_source'], '_score': (aarecord.get('_score') or 0.0), 'additional': get_additional_for_aarecord(aarecord['_source']) }

@page.get("/md5/<string:md5_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def md5_page(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    return render_aarecord(f"md5:{canonical_md5}")

@page.get("/ia/<string:ia_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def ia_page(ia_input):
    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        count = cursor.execute('SELECT md5 FROM aa_ia_2023_06_files WHERE ia_id = %(ia_input)s LIMIT 1', { "ia_input": ia_input })
        if count > 0:
            md5 = cursor.fetchone()['md5']
            return redirect(f"/md5/{md5}", code=301)

        return render_aarecord(f"ia:{ia_input}")

@page.get("/isbn/<string:isbn_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def isbn_page(isbn_input):
    return redirect(f"/isbndb/{isbn_input}", code=302)

@page.get("/isbndb/<string:isbn_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def isbndb_page(isbn_input):
    return render_aarecord(f"isbn:{isbn_input}")

@page.get("/ol/<string:ol_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def ol_page(ol_input):
    return render_aarecord(f"ol:{ol_input}")

@page.get("/doi/<path:doi_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def doi_page(doi_input):
    return render_aarecord(f"doi:{doi_input}")

@page.get("/oclc/<string:oclc_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def oclc_page(oclc_input):
    return render_aarecord(f"oclc:{oclc_input}")

@page.get("/duxiu_ssid/<string:duxiu_ssid_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def duxiu_ssid_page(duxiu_ssid_input):
    return render_aarecord(f"duxiu_ssid:{duxiu_ssid_input}")

@page.get("/cadal_ssno/<string:cadal_ssno_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def cadal_ssno_page(cadal_ssno_input):
    return render_aarecord(f"cadal_ssno:{cadal_ssno_input}")

@page.get("/magzdb/<string:magzdb_id>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def magzdb_page(magzdb_id):
    return render_aarecord(f"magzdb:{magzdb_id}")

@page.get("/nexusstc/<string:nexusstc_id>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def nexusstc_page(nexusstc_id):
    return render_aarecord(f"nexusstc:{nexusstc_id}")

@page.get("/nexusstc_download/<string:nexusstc_id>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def nexusstc_download_page(nexusstc_id):
    return render_aarecord(f"nexusstc_download:{nexusstc_id}")

@page.get("/edsebk/<string:edsebk_id>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def edsebk_page(edsebk_id):
    return render_aarecord(f"edsebk:{edsebk_id}")

def render_aarecord(record_id):
    if allthethings.utils.DOWN_FOR_MAINTENANCE:
        return render_template("page/maintenance.html", header_active="")

    with Session(engine):
        ids = [record_id]
        if not allthethings.utils.validate_aarecord_ids(ids):
            return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=record_id), 404

        aarecords = get_aarecords_elasticsearch(ids)
        if aarecords is None:
            return render_template("page/aarecord_issue.html", header_active="search"), 500
        if len(aarecords) == 0:
            code = record_id.replace('isbn:', 'isbn13:')
            return redirect(f'/search?q="{code}"', code=301)
            # return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=record_id), 404

        aarecord = aarecords[0]

        render_fields = {
            "header_active": "home/search",
            "aarecord_id": aarecord['id'],
            "aarecord_id_split": aarecord['id'].split(':', 1),
            "aarecord": aarecord,
            "md5_problem_type_mapping": get_md5_problem_type_mapping(),
            "md5_report_type_mapping": allthethings.utils.get_md5_report_type_mapping()
        }
        return render_template("page/aarecord.html", **render_fields)

@page.get("/scidb")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def scidb_home_page():
    return render_template("page/scidb_home.html", header_active="home/scidb", doi_input=request.args.get('doi'))

@page.post("/scidb")
@allthethings.utils.no_cache()
def scidb_redirect_page():
    doi_input = request.args.get("doi", "").strip()
    return redirect(f"/scidb/{doi_input}", code=302)

@page.get("/scidb/<path:doi_input>")
@page.post("/scidb/<path:doi_input>")
@allthethings.utils.no_cache()
def scidb_page(doi_input):
    # account_id = allthethings.utils.get_account_id(request.cookies)
    # if account_id is None:
    #     return render_template("page/login_to_view.html", header_active="")

    doi_input = doi_input.strip()

    if not doi_input.startswith('10.'):
        if '10.' in doi_input:
            return redirect(f"/scidb/{doi_input[doi_input.find('10.'):].strip()}", code=302)    
        return redirect(f"/search?index=journals&q={doi_input}", code=302)

    if allthethings.utils.doi_is_isbn(doi_input):
        return redirect(f'/search?index=journals&q="doi:{doi_input}"', code=302)

    fast_scidb = False
    # verified = False
    # if str(request.args.get("scidb_verified") or "") == "1":
    #     verified = True
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is not None:
        with Session(mariapersist_engine) as mariapersist_session:
            account_fast_download_info = allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id)
            if account_fast_download_info is not None:
                fast_scidb = True
            # verified = True
    # if not verified:
    #     return redirect(f"/scidb/{doi_input}?scidb_verified=1", code=302)

    with Session(engine):
        try:
            search_results_raw1 = es_aux.search(
                index=allthethings.utils.all_virtshards_for_index("aarecords_journals"),
                size=50,
                query={ "term": { "search_only_fields.search_doi": doi_input } },
                timeout="2s",
            )
            search_results_raw2 = es.search(
                index=allthethings.utils.all_virtshards_for_index("aarecords"),
                size=50,
                query={ "term": { "search_only_fields.search_doi": doi_input } },
                timeout="2s",
            )
        except Exception:
            return redirect(f'/search?index=journals&q="doi:{doi_input}"', code=302)
        aarecords = [add_additional_to_aarecord(aarecord) for aarecord in (search_results_raw1['hits']['hits']+search_results_raw2['hits']['hits'])]
        aarecords = [aarecord for aarecord in aarecords if aarecord['additional']['scidb_info'] is not None]
        aarecords.sort(key=lambda aarecord: aarecord['additional']['scidb_info']['priority'])

        if len(aarecords) == 0:
            return redirect(f'/search?index=journals&q="doi:{doi_input}"', code=302)

        aarecord = aarecords[0]
        scidb_info = aarecord['additional']['scidb_info']

        pdf_url = None
        download_url = None
        path_info = scidb_info['path_info']
        if path_info:
            domain = random.choice(allthethings.utils.SCIDB_SLOW_DOWNLOAD_DOMAINS)
            targeted_seconds_multiplier = 1.0
            minimum = 100
            maximum = 500
            if fast_scidb:
                domain = random.choice(allthethings.utils.SCIDB_FAST_DOWNLOAD_DOMAINS)
                minimum = 1000
                maximum = 5000
            speed = compute_download_speed(path_info['targeted_seconds']*targeted_seconds_multiplier, aarecord['file_unified_data']['filesize_best'], minimum, maximum)
            pdf_url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(False, speed, path_info['path'], aarecord['additional']['filename'], domain)
            download_url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(True, speed, path_info['path'], aarecord['additional']['filename'], domain)
        
        render_fields = {
            "header_active": "home/search",
            "aarecord_id": aarecord['id'],
            "aarecord_id_split": aarecord['id'].split(':', 1),
            "aarecord": aarecord,
            "doi_input": doi_input,
            "pdf_url": pdf_url,
            "download_url": download_url,
            "scihub_link": scidb_info['scihub_link'],
            "ipfs_url": scidb_info['ipfs_url'],
            "nexusstc_id": scidb_info['nexusstc_id'],
            "fast_scidb": fast_scidb,
        }
        return render_template("page/scidb.html", **render_fields)

@page.get("/db/aarecord/<path:aarecord_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def md5_json(aarecord_id):
    aarecords = get_aarecords_elasticsearch([aarecord_id])
    if aarecords is None:
        return '"Page loading issue"', 500
    if len(aarecords) == 0:
        return "{}", 404
    
    aarecord_comments = {
        "id": ("before", ["File from the combined collections of Anna's Archive.",
                           "More details at https://annas-archive.se/datasets",
                           allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        "lgrsnf_book": ("before", ["Source data at: https://annas-archive.se/db/lgrsnf/<id>.json"]),
        "lgrsfic_book": ("before", ["Source data at: https://annas-archive.se/db/lgrsfic/<id>.json"]),
        "lgli_file": ("before", ["Source data at: https://annas-archive.se/db/lgli/<f_id>.json"]),
        "zlib_book": ("before", ["Source data at: https://annas-archive.se/db/zlib/<zlibrary_id>.json"]),
        "aac_zlib3_book": ("before", ["Source data at: https://annas-archive.se/db/aac_zlib3/<zlibrary_id>.json"]),
        "ia_record": ("before", ["Source data at: https://annas-archive.se/db/ia/<ia_id>.json"]),
        "isbndb": ("before", ["Source data at: https://annas-archive.se/db/isbndb/<isbn13>.json"]),
        "ol": ("before", ["Source data at: https://annas-archive.se/db/ol/<ol_edition>.json"]),
        "scihub_doi": ("before", ["Source data at: https://annas-archive.se/db/scihub_doi/<doi>.json"]),
        "oclc": ("before", ["Source data at: https://annas-archive.se/db/oclc/<oclc>.json"]),
        "duxiu": ("before", ["Source data at: https://annas-archive.se/db/duxiu_ssid/<duxiu_ssid>.json or https://annas-archive.se/db/cadal_ssno/<cadal_ssno>.json or https://annas-archive.se/db/duxiu_md5/<md5>.json"]),
        "aac_upload": ("before", ["Source data at: https://annas-archive.se/db/aac_upload/<md5>.json"]),
        "aac_magzdb": ("before", ["Source data at: https://annas-archive.se/db/aac_magzdb/<requested_value>.json or https://annas-archive.se/db/aac_magzdb_md5/<requested_value>.json"]),
        "aac_nexusstc": ("before", ["Source data at: https://annas-archive.se/db/aac_nexusstc/<requested_value>.json or https://annas-archive.se/db/aac_nexusstc_download/<requested_value>.json or https://annas-archive.se/db/aac_nexusstc_md5/<requested_value>.json"]),
        "aac_edsebk": ("before", ["Source data at: https://annas-archive.se/db/aac_edsebk/<edsebk_id>.json"]),
        "file_unified_data": ("before", ["Combined data by Anna's Archive from the various source collections, attempting to get pick the best field where possible."]),
        "ipfs_infos": ("before", ["Data about the IPFS files."]),
        "search_only_fields": ("before", ["Data that is used during searching."]),
        "additional": ("before", ["Data that is derived at a late stage, and not stored in the search index."]),
    }
    aarecord = add_comments_to_dict(aarecords[0], aarecord_comments)

    aarecord['additional'].pop('fast_partner_urls')
    aarecord['additional'].pop('slow_partner_urls')

    return allthethings.utils.nice_json(aarecord), {'Content-Type': 'text/json; charset=utf-8'}

# IMPORTANT: Keep in sync with api_md5_fast_download.
@page.get("/fast_download/<string:md5_input>/<int:path_index>/<int:domain_index>")
@allthethings.utils.no_cache()
def md5_fast_download(md5_input, path_index, domain_index):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]) or canonical_md5 != md5_input:
        return redirect(f"/md5/{md5_input}", code=302)
    
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return redirect("/fast_download_not_member", code=302)

    with Session(mariapersist_engine) as mariapersist_session:
        account_fast_download_info = allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id)
        if account_fast_download_info is None:
            return redirect("/fast_download_not_member", code=302)

        with Session(engine):
            aarecords = get_aarecords_elasticsearch([f"md5:{canonical_md5}"])
            if aarecords is None:
                return render_template("page/aarecord_issue.html", header_active="search"), 500
            if len(aarecords) == 0:
                return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=md5_input), 404
            aarecord = aarecords[0]
            try:
                domain = allthethings.utils.FAST_DOWNLOAD_DOMAINS[domain_index]
                path_info = aarecord['additional']['partner_url_paths'][path_index]
            except Exception:
                return redirect(f"/md5/{md5_input}", code=302)
            url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(False, 20000, path_info['path'], aarecord['additional']['filename'], domain)

        if canonical_md5 not in account_fast_download_info['recently_downloaded_md5s']:
            if account_fast_download_info['downloads_left'] <= 0:
                return redirect("/fast_download_no_more", code=302)
            data_md5 = bytes.fromhex(canonical_md5)
            data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
            mariapersist_session.connection().execute(text('INSERT INTO mariapersist_fast_download_access (md5, ip, account_id) VALUES (:md5, :ip, :account_id)').bindparams(md5=data_md5, ip=data_ip, account_id=account_id))
            mariapersist_session.commit()
        return redirect(url, code=302)

def compute_download_speed(targeted_seconds, filesize, minimum, maximum):
    return min(maximum, max(minimum, int(filesize/1000/targeted_seconds)))

@cachetools.cached(cache=cachetools.TTLCache(maxsize=50000, ttl=30*60), lock=threading.Lock())
def get_daily_download_count_from_ip(data_pseudo_ipv4):
    with Session(mariapersist_engine) as mariapersist_session:
        data_hour_since_epoch = int(time.time() / 3600)
        cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT SUM(count) AS count FROM mariapersist_slow_download_access_pseudo_ipv4_hourly WHERE pseudo_ipv4 = %(pseudo_ipv4)s AND hour_since_epoch > %(hour_since_epoch)s LIMIT 1', { "pseudo_ipv4": data_pseudo_ipv4, "hour_since_epoch": data_hour_since_epoch-24 })
        return ((cursor.fetchone() or {}).get('count') or 0)

@page.get("/slow_download/<string:md5_input>/<int:path_index>/<int:domain_index>")
@page.post("/slow_download/<string:md5_input>/<int:path_index>/<int:domain_index>")
@allthethings.utils.no_cache()
def md5_slow_download(md5_input, path_index, domain_index):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if (request.headers.get('cf-worker') or '') != '':
        return render_template(
            "page/partner_download.html",
            header_active="search",
            only_official=True,
            canonical_md5=canonical_md5,
        )

    data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)

    # We blocked Cloudflare because otherwise VPN users circumvent the CAPTCHA.
    # But it also blocks some TOR users who get Cloudflare exit nodes.
    # Perhaps not as necessary anymore now that we have waitlists, and extra throttling by IP.
    # if allthethings.utils.is_canonical_ip_cloudflare(data_ip):
    #     return render_template(
    #         "page/partner_download.html",
    #         header_active="search",
    #         no_cloudflare=True,
    #         canonical_md5=canonical_md5,
    #     )

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]) or canonical_md5 != md5_input:
        return redirect(f"/md5/{md5_input}", code=302)

    data_pseudo_ipv4 = allthethings.utils.pseudo_ipv4_bytes(request.remote_addr)
    account_id = allthethings.utils.get_account_id(request.cookies)

    aarecords = get_aarecords_elasticsearch([f"md5:{canonical_md5}"])
    if aarecords is None:
        return render_template("page/aarecord_issue.html", header_active="search"), 500
    if len(aarecords) == 0:
        return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=md5_input), 404
    aarecord = aarecords[0]
    try:
        domain_slow = allthethings.utils.SLOW_DOWNLOAD_DOMAINS[domain_index]
        domain_slowest = allthethings.utils.SLOWEST_DOWNLOAD_DOMAINS[domain_index]
        path_info = aarecord['additional']['partner_url_paths'][path_index]
    except Exception:
        return redirect(f"/md5/{md5_input}", code=302)

    daily_download_count_from_ip = get_daily_download_count_from_ip(data_pseudo_ipv4)

    # minimum = 10
    # maximum = 100
    # minimum = 100
    # maximum = 300
    # targeted_seconds_multiplier = 1.0
    warning = False
    # These waitlist_max_wait_time_seconds values must be multiples, under the current modulo scheme.
    # Also WAITLIST_DOWNLOAD_WINDOW_SECONDS gets subtracted from it.
    waitlist_max_wait_time_seconds = 15*60
    domain = domain_slow
    if daily_download_count_from_ip >= 50:
        # targeted_seconds_multiplier = 2.0
        # minimum = 20
        # maximum = 100
        # waitlist_max_wait_time_seconds *= 2
        # warning = True
        domain = domain_slowest
    elif daily_download_count_from_ip >= 20:
        domain = domain_slowest

    if allthethings.utils.SLOW_DOWNLOAD_DOMAINS_SLIGHTLY_FASTER[domain_index]:
        WAITLIST_DOWNLOAD_WINDOW_SECONDS = 2*60
        hashed_md5_bytes = int.from_bytes(hashlib.sha256(bytes.fromhex(canonical_md5) + HASHED_DOWNLOADS_SECRET_KEY).digest(), byteorder='big')
        seconds_since_epoch = int(time.time())
        wait_seconds = ((hashed_md5_bytes-seconds_since_epoch) % waitlist_max_wait_time_seconds) - WAITLIST_DOWNLOAD_WINDOW_SECONDS
        if wait_seconds > 1:
            return render_template(
                "page/partner_download.html",
                header_active="search",
                wait_seconds=wait_seconds,
                canonical_md5=canonical_md5,
                daily_download_count_from_ip=daily_download_count_from_ip,
            )

    # speed = compute_download_speed(path_info['targeted_seconds']*targeted_seconds_multiplier, aarecord['file_unified_data']['filesize_best'], minimum, maximum)
    speed = 10000

    url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(True, speed, path_info['path'], aarecord['additional']['filename'], domain)

    data_md5 = bytes.fromhex(canonical_md5)
    with Session(mariapersist_engine) as mariapersist_session:
        mariapersist_session.connection().execute(text('INSERT IGNORE INTO mariapersist_slow_download_access (md5, ip, account_id, pseudo_ipv4) VALUES (:md5, :ip, :account_id, :pseudo_ipv4)').bindparams(md5=data_md5, ip=data_ip, account_id=account_id, pseudo_ipv4=data_pseudo_ipv4))
        mariapersist_session.commit()
        data_hour_since_epoch = int(time.time() / 3600)
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_slow_download_access_pseudo_ipv4_hourly (pseudo_ipv4, hour_since_epoch, count) VALUES (:pseudo_ipv4, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch, pseudo_ipv4=data_pseudo_ipv4))
        mariapersist_session.commit()

    return render_template(
        "page/partner_download.html",
        header_active="search",
        url=url,
        warning=warning,
        canonical_md5=canonical_md5,
        daily_download_count_from_ip=daily_download_count_from_ip,
        # pseudo_ipv4=f"{data_pseudo_ipv4[0]}.{data_pseudo_ipv4[1]}.{data_pseudo_ipv4[2]}.{data_pseudo_ipv4[3]}",
    )

@page.get("/ipfs_downloads/<path:aarecord_id>")
@allthethings.utils.no_cache()
def ipfs_downloads(aarecord_id):
    # We show the CID on the book page, so no real reason to block this.
    # if (request.headers.get('cf-worker') or '') != '':
    #     return redirect(f"/md5/{md5_input}", code=302)
    # data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
    # if allthethings.utils.is_canonical_ip_cloudflare(data_ip):
    #     return redirect(f"/md5/{md5_input}", code=302)

    if not allthethings.utils.validate_aarecord_ids([aarecord_id]):
        return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=aarecord_id), 404

    aarecords = get_aarecords_elasticsearch([aarecord_id])
    if aarecords is None:
        return render_template("page/aarecord_issue.html", header_active="search"), 500
    if len(aarecords) == 0:
        return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=aarecord_id), 404
    aarecord = aarecords[0]
    try:
        ipfs_urls = aarecord['additional']['ipfs_urls']
    except Exception:
        return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=aarecord_id), 404

    return render_template(
        "page/ipfs_downloads.html",
        header_active="search",
        ipfs_urls=ipfs_urls,
        original_path=allthethings.utils.path_for_aarecord_id(aarecord_id),
    )

def search_query_aggs(search_index_long):
    return {
        "search_content_type": { "terms": { "field": "search_only_fields.search_content_type", "size": 200 } },
        "search_extension": { "terms": { "field": "search_only_fields.search_extension", "size": 9 } },
        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "size": 100 } },
        "search_record_sources": { "terms": { "field": "search_only_fields.search_record_sources", "size": 100 } },
        "search_most_likely_language_code": { "terms": { "field": "search_only_fields.search_most_likely_language_code", "size": 70 } },
    }

@cachetools.cached(cache=cachetools.TTLCache(maxsize=30000, ttl=60*60), lock=threading.Lock())
def all_search_aggs(display_lang, search_index_long):
    try:
        search_results_raw = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long].search(index=allthethings.utils.all_virtshards_for_index(search_index_long), size=0, aggs=search_query_aggs(search_index_long), timeout=ES_TIMEOUT_ALL_AGG)
    except Exception:
        # Simple retry, just once.
        search_results_raw = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long].search(index=allthethings.utils.all_virtshards_for_index(search_index_long), size=0, aggs=search_query_aggs(search_index_long), timeout=ES_TIMEOUT_ALL_AGG)

    all_aggregations = {}
    # Unfortunately we have to special case the "unknown language", which is currently represented with an empty string `bucket['key'] != ''`, otherwise this gives too much trouble in the UI.
    all_aggregations['search_most_likely_language_code'] = []
    for bucket in search_results_raw['aggregations']['search_most_likely_language_code']['buckets']:
        if bucket['key'] == '':
            all_aggregations['search_most_likely_language_code'].append({ 'key': '_empty', 'label': get_display_name_for_lang('', display_lang), 'doc_count': bucket['doc_count'] })
        else:
            all_aggregations['search_most_likely_language_code'].append({ 'key': bucket['key'], 'label': get_display_name_for_lang(bucket['key'], display_lang), 'doc_count': bucket['doc_count'] })
    all_aggregations['search_most_likely_language_code'].sort(key=lambda bucket: bucket['doc_count'] + (1000000000 if bucket['key'] == display_lang else 0), reverse=True)

    content_type_buckets = list(search_results_raw['aggregations']['search_content_type']['buckets'])
    md5_content_type_mapping = get_md5_content_type_mapping(display_lang)
    all_aggregations['search_content_type'] = [{ 'key': bucket['key'], 'label': md5_content_type_mapping[bucket['key']], 'doc_count': bucket['doc_count'] } for bucket in content_type_buckets]
    # content_type_keys_present = set([bucket['key'] for bucket in content_type_buckets])
    # for key, label in md5_content_type_mapping.items():
    #     if key not in content_type_keys_present:
    #         all_aggregations['search_content_type'].append({ 'key': key, 'label': label, 'doc_count': 0 })
    search_content_type_sorting = ['book_nonfiction', 'book_fiction', 'book_unknown', 'journal_article']
    all_aggregations['search_content_type'].sort(key=lambda bucket: (search_content_type_sorting.index(bucket['key']) if bucket['key'] in search_content_type_sorting else 99999, -bucket['doc_count']))

    # Similarly to the "unknown language" issue above, we have to filter for empty-string extensions, since it gives too much trouble.
    all_aggregations['search_extension'] = []
    for bucket in search_results_raw['aggregations']['search_extension']['buckets']:
        if bucket['key'] == '':
            all_aggregations['search_extension'].append({ 'key': '_empty', 'label': 'unknown', 'doc_count': bucket['doc_count'] })
        else:
            all_aggregations['search_extension'].append({ 'key': bucket['key'], 'label': bucket['key'], 'doc_count': bucket['doc_count'] })

    access_types_buckets = list(search_results_raw['aggregations']['search_access_types']['buckets'])
    access_types_mapping = get_access_types_mapping(display_lang)
    all_aggregations['search_access_types'] = [{ 'key': bucket['key'], 'label': access_types_mapping[bucket['key']], 'doc_count': bucket['doc_count'] } for bucket in access_types_buckets]
    # content_type_keys_present = set([bucket['key'] for bucket in access_types_buckets])
    # for key, label in access_types_mapping.items():
    #     if key not in content_type_keys_present:
    #         all_aggregations['search_access_types'].append({ 'key': key, 'label': label, 'doc_count': 0 })
    search_access_types_sorting = list(access_types_mapping.keys())
    all_aggregations['search_access_types'].sort(key=lambda bucket: (search_access_types_sorting.index(bucket['key']) if bucket['key'] in search_access_types_sorting else 99999, -bucket['doc_count']))

    record_sources_buckets = list(search_results_raw['aggregations']['search_record_sources']['buckets'])
    record_sources_mapping = get_record_sources_mapping(display_lang)
    all_aggregations['search_record_sources'] = [{ 'key': bucket['key'], 'label': record_sources_mapping[bucket['key']], 'doc_count': bucket['doc_count'] } for bucket in record_sources_buckets]
    # content_type_keys_present = set([bucket['key'] for bucket in record_sources_buckets])
    # for key, label in record_sources_mapping.items():
    #     if key not in content_type_keys_present:
    #         all_aggregations['search_record_sources'].append({ 'key': key, 'label': label, 'doc_count': 0 })

    es_stat = { 'name': 'all_search_aggs//' + search_index_long, 'took': search_results_raw.get('took'), 'timed_out': search_results_raw.get('timed_out') }

    return (all_aggregations, es_stat)

number_of_search_primary_exceptions = 0
@page.get("/search")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def search_page():
    global number_of_search_primary_exceptions

    if allthethings.utils.DOWN_FOR_MAINTENANCE:
        return render_template("page/maintenance.html", header_active="")

    search_page_timer = time.perf_counter()
    had_es_timeout = False
    had_primary_es_timeout = False
    had_fatal_es_timeout = False
    es_stats = []

    search_input = request.args.get("q", "").strip()
    filter_values = {
        'search_most_likely_language_code': [val.strip()[0:15] for val in request.args.getlist("lang")],
        'search_content_type': [val.strip()[0:25] for val in request.args.getlist("content")],
        'search_extension': [val.strip()[0:10] for val in request.args.getlist("ext")],
        'search_access_types': [val.strip()[0:50] for val in request.args.getlist("acc")],
        'search_record_sources': [val.strip()[0:20] for val in request.args.getlist("src")],
    }
    search_desc = (request.args.get("desc", "").strip() == "1")
    page_value_str = request.args.get("page", "").strip()
    page_value = 1
    try:
        page_value = int(page_value_str)
    except Exception:
        pass
    sort_value = request.args.get("sort", "").strip()
    search_index_short = request.args.get("index", "").strip()
    if search_index_short not in allthethings.utils.SEARCH_INDEX_SHORT_LONG_MAPPING:
        search_index_short = ""
    search_index_long = allthethings.utils.SEARCH_INDEX_SHORT_LONG_MAPPING[search_index_short]
    if search_index_short == 'digital_lending':
        filter_values['search_extension'] = []

    # Correct ISBN by removing spaces so our search for them actually works.
    potential_isbn = search_input.replace('-', '')
    if search_input != potential_isbn and (isbnlib.is_isbn13(potential_isbn) or isbnlib.is_isbn10(potential_isbn)):
        return redirect(f"/search?q={potential_isbn}", code=302)

    post_filter = []
    for key, values in filter_values.items():
        if values != []:
            post_filter.append({ "terms": { f"search_only_fields.{key}": [value if value != '_empty' else '' for value in values] } })

    custom_search_sorting = ['_score']
    if sort_value == "newest":
        custom_search_sorting = [{ "search_only_fields.search_year": "desc" }, '_score']
    if sort_value == "oldest":
        custom_search_sorting = [{ "search_only_fields.search_year": "asc" }, '_score']
    if sort_value == "largest":
        custom_search_sorting = [{ "search_only_fields.search_filesize": "desc" }, '_score']
    if sort_value == "smallest":
        custom_search_sorting = [{ "search_only_fields.search_filesize": "asc" }, '_score']
    if sort_value == "newest_added":
        custom_search_sorting = [{ "search_only_fields.search_added_date": "desc" }, '_score']
    if sort_value == "oldest_added":
        custom_search_sorting = [{ "search_only_fields.search_added_date": "asc" }, '_score']

    main_search_fields = []
    if len(search_input) > 0:
        main_search_fields.append(('search_only_fields.search_text', search_input))
        if search_desc:
            main_search_fields.append(('search_only_fields.search_description_comments', search_input))

    specific_search_fields_mapping = get_specific_search_fields_mapping(get_locale())

    specific_search_fields = []
    for number in range(1,10):
        term_type = request.args.get(f"termtype_{number}") or ""
        term_val = request.args.get(f"termval_{number}") or ""
        if (len(term_val) > 0) and (term_type in specific_search_fields_mapping):
            specific_search_fields.append((term_type, term_val))

    if (len(main_search_fields) == 0) and (len(specific_search_fields) == 0):
        search_query = { "match_all": {} }
        if custom_search_sorting == ['_score']:
            custom_search_sorting = [{ "search_only_fields.search_added_date": "desc" }, '_score']
    else:
        search_query = {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "should": [
                                # The 3.0 is from the 3x "boost" of title/author/etc in search_text.
                                { "rank_feature": { "field": "search_only_fields.search_score_base_rank", "boost": 3.0*10000.0 } },
                                { 
                                    "constant_score": {
                                        "filter": { "term": { "search_only_fields.search_most_likely_language_code": { "value": allthethings.utils.get_base_lang_code(get_locale()) } } },
                                        "boost": 3.0*50000.0,
                                    },
                                },
                            ],
                            "must": [
                                { 
                                    "bool": {
                                        "must": [
                                            {
                                                "bool": {
                                                    "should": [{ "match_phrase": { field_name: { "query": field_value } } } for field_name, field_value in main_search_fields ],
                                                },
                                            },
                                            *[{ "match_phrase": { f'search_only_fields.search_{field_name}': { "query": field_value } } } for field_name, field_value in specific_search_fields ],
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                ],
                "must": [
                    {
                        "bool": {
                            "should": [
                                { "rank_feature": { "field": "search_only_fields.search_score_base_rank", "boost": 3.0*10000.0/100000.0 } },
                                {
                                    "constant_score": {
                                        "filter": { "term": { "search_only_fields.search_most_likely_language_code": { "value": allthethings.utils.get_base_lang_code(get_locale()) } } },
                                        "boost": 3.0*50000.0/100000.0,
                                    },
                                },
                            ],
                            "must": [
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "bool": {
                                                    "should": [{ "simple_query_string": { "query": field_value, "fields": [field_name], "default_operator": "and" } } for field_name, field_value in main_search_fields ],
                                                },
                                            },
                                            *[{ "simple_query_string": { "query": field_value, "fields": [f'search_only_fields.search_{field_name}'], "default_operator": "and" } } for field_name, field_value in specific_search_fields ],
                                        ],
                                        "boost": 1.0/100000.0,
                                    },
                                },
                            ],
                        },
                    },
                ],
            },
        }

    max_display_results = 100

    es_handle = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long]

    primary_search_searches = [
        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
        {
            "size": max_display_results, 
            "from": (page_value-1)*max_display_results,
            "query": search_query,
            "aggs": search_query_aggs(search_index_long),
            "post_filter": { "bool": { "filter": post_filter } },
            "sort": custom_search_sorting,
            # "track_total_hits": False, # Set to default
            "timeout": ES_TIMEOUT_PRIMARY,
            # "knn": { "field": "search_only_fields.search_e5_small_query", "query_vector": list(map(float, get_e5_small_model().encode(f"query: {search_input}", normalize_embeddings=True))), "k": 10, "num_candidates": 1000 },
        },
    ]

    search_names = ['search1_primary']
    search_results_raw = {'responses': [{} for search_name in search_names]}
    for attempt in range(1, 100):
        try:
            search_results_raw = dict(es_handle.msearch(
                request_timeout=5,
                max_concurrent_searches=64,
                max_concurrent_shard_requests=64,
                searches=primary_search_searches,
            ))
            number_of_search_primary_exceptions = 0
            break
        except Exception as err:
            print(f"Warning: another attempt during primary ES search {search_input=}")
            if attempt >= 2:
                had_es_timeout = True
                had_primary_es_timeout = True
                had_fatal_es_timeout = True

                number_of_search_primary_exceptions += 1
                if number_of_search_primary_exceptions > 5:
                    print(f"Exception during primary ES search {attempt=} {search_input=} ///// {repr(err)} ///// {traceback.format_exc()}\n")
                else:
                    print("Haven't reached number_of_search_primary_exceptions limit yet, so not raising")
                break
    for num, response in enumerate(search_results_raw['responses']):
        es_stats.append({ 'name': search_names[num], 'took': response.get('took'), 'timed_out': response.get('timed_out'), 'searches': primary_search_searches })
        if response.get('timed_out') or (response == {}):
            had_es_timeout = True
            had_primary_es_timeout = True
    primary_response_raw = search_results_raw['responses'][0]

    display_lang = allthethings.utils.get_base_lang_code(get_locale())
    try:
        all_aggregations, all_aggregations_es_stat = all_search_aggs(display_lang, search_index_long)
    except Exception:
        return 'Page loading issue', 500
    es_stats.append(all_aggregations_es_stat)

    doc_counts = {}
    doc_counts['search_most_likely_language_code'] = {}
    doc_counts['search_content_type'] = {}
    doc_counts['search_extension'] = {}
    doc_counts['search_access_types'] = {}
    doc_counts['search_record_sources'] = {}
    if search_input == '':
        for bucket in all_aggregations['search_most_likely_language_code']:
            doc_counts['search_most_likely_language_code'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_content_type']:
            doc_counts['search_content_type'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_extension']:
            doc_counts['search_extension'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_access_types']:
            doc_counts['search_access_types'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_record_sources']:
            doc_counts['search_record_sources'][bucket['key']] = bucket['doc_count']
    elif 'aggregations' in primary_response_raw:
        if 'search_most_likely_language_code' in primary_response_raw['aggregations']:
            for bucket in primary_response_raw['aggregations']['search_most_likely_language_code']['buckets']:
                doc_counts['search_most_likely_language_code'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']
        for bucket in primary_response_raw['aggregations']['search_content_type']['buckets']:
            doc_counts['search_content_type'][bucket['key']] = bucket['doc_count']
        for bucket in primary_response_raw['aggregations']['search_extension']['buckets']:
            doc_counts['search_extension'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']
        for bucket in primary_response_raw['aggregations']['search_access_types']['buckets']:
            doc_counts['search_access_types'][bucket['key']] = bucket['doc_count']
        for bucket in primary_response_raw['aggregations']['search_record_sources']['buckets']:
            doc_counts['search_record_sources'][bucket['key']] = bucket['doc_count']

    aggregations = {}
    aggregations['search_most_likely_language_code'] = [{
            **bucket,
            'doc_count': doc_counts['search_most_likely_language_code'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_most_likely_language_code']),
        } for bucket in all_aggregations['search_most_likely_language_code']]
    aggregations['search_content_type'] = [{
            **bucket,
            'doc_count': doc_counts['search_content_type'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_content_type']),
        } for bucket in all_aggregations['search_content_type']]
    aggregations['search_extension'] = [{
            **bucket,
            'doc_count': doc_counts['search_extension'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_extension']),
        } for bucket in all_aggregations['search_extension']]
    aggregations['search_access_types'] = [{
            **bucket,
            'doc_count': doc_counts['search_access_types'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_access_types']),
        } for bucket in all_aggregations['search_access_types']]
    aggregations['search_record_sources'] = [{
            **bucket,
            'doc_count': doc_counts['search_record_sources'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_record_sources']),
        } for bucket in all_aggregations['search_record_sources']]

    # Only sort languages, for the other lists we want consistency.
    aggregations['search_most_likely_language_code'] = sorted(aggregations['search_most_likely_language_code'], key=lambda bucket: bucket['doc_count'] + (1000000000 if bucket['key'] == display_lang else 0), reverse=True)

    search_aarecords = []
    primary_hits_total_obj = { 'value': 0, 'relation': 'eq' }
    if 'hits' in primary_response_raw:
        search_aarecords = [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in primary_response_raw['hits']['hits'] if aarecord_raw['_id'] not in allthethings.utils.SEARCH_FILTERED_BAD_AARECORD_IDS]
        primary_hits_total_obj = primary_response_raw['hits']['total']

    additional_search_aarecords = []
    additional_display_results = max(0, max_display_results-len(search_aarecords))
    if (page_value == 1) and (additional_display_results > 0) and (len(specific_search_fields) == 0):
        search_names2 = ['search2', 'search3', 'search4']
        search_results_raw2 = {'responses': [{} for search_name in search_names2]}
        for attempt in range(1, 100):
            try:
                search_results_raw2 = dict(es_handle.msearch(
                    request_timeout=4,
                    max_concurrent_searches=64,
                    max_concurrent_shard_requests=64,
                    searches=[
                        # For partial matches, first try our original query again but this time without filters.
                        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
                        {
                            "size": additional_display_results,
                            "query": search_query,
                            "sort": custom_search_sorting,
                            "track_total_hits": False,
                            "timeout": ES_TIMEOUT,
                        },
                        # Then do an "OR" query, but this time with the filters again.
                        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
                        {
                            "size": additional_display_results,
                            "query": {"bool": { "must": { "multi_match": { "query": search_input, "fields": "search_only_fields.search_text" }  }, "filter": post_filter } },
                            # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                            "sort": ['_score'],
                            "track_total_hits": False,
                            "timeout": ES_TIMEOUT,
                        },
                        # If we still don't have enough, do another OR query but this time without filters.
                        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
                        {
                            "size": additional_display_results,
                            "query": {"bool": { "must": { "multi_match": { "query": search_input, "fields": "search_only_fields.search_text" }  } } },
                            # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                            "sort": ['_score'],
                            "track_total_hits": False,
                            "timeout": ES_TIMEOUT,
                        },
                    ]
                ))
                break
            except Exception:
                if attempt < 2:
                    print(f"Warning: another attempt during secondary ES search {search_input=}")
                else:
                    had_es_timeout = True
                    print(f"Warning: issue during secondary ES search {search_input=}")
                    break
        for num, response in enumerate(search_results_raw2['responses']):
            es_stats.append({ 'name': search_names2[num], 'took': response.get('took'), 'timed_out': response.get('timed_out') })
            if response.get('timed_out'):
                had_es_timeout = True

        seen_ids = set([aarecord['id'] for aarecord in search_aarecords])
        search_result2_raw = search_results_raw2['responses'][0]
        if 'hits' in search_result2_raw:
            additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in search_result2_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in allthethings.utils.SEARCH_FILTERED_BAD_AARECORD_IDS]

        if len(additional_search_aarecords) < additional_display_results:
            seen_ids = seen_ids.union(set([aarecord['id'] for aarecord in additional_search_aarecords]))
            search_result3_raw = search_results_raw2['responses'][1]
            if 'hits' in search_result3_raw:
                additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in search_result3_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in allthethings.utils.SEARCH_FILTERED_BAD_AARECORD_IDS]

            if len(additional_search_aarecords) < additional_display_results:
                seen_ids = seen_ids.union(set([aarecord['id'] for aarecord in additional_search_aarecords]))
                search_result4_raw = search_results_raw2['responses'][2]
                if 'hits' in search_result4_raw:
                    additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in search_result4_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in allthethings.utils.SEARCH_FILTERED_BAD_AARECORD_IDS]

    es_stats.append({ 'name': 'search_page_timer', 'took': (time.perf_counter() - search_page_timer) * 1000, 'timed_out': False })

    primary_hits_pages = 1 + (max(0, primary_hits_total_obj['value'] - 1) // max_display_results)

    search_dict = {}
    search_dict['search_aarecords'] = search_aarecords[0:max_display_results]
    search_dict['additional_search_aarecords'] = additional_search_aarecords[0:additional_display_results]
    search_dict['max_search_aarecords_reached'] = (len(search_aarecords) >= max_display_results)
    search_dict['max_additional_search_aarecords_reached'] = (len(additional_search_aarecords) >= additional_display_results)
    search_dict['aggregations'] = aggregations
    search_dict['sort_value'] = sort_value
    search_dict['search_index_short'] = search_index_short
    search_dict['es_stats_json'] = es_stats
    search_dict['had_primary_es_timeout'] = had_primary_es_timeout
    search_dict['had_es_timeout'] = had_es_timeout
    search_dict['had_fatal_es_timeout'] = had_fatal_es_timeout
    search_dict['page_value'] = page_value
    search_dict['primary_hits_pages'] = primary_hits_pages
    search_dict['pagination_pages_with_dots_large'] = allthethings.utils.build_pagination_pages_with_dots(primary_hits_pages, page_value, True)
    search_dict['pagination_pages_with_dots_small'] = allthethings.utils.build_pagination_pages_with_dots(primary_hits_pages, page_value, False)
    search_dict['pagination_base_url'] = request.path + '?' + urllib.parse.urlencode([(k,v) for k,values in request.args.lists() for v in values if k != 'page'] + [('page', '')])
    search_dict['primary_hits_total_obj'] = primary_hits_total_obj
    search_dict['max_display_results'] = max_display_results
    search_dict['search_desc'] = search_desc
    search_dict['specific_search_fields'] = specific_search_fields
    search_dict['specific_search_fields_mapping'] = specific_search_fields_mapping

    g.hide_search_bar = True

    r = make_response((render_template(
            "page/search.html",
            header_active="home/search",
            search_input=search_input,
            search_dict=search_dict,
        ), 200))
    if had_es_timeout or (len(search_aarecords) == 0):
        r.headers.add('Cache-Control', 'no-cache')
    return r
