import collections
import jwt
import re
import ipaddress
import flask
import functools
import datetime
import cachetools
import babel.numbers
import babel
import os
import base64
import base58
import hashlib
import urllib.parse
import orjson
import isbnlib
import math
import shortuuid
import pymysql
import httpx
import indexed_zstd
import threading
import traceback
import time

from flask_babel import gettext, get_babel, force_locale

from allthethings.extensions import es, es_aux, engine
from config.settings import SECRET_KEY, DOWNLOADS_SECRET_KEY, MEMBERS_TELEGRAM_URL, PAYMENT2_URL, PAYMENT2_API_KEY, PAYMENT2_PROXIES, FAST_PARTNER_SERVER1, HOODPAY_URL, HOODPAY_AUTH, PAYMENT3_DOMAIN, PAYMENT3_KEY, AACID_SMALL_DATA_IMPORTS

FEATURE_FLAGS = {}

FAST_DOWNLOAD_DOMAINS = [x for x in [FAST_PARTNER_SERVER1, 'nrzr.li', 'wbsg8v.xyz', 'momot.rs'] if x is not None]
# SLOW_DOWNLOAD_DOMAINS = ['momot.rs', 'ktxr.rs', 'nrzr.li']
SLOW_DOWNLOAD_DOMAINS_SLIGHTLY_FASTER = [True, True, False] # KEEP SAME LENGTH
SLOW_DOWNLOAD_DOMAINS = ['momot.rs', 'wbsg8v.xyz', 'nrzr.li'] # KEEP SAME LENGTH
SLOWEST_DOWNLOAD_DOMAINS = ['nrzr.li', 'momot.rs', 'momot.rs'] # KEEP SAME LENGTH
SCIDB_SLOW_DOWNLOAD_DOMAINS = ['wbsg8v.xyz']
SCIDB_FAST_DOWNLOAD_DOMAINS = [FAST_PARTNER_SERVER1 if FAST_PARTNER_SERVER1 is not None else 'nrzr.li']

DOWN_FOR_MAINTENANCE = False

AARECORDS_CODES_CODE_LENGTH = 680
AARECORDS_CODES_AARECORD_ID_LENGTH = 300
AARECORDS_CODES_AARECORD_ID_PREFIX_LENGTH = 20

# Per https://software.annas-archive.li/AnnaArchivist/annas-archive/-/issues/37
SEARCH_FILTERED_BAD_AARECORD_IDS = [
    "md5:d41d8cd98f00b204e9800998ecf8427e", # md5("")
    "md5:5058f1af8388633f609cadb75a75dc9d", # md5(".")

    "md5:b0647953a182171074873b61200c71dd",
    "md5:820a4f8961ae0a76ad265f1678b7dfa5",

    # Likely CSAM
    "md5:d897ffc4e64cbaeae53a6005b6f155cc",
    "md5:8ae28a86719e3a4400145ac18b621efd",
    "md5:285171dbb2d1d56aa405ad3f5e1bc718",
    "md5:8ac4facd6562c28d7583d251aa2c9020",
    "md5:6c1b1ea486960a1ad548cd5c02c465a1",
    "md5:414e8f3a8bc0f63de37cd52bd6d8701e",
    "md5:c6cddcf83c558b758094e06b97067c89",
    "md5:5457b152ef9a91ca3e2d8b3a2309a106",
    "md5:02973f6d111c140510fcdf84b1d00c35",
    "md5:d4c01f9370c5ac93eb5ee5c2037ac794",
    "md5:08499f336fbf8d31f8e7fadaaa517477",
    "md5:351024f9b101ac7797c648ff43dcf76e",
    "md5:ffdbec06986b84f24fc786d89ce46528",
    "md5:ca10d6b2ee5c758955ff468591ad67d9",
]

def validate_canonical_md5s(canonical_md5s):
    return all([bool(re.match(r"^[a-f\d]{32}$", canonical_md5)) for canonical_md5 in canonical_md5s])

def validate_ol_editions(ol_editions):
    return all([bool(re.match(r"^OL[\d]+M$", ol_edition)) for ol_edition in ol_editions])

def validate_oclc_ids(oclc_ids):
    return all([str(oclc_id).isdigit() for oclc_id in oclc_ids])

def validate_duxiu_ssids(duxiu_ssids):
    return all([str(duxiu_ssid).isdigit() for duxiu_ssid in duxiu_ssids])

def validate_magzdb_ids(magzdb_ids):
    return all([str(magzdb_id).isdigit() for magzdb_id in magzdb_ids])

def validate_nexusstc_ids(nexusstc_ids):
    return all([bool(re.match(r"^[a-z\d]+$", nexusstc_id)) for nexusstc_id in nexusstc_ids])

def validate_edsebk_ids(edsebk_ids):
    return all([str(edsebk_id).isdigit() for edsebk_id in edsebk_ids])

def validate_aarecord_ids(aarecord_ids):
    try:
        split_ids = split_aarecord_ids(aarecord_ids)
    except Exception:
        return False
    return validate_canonical_md5s(split_ids['md5']) and validate_ol_editions(split_ids['ol']) and validate_oclc_ids(split_ids['oclc']) and validate_duxiu_ssids(split_ids['duxiu_ssid']) and validate_magzdb_ids(split_ids['magzdb']) and validate_nexusstc_ids(split_ids['nexusstc']) and validate_nexusstc_ids(split_ids['nexusstc_download']) and validate_edsebk_ids(split_ids['edsebk'])

def split_aarecord_ids(aarecord_ids):
    ret = {
        'md5': [],
        'ia': [],
        'isbndb': [],
        'ol': [],
        'doi': [],
        'oclc': [],
        'duxiu_ssid': [],
        'cadal_ssno': [],
        'magzdb': [],
        'nexusstc': [],
        'nexusstc_download': [],
        'edsebk': [],
        'cerlalc': [],
        'czech_oo42hcks': [],
        'gbooks': [],
        'goodreads': [],
        'isbngrp': [],
        'libby': [],
        'rgb': [],
        'trantor': [],
    }
    for aarecord_id in aarecord_ids:
        split_aarecord_id = aarecord_id.split(':', 1)
        ret[split_aarecord_id[0]].append(split_aarecord_id[1])
    return ret

def path_for_aarecord_id(aarecord_id):
    aarecord_id_split = aarecord_id.split(':', 1)
    return '/' + aarecord_id_split[0] + '/' + aarecord_id_split[1]

def validate_year(year):
    year_str = str(year)
    return year_str.isdigit() and int(year_str) >= 1600 and int(year_str) < 2100

def doi_is_isbn(doi):
    return doi.startswith('10.978.') or doi.startswith('10.979.')

def make_file_unified_data():
    output = {
        "original_filename_best": '',
        "original_filename_additional": [],
        "cover_url_best": '',
        "cover_url_additional": [],
        "extension_best": '',
        "extension_additional": [],
        "filesize_best": 0,
        "filesize_additional": [],
        "title_best": '',
        "title_additional": [],
        "author_best": '',
        "author_additional": [],
        "publisher_best": '',
        "publisher_additional": [],
        "edition_varia_best": '',
        "edition_varia_additional": [],
        "year_best": '',
        "year_additional": [],
        "comments_multiple": [],
        "stripped_description_best": '',
        "stripped_description_additional": [],
        "language_codes": [],
        "added_date_unified": {},
        "problems": [],
        "content_type_best": '',
        "ipfs_infos": [],
    }
    init_identifiers_and_classification_unified(output)
    return output

def scidb_info(aarecord, additional=None):
    if additional is None:
        additional = aarecord['additional']

    if aarecord['indexes'] != ['aarecords_journals']:
        return None

    valid_dois = [doi for doi in aarecord['file_unified_data']['identifiers_unified'].get('doi') or [] if not doi_is_isbn(doi)]
    if len(valid_dois) == 0:
        return None
    if aarecord['file_unified_data']['extension_best'] != "pdf":
        return None

    scihub_link = None
    scihub_dois = [source_record['source_record'] for source_record in aarecord['source_records'] if source_record['source_type'] == 'scihub_doi']
    if len(scihub_dois) > 0:
        scihub_link = f"https://sci-hub.ru/{scihub_dois[0]['doi']}"

    # TODO:SOURCE remove backwards compatibility.
    content_type = aarecord['file_unified_data'].get('content_type_best') or aarecord['file_unified_data'].get('content_type') or ''
    if (content_type != "journal_article") and (scihub_link is None):
        return None

    path_info = None
    if len(additional['partner_url_paths']) > 0:
        path_info = additional['partner_url_paths'][0]

    ipfs_url = None
    if len(additional['ipfs_urls']) > 0:
        ipfs_url = additional['ipfs_urls'][0]['url']

    nexusstc_id = None
    aac_nexusstcs = [source_record['source_record'] for source_record in aarecord['source_records'] if source_record['source_type'] == 'aac_nexusstc']
    if len(aac_nexusstcs) > 0:
        nexusstc_id = aac_nexusstcs[0]['id']

    if path_info:
        priority = 1
    elif scihub_link:
        priority = 2
    elif ipfs_url:
        priority = 3
    else:
        return None

    return { "priority": priority, "doi": valid_dois[0], "path_info": path_info, "scihub_link": scihub_link, "ipfs_url": ipfs_url, "nexusstc_id": nexusstc_id }

JWT_PREFIX = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'

ACCOUNT_COOKIE_NAME = "aa_account_id2"

def strip_jwt_prefix(jwt_payload):
    if not jwt_payload.startswith(JWT_PREFIX):
        raise Exception("Invalid jwt_payload; wrong prefix")
    return jwt_payload[len(JWT_PREFIX):]

def get_account_id(cookies):
    if len(cookies.get(ACCOUNT_COOKIE_NAME, "")) > 0:
        try:
            account_data = jwt.decode(
                jwt=JWT_PREFIX + cookies[ACCOUNT_COOKIE_NAME],
                key=SECRET_KEY,
                algorithms=["HS256"],
                options={ "verify_signature": True, "require": ["iat"], "verify_iat": True }
            )
        except jwt.exceptions.InvalidTokenError:
            return None
        return account_data["a"]
    return None

def secret_key_from_account_id(account_id):
    hashkey = base58.b58encode(hashlib.md5(f"{SECRET_KEY}{account_id}".encode('utf-8')).digest()).decode('utf-8')
    return f"{account_id}{hashkey}"

def account_id_from_secret_key(secret_key):
    account_id = secret_key[0:7]
    correct_secret_key = secret_key_from_account_id(account_id)
    if secret_key != correct_secret_key:
        return None
    return account_id

def get_domain_lang_code(locale):
    if locale.script == 'Hant':
        return 'tw'
    elif str(locale) == 'nb_NO':
        return 'no'
    elif str(locale) == 'pt_BR':
        return 'br'
    elif str(locale) == 'pt_PT':
        return 'pt'
    elif str(locale) == 'ku_TR':
        return 'kmr'
    else:
        return str(locale)

def domain_lang_code_to_full_lang_code(domain_lang_code):
    if domain_lang_code == "tw":
        return 'zh_Hant'
    elif domain_lang_code == "no":
        return 'nb_NO'
    elif domain_lang_code == "br":
        return 'pt_BR'
    elif domain_lang_code == "pt":
        return 'pt_PT'
    elif domain_lang_code == "kmr":
        return 'ku_TR'
    else:
        return domain_lang_code

def get_domain_lang_code_display_name(locale):
    if str(locale) == 'nb_NO':
        return 'norsk bokmål'
    elif str(locale) == 'pt_BR':
        return 'Brasil: português'
    elif str(locale) == 'pt_PT':
        return 'Portugal: português'
    elif str(locale) == 'ku_TR':
        return 'Kurdish (Northern)'
    else:
        return locale.get_display_name()

def get_full_lang_code(locale):
    return str(locale)

def get_base_lang_code(locale):
    return locale.language


# Adapted from https://github.com/python-babel/flask-babel/blob/69d3340cd0ff52f3e23a47518285a7e6d8f8c640/flask_babel/__init__.py#L175
def list_translations():
    # return [locale for locale in babel.list_translations() if is_locale(locale)]
    result = {}
    for dirname in get_babel().translation_directories:
        if not os.path.isdir(dirname):
            continue
        for folder in os.listdir(dirname):
            locale_dir = os.path.join(dirname, folder, 'LC_MESSAGES')
            if not os.path.isdir(locale_dir):
                continue
            if any(x.endswith('.mo') for x in os.listdir(locale_dir)) and any(x.endswith('.po') for x in os.listdir(locale_dir)):
                if folder in result:
                    raise Exception("Duplicate {folder=}")
                try:
                    result[folder] = babel.Locale.parse(folder)
                except babel.UnknownLocaleError:
                    example_code = "[print(row) for row in sorted([{ 'code': code, 'name': babel.Locale.parse(code).get_display_name('en'), 'writing_population': langcodes.get(code).writing_population() } for code in babel.localedata.locale_identifiers()], key=lambda a: -a['writing_population']) if row['writing_population']>1000000]"
                    raise Exception(f"WARNING unknown language code: {folder=}. Be sure to use a language code that works with this: {example_code=}")
                if get_domain_lang_code(result[folder]) != folder and folder not in ['pt_BR', 'pt_PT', 'nb_NO', 'zh_Hant']:
                    raise Exception(f"get_domain_lang_code does not roundtrip to folder: {result[folder]=} {folder=} {get_domain_lang_code(result[folder])}")
    return result

# Example to convert back from MySQL to IPv4:
# import ipaddress
# ipaddress.ip_address(0x2002AC16000100000000000000000000).sixtofour
# ipaddress.ip_address().sixtofour
def canonical_ip_bytes(ip):
    # Canonicalize to IPv6
    ipv6 = ipaddress.ip_address(ip)
    if ipv6.version == 4:
        # https://stackoverflow.com/a/19853184
        prefix = int(ipaddress.IPv6Address('2002::'))
        ipv6 = ipaddress.ip_address(prefix | (int(ipv6) << 80))
    return ipv6.packed

def pseudo_ipv4_bytes(ip):
    ipv4orv6 = ipaddress.ip_address(ip)
    if ipv4orv6.version == 4:
        output = ipv4orv6.packed
    else:
        # Pseudo ipv4 algorithm from https://blog.cloudflare.com/eliminating-the-last-reasons-to-not-enable-ipv6/
        last_4_bytes_of_md5 = hashlib.md5(ipv4orv6.packed[0:8]).digest()[-4:]
        output = bytes([0xF0 | (last_4_bytes_of_md5[0] & 0x0F)]) + last_4_bytes_of_md5[1:]
    if len(output) != 4:
        raise Exception(f"Unexpected output length in pseudo_ipv4_bytes: {output=}")
    return output

# Hardcoded for now from https://www.cloudflare.com/ips/
CLOUDFLARE_NETWORKS = [ipaddress.ip_network(row) for row in [
    '173.245.48.0/20',
    '103.21.244.0/22',
    '103.22.200.0/22',
    '103.31.4.0/22',
    '141.101.64.0/18',
    '108.162.192.0/18',
    '190.93.240.0/20',
    '188.114.96.0/20',
    '197.234.240.0/22',
    '198.41.128.0/17',
    '162.158.0.0/15',
    '104.16.0.0/13',
    '104.24.0.0/14',
    '172.64.0.0/13',
    '131.0.72.0/22',
    '2400:cb00::/32',
    '2606:4700::/32',
    '2803:f800::/32',
    '2405:b500::/32',
    '2405:8100::/32',
    '2a06:98c0::/29',
    '2c0f:f248::/32',
]]

def is_canonical_ip_cloudflare(canonical_ip_bytes):
    if not isinstance(canonical_ip_bytes, bytes):
        raise Exception("Bad instance in is_canonical_ip_cloudflare")
    ipv6 = ipaddress.ip_address(canonical_ip_bytes)
    if ipv6.version != 6:
        raise Exception("Bad ipv6.version in is_canonical_ip_cloudflare")
    if ipv6.sixtofour is not None:
        for network in CLOUDFLARE_NETWORKS:
            if ipv6.sixtofour in network:
                return True
    for network in CLOUDFLARE_NETWORKS:
        if ipv6 in network:
            return True
    return False

def public_cache(cloudflare_minutes=0, minutes=0):
    def fwrap(f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            r = flask.make_response(f(*args, **kwargs))
            if r.headers.get('Cache-Control') is not None:
                r.headers.add('Cloudflare-CDN-Cache-Control', r.headers.get('Cache-Control'))
            elif r.status_code <= 299:
                r.headers.add('Cache-Control', f"public,max-age={int(60 * minutes)},s-maxage={int(60 * minutes)}")
                r.headers.add('Cloudflare-CDN-Cache-Control', f"max-age={int(60 * cloudflare_minutes)}")
            else:
                r.headers.add('Cache-Control', 'no-cache,must-revalidate,max-age=0,stale-if-error=0')
                r.headers.add('Cloudflare-CDN-Cache-Control', 'no-cache,must-revalidate,max-age=0,stale-if-error=0')
            return r
        return wrapped_f
    return fwrap

def no_cache():
    def fwrap(f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            r = flask.make_response(f(*args, **kwargs))
            r.headers.add('Cache-Control', 'no-cache,must-revalidate,max-age=0,stale-if-error=0')
            r.headers.add('Cloudflare-CDN-Cache-Control', 'no-cache,must-revalidate,max-age=0,stale-if-error=0')
            return r
        return wrapped_f
    return fwrap

def get_md5_report_type_mapping():
    return {
        'metadata': gettext('common.md5_report_type_mapping.metadata'),
        'download': gettext('common.md5_report_type_mapping.download'),
        'broken': gettext('common.md5_report_type_mapping.broken'),
        'pages': gettext('common.md5_report_type_mapping.pages'),
        'spam': gettext('common.md5_report_type_mapping.spam'),
        'copyright': gettext('common.md5_report_type_mapping.copyright'),
        'other': gettext('common.md5_report_type_mapping.other'),
    }

def nice_json(some_dict):
    json_str = orjson.dumps(some_dict, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS, default=str).decode('utf-8')
    # Triple-slashes means it shouldn't be put on the previous line.
    return re.sub(r'[ \n]*"//(?!/)', ' "//', json_str, flags=re.MULTILINE)

def donation_id_to_receipt_id(donation_id):
    return shortuuid.ShortUUID(alphabet="23456789abcdefghijkmnopqrstuvwxyz").encode(shortuuid.decode(donation_id))

def receipt_id_to_donation_id(receipt_id):
    return shortuuid.encode(shortuuid.ShortUUID(alphabet="23456789abcdefghijkmnopqrstuvwxyz").decode(receipt_id))

@cachetools.cached(cache=cachetools.TTLCache(maxsize=1024, ttl=6*60*60), lock=threading.Lock())
def usd_currency_rates_cached():
    # try:
    #     return forex_python.converter.CurrencyRates().get_rates('USD')
    # except forex_python.converter.RatesNotAvailableError:
    #     print("RatesNotAvailableError -- using fallback!")
    #     # 2023-05-04 fallback
    return {'EUR': 0.9161704076958315, 'JPY': 131.46129180027486, 'BGN': 1.7918460833715073, 'CZK': 21.44663307375172, 'DKK': 6.8263857077416406, 'GBP': 0.8016032982134678, 'HUF': 344.57169033440226, 'PLN': 4.293449381584975, 'RON': 4.52304168575355, 'SEK': 10.432890517636281, 'CHF': 0.9049931287219424, 'ISK': 137.15071003206597, 'NOK': 10.43105817682089, 'TRY': 19.25744388456253, 'AUD': 1.4944571690334403, 'BRL': 5.047732478240953, 'CAD': 1.3471369674759506, 'CNY': 6.8725606962895105, 'HKD': 7.849931287219422, 'IDR': 14924.993128721942, 'INR': 81.87402656894183, 'KRW': 1318.1951442968393, 'MXN': 18.288960146587264, 'MYR': 4.398992212551534, 'NZD': 1.592945487860742, 'PHP': 54.56894182317912, 'SGD': 1.3290884104443428, 'THB': 34.054970224461755, 'ZAR': 18.225286303252407}

@functools.cache
def membership_tier_names(locale):
    with force_locale(locale):
        return {
            "1": gettext('common.membership.tier_name.bonus'),
            "2": gettext('common.membership.tier_name.2'),
            "3": gettext('common.membership.tier_name.3'),
            "4": gettext('common.membership.tier_name.4'),
            "5": gettext('common.membership.tier_name.5'),
        }

MEMBERSHIP_TIER_COSTS = {
    "2": 7, "3": 10, "4": 30, "5": 100,
}
MEMBERSHIP_METHOD_DISCOUNTS = {
    # Note: keep manually in sync with HTML.
    # "crypto": 20,
    # "payment2": 20,
    # # "cc":     20,
    # "binance": 20,
    # "paypal": 20,
    # "payment2paypal": 20,
    # "payment2cc": 20,
    # "payment2cashapp": 20,

    "crypto": 0,
    "payment2": 10,
    # "cc":     0,
    "binance": 0,
    "paypal": 0,
    "payment2paypal": 0,
    "payment2cc": 0,
    "payment2cashapp": 10,
    "payment2revolut": 10,

    "paypalreg": 0,
    "amazon": 0,
    # "bmc":    0,
    # "alipay": 0,
    # "pix":    0,
    "payment1": 0,
    "payment1_alipay": 0,
    "payment1_wechat": 0,
    "payment1b": 0,
    "payment1bb": 0,
    "payment1c": 0,
    "payment3a": 0,
    "payment3a_cc": 0,
    "payment3b": 0,
    "givebutter": 0,
    "hoodpay": 0,
    "ccexp": 0,
}
MEMBERSHIP_DURATION_DISCOUNTS = {
    # Note: keep manually in sync with HTML.
    "1": 0, "3": 5, "6": 10, "12": 20, "24": 30, "48": 40, "96": 50,
}
MEMBERSHIP_DOWNLOADS_PER_DAY = {
    "1": 0, "2": 25, "3": 50, "4": 200, "5": 1000,
}
# Keep in sync.
MEMBERSHIP_BONUSDOWNLOADS_PER_DAY = {
    "1": 0, "2": 10, "3": 25, "4": 50, "5": 500,
}
MEMBERSHIP_TELEGRAM_URL = {
    "1": "", "2": "", "3": "", "4": MEMBERS_TELEGRAM_URL, "5": MEMBERS_TELEGRAM_URL,
}
MEMBERSHIP_METHOD_MINIMUM_CENTS_USD = {
    "crypto": 0,
    "payment2": 0,
    # "cc":     20,
    "binance": 0,
    "paypal": 3500,
    "payment2paypal": 2500,
    "payment2cashapp": 2500,
    "payment2revolut": 2500,
    "payment2cc": 0,
    "paypalreg": 0,
    "amazon": 1000,
    # "bmc":    0,
    # "alipay": 0,
    # "pix":    0,
    "payment1": 0,
    "payment1_alipay": 0,
    "payment1_wechat": 0,
    "payment1b": 0,
    "payment1bb": 0,
    "payment1c": 0,
    "payment3a": 0,
    "payment3a_cc": 0,
    "payment3b": 0,
    "givebutter": 500,
    "hoodpay": 1000,
    "ccexp": 99999999,
}
MEMBERSHIP_METHOD_MAXIMUM_CENTS_NATIVE = {
    "payment1":  13000,
    "payment1_alipay":  100000,
    "payment1_wechat":  100000,
    "payment1b": 100000,
    "payment1bb": 100000,
    "payment1c": 100000,
    "payment3a": 150000,
    "payment3a_cc": 150000,
    "payment3b": 500000,
    "amazon": 20000,
}
MEMBERSHIP_MAX_BONUS_DOWNLOADS = 10000

MEMBERSHIP_EXCHANGE_RATE_RMB = 7.25

def get_is_membership_double():
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    return now.strftime("%Y-%m") == '2024-10'
def get_is_membership_double_with_leeway():
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    return get_is_membership_double() or (now.strftime("%Y-%m") == '2024-10' and now.day <= 2)

def get_account_fast_download_info(mariapersist_session, account_id):
    mariapersist_session.connection().connection.ping(reconnect=True)
    cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute('SELECT mariapersist_memberships.membership_tier AS membership_tier, mariapersist_memberships.bonus_downloads AS bonus_downloads FROM mariapersist_accounts INNER JOIN mariapersist_memberships USING (account_id) WHERE mariapersist_accounts.account_id = %(account_id)s AND mariapersist_memberships.membership_expiration >= CURDATE()', { 'account_id': account_id })
    memberships = cursor.fetchall()
    if len(memberships) == 0:
        return None

    downloads_per_day = 0
    bonus_downloads = 0
    for membership in memberships:
        downloads_per_day += MEMBERSHIP_DOWNLOADS_PER_DAY[membership['membership_tier']]
        bonus_downloads += membership['bonus_downloads']

    if bonus_downloads > MEMBERSHIP_MAX_BONUS_DOWNLOADS:
        bonus_downloads = MEMBERSHIP_MAX_BONUS_DOWNLOADS
    downloads_per_day += bonus_downloads

    downloads_left = downloads_per_day
    cursor.execute("SELECT md5 FROM mariapersist_fast_download_access "
                   "WHERE timestamp >= %(timestamp)s AND account_id = %(account_id)s "
                   "LIMIT 50000",
                   { 'timestamp': datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=18), 'account_id': account_id })
    recently_downloaded_md5s = [md5.hex() for md5 in fetch_scalars(cursor)]
    downloads_left -= len(recently_downloaded_md5s)

    max_tier = str(max([int(membership['membership_tier']) for membership in memberships]))

    return { 'downloads_left': max(0, downloads_left), 'recently_downloaded_md5s': recently_downloaded_md5s, 'downloads_per_day': downloads_per_day, 'telegram_url': MEMBERSHIP_TELEGRAM_URL[max_tier] }

# def get_referral_account_id(mariapersist_session, potential_ref_account_id, current_account_id):
#     if potential_ref_account_id is None:
#         return None
#     if potential_ref_account_id == current_account_id:
#         return None
#     if account_can_make_referrals(mariapersist_session, current_account_id):
#         return potential_ref_account_id
#     else:
#         return None

# def account_can_make_referrals(mariapersist_session, account_id):
#     mariapersist_session.connection().connection.ping(reconnect=True)
#     cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)
#     # Note the mariapersist_memberships.membership_tier >= 2 so we don't count bonus memberships.
#     cursor.execute('SELECT COUNT(*) AS count FROM mariapersist_accounts INNER JOIN mariapersist_memberships USING (account_id) WHERE mariapersist_accounts.account_id = %(account_id)s AND mariapersist_memberships.membership_expiration >= CURDATE() AND mariapersist_memberships.membership_tier >= 2', { 'account_id': account_id })
#     return (cursor.fetchone()['count'] > 0)

def cents_to_usd_str(cents):
    return str(cents)[:-2] + "." + str(cents)[-2:]

def format_currency(cost_cents_native_currency, native_currency_code, locale):
    output = babel.numbers.format_currency(cost_cents_native_currency / 100, native_currency_code, locale=locale)
    if output.endswith('.00') or output.endswith(',00'):
        output = output[0:-3]
    return output

def membership_format_native_currency(locale, native_currency_code, cost_cents_native_currency, cost_cents_usd):
    with force_locale(locale):
        if native_currency_code != 'USD':
            return {
                'cost_cents_native_currency_str_calculator': gettext('common.membership.format_currency.total_with_usd', amount=format_currency(cost_cents_native_currency, native_currency_code, locale), amount_usd=format_currency(cost_cents_usd, 'USD', locale)),
                'cost_cents_native_currency_str_button': f"{format_currency(cost_cents_native_currency, native_currency_code, locale)}",
                'cost_cents_native_currency_str_donation_page_formal': gettext('common.membership.format_currency.amount_with_usd', amount=format_currency(cost_cents_native_currency, native_currency_code, locale), amount_usd=format_currency(cost_cents_usd, 'USD', locale)),
                'cost_cents_native_currency_str_donation_page_instructions': gettext('common.membership.format_currency.amount_with_usd', amount=format_currency(cost_cents_native_currency, native_currency_code, locale), amount_usd=format_currency(cost_cents_usd, 'USD', locale)),
            }
        # elif native_currency_code == 'COFFEE':
        #     return {
        #         'cost_cents_native_currency_str_calculator': f"{format_currency(cost_cents_native_currency * 5, 'USD', locale)} ({cost_cents_native_currency} ☕️) total",
        #         'cost_cents_native_currency_str_button': f"{format_currency(cost_cents_native_currency * 5, 'USD', locale)}",
        #         'cost_cents_native_currency_str_donation_page_formal': f"{format_currency(cost_cents_native_currency * 5, 'USD', locale)} ({cost_cents_native_currency} ☕️)",
        #         'cost_cents_native_currency_str_donation_page_instructions': f"{cost_cents_native_currency} “coffee” ({format_currency(cost_cents_native_currency * 5, 'USD', locale)})",
        #     }
        else:
            return {
                'cost_cents_native_currency_str_calculator': gettext('common.membership.format_currency.total', amount=format_currency(cost_cents_usd, 'USD', locale)),
                'cost_cents_native_currency_str_button': f"{format_currency(cost_cents_native_currency, 'USD', locale)}",
                'cost_cents_native_currency_str_donation_page_formal': f"{format_currency(cost_cents_native_currency, 'USD', locale)}",
                'cost_cents_native_currency_str_donation_page_instructions': f"{format_currency(cost_cents_native_currency, 'USD', locale)}",
            }

@cachetools.cached(cache=cachetools.TTLCache(maxsize=1024, ttl=60*60), lock=threading.Lock())
def membership_costs_data(locale):
    usd_currency_rates = usd_currency_rates_cached()

    def calculate_membership_costs(inputs):
        tier = inputs['tier']
        method = inputs['method']
        duration = inputs['duration']
        if (tier not in MEMBERSHIP_TIER_COSTS.keys()) or (method not in MEMBERSHIP_METHOD_DISCOUNTS.keys()) or (duration not in MEMBERSHIP_DURATION_DISCOUNTS.keys()):
            raise Exception("Invalid fields")

        discounts = MEMBERSHIP_METHOD_DISCOUNTS[method] + MEMBERSHIP_DURATION_DISCOUNTS[duration]
        monthly_cents = round(MEMBERSHIP_TIER_COSTS[tier]*(100-discounts))
        cost_cents_usd = monthly_cents * int(duration)

        native_currency_code = 'USD'
        cost_cents_native_currency = cost_cents_usd
        if method in ['alipay', 'payment1', 'payment1_alipay', 'payment1_wechat', 'payment1b', 'payment1bb', 'payment1c', 'payment3a', 'payment3b']:
            native_currency_code = 'CNY'
            cost_cents_native_currency = math.floor(cost_cents_usd * MEMBERSHIP_EXCHANGE_RATE_RMB / 100) * 100
        # elif method == 'bmc':
        #     native_currency_code = 'COFFEE'
        #     cost_cents_native_currency = round(cost_cents_usd / 500)
        elif method == 'amazon':
            if cost_cents_usd <= 500:
                cost_cents_usd = 500
            elif cost_cents_usd <= 700:
                cost_cents_usd = 700
            elif cost_cents_usd <= 1000:
                cost_cents_usd = 1000
            elif cost_cents_usd <= 1500:
                cost_cents_usd = 1500
            elif cost_cents_usd <= 2200:
                cost_cents_usd = 2000
            elif cost_cents_usd <= 2700:
                cost_cents_usd = 2500
            elif cost_cents_usd <= 10000:
                cost_cents_usd = (cost_cents_usd // 500) * 500
            elif cost_cents_usd <= 100000:
                cost_cents_usd = round(cost_cents_usd / 1000) * 1000
            elif cost_cents_usd <= 200000:
                cost_cents_usd = math.ceil(cost_cents_usd / 5000) * 5000
            else:
                cost_cents_usd = math.ceil(cost_cents_usd / 10000) * 10000
            cost_cents_native_currency = cost_cents_usd
        elif method == 'pix':
            native_currency_code = 'BRL'
            cost_cents_native_currency = round(cost_cents_usd * usd_currency_rates['BRL'] / 100) * 100

        formatted_native_currency = membership_format_native_currency(locale, native_currency_code, cost_cents_native_currency, cost_cents_usd)

        return {
            'cost_cents_usd': cost_cents_usd,
            'cost_cents_usd_str': babel.numbers.format_currency(cost_cents_usd / 100.0, 'USD', locale=locale),
            'cost_cents_native_currency': cost_cents_native_currency,
            'cost_cents_native_currency_str_calculator': formatted_native_currency['cost_cents_native_currency_str_calculator'],
            'cost_cents_native_currency_str_button': formatted_native_currency['cost_cents_native_currency_str_button'],
            'native_currency_code': native_currency_code,
            'monthly_cents': monthly_cents,
            'monthly_cents_str': babel.numbers.format_currency(monthly_cents / 100.0, 'USD', locale=locale),
            'discounts': discounts,
            'duration': duration,
            'tier_name': membership_tier_names(locale)[tier],
        }

    data = {}
    for tier in MEMBERSHIP_TIER_COSTS.keys():
        for method in MEMBERSHIP_METHOD_DISCOUNTS.keys():
            for duration in MEMBERSHIP_DURATION_DISCOUNTS.keys():
                inputs = { 'tier': tier, 'method': method, 'duration': duration }
                data[f"{tier},{method},{duration}"] = calculate_membership_costs(inputs)
    return data


def get_cursor_ping(session):
    session.connection().connection.ping(reconnect=True)
    return session.connection().connection.cursor(pymysql.cursors.DictCursor)


def get_cursor_ping_conn(connection):
    connection.connection.ping(reconnect=True)
    return connection.connection.cursor(pymysql.cursors.DictCursor)


def fetch_one_field(cursor):
    row = cursor.fetchone()
    if row is None:
        return None
    return row[next(iter(row))]


def fetch_scalars(cursor) -> list | tuple:
    """
    Fetches value of the first column from all the rows using the cursor
    :return: A list of values of the first column.
    """
    rows = cursor.fetchall()
    if rows is None or len(rows) <= 0:
        return []
    scalars = []
    for row in rows:
        scalars.append(row[next(iter(row))])
    return scalars


def split_columns_row(row: dict | None, column_count: list[int]) -> tuple | None:
    """ Splits separate table columns into tuple values
        Example: SELECT * FROM table1.*, table2.* JOIN table2 USING (id)
        Returns: tuple( {table1 dict}, {table2 dict} )
    """
    if row is None:
        return None

    column_count_index = 0
    column_index = 0
    tuple_values: list[dict | None] = [dict() for _ in column_count]
    for column in iter(row):
        # Remove any table name prefixes
        # These appear if two columns with the same name appear in a single SQL query (e.g. table1.id and table2.id)
        # Columns with the same name cannot appear in a single table so it's safe to just trim out the prefix here
        dict_column_name = column[(column.find('.') + 1):]

        tuple_values[column_count_index][dict_column_name] = row[column]
        column_index += 1

        if column_count[column_count_index] <= column_index:
            found_non_none = False
            for column_value in tuple_values[column_count_index].values():
                if column_value is not None:
                    found_non_none = True
                    break

            if not found_non_none:
                # Set tuple value to None if the entire list was just containing Nones
                tuple_values[column_count_index] = None

            column_count_index += 1
            column_index = 0

    return tuple(tuple_values)


def split_columns(rows: list[dict], column_count: list[int]) -> list[tuple]:
    """ Splits separate table columns into tuple values
        Example: SELECT * FROM table1.*, table2.* JOIN table2 USING (id)
        Returns: tuple( {table1 dict}, {table2 dict} )
    """
    tuples = []
    for row in rows:
        tuples.append(split_columns_row(row, column_count))
    return tuples


def get_account_by_id(cursor, account_id: str) -> dict | tuple | None:
    cursor.execute('SELECT * FROM mariapersist_accounts WHERE account_id = %(account_id)s LIMIT 1', {'account_id': account_id})
    return cursor.fetchone()


# Keep in sync.
def confirm_membership(cursor, donation_id, data_key, data_value):
    cursor.execute('SELECT * FROM mariapersist_donations WHERE donation_id=%(donation_id)s LIMIT 1', { 'donation_id': donation_id })
    donation = cursor.fetchone()
    if donation is None:
        print(f"Warning: failed {data_key} request because of donation not found: {donation_id}")
        return False
    if donation['processing_status'] == 1:
        # Already confirmed
        return True
    if donation['processing_status'] not in [0, 2, 4]:
        print(f"Warning: failed {data_key} request because processing_status != 0,2,4: {donation_id}")
        return False
    # # Allow for 10% margin
    # if float(data['money']) * 110 < donation['cost_cents_native_currency']:
    #     print(f"Warning: failed {data_key} request of 'money' being too small: {data}")
    #     return False

    donation_json = orjson.loads(donation['json'])
    if donation_json['method'] not in ['payment1', 'payment1_alipay', 'payment1_wechat', 'payment1b', 'payment1bb', 'payment1c', 'payment2', 'payment2paypal', 'payment2cashapp', 'payment2revolut', 'payment2cc', 'amazon', 'hoodpay', 'payment3a', 'payment3a_cc', 'payment3b']:
        print(f"Warning: failed {data_key} request because method is not valid: {donation_id}")
        return False

    cursor.execute('SELECT * FROM mariapersist_accounts WHERE account_id=%(account_id)s LIMIT 1', { 'account_id': donation['account_id'] })
    account = cursor.fetchone()
    if account is None:
        print(f"Warning: failed {data_key} request because of account not found: {donation_id}")
        return False

    new_tier = int(donation_json['tier'])
    datetime_today = datetime.datetime.combine(datetime.datetime.utcnow().date(), datetime.datetime.min.time())
    new_membership_expiration = datetime_today + datetime.timedelta(days=1) + datetime.timedelta(days=31*int(donation_json['duration']))

    bonus_downloads = 0
    # ref_account_id = donation_json.get('ref_account_id')
    # ref_account_dict = None
    # if ref_account_id is not None:
    #     cursor.execute('SELECT * FROM mariapersist_accounts WHERE account_id=%(account_id)s LIMIT 1', { 'account_id': ref_account_id })
    #     ref_account_dict = cursor.fetchone()
    #     if ref_account_dict is None:
    #         print(f"Warning: failed {data_key} request because of ref_account_dict not found: {donation_id}")
    #         return False
    #     bonus_downloads = MEMBERSHIP_BONUSDOWNLOADS_PER_DAY[str(new_tier)]

    donation_json[data_key] = data_value
    for inserts in ([1,2] if get_is_membership_double_with_leeway() else [1]):
        cursor.execute('INSERT INTO mariapersist_memberships (account_id, membership_tier, membership_expiration, from_donation_id, bonus_downloads) VALUES (%(account_id)s, %(membership_tier)s, %(membership_expiration)s, %(donation_id)s, %(bonus_downloads)s)', { 'membership_tier': new_tier, 'membership_expiration': new_membership_expiration, 'account_id': donation['account_id'], 'donation_id': donation_id, 'bonus_downloads': bonus_downloads })
    # if (ref_account_dict is not None) and (bonus_downloads > 0):
    #     cursor.execute('INSERT INTO mariapersist_memberships (account_id, membership_tier, membership_expiration, from_donation_id, bonus_downloads) VALUES (%(account_id)s, 1, %(membership_expiration)s, %(donation_id)s, %(bonus_downloads)s)', { 'membership_expiration': new_membership_expiration, 'account_id': ref_account_dict['account_id'], 'donation_id': donation_id, 'bonus_downloads': bonus_downloads })
    cursor.execute('UPDATE mariapersist_donations SET json=%(json)s, processing_status=1, paid_timestamp=NOW() WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
    cursor.execute('COMMIT')
    return True


def payment2_check(cursor, payment_id):
    payment2_status = None
    for attempt in [1,2,3,4,5]:
        try:
            payment2_request = httpx.get(f"{PAYMENT2_URL}{payment_id}", headers={'x-api-key': PAYMENT2_API_KEY}, proxies=PAYMENT2_PROXIES, timeout=10.0)
            payment2_request.raise_for_status()
            payment2_status = payment2_request.json()
            break
        except Exception:
            if attempt == 5:
                raise
            time.sleep(1)
    if payment2_status['payment_status'] in ['confirmed', 'sending', 'finished']:
        if confirm_membership(cursor, payment2_status['order_id'], 'payment2_status', payment2_status):
            return (payment2_status, True)
        else:
            return (payment2_status, False)
    return (payment2_status, True)

def payment3_check(cursor, donation_id):
    payment3_status = None
    for attempt in range(1, 100):
        try:
            data = {
                # Note that these are sorted by key.
                "mchId": 20000007,
                "mchOrderId": donation_id,
                "time": int(time.time()),
            }
            sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + "&key=" + PAYMENT3_KEY
            sign = hashlib.md5((sign_str).encode()).hexdigest()
            response = httpx.post(f"https://{PAYMENT3_DOMAIN}/api/deposit/order-info", data={ **data, "sign": sign }, proxies=PAYMENT2_PROXIES, timeout=10.0)
            response.raise_for_status()
            payment3_status = response.json()
            if str(payment3_status['code']) != '1':
                raise Exception(f"Invalid payment3_status {donation_id=}: {payment3_status}")
            break
        except Exception:
            if attempt == 5:
                raise
            time.sleep(1)
    if str(payment3_status['data']['status']) in ['2','3']:
        if confirm_membership(cursor, donation_id, 'payment3_status', payment3_status):
            return (payment3_status, True)
        else:
            return (payment3_status, False)
    return (payment3_status, True)

def hoodpay_check(cursor, hoodpay_id, donation_id):
    hoodpay_status = httpx.get(HOODPAY_URL.split('/v1/businesses/', 1)[0] + '/v1/public/payments/hosted-page/' + hoodpay_id, headers={"Authorization": f"Bearer {HOODPAY_AUTH}"}, proxies=PAYMENT2_PROXIES, timeout=10.0).json()['data']
    if hoodpay_status['status'] in ['COMPLETED']:
        if confirm_membership(cursor, donation_id, 'hoodpay_status', hoodpay_status):
            return (hoodpay_status, True)
        else:
            return (hoodpay_status, False)
    return (hoodpay_status, True)

def make_anon_download_uri(limit_multiple, speed_kbps, path, filename, domain):
    limit_multiple_field = 'y' if limit_multiple else 'x'
    expiry = int((datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=2)).timestamp())
    secure_str = f"{domain}/{limit_multiple_field}/{expiry}/{speed_kbps}/{path},{DOWNLOADS_SECRET_KEY}"
    md5 = base64.urlsafe_b64encode(hashlib.md5(secure_str.encode('utf-8')).digest()).decode('utf-8').rstrip('=')
    return f"d3/{limit_multiple_field}/{expiry}/{speed_kbps}/{urllib.parse.quote(path)}~/{md5}/{filename}"

DICT_COMMENTS_NO_API_DISCLAIMER = "This page is *not* intended as an API. If you need programmatic access to this JSON, please set up your own instance. For more information, see: https://annas-archive.li/datasets and https://software.annas-archive.li/AnnaArchivist/annas-archive/-/tree/main/data-imports"

COMMON_DICT_COMMENTS = {
    "identifier": ("after", ["Typically ISBN-10 or ISBN-13."]),
    "identifierwodash": ("after", ["Same as 'identifier' but without dashes."]),
    "locator": ("after", ["Original filename or path on the Library Genesis servers."]),
    "stripped_description": ("before", ["Anna's Archive version of the 'descr' or 'description' field, with HTML tags removed or replaced with regular whitespace."]),
    "language_codes": ("before", ["Anna's Archive version of the 'language' field, where we attempted to parse it into BCP 47 tags."]),
    "cover_url_normalized": ("after", ["Anna's Archive version of the 'coverurl' field, where we attempted to turn it into a full URL."]),
    "edition_varia_normalized": ("after", ["Anna's Archive version of the 'series', 'volume', 'edition', 'periodical', and 'year' fields; combining them into a single field for display and search."]),
    "topic_descr": ("after", ["A description of the 'topic' field using the 'topics' database table, which seems to have its roots in the Kolxo3 library that Libgen was originally based on.",
                    "https://wiki.mhut.org/content:bibliographic_data says that this field will be deprecated in favor of Dewey Decimal."]),
    "topic": ("after", ["See 'topic_descr' below."]),
    "searchable": ("after", ["This seems to indicate that the book has been OCR'ed."]),
    "generic": ("after", ["If this is set to a different md5, then that version is preferred over this one, and should be shown in search results instead."]),
    "visible": ("after", ["If this is set, the book is in fact *not* visible in Libgen, and this string describes the reason."]),
    "commentary": ("after", ["Comments left by the uploader, an admin, or an automated process."]),
    "toc": ("before", ["Table of contents. May contain HTML."]),
    "ddc": ("after", ["See also https://libgen.li/biblioservice.php?type=ddc"]),
    "udc": ("after", ["See also https://libgen.li/biblioservice.php?type=udc"]),
    "lbc": ("after", ["See also https://libgen.li/biblioservice.php?type=bbc and https://www.isko.org/cyclo/lbc"]),
    "descriptions_mapped": ("before", ["Normalized fields by Anna's Archive, taken from the various `*_add_descr` Libgen.li tables, with comments taken from the `elem_descr` table which contain metadata about these fields, as well as sometimes our own metadata.",
                                       "The names themselves are taken from `name_en` in the corresponding `elem_descr` entry (lowercased, whitespace removed), with `name_add{1,2,3}_en` to create the compound keys, such as `isbn_isbnnotes`."]),
    "identifiers_unified": ("before", ["Anna's Archive version of various identity-related fields."]),
    "classifications_unified": ("before", ["Anna's Archive version of various classification-related fields."]),
    "added_date_unified": ("before", ["Anna's Archive notion of when records were added to the source library, or when they were scraped."]),
}

# Hardcoded from the `descr_elems` table.
LGLI_EDITION_TYPE_MAPPING = {
    "b":"book",
    "ch":"book-chapter",
    "bpart":"book-part",
    "bsect":"book-section",
    "bs":"book-series",
    "bset":"book-set",
    "btrack":"book-track",
    "component":"component",
    "dataset":"dataset",
    "diss":"dissertation",
    "j":"journal",
    "a":"journal-article",
    "ji":"journal-issue",
    "jv":"journal-volume",
    "mon":"monograph",
    "oth":"other",
    "peer-review":"peer-review",
    "posted-content":"posted-content",
    "proc":"proceedings",
    "proca":"proceedings-article",
    "ref":"reference-book",
    "refent":"reference-entry",
    "rep":"report",
    "repser":"report-series",
    "s":"standard",
    "fnz":"Fanzine",
    "m":"Magazine issue",
    "col":"Collection",
    "chb":"Chapbook",
    "nonfict":"Nonfiction",
    "omni":"Omnibus",
    "nov":"Novel",
    "ant":"Anthology",
    "c":"Comics issue",
}
LGLI_ISSUE_OTHER_FIELDS = [
    "issue_number_in_year",
    "issue_year_number",
    "issue_number",
    "issue_volume",
    "issue_split",
    "issue_total_number",
    "issue_first_page",
    "issue_last_page",
    "issue_year_end",
    "issue_month_end",
    "issue_day_end",
    "issue_closed",
]
LGLI_STANDARD_INFO_FIELDS = [
    "standardtype",
    "standardtype_standartnumber",
    "standardtype_standartdate",
    "standartnumber",
    "standartstatus",
    "standartstatus_additionalstandartstatus",
]
LGLI_DATE_INFO_FIELDS = [
    "datepublication",
    "dateintroduction",
    "dateactualizationtext",
    "dateregistration",
    "dateactualizationdescr",
    "dateexpiration",
    "datelastedition",
]
# Hardcoded from the `libgenli_elem_descr` table.
LGLI_IDENTIFIERS = {
    "asin": { "label": "ASIN", "url": "https://www.amazon.com/dp/%s", "description": "Amazon Standard Identification Number"},
    "audibleasin": { "label": "Audible-ASIN", "url": "https://www.audible.com/pd/%s", "description": "Audible ASIN"},
    "bl": { "label": "BL", "url": "http://explore.bl.uk/primo_library/libweb/action/dlDisplay.do?vid=BLVU1&amp;docId=BLL01%s", "description": "The British Library"},
    "bleilerearlyyears": { "label": "Bleiler Early Years", "url": "", "description": "Richard Bleiler, Everett F. Bleiler. Science-Fiction: The Early Years. Kent State University Press, 1991, xxiii+998 p."},
    "bleilergernsback": { "label": "Bleiler Gernsback", "url": "", "description": "Everett F. Bleiler, Richard Bleiler. Science-Fiction: The Gernsback Years. Kent State University Press, 1998, xxxii+730pp"},
    "bleilersupernatural": { "label": "Bleiler Supernatural", "url": "", "description": "Everett F. Bleiler. The Guide to Supernatural Fiction. Kent State University Press, 1983, xii+723 p."},
    "bn": { "label": "BN", "url": "http://www.barnesandnoble.com/s/%s", "description": "Barnes and Noble"},
    "bnb": { "label": "BNB", "url": "http://search.bl.uk/primo_library/libweb/action/search.do?fn=search&vl(freeText0)=%s", "description": "The British National Bibliography"},
    "bnf": { "label": "BNF", "url": "http://catalogue.bnf.fr/ark:/12148/%s", "description": "Bibliotheque nationale de France"},
    "coollibbookid": { "label": "Coollib", "url": "https://coollib.ru/b/%s", "description":""},
    "copac": { "label": "COPAC", "url": "http://copac.jisc.ac.uk/id/%s?style=html", "description": "UK/Irish union catalog"},
    "crossrefbookid": { "label": "Crossref", "url": "https://data.crossref.org/depositorreport?pubid=%s", "description":""},
    "dnb": { "label": "DNB", "url": "http://d-nb.info/%s", "description": "Deutsche Nationalbibliothek"},
    "fantlabeditionid": { "label": "FantLab Edition ID", "url": "https://fantlab.ru/edition%s", "description": "Лаболатория фантастики"},
    "flibustabookid": { "label": "Flibusta", "url": "https://flibusta.is/b/%s", "description":""},
    "goodreads": { "label": "Goodreads", "url": "http://www.goodreads.com/book/show/%s", "description": "Goodreads social cataloging site", "website": "/datasets/goodreads" },
    "googlebookid": { "label": "Google Books", "url": "https://books.google.com/books?id=%s", "description": "", "website": "/datasets/gbooks"},
    "isfdbpubideditions": { "label": "ISFDB (editions)", "url": "http://www.isfdb.org/cgi-bin/pl.cgi?%s", "description": ""},
    "issn": { "label": "ISSN", "url": "https://urn.issn.org/urn:issn:%s", "description": "International Standard Serial Number"},
    "jnbjpno": { "label": "JNB/JPNO", "url": "https://iss.ndl.go.jp/api/openurl?ndl_jpno=%s&amp;locale=en", "description": "The Japanese National Bibliography"},
    "jstorstableid": { "label": "JSTOR Stable", "url": "https://www.jstor.org/stable/%s", "description": ""},
    "kbr": { "label": "KBR", "url": "https://opac.kbr.be/Library/doc/SYRACUSE/%s/", "description": "De Belgische Bibliografie/La Bibliographie de Belgique"},
    "lccn": { "label": "LCCN", "url": "http://lccn.loc.gov/%s", "description": "Library of Congress Control Number"},
    "librusecbookid": { "label": "Librusec", "url": "https://lib.rus.ec/b/%s", "description":""},
    "litmirbookid": { "label": "Litmir", "url": "https://www.litmir.me/bd/?b=%s", "description":""},
    "ltf": { "label": "LTF", "url": "http://www.tercerafundacion.net/biblioteca/ver/libro/%s", "description": "La Tercera Fundaci&#243;n"},
    "maximabookid": { "label": "Maxima", "url": "http://maxima-library.org/mob/b/%s", "description":""},
    "ndl": { "label": "NDL", "url": "http://id.ndl.go.jp/bib/%s/eng", "description": "National Diet Library"},
    "nilf": { "label": "NILF", "url": "http://nilf.it/%s/", "description": "Numero Identificativo della Letteratura Fantastica / Fantascienza"},
    "nla": { "label": "NLA", "url": "https://nla.gov.au/nla.cat-vn%s", "description": "National Library of Australia"},
    "noosfere": { "label": "NooSFere", "url": "https://www.noosfere.org/livres/niourf.asp?numlivre=%s", "description": "NooSFere"},
    "oclcworldcat": { "label": "OCLC/WorldCat", "url": "https://www.worldcat.org/oclc/%s", "description": "Online Computer Library Center"},
    "openlibrary": { "label": "Open Library", "url": "https://openlibrary.org/books/%s", "description": ""},
    "pii": { "label": "PII", "url": "", "description": "Publisher Item Identifier", "website": "https://en.wikipedia.org/wiki/Publisher_Item_Identifier"},
    "pmcid": { "label": "PMC ID", "url": "https://www.ncbi.nlm.nih.gov/pmc/articles/%s/", "description": "PubMed Central ID"},
    "pmid": { "label": "PMID", "url": "https://pubmed.ncbi.nlm.nih.gov/%s/", "description": "PubMed ID"},
    "porbase": { "label": "PORBASE", "url": "http://id.bnportugal.gov.pt/bib/porbase/%s", "description": "Biblioteca Nacional de Portugal"},
    "ppn": { "label": "PPN", "url": "http://picarta.pica.nl/xslt/DB=3.9/XMLPRS=Y/PPN?PPN=%s", "description": "De Nederlandse Bibliografie Pica Productie Nummer"},
    "reginald1": { "label": "Reginald-1", "url": "", "description": "R. Reginald. Science Fiction and Fantasy Literature: A Checklist, 1700-1974, with Contemporary Science Fiction Authors II. Gale Research Co., 1979, 1141p."},
    "reginald3": { "label": "Reginald-3", "url": "", "description": "Robert Reginald. Science Fiction and Fantasy Literature, 1975-1991: A Bibliography of Science Fiction, Fantasy, and Horror Fiction Books and Nonfiction Monographs. Gale Research Inc., 1992, 1512 p."},
    "sfbg": { "label": "SFBG", "url": "http://www.sfbg.us/book/%s", "description": "Catalog of books published in Bulgaria"},
    "sfleihbuch": { "label": "SF-Leihbuch", "url": "http://www.sf-leihbuch.de/index.cfm?bid=%s", "description": "Science Fiction-Leihbuch-Datenbank"},
}
# Hardcoded from the `libgenli_elem_descr` table.
LGLI_CLASSIFICATIONS = {
    "classification": { "label": "Classification", "url": "", "description": "" },
    "classificationokp": { "label": "OKP", "url": "https://classifikators.ru/okp/%s", "description": "" },
    "classificationgostgroup": { "label": "GOST group", "url": "", "description": "", "website": "https://en.wikipedia.org/wiki/GOST" },
    "classificationoks": { "label": "OKS", "url": "", "description": "" },
    "libraryofcongressclassification": { "label": "LCC", "url": "https://catalog.loc.gov/vwebv/search?searchCode=CALL%2B&searchArg=%s&searchType=1&limitTo=none&fromYear=&toYear=&limitTo=LOCA%3Dall&limitTo=PLAC%3Dall&limitTo=TYPE%3Dall&limitTo=LANG%3Dall&recCount=25", "description": "Library of Congress Classification", "website": "https://en.wikipedia.org/wiki/Library_of_Congress_Classification" },
    "udc": { "label": "UDC", "url": "https://libgen.li/biblioservice.php?value=%s&type=udc", "description": "Universal Decimal Classification", "website": "https://en.wikipedia.org/wiki/Universal_Decimal_Classification" },
    "ddc": { "label": "DDC", "url": "https://libgen.li/biblioservice.php?value=%s&type=ddc", "description": "Dewey Decimal", "website": "https://en.wikipedia.org/wiki/List_of_Dewey_Decimal_classes" },
    "lbc": { "label": "LBC", "url": "https://libgen.li/biblioservice.php?value=%s&type=bbc", "description": "Library-Bibliographical Classification", "website": "https://www.isko.org/cyclo/lbc" },
}
LGLI_IDENTIFIERS_MAPPING = {
    "oclcworldcat": "oclc",
    "openlibrary": "ol",
    "googlebookid": "gbooks",
}
LGLI_CLASSIFICATIONS_MAPPING = {
    "classification": "class",
    "classificationokp": "okp",
    "classificationgostgroup": "gost",
    "classificationoks": "oks",
    "libraryofcongressclassification": "lcc",
}

LGRS_TO_UNIFIED_IDENTIFIERS_MAPPING = {
    'asin': 'asin',
    'googlebookid': 'gbooks',
    'openlibraryid': 'ol',
    'doi': 'doi',
    'issn': 'issn',
}
LGRS_TO_UNIFIED_CLASSIFICATIONS_MAPPING = {
    'udc': 'udc',
    'ddc': 'ddc',
    'lbc': 'lbc',
    'lcc': 'lcc',
}

UNIFIED_IDENTIFIERS = {
    "md5": { "shortenvalue": True, "label": "MD5", "website": "https://en.wikipedia.org/wiki/MD5", "description": "" },
    "sha1": { "shortenvalue": True, "label": "SHA-1", "website": "https://en.wikipedia.org/wiki/SHA-1", "description": "" },
    "sha256": { "shortenvalue": True, "label": "SHA-256", "website": "https://en.wikipedia.org/wiki/SHA-2", "description": "" },
    "aarecord_id": { "shortenvalue": True, "label": "AA Record ID", "website": "", "description": "Anna’s Archive record ID." },
    "openlib_source_record": { "shortenvalue": True, "label": "Open Library Source Record", "url": "https://openlibrary.org/show-records/%s", "description": "The code for a source record that Open Library imported from.", "website": "/datasets/ol" },
    "link": { "label": "Link", "url": "%s", "description": "Arbitrary external link, optionally suffixed with a description (after ###)." },
    "isbn10": { "label": "ISBN-10", "url": "https://en.wikipedia.org/wiki/Special:BookSources?isbn=%s", "description": "", "website": "https://en.wikipedia.org/wiki/ISBN" },
    "isbn13": { "label": "ISBN-13", "url": "https://en.wikipedia.org/wiki/Special:BookSources?isbn=%s", "description": "", "website": "https://en.wikipedia.org/wiki/ISBN" },
    "isbn_invalid": { "label": "ISBN Invalid", "url": "", "description": "Marked as ISBN value, but has a bad check digit or is otherwise invalid.", "website": "https://en.wikipedia.org/wiki/ISBN" },
    "isbn_cancelled": { "label": "ISBN Cancelled", "url": "", "description": "ISBN value known to be cancelled (as indicated by the source metadata).", "website": "https://en.wikipedia.org/wiki/ISBN" },
    "doi": { "label": "DOI", "url": "https://doi.org/%s", "description": "Digital Object Identifier", "website": "https://en.wikipedia.org/wiki/Digital_object_identifier" },
    "lgrsnf": { "label": "Libgen.rs Non-Fiction", "url": "https://libgen.is/json.php?fields=*&ids=%s", "description": "Repository ID for the non-fiction ('libgen') repository in Libgen.rs. Directly taken from the 'id' field in the 'updated' table. Corresponds to the 'thousands folder' torrents.", "website": "/datasets/lgrs" },
    "lgrsfic": { "label": "Libgen.rs Fiction", "url": "https://libgen.is/fiction/", "description": "Repository ID for the fiction repository in Libgen.rs. Directly taken from the 'id' field in the 'fiction' table. Corresponds to the 'thousands folder' torrents.", "website": "/datasets/lgrs" },
    "lgli": { "label": "Libgen.li File", "url": "https://libgen.li/file.php?id=%s", "description": "Global file ID in Libgen.li. Directly taken from the 'f_id' field in the 'files' table.", "website": "/datasets/lgli" },
    "zlib": { "label": "Z-Library", "url": "https://z-lib.gs/", "description": "ID in Z-Library.", "website": "/datasets/zlib" },
    "csbn": { "label": "CSBN", "url": "", "description": "China Standard Book Number, predecessor of ISBN in China", "website": "https://zh.wikipedia.org/zh-cn/%E7%BB%9F%E4%B8%80%E4%B9%A6%E5%8F%B7" },
    "ean13": { "label": "EAN-13", "url": "", "description": "", "website": "https://en.wikipedia.org/wiki/International_Article_Number" },
    "duxiu_ssid": { "label": "DuXiu SSID", "url": "", "description": "", "website": "/datasets/duxiu" },
    "duxiu_dxid": { "label": "DuXiu DXID", "url": "", "description": "", "website": "/datasets/duxiu" },
    "cadal_ssno": { "label": "CADAL SSNO", "url": "", "description": "", "website": "/datasets/duxiu" },
    "lgli_libgen_id": { "label": "Libgen.li libgen_id", "description": "Repository ID for the 'libgen' repository in Libgen.li. Directly taken from the 'libgen_id' field in the 'files' table. Corresponds to the 'thousands folder' torrents.", "website": "/datasets/lgli" },
    "lgli_fiction_id": { "label": "Libgen.li fiction_id", "description": "Repository ID for the 'fiction' repository in Libgen.li. Directly taken from the 'fiction_id' field in the 'files' table. Corresponds to the 'thousands folder' torrents.", "website": "/datasets/lgli" },
    "lgli_fiction_rus_id": { "label": "Libgen.li fiction_rus_id", "description": "Repository ID for the 'fiction_rus' repository in Libgen.li. Directly taken from the 'fiction_rus_id' field in the 'files' table. Corresponds to the 'thousands folder' torrents.", "website": "/datasets/lgli" },
    "lgli_comics_id": { "label": "Libgen.li comics_id", "description": "Repository ID for the 'comics' repository in Libgen.li. Directly taken from the 'comics_id' field in the 'files' table. Corresponds to the 'thousands folder' torrents.", "website": "/datasets/lgli" },
    "lgli_scimag_id": { "label": "Libgen.li scimag_id", "description": "Repository ID for the 'scimag' repository in Libgen.li. Directly taken from the 'scimag_id' field in the 'files' table. Corresponds to the 'thousands folder' torrents.", "website": "/datasets/lgli" },
    "lgli_standarts_id": { "label": "Libgen.li standarts_id", "description": "Repository ID for the 'standarts' repository in Libgen.li. Directly taken from the 'standarts_id' field in the 'files' table. Corresponds to the 'thousands folder' torrents.", "website": "/datasets/lgli" },
    "lgli_magz_id": { "label": "Libgen.li magz_id", "description": "Repository ID for the 'magz' repository in Libgen.li. Directly taken from the 'magz_id' field in the 'files' table. Corresponds to the 'thousands folder' torrents.", "website": "/datasets/lgli" },
    "filepath": { "label": "Filepath", "description": "Original filepath in source library." },
    "server_path": { "label": "Server Path", "description": "Path on Anna’s Archive partner servers." },
    "aacid": { "shortenvalue": True, "label": "AacId", "website": "/blog/annas-archive-containers.html", "description": "Anna’s Archive Container identifier." },
    "magzdb": { "label": "MagzDB Edition ID", "url": "http://magzdb.org/num/%s", "description": "ID of an individual edition of a magazine in MagzDB.", "website": "/datasets/magzdb" },
    "nexusstc": { "shortenvalue": True, "label": "Nexus/STC", "url": "https://libstc.cc/#/stc/nid:%s", "description": "ID of an individual edition of a file in Nexus/STC.", "website": "/datasets/nexusstc" },
    "ipfs_cid": { "shortenvalue": True, "label": "IPFS CID", "url": "ipfs://%s", "description": "Content Identifier (CID) of the InterPlanetary File System (IPFS).", "website": "https://ipfs.tech/" },
    "manualslib": { "label": "ManualsLib", "url": "https://www.manualslib.com/manual/%s/manual.html", "description": "File ID in ManualsLib", "website": "https://www.manualslib.com/" },
    "iso": { "label": "ISO", "url": "https://iso.org/standard/%s.html", "description": "ISO standard number.", "website": "https://iso.org/" },
    "british_standard": { "label": "British Standard", "url": "", "description": "British Standards (BS) are the standards produced by the BSI Group.", "website": "https://en.wikipedia.org/wiki/British_Standards" },
    "edsebk": { "label": "EBSCOhost eBook Index Accession Number", "url": "https://library.macewan.ca/full-record/edsebk/%s", "description": "ID in the EBSCOhost eBook Index (edsebk).", "website": "/datasets/edsebk" },
    "cerlalc": { "label": "Cerlalc ID", "url": "", "description": "Cerlalc ID.", "website": "/datasets/cerlalc" },
    "czech_oo42hcks": { "label": "Czech Metadata ID", "url": "", "description": "Czech metadata ID.", "website": "/datasets/czech_oo42hcks" },
    "isbngrp": { "label": "ISBN GRP ID", "url": "", "description": "ISBN GRP ID.", "website": "/datasets/isbngrp" },
    "libby": { "label": "Libby ID", "url": "", "description": "Libby ID.", "website": "/datasets/libby" },
    "rgb": { "label": "Russian State Library ID", "url": "", "description": "Russian State Library ID.", "website": "/datasets/rgb" },
    "trantor": { "label": "Trantor ID", "url": "", "description": "Trantor ID.", "website": "/datasets/trantor" },
    "czech_oo42hcks_filename": { "label": "Czech Metadata Filename", "url": "", "description": "Czech metadata canonical filename.", "website": "/datasets/czech_oo42hcks" },
    **{LGLI_IDENTIFIERS_MAPPING.get(key, key): value for key, value in LGLI_IDENTIFIERS.items()},
    # Plus more added below!
}
UNIFIED_CLASSIFICATIONS = {
    "isbn13_prefix": { "label": "ISBN-13 Prefix", "url": "", "description": "ISBN-13 range, associated with a country or agency.", "website": "https://en.wikipedia.org/wiki/ISBN" },
    "lgrsnf_topic": { "label": "Libgen.rs Non-Fiction Topic", "description": "Libgen’s own classification system of 'topics' for non-fiction books. Obtained from the 'topic' metadata field, using the 'topics' database table, which seems to have its roots in the Kolxo3 library that Libgen was originally based on. https://wiki.mhut.org/content:bibliographic_data says that this field will be deprecated in favor of Dewey Decimal.", "website": "/datasets/lgrs" },
    "torrent": { "label": "Torrent", "url": "/dyn/small_file/torrents/%s", "description": "Bulk torrent for long-term preservation.", "website": "/torrents" },
    "collection": { "label": "Collection", "url": "/datasets/%s", "description": "The collection on Anna’s Archive that provided data for this record.", "website": "/datasets" },
    "ia_collection": { "label": "IA Collection", "url": "https://archive.org/details/%s", "description": "Internet Archive collection which this file is part of.", "website": "https://help.archive.org/help/collections-a-basic-guide/" },
    "lang": { "label": "Language", "website": "https://en.wikipedia.org/wiki/IETF_language_tag", "description": "IETF language tag." },
    "year": { "label": "Year", "description": "Publication year." },
    # TODO:SOURCE Remove on index refresh.
    "duxiu_filegen": { "label": "DuXiu File Generated", "website": "/datasets/duxiu", "description": "Date Anna’s Archive generated the file in the DuXiu collection." },
    "date_duxiu_filegen": { "label": "DuXiu File Generated", "website": "/datasets/duxiu", "description": "Date Anna’s Archive generated the file in the DuXiu collection." },
    # TODO:SOURCE Remove on index refresh.
    "duxiu_meta_scrape": { "label": "DuXiu Source Scrape Date", "website": "/datasets/duxiu", "description": "Date Anna’s Archive scraped the DuXiu collection." },
    "date_duxiu_meta_scrape": { "label": "DuXiu Source Scrape Date", "website": "/datasets/duxiu", "description": "Date Anna’s Archive scraped the DuXiu collection." },
    # TODO:SOURCE Remove on index refresh.
    "file_created_date": { "label": "File Exiftool Created Date", "website": "/datasets/upload", "description": "Date of creation from the file’s own metadata." },
    "date_file_created": { "label": "File Exiftool Created Date", "website": "/datasets/upload", "description": "Date of creation from the file’s own metadata." },
    # TODO:SOURCE Remove on index refresh.
    "ia_file_scrape": { "label": "IA File Scraped", "website": "/datasets/ia", "description": "Date Anna’s Archive scraped the file from the Internet Archive." },
    "date_ia_file_scrape": { "label": "IA File Scraped", "website": "/datasets/ia", "description": "Date Anna’s Archive scraped the file from the Internet Archive." },
    "date_ia_record_scrape": { "label": "IA Record Scraped", "website": "/datasets/ia", "description": "Date Anna’s Archive scraped the record from the Internet Archive." },
    # TODO:SOURCE Remove on index refresh.
    "ia_source": { "label": "IA 'publicdate' Date", "website": "/datasets/ia", "description": "The 'publicdate' metadata field on the Internet Archive website, which usually indicates when they published the file, usually shortly after scanning." },
    "date_ia_source": { "label": "IA 'publicdate' Date", "website": "/datasets/ia", "description": "The 'publicdate' metadata field on the Internet Archive website, which usually indicates when they published the file, usually shortly after scanning." },
    # TODO:SOURCE Remove on index refresh.
    "isbndb_scrape": { "label": "ISBNdb Scrape Date", "website": "/datasets/isbndb", "description": "The date that Anna’s Archive scraped this ISBNdb record." },
    "date_isbndb_scrape": { "label": "ISBNdb Scrape Date", "website": "/datasets/isbndb", "description": "The date that Anna’s Archive scraped this ISBNdb record." },
    # TODO:SOURCE Remove on index refresh.
    "lgli_source": { "label": "Libgen.li Source Date", "website": "/datasets/lgli", "description": "Date Libgen.li published this file." },
    "date_lgli_source": { "label": "Libgen.li Source Date", "website": "/datasets/lgli", "description": "Date Libgen.li published this file." },
    # TODO:SOURCE Remove on index refresh.
    "lgrsfic_source": { "label": "Libgen.rs Fiction Date", "website": "/datasets/lgrs", "description": "Date Libgen.rs Fiction published this file." },
    "date_lgrsfic_source": { "label": "Libgen.rs Fiction Date", "website": "/datasets/lgrs", "description": "Date Libgen.rs Fiction published this file." },
    # TODO:SOURCE Remove on index refresh.
    "lgrsnf_source": { "label": "Libgen.rs Non-Fiction Date", "website": "/datasets/lgrs", "description": "Date Libgen.rs Non_Fiction published this file." },
    "date_lgrsnf_source": { "label": "Libgen.rs Non-Fiction Date", "website": "/datasets/lgrs", "description": "Date Libgen.rs Non_Fiction published this file." },
    # TODO:SOURCE Remove on index refresh.
    "oclc_scrape": { "label": "OCLC Scrape Date", "website": "/datasets/oclc", "description": "The date that Anna’s Archive scraped this OCLC/WorldCat record." },
    "date_oclc_scrape": { "label": "OCLC Scrape Date", "website": "/datasets/oclc", "description": "The date that Anna’s Archive scraped this OCLC/WorldCat record." },
    # TODO:SOURCE Remove on index refresh.
    "ol_source": { "label": "OpenLib 'created' Date", "website": "/datasets/ol", "description": "The 'created' metadata field on the Open Library, indicating when the first version of this record was created." },
    "date_ol_source": { "label": "OpenLib 'created' Date", "website": "/datasets/ol", "description": "The 'created' metadata field on the Open Library, indicating when the first version of this record was created." },
    # TODO:SOURCE Remove on index refresh.
    "upload_record_date": { "label": "Upload Collection Date", "website": "/datasets/upload", "description": "Date Anna’s Archive indexed this file in our 'upload' collection." },
    "date_upload_record": { "label": "Upload Collection Date", "website": "/datasets/upload", "description": "Date Anna’s Archive indexed this file in our 'upload' collection." },
    # TODO:SOURCE Remove on index refresh.
    "zlib_source": { "label": "Z-Library Source Date", "website": "/datasets/zlib", "description": "Date Z-Library published this file." },
    "date_zlib_source": { "label": "Z-Library Source Date", "website": "/datasets/zlib", "description": "Date Z-Library published this file." },
    "magzdb_pub": { "label": "MagzDB Publication ID", "url": "http://magzdb.org/j/%s", "description": "ID of a publication in MagzDB.", "website": "/datasets/magzdb" },
    # TODO:SOURCE Remove on index refresh.
    "magzdb_meta_scrape": { "label": "MagzDB Source Scrape Date", "website": "/datasets/magzdb", "description": "Date Anna’s Archive scraped the MagzDB metadata." },
    "date_magzdb_meta_scrape": { "label": "MagzDB Source Scrape Date", "website": "/datasets/magzdb", "description": "Date Anna’s Archive scraped the MagzDB metadata." },
    "magzdb_keyword": { "label": "MagzDB Keyword", "url": "", "description": "Publication keyword in MagzDB (in Russian).", "website": "/datasets/magzdb" },
    # TODO:SOURCE Remove on index refresh.
    "nexusstc_source_issued_at_date": { "label": "Nexus/STC Source issued_at Date", "website": "/datasets/nexusstc", "description": "Date Nexus/STC reports in their issued_at field, which is the “issuing time of the item described by record.”" },
    "date_nexusstc_source_issued_at": { "label": "Nexus/STC Source issued_at Date", "website": "/datasets/nexusstc", "description": "Date Nexus/STC reports in their issued_at field, which is the “issuing time of the item described by record.”" },
    # TODO:SOURCE Remove on index refresh.
    "nexusstc_source_update_date": { "label": "Nexus/STC Source Updated Date", "website": "/datasets/nexusstc", "description": "Date Nexus/STC last updated this record." },
    "date_nexusstc_source_update": { "label": "Nexus/STC Source Updated Date", "website": "/datasets/nexusstc", "description": "Date Nexus/STC last updated this record." },
    "nexusstc_tag": { "label": "Nexus/STC Tag", "url": "", "description": "Tag in Nexus/STC.", "website": "/datasets/nexusstc" },
    "orcid": { "label": "ORCID", "url": "https://orcid.org/%s", "description": "Open Researcher and Contributor ID.", "website": "https://orcid.org/" },
    "date_edsebk_meta_scrape": { "label": "EBSCOhost eBook Index Source Scrape Date", "website": "/datasets/edsebk", "description": "Date Anna’s Archive scraped the EBSCOhost metadata." },
    "edsebk_subject": { "label": "EBSCOhost eBook Index Subject", "url": "", "description": "Tag in EBSCOhost eBook Index.", "website": "/datasets/edsebk" },
    "openlib_subject": { "label": "Open Library Subject", "url": "", "description": "Tag in Open Library.", "website": "/datasets/ol" },
    "rgb_subject": { "label": "Russian State Library Subject", "url": "", "description": "Tag in Russian State Library.", "website": "/datasets/rgb" },
    "file_problem": { "label": "File Problem", "url": "", "description": "Problem type indicated by a source library.", "website": "" },
    "better_aarecord_id": { "label": "Better AA Record ID", "shortenvalue": True, "url": "", "description": "Source library has indicated this record to be better.", "website": "" },
    "content_type": { "label": "Content Type", "url": "", "description": "Content type, determined by Anna’s Archive.", "website": "" },
    "date_cerlalc_meta_scrape": { "label": "Cerlalc Source Scrape Date", "website": "/datasets/cerlalc", "description": "Date Anna’s Archive scraped the Cerlalc collection." },
    "date_czech_oo42hcks_meta_scrape": { "label": "Czech Metadata Source Scrape Date", "website": "/datasets/czech_oo42hcks", "description": "Date Anna’s Archive scraped the Czech metadata collection." },
    "date_gbooks_meta_scrape": { "label": "Google Books Source Scrape Date", "website": "/datasets/gbooks", "description": "Date Anna’s Archive scraped the Google Books collection." },
    "date_goodreads_meta_scrape": { "label": "Goodreads Source Scrape Date", "website": "/datasets/goodreads", "description": "Date Anna’s Archive scraped the Goodreads collection." },
    "date_isbngrp_meta_scrape": { "label": "ISBN GRP Source Scrape Date", "website": "/datasets/isbngrp", "description": "Date Anna’s Archive scraped the ISBN GRP collection." },
    "date_libby_meta_scrape": { "label": "Libby Source Scrape Date", "website": "/datasets/libby", "description": "Date Anna’s Archive scraped the Libby collection." },
    "date_rgb_meta_scrape": { "label": "Russian State Library Source Scrape Date", "website": "/datasets/rgb", "description": "Date Anna’s Archive scraped the Russian State Library collection." },
    "date_trantor_meta_scrape": { "label": "Trantor Source Scrape Date", "website": "/datasets/trantor", "description": "Date Anna’s Archive scraped the Trantor collection." },
    **{LGLI_CLASSIFICATIONS_MAPPING.get(key, key): value for key, value in LGLI_CLASSIFICATIONS.items()},
    # Plus more added below!
}

OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING = {
    'abebooks,de': 'abebooks.de',
    'amazon': 'asin',
    'amazon.ca_asin': 'asin',
    'amazon.co.jp_asin': 'asin',
    'amazon.co.uk_asin': 'asin',
    'amazon.de_asin': 'asin',
    'amazon.it_asin': 'asin',
    'annas_archive': 'md5', # TODO: Do reverse lookup based on this.
    'bibliothèque_nationale_de_france_(bnf)': 'bibliothèque_nationale_de_france',
    'british_library': 'bl',
    'british_national_bibliography': 'bnb',
    'depósito_legal_n.a.': 'depósito_legal',
    'doi': 'doi', # TODO: Do reverse lookup based on this.
    'gallica_(bnf)': 'bibliothèque_nationale_de_france',
    'google': 'gbooks',
    'harvard_university_library': 'harvard',
    'isbn_10': 'isbn10',
    'isbn_13': 'isbn13',
    'isfdb': 'isfdbpubideditions',
    'lccn_permalink': 'lccn',
    'library_of_congress': 'lccn',
    'library_of_congress_catalog_no.': 'lccn',
    'library_of_congress_catalogue_number': 'lccn',
    'national_diet_library,_japan': 'ndl',
    'oclc_numbers': 'oclc',
    **{key: key for key in UNIFIED_IDENTIFIERS.keys()},
    # Plus more added below!
}
OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING = {
    'dewey_decimal_class': 'ddc',
    'dewey_number': 'ddc',
    'lc_classifications': 'lcc',
    'library_bibliographical_classification': 'lbc',
    'udc': 'udc',
    'library_of_congress_classification_(lcc)': 'lcc',
    'dewey_decimal_classification_(ddc)': 'ddc',
    **{key: key for key in UNIFIED_CLASSIFICATIONS.keys()},
    # Plus more added below!
}
# Hardcoded labels for OL. The "label" fields in ol_edition.json become "description" instead.
OPENLIB_LABELS = {
    "abaa": "ABAA",
    "abebooks.de": "Abebooks",
    "abwa_bibliographic_number": "ABWA",
    "alibris_id": "Alibris",
    "bayerische_staatsbibliothek": "BSB-ID",
    "bcid": "BCID",
    "better_world_books": "BWB",
    "bhl": "BHL",
    "bibliothèque_nationale_de_france": "BnF",
    "bibsys": "Bibsys",
    "bodleian,_oxford_university": "Bodleian",
    "bookbrainz": "BookBrainz",
    "booklocker.com": "BookLocker",
    "bookmooch": "Book Mooch",
    "booksforyou": "Books For You",
    "bookwire": "BookWire",
    "boston_public_library": "BPL",
    "canadian_national_library_archive": "CNLA",
    "choosebooks": "Choosebooks",
    "cornell_university_library": "Cornell",
    "cornell_university_online_library": "Cornell",
    "dc_books": "DC",
    "depósito_legal": "Depósito Legal",
    "digital_library_pomerania": "Pomerania",
    "discovereads": "Discovereads",
    "dnb": "DNB",
    "dominican_institute_for_oriental_studies_library": "Al Kindi",
    "etsc": "ETSC",
    "fennica": "Fennica",
    "finnish_public_libraries_classification_system": "FPL",
    "folio": "Folio",
    "freebase": "Freebase",
    "goethe_university_library,_frankfurt": "Goethe",
    "goodreads": "Goodreads",
    "grand_comics_database": "Grand Comics DB",
    "harvard": "Harvard",
    "hathi_trust": "Hathi",
    "identificativo_sbn": "SBN",
    "ilmiolibro": "Ilmiolibro",
    "inducks": "INDUCKS",
    "infosoup": "Infosoup",
    "issn": "ISSN",
    "istc": "ISTC",
    "lccn": "LCCN",
    "learnawesome": "LearnAwesome",
    "library_and_archives_canada_cataloguing_in_publication": "CIP",
    "librarything": "Library Thing",
    "libris": "Libris",
    "librivox": "LibriVox",
    "lulu": "Lulu",
    "magcloud": "Magcloud",
    "musicbrainz": "MusicBrainz",
    "nbuv": "NBUV",
    "nla": "NLA",
    "nur": "NUR",
    "ocaid": "IA",
    "open_alex": "OpenAlex",
    "open_textbook_library": "OTL",
    "openstax": "OpenStax",
    "overdrive": "OverDrive",
    "paperback_swap": "Paperback Swap",
    "project_gutenberg": "Gutenberg",
    "publishamerica": "PublishAmerica",
    "rvk": "RVK",
    "sab": "SAB",
    "scribd": "Scribd",
    "shelfari": "Shelfari",
    "siso": "SISO",
    "smashwords_book_download": "Smashwords",
    "standard_ebooks": "Standard Ebooks",
    "storygraph": "Storygraph",
    "ulrls": "ULRLS",
    "ulrls_classmark": "ULRLS Classmark",
    "w._w._norton": "W.W.Norton",
    "wikidata": "Wikidata",
    "wikisource": "Wikisource",
    "yakaboo": "Yakaboo",
    "zdb-id": "ZDB-ID",
}
# Retrieved from https://openlibrary.org/config/edition.json on 2023-07-02
ol_edition_json = orjson.loads(open(os.path.dirname(os.path.realpath(__file__)) + '/page/ol_edition.json').read())
for identifier in ol_edition_json['identifiers']:
    if 'url' in identifier:
        identifier['url'] = identifier['url'].replace('@@@', '%s')
    unified_name = identifier['name']
    if unified_name in OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING:
        unified_name = OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[unified_name]
        if unified_name not in UNIFIED_IDENTIFIERS:
            raise Exception(f"unified_name '{unified_name}' should be in UNIFIED_IDENTIFIERS")
    else:
        OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[unified_name] = unified_name
        if unified_name not in UNIFIED_IDENTIFIERS:
            # If unified name is not in OPENLIB_TO_UNIFIED_*_MAPPING, then it *has* to be in OPENLIB_LABELS.
            label = OPENLIB_LABELS[unified_name]
            description = ''
            if identifier.get('description', '') != label:
                description = identifier.get('description', '')
            UNIFIED_IDENTIFIERS[unified_name] = { **identifier, 'label': label, 'description': description }
for classification in ol_edition_json['classifications']:
    if 'website' in classification:
        classification['website'] = classification['website'].split(' ')[0] # Sometimes there's a suffix in text..
    unified_name = classification['name']
    if unified_name in OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
        unified_name = OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[unified_name]
        if unified_name not in UNIFIED_CLASSIFICATIONS:
            raise Exception(f"unified_name '{unified_name}' should be in UNIFIED_CLASSIFICATIONS")
    else:
        OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[unified_name] = unified_name
        if unified_name not in UNIFIED_CLASSIFICATIONS:
            # If unified name is not in OPENLIB_TO_UNIFIED_*_MAPPING, then it *has* to be in OPENLIB_LABELS.
            label = OPENLIB_LABELS[unified_name]
            description = ''
            if classification.get('description', '') != label:
                description = classification.get('description', '')
            UNIFIED_CLASSIFICATIONS[unified_name] = { **classification, 'label': label, 'description': description }

def init_identifiers_and_classification_unified(output_dict):
    if 'identifiers_unified' not in output_dict:
        output_dict['identifiers_unified'] = {}
    if 'classifications_unified' not in output_dict:
        output_dict['classifications_unified'] = {}

def add_identifier_unified(output_dict, name, value):
    if value is None:
        print(f"Warning: 'None' found for add_identifier_unified {name}.. {traceback.format_exc()}")
        return
    name = name.strip()
    value = str(value).strip()
    if name == 'lccn' and 'http://lccn.loc.gov/' in value:
        value = value.replace('http://lccn.loc.gov/', '') # for lccn_permalink
        value = value.split('/')[0]
    if len(value) == 0:
        return
    unified_name = OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING.get(name, name)
    if unified_name in UNIFIED_IDENTIFIERS:
        if unified_name not in output_dict['identifiers_unified']:
            output_dict['identifiers_unified'][unified_name] = []
        if value not in output_dict['identifiers_unified'][unified_name]:
            output_dict['identifiers_unified'][unified_name].append(value)
    else:
        print(f"Warning: Unknown identifier in add_identifier_unified: {name}")

def add_classification_unified(output_dict, name, value):
    if value is None:
        print(f"Warning: 'None' found for add_classification_unified {name}")
        return
    name = name.strip()
    value = str(value).strip()
    if len(value) == 0:
        return
    unified_name = OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING.get(name, name)
    if unified_name in UNIFIED_CLASSIFICATIONS:
        if unified_name not in output_dict['classifications_unified']:
            output_dict['classifications_unified'][unified_name] = []
        if value not in output_dict['classifications_unified'][unified_name]:
            output_dict['classifications_unified'][unified_name].append(value)
    else:
        print(f"Warning: Unknown classification in add_classification_unified: {name}")

def normalize_isbn(string):
    canonical_isbn13 = isbnlib.get_canonical_isbn(string, output='isbn13')
    try:
        if len(canonical_isbn13) != 13 or len(isbnlib.info(canonical_isbn13)) == 0:
            return ''
    except Exception:
        return ''
    return canonical_isbn13

def add_isbns_unified(output_dict, potential_isbns):
    if len(potential_isbns) == 0:
        return
    isbn10s = set()
    isbn13s = set()
    csbns = set()
    isbns_invalid = set()
    isbn13s_invalid = set()
    for potential_isbn in potential_isbns:
        potential_isbn = potential_isbn.replace('-','').replace(' ', '')
        if '·' in potential_isbn:
            csbns.add(potential_isbn)
        else:
            isbn13 = normalize_isbn(potential_isbn)
            if isbn13 != '':
                isbn13s.add(isbn13)
                isbn10 = isbnlib.to_isbn10(isbn13)
                if isbnlib.is_isbn10(isbn10 or ''):
                    isbn10s.add(isbn10)
            else:
                isbns_invalid.add(potential_isbn)
    for isbn10 in isbn10s:
        add_identifier_unified(output_dict, 'isbn10', isbn10)
    for isbn13 in isbn13s:
        add_identifier_unified(output_dict, 'isbn13', isbn13)
    for isbn_invalid in isbns_invalid:
        add_identifier_unified(output_dict, 'isbn_invalid', isbn_invalid)
    for csbn in csbns:
        add_identifier_unified(output_dict, 'csbn', csbn)

def add_issn_unified(output_dict, issn):
    add_identifier_unified(output_dict, 'issn', issn.replace('-', '').strip())

def add_orcid_unified(output_dict, orcid):
    add_classification_unified(output_dict, 'orcid', orcid.replace('-', '').strip())

def merge_unified_fields(list_of_fields_unified):
    merged_sets = {}
    for fields_unified in list_of_fields_unified:
        for unified_name, values in fields_unified.items():
            if unified_name not in merged_sets:
                merged_sets[unified_name] = set()
            for value in values:
                merged_sets[unified_name].add(value)
    return { unified_name: list(merged_set) for unified_name, merged_set in merged_sets.items() }

CODES_HIGHLIGHT = ['isbn13', 'isbn10', 'csbn', 'doi', 'issn', 'duxiu_ssid', 'cadal_ssno', 'oclc']
def make_code_for_display(code_from_additional):
    return {
        'key': code_from_additional['key'],
        'value': code_from_additional['value'],
        'masked_isbn': isbnlib.mask(code_from_additional['value']) if (code_from_additional['key'] in ['isbn10', 'isbn13']) and (isbnlib.is_isbn10(code_from_additional['value']) or isbnlib.is_isbn13(code_from_additional['value'])) else '',
        'info': UNIFIED_IDENTIFIERS.get(code_from_additional['key']) or UNIFIED_CLASSIFICATIONS.get(code_from_additional['key']) or {},
        'highlight': (),
    }

def get_isbnlike(text):
    matches = set()
    # Special regex that works on filenames as well.
    for match in re.findall(r'(?:ISBN|isbn)[ _-]*([-_0-9X]{10,19})', text):
        for potential_isbn in isbnlib.get_isbnlike(match):
            if isbnlib.is_isbn13(potential_isbn) or isbnlib.is_isbn10(potential_isbn):
                matches.add(potential_isbn)

    for potential_isbn in isbnlib.get_isbnlike(text):
        # Only extract ISBN-13 when using regular matching, ISBN-10 yields too many false positives.
        if isbnlib.is_isbn13(potential_isbn):
            matches.add(potential_isbn)
    return list(matches)

SEARCH_INDEX_SHORT_LONG_MAPPING = {
    '': 'aarecords',
    'journals': 'aarecords_journals',
    'digital_lending': 'aarecords_digital_lending',
    'meta': 'aarecords_metadata',
}
def get_aarecord_id_prefix_is_metadata(id_prefix):
    return (id_prefix in ['isbndb', 'ol', 'oclc', 'duxiu_ssid', 'cadal_ssno', 'magzdb', 'nexusstc', 'edsebk', 'cerlalc', 'czech_oo42hcks', 'gbooks', 'goodreads', 'isbngrp', 'libby', 'rgb', 'trantor'])

def get_aarecord_search_indexes_for_id_prefix(id_prefix):
    if get_aarecord_id_prefix_is_metadata(id_prefix):
        return ['aarecords_metadata']
    elif id_prefix == 'ia':
        return ['aarecords_digital_lending']
    elif id_prefix in ['md5', 'doi', 'nexusstc_download']:
        return ['aarecords', 'aarecords_journals']
    else:
        raise Exception(f"Unknown aarecord_id prefix: {id_prefix}")
def get_aarecord_search_index(id_prefix, content_type):
    if get_aarecord_id_prefix_is_metadata(id_prefix):
        return 'aarecords_metadata'
    elif id_prefix == 'ia':
        return 'aarecords_digital_lending'
    elif id_prefix in ['md5', 'doi', 'nexusstc_download']:
        if content_type == 'journal_article':
            return 'aarecords_journals'
        else:
            return 'aarecords'
    else:
        raise Exception(f"Unknown aarecord_id prefix: {id_prefix}")
SEARCH_INDEX_TO_ES_MAPPING = {
    'aarecords': es,
    'aarecords_journals': es_aux,
    'aarecords_digital_lending': es_aux,
    'aarecords_metadata': es_aux,
}
MAIN_SEARCH_INDEXES = ['aarecords', 'aarecords_journals']
# TODO: Look into https://discuss.elastic.co/t/score-and-relevance-across-the-shards/5371
ES_VIRTUAL_SHARDS_NUM = 12
def virtshard_for_hashed_aarecord_id(hashed_aarecord_id):
    return int.from_bytes(hashed_aarecord_id, byteorder='big', signed=False) % ES_VIRTUAL_SHARDS_NUM
def virtshard_for_aarecord_id(aarecord_id):
    return virtshard_for_hashed_aarecord_id(hashlib.md5(aarecord_id.encode()).digest())
def all_virtshards_for_index(index_name):
    return [f'{index_name}__{virtshard}' for virtshard in range(0, ES_VIRTUAL_SHARDS_NUM)]

def attempt_fix_chinese_uninterrupted_text(text):
    try:
        return text.encode().decode('gbk')
    except Exception:
        return text

def attempt_fix_chinese_filepath(filepath):
    return '/'.join([attempt_fix_chinese_uninterrupted_text(part) for part in filepath.split('/')])

FILEPATH_PREFIXES = ['duxiu', 'ia', 'lgli', 'lgrsfic', 'lgrsnf', 'scihub', 'scimag', 'upload', 'magzdb', 'nexusstc', 'trantor']
def prefix_filepath(prefix, filepath):
    if prefix not in FILEPATH_PREFIXES:
        raise Exception(f"prefix_filepath: {prefix=} not in {FILEPATH_PREFIXES=}")
    filepath = filepath.strip()
    if filepath == '':
        return ""
    elif filepath.startswith('\\'):
        return f"{prefix}/{filepath[1:]}"
    elif filepath.startswith('/'):
        return f"{prefix}{filepath}"
    else:
        return f"{prefix}/{filepath}"


# TODO: translate?
def marc_country_code_to_english(marc_country_code):
    marc_country_code = marc_country_code.strip()
    return MARC_COUNTRY_CODES.get(marc_country_code) or MARC_DEPRECATED_COUNTRY_CODES.get(marc_country_code) or marc_country_code

# From https://www.loc.gov/marc/countries/countries_code.html
MARC_COUNTRY_CODES = {
    "aa"  : "Albania",
    "abc" : "Alberta",
    "aca" : "Australian Capital Territory",
    "ae"  : "Algeria",
    "af"  : "Afghanistan",
    "ag"  : "Argentina",
    "ai"  : "Armenia",
    "aj"  : "Azerbaijan",
    "aku" : "Alaska",
    "alu" : "Alabama",
    "am"  : "Anguilla",
    "an"  : "Andorra",
    "ao"  : "Angola",
    "aq"  : "Antigua and Barbuda",
    "aru" : "Arkansas",
    "as"  : "American Samoa",
    "at"  : "Australia",
    "au"  : "Austria",
    "aw"  : "Aruba",
    "ay"  : "Antarctica",
    "azu" : "Arizona",
    "ba"  : "Bahrain",
    "bb"  : "Barbados",
    "bcc" : "British Columbia",
    "bd"  : "Burundi",
    "be"  : "Belgium",
    "bf"  : "Bahamas",
    "bg"  : "Bangladesh",
    "bh"  : "Belize",
    "bi"  : "British Indian Ocean Territory",
    "bl"  : "Brazil",
    "bm"  : "Bermuda Islands",
    "bn"  : "Bosnia and Herzegovina",
    "bo"  : "Bolivia",
    "bp"  : "Solomon Islands",
    "br"  : "Burma",
    "bs"  : "Botswana",
    "bt"  : "Bhutan",
    "bu"  : "Bulgaria",
    "bv"  : "Bouvet Island",
    "bw"  : "Belarus",
    "bx"  : "Brunei",
    "ca"  : "Caribbean Netherlands",
    "cau" : "California",
    "cb"  : "Cambodia",
    "cc"  : "China",
    "cd"  : "Chad",
    "ce"  : "Sri Lanka",
    "cf"  : "Congo (Brazzaville)",
    "cg"  : "Democratic Republic of the Congo",
    "ch"  : "China (Republic : 1949- )",
    "ci"  : "Croatia",
    "cj"  : "Cayman Islands",
    "ck"  : "Colombia",
    "cl"  : "Chile",
    "cm"  : "Cameroon",
    "co"  : "Curaçao",
    "cou" : "Colorado",
    "cq"  : "Comoros",
    "cr"  : "Costa Rica",
    "ctu" : "Connecticut",
    "cu"  : "Cuba",
    "cv"  : "Cabo Verde",
    "cw"  : "Cook Islands",
    "cx"  : "Central African Republic",
    "cy"  : "Cyprus",
    "dcu" : "District of Columbia",
    "deu" : "Delaware",
    "dk"  : "Denmark",
    "dm"  : "Benin",
    "dq"  : "Dominica",
    "dr"  : "Dominican Republic",
    "ea"  : "Eritrea",
    "ec"  : "Ecuador",
    "eg"  : "Equatorial Guinea",
    "em"  : "Timor-Leste",
    "enk" : "England",
    "er"  : "Estonia",
    "es"  : "El Salvador",
    "et"  : "Ethiopia",
    "fa"  : "Faroe Islands",
    "fg"  : "French Guiana",
    "fi"  : "Finland",
    "fj"  : "Fiji",
    "fk"  : "Falkland Islands",
    "flu" : "Florida",
    "fm"  : "Micronesia",
    "fp"  : "French Polynesia",
    "fr"  : "France",
    "fs"  : "Terres australes et antarctiques françaises",
    "ft"  : "Djibouti",
    "gau" : "Georgia",
    "gb"  : "Kiribati",
    "gd"  : "Grenada",
    "gg"  : "Guernsey",
    "gh"  : "Ghana",
    "gi"  : "Gibraltar",
    "gl"  : "Greenland",
    "gm"  : "Gambia",
    "go"  : "Gabon",
    "gp"  : "Guadeloupe",
    "gr"  : "Greece",
    "gs"  : "Georgia (Republic)",
    "gt"  : "Guatemala",
    "gu"  : "Guam",
    "gv"  : "Guinea",
    "gw"  : "Germany",
    "gy"  : "Guyana",
    "gz"  : "Gaza Strip",
    "hiu" : "Hawaii",
    "hm"  : "Heard and McDonald Islands",
    "ho"  : "Honduras",
    "ht"  : "Haiti",
    "hu"  : "Hungary",
    "iau" : "Iowa",
    "ic"  : "Iceland",
    "idu" : "Idaho",
    "ie"  : "Ireland",
    "ii"  : "India",
    "ilu" : "Illinois",
    "im"  : "Isle of Man",
    "inu" : "Indiana",
    "io"  : "Indonesia",
    "iq"  : "Iraq",
    "ir"  : "Iran",
    "is"  : "Israel",
    "it"  : "Italy",
    "iv"  : "Côte d'Ivoire",
    "iy"  : "Iraq-Saudi Arabia Neutral Zone",
    "ja"  : "Japan",
    "je"  : "Jersey",
    "ji"  : "Johnston Atoll",
    "jm"  : "Jamaica",
    "jo"  : "Jordan",
    "ke"  : "Kenya",
    "kg"  : "Kyrgyzstan",
    "kn"  : "North Korea",
    "ko"  : "South Korea",
    "ksu" : "Kansas",
    "ku"  : "Kuwait",
    "kv"  : "Kosovo",
    "kyu" : "Kentucky",
    "kz"  : "Kazakhstan",
    "lau" : "Louisiana",
    "lb"  : "Liberia",
    "le"  : "Lebanon",
    "lh"  : "Liechtenstein",
    "li"  : "Lithuania",
    "lo"  : "Lesotho",
    "ls"  : "Laos",
    "lu"  : "Luxembourg",
    "lv"  : "Latvia",
    "ly"  : "Libya",
    "mau" : "Massachusetts",
    "mbc" : "Manitoba",
    "mc"  : "Monaco",
    "mdu" : "Maryland",
    "meu" : "Maine",
    "mf"  : "Mauritius",
    "mg"  : "Madagascar",
    "miu" : "Michigan",
    "mj"  : "Montserrat",
    "mk"  : "Oman",
    "ml"  : "Mali",
    "mm"  : "Malta",
    "mnu" : "Minnesota",
    "mo"  : "Montenegro",
    "mou" : "Missouri",
    "mp"  : "Mongolia",
    "mq"  : "Martinique",
    "mr"  : "Morocco",
    "msu" : "Mississippi",
    "mtu" : "Montana",
    "mu"  : "Mauritania",
    "mv"  : "Moldova",
    "mw"  : "Malawi",
    "mx"  : "Mexico",
    "my"  : "Malaysia",
    "mz"  : "Mozambique",
    "nbu" : "Nebraska",
    "ncu" : "North Carolina",
    "ndu" : "North Dakota",
    "ne"  : "Netherlands",
    "nfc" : "Newfoundland and Labrador",
    "ng"  : "Niger",
    "nhu" : "New Hampshire",
    "nik" : "Northern Ireland",
    "nju" : "New Jersey",
    "nkc" : "New Brunswick",
    "nl"  : "New Caledonia",
    "nmu" : "New Mexico",
    "nn"  : "Vanuatu",
    "no"  : "Norway",
    "np"  : "Nepal",
    "nq"  : "Nicaragua",
    "nr"  : "Nigeria",
    "nsc" : "Nova Scotia",
    "ntc" : "Northwest Territories",
    "nu"  : "Nauru",
    "nuc" : "Nunavut",
    "nvu" : "Nevada",
    "nw"  : "Northern Mariana Islands",
    "nx"  : "Norfolk Island",
    "nyu" : "New York State",
    "nz"  : "New Zealand",
    "ohu" : "Ohio",
    "oku" : "Oklahoma",
    "onc" : "Ontario",
    "oru" : "Oregon",
    "ot"  : "Mayotte",
    "pau" : "Pennsylvania",
    "pc"  : "Pitcairn Island",
    "pe"  : "Peru",
    "pf"  : "Paracel Islands",
    "pg"  : "Guinea-Bissau",
    "ph"  : "Philippines",
    "pic" : "Prince Edward Island",
    "pk"  : "Pakistan",
    "pl"  : "Poland",
    "pn"  : "Panama",
    "po"  : "Portugal",
    "pp"  : "Papua New Guinea",
    "pr"  : "Puerto Rico",
    "pw"  : "Palau",
    "py"  : "Paraguay",
    "qa"  : "Qatar",
    "qea" : "Queensland",
    "quc" : "Québec",
    "rb"  : "Serbia",
    "re"  : "Réunion",
    "rh"  : "Zimbabwe",
    "riu" : "Rhode Island",
    "rm"  : "Romania",
    "ru"  : "Russia",
    "rw"  : "Rwanda",
    "sa"  : "South Africa",
    "sc"  : "Saint-Barthélemy",
    "scu" : "South Carolina",
    "sd"  : "South Sudan",
    "sdu" : "South Dakota",
    "se"  : "Seychelles",
    "sf"  : "Sao Tome and Principe",
    "sg"  : "Senegal",
    "sh"  : "Spanish North Africa",
    "si"  : "Singapore",
    "sj"  : "Sudan",
    "sl"  : "Sierra Leone",
    "sm"  : "San Marino",
    "sn"  : "Sint Maarten",
    "snc" : "Saskatchewan",
    "so"  : "Somalia",
    "sp"  : "Spain",
    "sq"  : "Eswatini",
    "sr"  : "Surinam",
    "ss"  : "Western Sahara",
    "st"  : "Saint-Martin",
    "stk" : "Scotland",
    "su"  : "Saudi Arabia",
    "sw"  : "Sweden",
    "sx"  : "Namibia",
    "sy"  : "Syria",
    "sz"  : "Switzerland",
    "ta"  : "Tajikistan",
    "tc"  : "Turks and Caicos Islands",
    "tg"  : "Togo",
    "th"  : "Thailand",
    "ti"  : "Tunisia",
    "tk"  : "Turkmenistan",
    "tl"  : "Tokelau",
    "tma" : "Tasmania",
    "tnu" : "Tennessee",
    "to"  : "Tonga",
    "tr"  : "Trinidad and Tobago",
    "ts"  : "United Arab Emirates",
    "tu"  : "Turkey",
    "tv"  : "Tuvalu",
    "txu" : "Texas",
    "tz"  : "Tanzania",
    "ua"  : "Egypt",
    "uc"  : "United States Misc. Caribbean Islands",
    "ug"  : "Uganda",
    "un"  : "Ukraine",
    "up"  : "United States Misc. Pacific Islands",
    "utu" : "Utah",
    "uv"  : "Burkina Faso",
    "uy"  : "Uruguay",
    "uz"  : "Uzbekistan",
    "vau" : "Virginia",
    "vb"  : "British Virgin Islands",
    "vc"  : "Vatican City",
    "ve"  : "Venezuela",
    "vi"  : "Virgin Islands of the United States",
    "vm"  : "Vietnam",
    "vp"  : "Various places",
    "vra" : "Victoria",
    "vtu" : "Vermont",
    "wau" : "Washington State",
    "wea" : "Western Australia",
    "wf"  : "Wallis and Futuna",
    "wiu" : "Wisconsin",
    "wj"  : "West Bank of the Jordan River",
    "wk"  : "Wake Island",
    "wlk" : "Wales",
    "ws"  : "Samoa",
    "wvu" : "West Virginia",
    "wyu" : "Wyoming",
    "xa"  : "Christmas Island (Indian Ocean)",
    "xb"  : "Cocos (Keeling) Islands",
    "xc"  : "Maldives",
    "xd"  : "Saint Kitts-Nevis",
    "xe"  : "Marshall Islands",
    "xf"  : "Midway Islands",
    "xga" : "Coral Sea Islands Territory",
    "xh"  : "Niue",
    "xj"  : "Saint Helena",
    "xk"  : "Saint Lucia",
    "xl"  : "Saint Pierre and Miquelon",
    "xm"  : "Saint Vincent and the Grenadines",
    "xn"  : "North Macedonia",
    "xna" : "New South Wales",
    "xo"  : "Slovakia",
    "xoa" : "Northern Territory",
    "xp"  : "Spratly Island",
    "xr"  : "Czech Republic",
    "xra" : "South Australia",
    "xs"  : "South Georgia and the South Sandwich Islands",
    "xv"  : "Slovenia",
    "xx"  : "Unknown",
    "xxc" : "Canada",
    "xxk" : "United Kingdom",
    "xxu" : "United States",
    "ye"  : "Yemen",
    "ykc" : "Yukon Territory",
    "za"  : "Zambia",
}
MARC_DEPRECATED_COUNTRY_CODES = {
    "ac" : "Ashmore and Cartier Islands",
    "ai" : "Anguilla",
    "air"    : "Armenian S.S.R.",
    "ajr"    : "Azerbaijan S.S.R.",
    "bwr"    : "Byelorussian S.S.R.",
    "cn" : "Canada",
    "cp" : "Canton and Enderbury Islands",
    "cs" : "Czechoslovakia",
    "cz" : "Canal Zone",
    "err"    : "Estonia",
    "ge" : "East Germany",
    "gn" : "Gilbert and Ellice Islands",
    "gsr"    : "Georgian S.S.R.",
    "hk" : "Hong Kong",
    "iu" : "Israel-Syria Demilitarized Zones",
    "iw" : "Israel-Jordan Demilitarized Zones",
    "jn" : "Jan Mayen",
    "kgr"    : "Kirghiz S.S.R.",
    "kzr"    : "Kazakh S.S.R.",
    "lir"    : "Lithuania",
    "ln" : "Central and Southern Line Islands",
    "lvr"    : "Latvia",
    "mh" : "Macao",
    "mvr"    : "Moldavian S.S.R.",
    "na" : "Netherlands Antilles",
    "nm" : "Northern Mariana Islands",
    "pt" : "Portuguese Timor",
    "rur"    : "Russian S.F.S.R.",
    "ry" : "Ryukyu Islands, Southern",
    "sb" : "Svalbard",
    "sk" : "Sikkim",
    "sv" : "Swan Islands",
    "tar"    : "Tajik S.S.R.",
    "tkr"    : "Turkmen S.S.R.",
    "tt" : "Trust Territory of the Pacific Islands",
    "ui" : "United Kingdom Misc. Islands",
    "uik"    : "United Kingdom Misc. Islands",
    "uk" : "United Kingdom",
    "unr"    : "Ukraine",
    "ur" : "Soviet Union",
    "us" : "United States",
    "uzr"    : "Uzbek S.S.R.",
    "vn" : "Vietnam, North",
    "vs" : "Vietnam, South",
    "wb" : "West Berlin",
    "xi" : "Saint Kitts-Nevis-Anguilla",
    "xxr"    : "Soviet Union",
    "ys" : "Yemen (People's Democratic Republic)",
    "yu" : "Serbia and Montenegro",
}

def aac_path_prefix():
    return "/app/aacid_small/" if AACID_SMALL_DATA_IMPORTS else "/file-data/"

def aac_spot_check_line_bytes(line_bytes, other_info):
    if line_bytes[0:1] != b'{':
        raise Exception(f"Bad JSON (does not start with {{): {line_bytes[0:500]=} {other_info=}")
    if line_bytes[-2:] != b'}\n':
        raise Exception(f"Bad JSON (does not end with }}\\n): {line_bytes[0:500]=} {other_info=}")

# TODO: for a minor speed improvement we can cache the last read block,
# and then first read the byte offsets within that block.
aac_file_thread_local = threading.local()
def get_lines_from_aac_file(cursor, collection, offsets_and_lengths):
    file_cache = getattr(aac_file_thread_local, 'file_cache', None)
    if file_cache is None:
        file_cache = aac_file_thread_local.file_cache = {}

    if collection not in file_cache:
        cursor.execute('SELECT filename FROM annas_archive_meta_aac_filenames WHERE collection = %(collection)s', { 'collection': collection })
        filename = cursor.fetchone()['filename']
        full_filepath = f'{aac_path_prefix()}{filename}'
        full_filepath_decompressed = full_filepath.replace('.seekable.zst', '')
        if os.path.exists(full_filepath_decompressed):
            file_cache[collection] = open(full_filepath_decompressed, 'rb')
        else:
            file_cache[collection] = indexed_zstd.IndexedZstdFile(full_filepath)
    file = file_cache[collection]

    lines = [None]*len(offsets_and_lengths)
    for byte_offset, byte_length, index in sorted([(row[0], row[1], index) for index, row in enumerate(offsets_and_lengths)]):
        file.seek(byte_offset)
        line_bytes = file.read(byte_length)
        if len(line_bytes) != byte_length:
            raise Exception(f"Invalid {len(line_bytes)=} != {byte_length=}")
        aac_spot_check_line_bytes(line_bytes, (byte_offset, byte_length, index))
        # Uncomment to fully verify JSON after read.
        # try:
        #     orjson.loads(line_bytes)
        # except:
        #     raise Exception(f"Bad JSON: {collection=} {byte_offset=} {byte_length=} {index=} {line_bytes=}")
        lines[index] = line_bytes
    return lines

def aa_currently_seeding(metadata):
    return ((datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.strptime(metadata['seeding_at'], "%Y-%m-%dT%H:%M:%S%z")) < datetime.timedelta(days=7)) if ('seeding_at' in metadata) else False

@functools.cache
def get_torrents_json_aa_currently_seeding_by_torrent_path():
    try:
        with engine.connect() as connection:
            connection.connection.ping(reconnect=True)
            cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
            cursor.execute('SELECT 1')
    except Exception:
        return {}

    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT json FROM torrents_json LIMIT 1')
        return { row['url'].split('dyn/small_file/torrents/', 1)[1]: row['aa_currently_seeding'] for row in orjson.loads(cursor.fetchone()['json']) }

# These are marked as not seeding because an issue with the torrent but are actually seeding.
# Keep in sync.
TORRENT_PATHS_PARTIALLY_BROKEN = [
    'torrents/external/libgen_li_fic/f_2869000.torrent',
    'torrents/external/libgen_li_fic/f_2896000.torrent',
    'torrents/external/libgen_li_fic/f_2945000.torrent',
    'torrents/external/libgen_li_fic/f_2966000.torrent',
    'torrents/external/libgen_li_fic/f_3412000.torrent',
    'torrents/external/libgen_li_fic/f_3453000.torrent',
    'torrents/external/libgen_li_comics/c_1137000.torrent',
]

def build_pagination_pages_with_dots(primary_hits_pages, page_value, large):
    pagination_pages_with_dots = []
    for page in sorted(set(list(range(1,min(primary_hits_pages+1, (4 if large else 3)))) + list(range(max(1,page_value-1),min(page_value+2,primary_hits_pages+1))) + list(range(max(1,primary_hits_pages-(2 if large else 0)),primary_hits_pages+1)))):
        if (len(pagination_pages_with_dots) > 0) and (pagination_pages_with_dots[-1] != (page-1)):
            pagination_pages_with_dots.append('…')
        pagination_pages_with_dots.append(page)
    if len(pagination_pages_with_dots) == 0:
        return [1]
    else:
        return pagination_pages_with_dots

def escape_mysql_like(input_string):
    return input_string.replace('%', '\\%').replace('_', '\\_')

def extract_ssid_or_ssno_from_filepath(filepath):
    for part in reversed(filepath.split('/')):
        ssid_match_underscore = re.search(r'_(\d{8})(?:\D|$)', part)
        if ssid_match_underscore is not None:
            return ssid_match_underscore[1]
    for part in reversed(filepath.split('/')):
        ssid_match = re.search(r'(?:^|\D)(\d{8})(?:\D|$)', part)
        if ssid_match is not None:
            return ssid_match[1]
    ssid_match_underscore = re.search(r'_(\d{8})(?:\D|$)', filepath)
    if ssid_match_underscore is not None:
        return ssid_match_underscore[1]
    ssid_match = re.search(r'(?:^|\D)(\d{8})(?:\D|$)', filepath)
    if ssid_match is not None:
        return ssid_match[1]
    return None

def extract_doi_from_filepath(filepath):
    filepath_without_extension = filepath
    if '.' in filepath:
        filepath_without_extension, extension = filepath.rsplit('.', 1)
        if len(extension) > 4:
            filepath_without_extension = filepath
    filepath_without_extension_split = filepath_without_extension.split('/')
    for index, part in reversed(list(enumerate(filepath_without_extension_split))):
        if part.startswith('10.'):
            if part == filepath_without_extension_split[-1]:
                return part.replace('_', '/')
            else:
                return '/'.join(filepath_without_extension_split[index:])
    return None

# Taken from https://github.com/alejandrogallo/python-doi/blob/03d51be3c1f4e362523f4912058ca3cb01b98e91/src/doi/__init__.py#L82C1-L95C15
def get_clean_doi(doi):
    """Check if doi is actually a url and in that case just get
    the exact doi.

    :doi: String containing a doi
    :returns: The pure doi
    """
    doi = re.sub(r'%2F', '/', doi)
    # For pdfs
    doi = re.sub(r'\)>', ' ', doi)
    doi = re.sub(r'\)/S/URI', ' ', doi)
    doi = re.sub(r'(/abstract)', '', doi)
    doi = re.sub(r'\)$', '', doi)
    return doi

# Taken from https://github.com/alejandrogallo/python-doi/blob/03d51be3c1f4e362523f4912058ca3cb01b98e91/src/doi/__init__.py#L98C1-L125C16
def find_doi_in_text(text):
    """
    Try to find a doi in a text
    """
    text = get_clean_doi(text)
    forbidden_doi_characters = r'"\s%$^\'<>@,;:#?&'
    # Sometimes it is in the javascript defined
    var_doi = re.compile(
        r'doi(.org)?'
        r'\s*(=|:|/|\()\s*'
        r'("|\')?'
        r'(?P<doi>[^{fc}]+)'
        r'("|\'|\))?'
        .format(
            fc=forbidden_doi_characters
        ), re.I
    )

    for regex in [var_doi]:
        miter = regex.finditer(text)
        try:
            m = next(miter)
            if m:
                doi = m.group('doi')
                return get_clean_doi(doi)
        except StopIteration:
            pass
    return None

def extract_ia_archive_org_from_string(string):
    return list(dict.fromkeys(re.findall(r'archive.org\/details\/([^\n\r\/ ]+)', string)))

def groupby(dicts, index_field, unpack_field=None):
    if type(index_field) is str:
        index_field_func = lambda row: row[index_field]  # noqa: E731
    else:
        index_field_func = index_field
    if unpack_field is None:
        unpack_field_func = lambda row: row  # noqa: E731
    elif type(unpack_field) is str:
        unpack_field_func = lambda row: row[unpack_field]  # noqa: E731
    else:
        unpack_field_func = unpack_field
    output = collections.defaultdict(list)
    for row in dicts:
        index_field_value = index_field_func(row)
        unpack_field_value = unpack_field_func(row)
        output[index_field_value].append(unpack_field_value)
    return output
