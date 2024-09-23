import time
import orjson
import datetime
import re
import collections
import shortuuid
import pymysql
import hashlib
import hmac
import httpx
import email
import email.policy
import traceback
import curlify2
import babel.numbers as babel_numbers
import io
import random

from flask import Blueprint, request, g, make_response, render_template, send_file
from flask_cors import cross_origin
from sqlalchemy import text
from sqlalchemy.orm import Session
from flask_babel import gettext, get_locale

from allthethings.extensions import es, engine, mariapersist_engine
from config.settings import PAYMENT1_KEY, PAYMENT1B_KEY, PAYMENT2_URL, PAYMENT2_API_KEY, PAYMENT2_PROXIES, PAYMENT2_HMAC, PAYMENT2_SIG_HEADER, GC_NOTIFY_SIG, HOODPAY_URL, HOODPAY_AUTH, PAYMENT3_DOMAIN, PAYMENT3_KEY
from allthethings.page.views import get_aarecords_elasticsearch, ES_TIMEOUT_PRIMARY, get_torrents_data

import allthethings.utils


dyn = Blueprint("dyn", __name__, template_folder="templates", url_prefix="/dyn")

@dyn.get("/translations/")
@allthethings.utils.no_cache()
def language_codes():
    return orjson.dumps({ "translations": sorted(str(t) for t in allthethings.utils.list_translations()) })


@dyn.get("/up/")
@allthethings.utils.no_cache()
@cross_origin()
def index():
    # For testing, uncomment:
    # if "testing_redirects" not in request.headers['Host']:
    #     return "Simulate server down", 513

    account_id = allthethings.utils.get_account_id(request.cookies)
    aa_logged_in = 0 if account_id is None else 1
    return orjson.dumps({ "aa_logged_in": aa_logged_in })

number_of_db_exceptions = 0
@dyn.get("/up/databases/")
@allthethings.utils.no_cache()
def databases():
    global number_of_db_exceptions
    try:
        # Local MariaDB is not really necessary for most pages.
        # with engine.connect() as conn:
        #     conn.execute(text("SELECT 1 FROM zlib_book LIMIT 1"))
        if not allthethings.utils.DOWN_FOR_MAINTENANCE:
            with mariapersist_engine.connect() as mariapersist_conn:
                mariapersist_conn.execute(text("SELECT 1 FROM mariapersist_downloads_total_by_md5 LIMIT 1"))
        if not es.ping():
            raise Exception("es.ping failed!")
        # if not es_aux.ping():
        #     raise Exception("es_aux.ping failed!")
    except Exception:
        number_of_db_exceptions += 1
        if number_of_db_exceptions > 10:
            raise
        return "", 500
    number_of_db_exceptions = 0
    return ""

def api_md5_fast_download_get_json(download_url, other_fields):
    return allthethings.utils.nice_json({
        "///download_url": [
            "This API is intended as a stable JSON API for getting fast download files as a member.",
            "A successful request will return status code 200 or 204, a `download_url` field and `account_fast_download_info`.",
            "Bad responses use different status codes, a `download_url` set to `null`, and `error` field with string description.",
            "Accepted query parameters:",
            "- `md5` (required): the md5 string of the requested file.",
            "- `key` (required): the secret key for your account (which must have membership).",
            "- `path_index` (optional): Integer, 0 or larger, indicating the collection (if the file is present in more than one).",
            "- `domain_index` (optional): Integer, 0 or larger, indicating the download server, e.g. 0='Fast Partner Server #1'.",
            "These parameters correspond to the fast download page like this: /fast_download/{md5}/{path_index}/{domain_index}",
            "Example: /dyn/api/fast_download.json?md5=d6e1dc51a50726f00ec438af21952a45&key=YOUR_SECRET_KEY",
        ],
        "download_url": download_url,
        **other_fields,
    })

# IMPORTANT: Keep in sync with md5_fast_download.
@dyn.get("/api/fast_download.json")
@allthethings.utils.no_cache()
def api_md5_fast_download():
    key_input = request.args.get('key', '')
    md5_input = request.args.get('md5', '')
    domain_index = int(request.args.get('domain_index', '0'))
    path_index = int(request.args.get('path_index', '0'))

    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]) or canonical_md5 != md5_input:
        return api_md5_fast_download_get_json(None, { "error": "Invalid md5" }), 400, {'Content-Type': 'text/json; charset=utf-8'}

    account_id = allthethings.utils.account_id_from_secret_key(key_input)
    if account_id is None:
        return api_md5_fast_download_get_json(None, { "error": "Invalid secret key" }), 401, {'Content-Type': 'text/json; charset=utf-8'}

    aarecords = get_aarecords_elasticsearch([f"md5:{canonical_md5}"])
    if aarecords is None:
        return api_md5_fast_download_get_json(None, { "error": "Error during fetching" }), 500, {'Content-Type': 'text/json; charset=utf-8'}
    if len(aarecords) == 0:
        return api_md5_fast_download_get_json(None, { "error": "Record not found" }), 404, {'Content-Type': 'text/json; charset=utf-8'}
    aarecord = aarecords[0]
    try:
        domain = allthethings.utils.FAST_DOWNLOAD_DOMAINS[domain_index]
        path_info = aarecord['additional']['partner_url_paths'][path_index]
    except Exception:
        return api_md5_fast_download_get_json(None, { "error": "Invalid domain_index or path_index" }), 400, {'Content-Type': 'text/json; charset=utf-8'}
    url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(False, 20000, path_info['path'], aarecord['additional']['filename'], domain)

    with Session(mariapersist_engine) as mariapersist_session:
        account_fast_download_info = allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id)
        if account_fast_download_info is None:
            return api_md5_fast_download_get_json(None, { "error": "Not a member" }), 403, {'Content-Type': 'text/json; charset=utf-8'}

        if canonical_md5 not in account_fast_download_info['recently_downloaded_md5s']:
            if account_fast_download_info['downloads_left'] <= 0:
                return api_md5_fast_download_get_json(None, { "error": "No downloads left" }), 429, {'Content-Type': 'text/json; charset=utf-8'}

            data_md5 = bytes.fromhex(canonical_md5)
            data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
            mariapersist_session.connection().execute(text('INSERT INTO mariapersist_fast_download_access (md5, ip, account_id) VALUES (:md5, :ip, :account_id)').bindparams(md5=data_md5, ip=data_ip, account_id=account_id))
            mariapersist_session.commit()
    return api_md5_fast_download_get_json(url, {
        "account_fast_download_info": {
            "downloads_left": account_fast_download_info['downloads_left'],
            "downloads_per_day": account_fast_download_info['downloads_per_day'],
            "recently_downloaded_md5s": account_fast_download_info['recently_downloaded_md5s'],
        },
    }), {'Content-Type': 'text/json; charset=utf-8'}

def make_torrent_url(file_path):
    return f"{g.full_domain}/dyn/small_file/{file_path}"

def make_torrent_json(top_level_group_name, group_name, row):
    return {
        'url': make_torrent_url(row['file_path']),
        'top_level_group_name': top_level_group_name,
        'group_name': group_name,
        'display_name': row['display_name'],
        'added_to_torrents_list_at': row['created'],
        'is_metadata': row['is_metadata'],
        'btih': row['metadata']['btih'],
        'magnet_link': row['magnet_link'],
        'torrent_size': row['metadata']['torrent_size'],
        'num_files': row['metadata']['num_files'],
        'data_size': row['metadata']['data_size'],
        'aa_currently_seeding': row['aa_currently_seeding'],
        'obsolete': row['obsolete'],
        'embargo': (row['metadata'].get('embargo') or False),
        'seeders': ((row['scrape_metadata'].get('scrape') or {}).get('seeders') or 0),
        'leechers': ((row['scrape_metadata'].get('scrape') or {}).get('leechers') or 0),
        'completed': ((row['scrape_metadata'].get('scrape') or {}).get('completed') or 0),
        'stats_scraped_at': row['scrape_created'],
        'partially_broken': row['partially_broken'],
        'random': row['temp_uuid'],
    }

@dyn.get("/torrents.json")
@allthethings.utils.no_cache()
def torrents_json_page():
    torrents_data = get_torrents_data()
    output_rows = []
    for top_level_group_name, small_files_groups in torrents_data['small_file_dicts_grouped'].items():
        for group_name, small_files in small_files_groups.items():
            for small_file in small_files:
                output_rows.append(make_torrent_json(top_level_group_name, group_name, small_file))
    return orjson.dumps(output_rows), {'Content-Type': 'text/json; charset=utf-8'}

@dyn.get("/generate_torrents")
@allthethings.utils.no_cache()
def generate_torrents_page():
    torrents_data = get_torrents_data()
    max_tb = 10000000
    try:
        max_tb = float(request.args.get('max_tb'))
    except Exception:
        pass
    if max_tb < 0.00001:
        max_tb = 10000000
    max_bytes = 1000000000000 * max_tb

    potential_output_rows = []
    total_data_size = 0
    for top_level_group_name, small_files_groups in torrents_data['small_file_dicts_grouped'].items():
        for group_name, small_files in small_files_groups.items():
            for small_file in small_files:
                output_row = make_torrent_json(top_level_group_name, group_name, small_file)
                if not output_row['embargo'] and not output_row['obsolete'] and output_row['seeders'] > 0 and output_row['top_level_group_name'] != 'other_aa':
                    potential_output_rows.append({ **output_row, "random_increment": random.random()*2.0 })
                    total_data_size += output_row['data_size']

    avg_data_size = 1
    if len(potential_output_rows) > 0:
        avg_data_size = total_data_size/len(potential_output_rows)
    output_rows = []
    for output_row in potential_output_rows:
        # Note, this is intentionally inverted, because larger torrents should be proportionally sorted higher in ascending order! Think of it as an adjustment for "seeders per MB".
        data_size_multiplier = avg_data_size/output_row['data_size']
        total_sort_score = ((output_row['seeders'] + (0.1 * output_row['leechers'])) * data_size_multiplier) + output_row['random_increment']
        output_rows.append({ **output_row, "data_size_multiplier": data_size_multiplier, "total_sort_score": total_sort_score })

    output_rows.sort(key=lambda output_row: output_row['total_sort_score'])

    total_bytes = 0
    filtered_output_rows = []
    for output_row in output_rows:
        if (total_bytes + output_row['data_size']) >= max_bytes:
            continue
        total_bytes += output_row['data_size']
        filtered_output_rows.append(output_row)

    output_format = (request.args.get('format') or 'json')
    if output_format == 'url':
        return '\n'.join([output_row['url'] for output_row in filtered_output_rows]), {'Content-Type': 'text/json; charset=utf-8'}
    elif output_format == 'magnet':
        return '\n'.join([output_row['magnet_link'] for output_row in filtered_output_rows]), {'Content-Type': 'text/json; charset=utf-8'}
    else:
        return orjson.dumps(filtered_output_rows), {'Content-Type': 'text/json; charset=utf-8'}

@dyn.get("/torrents/latest_aac_meta/<string:collection>.torrent")
@allthethings.utils.no_cache()
def torrents_latest_aac_page(collection):
    with mariapersist_engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT data FROM mariapersist_small_files WHERE file_path LIKE CONCAT("torrents/managed_by_aa/annas_archive_meta__aacid/annas_archive_meta__aacid__", %(collection)s, "%%") ORDER BY created DESC LIMIT 1', { "collection": collection })
        file = cursor.fetchone()
        if file is None:
            return "File not found", 404
        return send_file(io.BytesIO(file['data']), as_attachment=True, download_name=f'{collection}.torrent')

@dyn.get("/small_file/<path:file_path>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def small_file_page(file_path):
    with mariapersist_engine.connect() as connection:
        cursor = allthethings.utils.get_cursor_ping_conn(connection)
        # SQLAlchemy query originally had LIMIT 10000, but was fetching only the first row (.first())??
        cursor.execute('SELECT data FROM mariapersist_small_files WHERE file_path = %(file_path)s LIMIT 1', { 'file_path': file_path })
        file = cursor.fetchone()
        if file is None:
            return "File not found", 404
        return send_file(io.BytesIO(file['data']), as_attachment=True, download_name=file_path.split('/')[-1])

@dyn.post("/downloads/increment/<string:md5_input>")
@allthethings.utils.no_cache()
def downloads_increment(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        return "Non-canonical md5", 404

    # Prevent hackers from filling up our database with non-existing MD5s.
    aarecord_id = f"md5:{canonical_md5}"
    if not es.exists(index=f"aarecords__{allthethings.utils.virtshard_for_aarecord_id(aarecord_id)}", id=aarecord_id):
        return "md5 not found", 404

    with Session(mariapersist_engine) as mariapersist_session:
        data_hour_since_epoch = int(time.time() / 3600)
        data_md5 = bytes.fromhex(canonical_md5)
        data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
        account_id = allthethings.utils.get_account_id(request.cookies)
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_downloads_hourly_by_ip (ip, hour_since_epoch, count) VALUES (:ip, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch, ip=data_ip))
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_downloads_hourly_by_md5 (md5, hour_since_epoch, count) VALUES (:md5, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch, md5=data_md5))
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_downloads_total_by_md5 (md5, count) VALUES (:md5, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(md5=data_md5))
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_downloads_hourly (hour_since_epoch, count) VALUES (:hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch))
        mariapersist_session.connection().execute(text('INSERT IGNORE INTO mariapersist_downloads (md5, ip, account_id) VALUES (:md5, :ip, :account_id)').bindparams(md5=data_md5, ip=data_ip, account_id=account_id))
        mariapersist_session.commit()
        return ""

@dyn.get("/downloads/stats/")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def downloads_stats_total():
    with mariapersist_engine.connect() as mariapersist_conn:
        hour_now = int(time.time() / 3600)
        hour_week_ago = hour_now - 24*31
        cursor = allthethings.utils.get_cursor_ping_conn(mariapersist_conn)
        cursor.execute('SELECT hour_since_epoch, count FROM mariapersist_downloads_hourly '
                       'WHERE hour_since_epoch >= %(hour_week_ago)s '#
                       'LIMIT %(limit)s',
                       { 'hour_week_ago': hour_week_ago, 'limit': hour_week_ago + 1 })
        timeseries = cursor.fetchall()
        timeseries_by_hour = {}
        for t in timeseries:
            timeseries_by_hour[t['hour_since_epoch']] = t['count']
        timeseries_x = list(range(hour_week_ago, hour_now))
        timeseries_y = [timeseries_by_hour.get(x, 0) for x in timeseries_x]
        return orjson.dumps({ "timeseries_x": timeseries_x, "timeseries_y": timeseries_y })

@dyn.get("/downloads/stats/<string:md5_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def downloads_stats_md5(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        return "Non-canonical md5", 404

    with mariapersist_engine.connect() as mariapersist_conn:
        cursor = allthethings.utils.get_cursor_ping_conn(mariapersist_conn)

        cursor.execute('SELECT count FROM mariapersist_downloads_total_by_md5 WHERE md5 = %(md5_digest)s LIMIT 1', { 'md5_digest': bytes.fromhex(canonical_md5) })
        total = allthethings.utils.fetch_one_field(cursor) or 0
        hour_now = int(time.time() / 3600)
        hour_week_ago = hour_now - 24*31
        cursor.execute('SELECT hour_since_epoch, count FROM mariapersist_downloads_hourly_by_md5 WHERE md5 = %(md5_digest)s AND hour_since_epoch >= %(hour_week_ago)s LIMIT %(limit)s', { 'md5_digest': bytes.fromhex(canonical_md5), 'hour_week_ago': hour_week_ago, 'limit': hour_week_ago + 1 })
        timeseries = cursor.fetchall()
        timeseries_by_hour = {}
        for t in timeseries:
            timeseries_by_hour[t['hour_since_epoch']] = t['count']
        timeseries_x = list(range(hour_week_ago, hour_now))
        timeseries_y = [timeseries_by_hour.get(x, 0) for x in timeseries_x]
        return orjson.dumps({ "total": int(total), "timeseries_x": timeseries_x, "timeseries_y": timeseries_y })


# @dyn.put("/account/access/")
# @allthethings.utils.no_cache()
# def account_access():
#     with Session(mariapersist_engine) as mariapersist_session:
#         email = request.form['email']
#         account = mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.email_verified == email).limit(1)).first()
#         if account is None:
#             return "{}"

#         url = g.full_domain + '/account/?key=' + allthethings.utils.secret_key_from_account_id(account.account_id)
#         subject = "Secret key for Anna’s Archive"
#         body = "Hi! Please use the following link to get your secret key for Anna’s Archive:\n\n" + url + "\n\nNote that we will discontinue email logins at some point, so make sure to save your secret key.\n-Anna"

#         email_msg = flask_mail.Message(subject=subject, body=body, recipients=[email])
#         mail.send(email_msg)
#         return "{}"


@dyn.put("/account/logout/")
@allthethings.utils.no_cache()
def account_logout():
    request.cookies[allthethings.utils.ACCOUNT_COOKIE_NAME] # Error if cookie is not set.
    resp = make_response(orjson.dumps({ "aa_logged_in": 0 }))
    resp.set_cookie(
        key=allthethings.utils.ACCOUNT_COOKIE_NAME,
        httponly=True,
        secure=g.secure_domain,
        domain=g.base_domain,
    )
    return resp


@dyn.put("/copyright/")
@allthethings.utils.no_cache()
def copyright():
    with Session(mariapersist_engine) as mariapersist_session:
        data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
        data_json = orjson.dumps(request.form)
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_copyright_claims (ip, json) VALUES (:ip, :json)').bindparams(ip=data_ip, json=data_json))
        mariapersist_session.commit()
        return "{}"


@dyn.get("/md5/summary/<string:md5_input>")
@allthethings.utils.no_cache()
def md5_summary(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        return "Non-canonical md5", 404

    account_id = allthethings.utils.get_account_id(request.cookies)

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

        data_md5 = bytes.fromhex(canonical_md5)

        cursor.execute('SELECT COUNT(*) FROM mariapersist_md5_report WHERE md5 = %(md5_digest)s LIMIT 1', { 'md5_digest': data_md5 })
        reports_count = allthethings.utils.fetch_one_field(cursor)

        cursor.execute('SELECT COUNT(*) FROM mariapersist_comments WHERE resource = %(resource)s LIMIT 1', { 'resource': f"md5:{canonical_md5}" })
        comments_count = allthethings.utils.fetch_one_field(cursor)

        cursor.execute('SELECT COUNT(*) FROM mariapersist_list_entries WHERE resource = %(resource)s LIMIT 1', { 'resource': f"md5:{canonical_md5}" })
        lists_count = allthethings.utils.fetch_one_field(cursor)

        cursor.execute('SELECT count FROM mariapersist_downloads_total_by_md5 WHERE md5 = %(md5_digest)s LIMIT 1', { 'md5_digest': data_md5 })
        downloads_total = allthethings.utils.fetch_one_field(cursor)

        cursor.execute('SELECT COUNT(*) FROM mariapersist_reactions WHERE resource = %(resource)s LIMIT 1', { 'resource': f"md5:{canonical_md5}" })
        great_quality_count = allthethings.utils.fetch_one_field(cursor)

        user_reaction = None
        downloads_left = 0
        is_member = 0
        download_still_active = 0
        if account_id is not None:
            cursor.execute('SELECT type FROM mariapersist_reactions WHERE resource = %(resource)s AND account_id = %(account_id)s LIMIT 1', { 'resource': f"md5:{canonical_md5}", 'account_id': account_id })
            user_reaction = allthethings.utils.fetch_one_field(cursor)

            account_fast_download_info = allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id)
            if account_fast_download_info is not None:
                is_member = 1
                downloads_left = account_fast_download_info['downloads_left']
                if canonical_md5 in account_fast_download_info['recently_downloaded_md5s']:
                    download_still_active = 1
        return orjson.dumps({ "reports_count": reports_count, "comments_count": comments_count, "lists_count": lists_count, "downloads_total": downloads_total, "great_quality_count": great_quality_count, "user_reaction": user_reaction, "downloads_left": downloads_left, "is_member": is_member, "download_still_active": download_still_active })


@dyn.put("/md5_report/<string:md5_input>")
@allthethings.utils.no_cache()
def md5_report(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        return "Non-canonical md5", 404

    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    report_type = request.form['type']
    if report_type not in ["metadata", "download", "broken", "pages", "spam", "other"]:
        raise Exception("Incorrect report_type")

    content = request.form['content']
    if len(content) == 0:
        raise Exception("Empty content")

    canonical_better_md5 = None
    if 'better_md5' in request.form:
        better_md5 = request.form['better_md5'][0:50]
        canonical_better_md5 = better_md5.strip().lower()
        if (len(canonical_better_md5) == 0) or (canonical_better_md5 == canonical_md5):
            canonical_better_md5 = None
        elif not allthethings.utils.validate_canonical_md5s([canonical_better_md5]):
            raise Exception("Non-canonical better_md5")

    with Session(mariapersist_engine) as mariapersist_session:
        data_md5 = bytes.fromhex(canonical_md5)
        data_better_md5 = None
        if canonical_better_md5 is not None:
            data_better_md5 = bytes.fromhex(canonical_better_md5)
        md5_report_id = mariapersist_session.connection().execute(text('INSERT INTO mariapersist_md5_report (md5, account_id, type, better_md5) VALUES (:md5, :account_id, :type, :better_md5) RETURNING md5_report_id').bindparams(md5=data_md5, account_id=account_id, type=report_type, better_md5=data_better_md5)).scalar()
        mariapersist_session.connection().execute(
            text('INSERT INTO mariapersist_comments (account_id, resource, content) VALUES (:account_id, :resource, :content)')
                .bindparams(account_id=account_id, resource=f"md5_report:{md5_report_id}", content=content))
        mariapersist_session.commit()
        return "{}"


@dyn.put("/account/display_name/")
@allthethings.utils.no_cache()
def put_display_name():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    display_name = request.form['display_name'].strip()

    if len(display_name) < 4:
        return "", 500
    if len(display_name) > 20:
        return "", 500

    with Session(mariapersist_engine) as mariapersist_session:
        mariapersist_session.connection().execute(text('UPDATE mariapersist_accounts SET display_name = :display_name WHERE account_id = :account_id LIMIT 1').bindparams(display_name=display_name, account_id=account_id))
        mariapersist_session.commit()
        return "{}"


@dyn.put("/list/name/<string:list_id>")
@allthethings.utils.no_cache()
def put_list_name(list_id):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    name = request.form['name'].strip()
    if len(name) == 0:
        return "", 500

    with Session(mariapersist_engine) as mariapersist_session:
        # Note, this also does validation by checking for account_id.
        mariapersist_session.connection().execute(text('UPDATE mariapersist_lists SET name = :name WHERE account_id = :account_id AND list_id = :list_id LIMIT 1').bindparams(name=name, account_id=account_id, list_id=list_id))
        mariapersist_session.commit()
        return "{}"


def get_resource_type(resource):
    if bool(re.match(r"^md5:[a-f\d]{32}$", resource)):
        return 'md5'
    if bool(re.match(r"^comment:[\d]+$", resource)):
        return 'comment'
    return None


@dyn.put("/comments/<string:resource>")
@allthethings.utils.no_cache()
def put_comment(resource):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    content = request.form['content'].strip()
    if len(content) == 0:
        raise Exception("Empty content")

    with Session(mariapersist_engine) as mariapersist_session:
        resource_type = get_resource_type(resource)
        if resource_type not in ['md5', 'comment']:
            raise Exception("Invalid resource")

        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)
        if resource_type == 'comment':
            cursor.execute('SELECT resource FROM mariapersist_comments WHERE comment_id = %(comment_id)s LIMIT 1', { 'comment_id': int(resource[len('comment:'):]) })
            parent_resource = allthethings.utils.fetch_one_field(cursor)
            if parent_resource is None:
                raise Exception("No parent comment")
            parent_resource_type = get_resource_type(parent_resource)
            if parent_resource_type == 'comment':
                raise Exception("Parent comment is itself a reply")

        cursor.execute('INSERT INTO mariapersist_comments (account_id, resource, content) VALUES (%(account_id)s, %(resource)s, %(content)s)',
                       { 'account_id': account_id, 'resource': resource, 'content': content })
        mariapersist_session.commit()
        return "{}"


def get_comment_dicts(cursor, resources):
    account_id = allthethings.utils.get_account_id(request.cookies)

    cursor.execute('SELECT c.*, a.display_name, r.type AS user_reaction FROM mariapersist_comments c '
                   'INNER JOIN mariapersist.mariapersist_accounts a USING(account_id) '
                   'LEFT JOIN mariapersist.mariapersist_reactions r '
                   '    ON r.resource = CONCAT(\'comment:\', c.comment_id) '
                   '        AND r.account_id = %(account_id)s '
                   'WHERE c.resource IN %(resources)s '
                   'LIMIT 10000',
                   { 'account_id': account_id, 'resources': resources })
    comments = list(cursor.fetchall())

    replies_res = [f"comment:{comment['comment_id']}" for comment in comments]
    # SQL does not allow empty IN() lists
    if len(replies_res) <= 0:
        replies_res.append('x')

    cursor.execute('SELECT c.*, a.display_name, r.type AS user_reaction FROM mariapersist_comments c '
                   'INNER JOIN mariapersist.mariapersist_accounts a USING(account_id) '
                   'LEFT JOIN mariapersist.mariapersist_reactions r '
                   '    ON c.account_id = r.account_id '
                   '        AND r.resource = CONCAT(\'comment:\', c.comment_id) '
                   '        AND r.account_id = %(account_id)s '
                   'WHERE c.resource IN %(resources)s '
                   'ORDER BY c.comment_id '
                   'LIMIT 10000',
                   { 'account_id': account_id, 'resources': replies_res })
    replies = list(cursor.fetchall())

    reactions_res = [f"comment:{comment['comment_id']}" for comment in (comments+replies)]
    # SQL does not allow empty IN() lists
    if len(reactions_res) <= 0:
        reactions_res.append('x')

    cursor.execute('SELECT resource, type, COUNT(*) as count FROM mariapersist_reactions '
                   'WHERE resource IN %(resources)s GROUP BY resource, type '
                   'LIMIT 10000', { 'resources': reactions_res })
    comment_reactions = list(cursor.fetchall())

    comment_reactions_by_id = collections.defaultdict(dict)
    for reaction in comment_reactions:
        comment_reactions_by_id[int(reaction['resource'][len("comment:"):])][reaction['type']] = reaction['count']

    reply_dicts_by_parent_comment_id = collections.defaultdict(list)
    for reply in replies: # Note: these are already sorted chronologically.
        reply_dicts_by_parent_comment_id[int(reply['resource'][len('comment:'):])].append({
            **reply,
            'created_delta': reply['created'] - datetime.datetime.now(),
            'abuse_total': comment_reactions_by_id[reply['comment_id']].get(1, 0),
            'thumbs_up': comment_reactions_by_id[reply['comment_id']].get(2, 0),
            'thumbs_down': comment_reactions_by_id[reply['comment_id']].get(3, 0),
        })

    comment_dicts = [{
        **comment,
        'created_delta': comment['created'] - datetime.datetime.now(),
        'abuse_total': comment_reactions_by_id[comment['comment_id']].get(1, 0),
        'thumbs_up': comment_reactions_by_id[comment['comment_id']].get(2, 0),
        'thumbs_down': comment_reactions_by_id[comment['comment_id']].get(3, 0),
        'reply_dicts': reply_dicts_by_parent_comment_id[comment['comment_id']],
        'can_have_replies': True,
    } for comment in comments]



    comment_dicts.sort(reverse=True, key=lambda c: 100000*(c['thumbs_up']-c['thumbs_down']-c['abuse_total']*5) + c['comment_id'] )
    return comment_dicts


# @dyn.get("/comments/<string:resource>")
# @allthethings.utils.no_cache()
# def get_comments(resource):
#     if not bool(re.match(r"^md5:[a-f\d]{32}$", resource)):
#         raise Exception("Invalid resource")

#     with Session(mariapersist_engine) as mariapersist_session:
#         comment_dicts = get_comment_dicts(mariapersist_session, [resource])        

#         return render_template(
#             "dyn/comments.html",
#             comment_dicts=comment_dicts,
#             current_account_id=allthethings.utils.get_account_id(request.cookies),
#             reload_url=f"/dyn/comments/{resource}",
#         )


@dyn.get("/md5_reports/<string:md5_input>")
@allthethings.utils.no_cache()
def md5_reports(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        return "Non-canonical md5", 404

    with Session(mariapersist_engine) as mariapersist_session:
        data_md5 = bytes.fromhex(canonical_md5)
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

        cursor.execute('SELECT md5_report_id, type, better_md5 FROM mariapersist_md5_report '
                       'WHERE md5 = %(data_md5)s '
                       'ORDER BY created DESC '
                       'LIMIT 10000',
                       { 'data_md5': data_md5 })
        reports = cursor.fetchall()
        report_dicts_by_resource = {}
        for r in reports:
            report_dict = dict(r)
            if better_md5 := report_dict.get("better_md5"):
                report_dict["better_md5"] = better_md5.hex()
            report_dicts_by_resource[f"md5_report:{report_dict['md5_report_id']}"] = report_dict


        comment_dicts = [{ 
            **comment_dict,
            'report_dict': report_dicts_by_resource.get(comment_dict['resource'], None),
        } for comment_dict in get_comment_dicts(cursor, ([f"md5:{canonical_md5}"] + list(report_dicts_by_resource.keys())))]

        return render_template(
            "dyn/comments.html",
            comment_dicts=comment_dicts,
            current_account_id=allthethings.utils.get_account_id(request.cookies),
            reload_url=f"/dyn/md5_reports/{canonical_md5}",
            md5_report_type_mapping=allthethings.utils.get_md5_report_type_mapping(),
        )


@dyn.put("/reactions/<int:reaction_type>/<string:resource>")
@allthethings.utils.no_cache()
def put_comment_reaction(reaction_type, resource):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with (Session(mariapersist_engine) as mariapersist_session):
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)
        resource_type = get_resource_type(resource)
        if resource_type not in ['md5', 'comment']:
            raise Exception("Invalid resource")
        if resource_type == 'comment':
            if reaction_type not in [0,1,2,3]:
                raise Exception("Invalid reaction_type")
            cursor.execute('SELECT resource FROM mariapersist_comments WHERE comment_id = %(comment_id)s LIMIT 1',
                           { 'comment_id': int(resource[len('comment:'):]) })
            comment_account_id = allthethings.utils.fetch_one_field(cursor)
            if comment_account_id is None:
                raise Exception("No parent comment")
            if comment_account_id == account_id:
                return "", 403
        elif resource_type == 'md5':
            if reaction_type not in [0,2]:
                raise Exception("Invalid reaction_type")

        if reaction_type == 0:
            cursor.execute('DELETE FROM mariapersist_reactions '
                           'WHERE account_id = %(account_id)s AND resource = %(resource)s',
                           { 'account_id': account_id, 'resource': resource })
        else:
            cursor.execute('INSERT INTO mariapersist_reactions (account_id, resource, type) '
                           'VALUES (%(account_id)s, %(resource)s, %(type)s) '
                           'ON DUPLICATE KEY UPDATE type = %(type)s',
                           { 'account_id': account_id, 'resource': resource, 'type': reaction_type })
        mariapersist_session.commit()
        return "{}"


@dyn.put("/lists_update/<string:resource>")
@allthethings.utils.no_cache()
def lists_update(resource):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with Session(mariapersist_engine) as mariapersist_session:
        resource_type = get_resource_type(resource)
        if resource_type not in ['md5']:
            raise Exception("Invalid resource")

        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)
        cursor.execute('SELECT l.list_id, le.list_entry_id FROM mariapersist_lists l '
                       'LEFT JOIN mariapersist_list_entries le ON l.list_id = le.list_id '
                       '    AND l.account_id = le.account_id AND le.resource = %(resource)s '
                       'WHERE l.account_id = %(account_id)s '
                       'ORDER BY l.updated DESC '
                       'LIMIT 10000',
                       { 'account_id': account_id, 'resource': resource })
        my_lists = cursor.fetchall()

        selected_list_ids = set([list_id for list_id in request.form.keys() if list_id != 'list_new_name' and request.form[list_id] == 'on'])
        list_ids_to_add = []
        list_ids_to_remove = []
        for list_record in my_lists:
            if list_record['list_entry_id'] is None and list_record['list_id'] in selected_list_ids:
                list_ids_to_add.append(list_record['list_id'])
            elif list_record['list_entry_id'] is not None and list_record['list_id'] not in selected_list_ids:
                list_ids_to_remove.append(list_record['list_id'])
        list_new_name = request.form['list_new_name'].strip()

        if len(list_new_name) > 0:
            for _ in range(5):
                insert_data = { 'list_id': shortuuid.random(length=7), 'account_id': account_id, 'name': list_new_name }
                try:
                    cursor.execute('INSERT INTO mariapersist_lists (list_id, account_id, name) VALUES (%(list_id)s, %(account_id)s, %(name)s)',
                                   insert_data)
                    list_ids_to_add.append(insert_data['list_id'])
                    break
                except Exception as err:
                    print("List creation error", err)
                    pass

        if len(list_ids_to_add) > 0:
            cursor.executemany('INSERT INTO mariapersist_list_entries (account_id, list_id, resource) VALUES (%(account_id)s, %(list_id)s, %(resource)s)',
                [{ 'account_id': account_id, 'list_id': list_id, 'resource': resource } for list_id in list_ids_to_add])
        if len(list_ids_to_remove) > 0:
            cursor.executemany('DELETE FROM mariapersist_list_entries WHERE account_id = %(account_id)s AND resource = %(resource)s AND list_id = %(list_id)s',
                [{ 'account_id': account_id, 'list_id': list_id, 'resource': resource } for list_id in list_ids_to_remove])
        mariapersist_session.commit()

        return '{}'


@dyn.get("/lists/<string:resource>")
@allthethings.utils.no_cache()
def lists(resource):
    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

        cursor.execute('SELECT l.list_id, l.name, a.display_name, a.account_id FROM mariapersist_lists l '
                       'INNER JOIN mariapersist_list_entries le USING(list_id) '
                       'INNER JOIN mariapersist_accounts a ON l.account_id = a.account_id '
                       'WHERE le.resource = %(resource)s '
                       'ORDER BY l.updated DESC '
                       'LIMIT 10000',
                       { 'resource': resource })
        resource_lists = cursor.fetchall()

        my_lists = []
        account_id = allthethings.utils.get_account_id(request.cookies)
        if account_id is not None:
            cursor.execute('SELECT l.list_id, l.name, le.list_entry_id FROM mariapersist_lists l '
                           'LEFT JOIN mariapersist_list_entries le ON l.list_id = le.list_id '
                           '    AND l.account_id = le.account_id AND le.resource = %(resource)s '
                           'WHERE l.account_id = %(account_id)s ' 
                           'ORDER BY l.updated DESC '
                           'LIMIT 10000',
                           { 'account_id': account_id, 'resource': resource })
            my_lists = cursor.fetchall()

        return render_template(
            "dyn/lists.html",
            resource_list_dicts=[dict(list_record) for list_record in resource_lists],
            my_list_dicts=[{ "list_id": list_record['list_id'], "name": list_record['name'], "selected": list_record['list_entry_id'] is not None } for list_record in my_lists],
            reload_url=f"/dyn/lists/{resource}",
            resource=resource,
        )

@dyn.get("/search_counts")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def search_counts_page():
    search_input = request.args.get("q", "").strip()

    search_query = None
    if search_input != "":
        search_query = {
            "bool": {
                "should": [
                    { "match_phrase": { "search_only_fields.search_text": { "query": search_input } } },
                    { "simple_query_string": {"query": search_input, "fields": ["search_only_fields.search_text"], "default_operator": "and"} },
                ],
            },
        }

    multi_searches_by_es_handle = collections.defaultdict(list)
    indexes = list(allthethings.utils.SEARCH_INDEX_SHORT_LONG_MAPPING.values())
    for search_index in indexes:
        multi_searches = multi_searches_by_es_handle[allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index]]
        multi_searches.append({ "index": allthethings.utils.all_virtshards_for_index(search_index) })
        if search_query is None:
            multi_searches.append({ "size": 0, "track_total_hits": True, "timeout": ES_TIMEOUT_PRIMARY })
        else:
            multi_searches.append({ "size": 0, "query": search_query, "track_total_hits": 100, "timeout": ES_TIMEOUT_PRIMARY })

    total_by_index_long = {index: {'value': -1, 'relation': ''} for index in indexes}
    any_timeout = False
    try:
        # TODO: do these in parallel?
        for es_handle, multi_searches in multi_searches_by_es_handle.items():
            total_all_indexes = es_handle.msearch(
                request_timeout=10,
                max_concurrent_searches=10,
                max_concurrent_shard_requests=10,
                searches=multi_searches,
            )
            for i, result in enumerate(total_all_indexes['responses']):
                if 'hits' in result:
                    result['hits']['total']['value_formatted'] = babel_numbers.format_number(result['hits']['total']['value'], locale=get_locale())
                    total_by_index_long[multi_searches[i*2]['index'][0].split('__', 1)[0]] = result['hits']['total']
                if result['timed_out']:
                    total_by_index_long[multi_searches[i*2]['index'][0].split('__', 1)[0]]['timed_out'] = True
                    any_timeout = True
                total_by_index_long[multi_searches[i*2]['index'][0].split('__', 1)[0]]['took'] = result['took']
    except Exception:
        pass

    r = make_response(orjson.dumps(total_by_index_long))
    if any_timeout:
        r.headers.add('Cache-Control', 'no-cache')
    return r


@dyn.put("/account/buy_membership/")
@allthethings.utils.no_cache()
def account_buy_membership():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    tier = request.form['tier']
    method = request.form['method']
    duration = request.form['duration']
    # This also makes sure that the values above are valid.
    membership_costs = allthethings.utils.membership_costs_data('en')[f"{tier},{method},{duration}"]

    cost_cents_usd_verification = request.form['costCentsUsdVerification']
    if str(membership_costs['cost_cents_usd']) != cost_cents_usd_verification:
        raise Exception("Invalid costCentsUsdVerification")

    donation_type = 0 # manual
    if method in ['payment1', 'payment1_alipay', 'payment1_wechat', 'payment1b', 'payment1bb', 'payment2', 'payment2paypal', 'payment2cashapp', 'payment2revolut', 'payment2cc', 'amazon', 'hoodpay', 'payment3a', 'payment3b']:
        donation_type = 1

    with Session(mariapersist_engine) as mariapersist_session:
        donation_id = shortuuid.uuid()
        donation_json = {
            'tier': tier,
            'method': method,
            'duration': duration,
            'monthly_cents': membership_costs['monthly_cents'],
            'discounts': membership_costs['discounts'],
            # 'ref_account_id': allthethings.utils.get_referral_account_id(mariapersist_session, request.cookies.get('ref_id'), account_id),
        }

        if method == 'hoodpay':
            payload = {
                "metadata": { "donation_id": donation_id },
                "name": "Anna",
                "currency": "USD",
                "amount": round(float(membership_costs['cost_cents_usd']) / 100.0, 2),
                "redirectUrl": "https://annas-archive.se/account",
                "notifyUrl": f"https://annas-archive.se/dyn/hoodpay_notify/{donation_id}",
            }
            response = httpx.post(HOODPAY_URL, json=payload, headers={"Authorization": f"Bearer {HOODPAY_AUTH}"}, proxies=PAYMENT2_PROXIES, timeout=10.0)
            response.raise_for_status()
            donation_json['hoodpay_request'] = response.json()

        if method in ['payment3a', 'payment3b']:
            data = {
                # Note that these are sorted by key.
                "amount": str(int(float(membership_costs['cost_cents_usd']) * allthethings.utils.MEMBERSHIP_EXCHANGE_RATE_RMB / 100.0)),
                "callbackUrl": "https://annas-archive.se/dyn/payment3_notify/",
                "clientIp": "1.1.1.1",
                "mchId": 20000007,
                "mchOrderId": donation_id,
                "payerName": "Anna",
                "productId": 8038 if method == 'payment3a' else 8055,
                "remark": "",
                "time": int(time.time()),
            }
            sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + "&key=" + PAYMENT3_KEY
            sign = hashlib.md5((sign_str).encode()).hexdigest()
            response = httpx.post(f"https://{PAYMENT3_DOMAIN}/api/deposit/create-order", data={ **data, "sign": sign }, proxies=PAYMENT2_PROXIES, timeout=10.0)
            response.raise_for_status()
            donation_json['payment3_request'] = response.json()
            if str(donation_json['payment3_request']['code']) != '1':
                print(f"Warning payment3_request error: {donation_json['payment3_request']}")
                return orjson.dumps({ 'error': gettext('dyn.buy_membership.error.unknown', email="https://annas-archive.se/contact") })

        if method in ['payment2', 'payment2paypal', 'payment2cashapp', 'payment2revolut', 'payment2cc']:
            if method == 'payment2':
                pay_currency = request.form['pay_currency']
            elif method == 'payment2paypal':
                pay_currency = 'pyusd'
            elif method in ['payment2cc', 'payment2cashapp', 'payment2revolut']:
                pay_currency = 'btc'
            if pay_currency not in ['btc','eth','ethbase','bch','ltc','xmr','ada','bnbbsc','busdbsc','dai','doge','dot','matic','near','pax','pyusd','sol','ton','trx','tusd','usdc','usdtbsc','usdterc20','usdttrc20','usdtsol']: # No XRP, needs a "tag"
                raise Exception(f"Invalid pay_currency: {pay_currency}")

            price_currency = 'usd'
            if pay_currency in ['busdbsc','dai','pyusd','tusd','usdc','usdterc20','usdttrc20']:
                price_currency = pay_currency

            if (pay_currency == 'btc') and (membership_costs['cost_cents_usd'] < 1000):
                return orjson.dumps({ 'error': gettext('dyn.buy_membership.error.minimum') })

            response = None
            try:
                response = httpx.post(PAYMENT2_URL, headers={'x-api-key': PAYMENT2_API_KEY}, proxies=PAYMENT2_PROXIES, timeout=10.0, json={
                    "price_amount": round(float(membership_costs['cost_cents_usd']) * (1.03 if price_currency == 'usd' else 1.0) / 100.0, 2),
                    "price_currency": price_currency,
                    "pay_currency": pay_currency,
                    "order_id": donation_id,
                })
                donation_json['payment2_request'] = response.json()
            except httpx.HTTPError:
                return orjson.dumps({ 'error': gettext('dyn.buy_membership.error.try_again', email="https://annas-archive.se/contact") })
            except Exception as err:
                print(f"Warning: unknown error in payment2 http request: {repr(err)} /// {traceback.format_exc()}")
                return orjson.dumps({ 'error': gettext('dyn.buy_membership.error.unknown', email="https://annas-archive.se/contact") })


            if 'code' in donation_json['payment2_request']:
                if donation_json['payment2_request']['code'] == 'AMOUNT_MINIMAL_ERROR':
                    return orjson.dumps({ 'error': gettext('dyn.buy_membership.error.minimum') })
                elif donation_json['payment2_request']['code'] == 'INTERNAL_ERROR':
                    print(f"Warning: internal error in payment2_request: {donation_json['payment2_request']=}")
                    return orjson.dumps({ 'error': gettext('dyn.buy_membership.error.wait', email="https://annas-archive.se/contact") })
                else:
                    print(f"Warning: unknown error in payment2 with code missing: {donation_json['payment2_request']} /// {curlify2.to_curl(response.request)}")
                    return orjson.dumps({ 'error': gettext('dyn.buy_membership.error.unknown', email="https://annas-archive.se/contact") })

        
        # existing_unpaid_donations_counts = mariapersist_session.connection().execute(select(func.count(MariapersistDonations.donation_id)).where((MariapersistDonations.account_id == account_id) & ((MariapersistDonations.processing_status == 0) | (MariapersistDonations.processing_status == 4))).limit(1)).scalar()
        # if existing_unpaid_donations_counts > 0:
        #     raise Exception(f"Existing unpaid or manualconfirm donations open")

        data = {
            'donation_id': donation_id,
            'account_id': account_id,
            'cost_cents_usd': membership_costs['cost_cents_usd'],
            'cost_cents_native_currency': membership_costs['cost_cents_native_currency'],
            'native_currency_code': membership_costs['native_currency_code'],
            'processing_status': 0, # unpaid
            'donation_type': donation_type,
            'ip': allthethings.utils.canonical_ip_bytes(request.remote_addr),
            'json': orjson.dumps(donation_json),
        }
        mariapersist_session.execute('INSERT INTO mariapersist_donations (donation_id, account_id, cost_cents_usd, cost_cents_native_currency, native_currency_code, processing_status, donation_type, ip, json) VALUES (:donation_id, :account_id, :cost_cents_usd, :cost_cents_native_currency, :native_currency_code, :processing_status, :donation_type, :ip, :json)', [data])
        mariapersist_session.commit()

        return orjson.dumps({ 'redirect_url': '/account/donations/' + data['donation_id'] })


@dyn.put("/account/mark_manual_donation_sent/<string:donation_id>")
@allthethings.utils.no_cache()
def account_mark_manual_donation_sent(donation_id):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

        cursor.execute('SELECT * FROM mariapersist_donations WHERE account_id = %(account_id)s AND processing_status = 0 AND donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'account_id': account_id })
        donation = cursor.fetchone()
        if donation is None:
            return "", 403

        cursor.execute('UPDATE mariapersist_donations SET processing_status = 4 WHERE donation_id = %(donation_id)s AND processing_status = 0 AND account_id = %(account_id)s LIMIT 1', { 'donation_id': donation_id, 'account_id': account_id })
        mariapersist_session.commit()
        return "{}"


@dyn.put("/account/cancel_donation/<string:donation_id>")
@allthethings.utils.no_cache()
def account_cancel_donation(donation_id):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

        cursor.execute('SELECT * FROM mariapersist_donations WHERE account_id = %(account_id)s AND (processing_status = 0 OR processing_status = 4) AND donation_id = %(donation_id)s LIMIT 1', { 'account_id': account_id, 'donation_id': donation_id })
        donation = cursor.fetchone()
        if donation is None:
            return "", 403

        cursor.execute('UPDATE mariapersist_donations SET processing_status = 2 WHERE donation_id = %(donation_id)s AND (processing_status = 0 OR processing_status = 4) AND account_id = %(account_id)s LIMIT 1', { 'donation_id': donation_id, 'account_id': account_id })
        mariapersist_session.commit()
        return "{}"


@dyn.get("/recent_downloads/")
@allthethings.utils.public_cache(minutes=1, cloudflare_minutes=1)
@cross_origin()
def recent_downloads():
    with Session(engine):
        with Session(mariapersist_engine) as mariapersist_session:
            cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

            cursor.execute('SELECT * FROM mariapersist_downloads ORDER BY timestamp DESC LIMIT 50')
            downloads = cursor.fetchall()

            aarecords = []
            if len(downloads) > 0:
                aarecords = get_aarecords_elasticsearch(['md5:' + download['md5'].hex() for download in downloads])
            seen_ids = set()
            seen_titles = set()
            output = []
            for aarecord in aarecords:
                title = aarecord['file_unified_data']['title_best']
                if aarecord['id'] not in seen_ids and title not in seen_titles:
                    output.append({ 'path': aarecord['additional']['path'], 'title': title })
                seen_ids.add(aarecord['id'])
                seen_titles.add(title)
            return orjson.dumps(output)

@dyn.post("/log_search")
@allthethings.utils.no_cache()
def log_search():
    # search_input = request.args.get("q", "").strip()
    # if len(search_input) > 0:
    #     with Session(mariapersist_engine) as mariapersist_session:
    #         mariapersist_session.connection().execute(text('INSERT INTO mariapersist_searches (search_input) VALUES (:search_input)').bindparams(search_input=search_input.encode('utf-8')))
    #         mariapersist_session.commit()
    return ""

@dyn.get("/payment1_notify/")
@allthethings.utils.no_cache()
def payment1_notify():
    return payment1_common_notify(PAYMENT1_KEY, 'payment1_notify')

@dyn.get("/payment1b_notify/")
@allthethings.utils.no_cache()
def payment1b_notify():
    return payment1_common_notify(PAYMENT1B_KEY, 'payment1b_notify')

def payment1_common_notify(sign_key, data_key):
    data = {
        # Note that these are sorted by key.
        "money": request.args.get('money'),
        "name": request.args.get('name'),
        "out_trade_no": request.args.get('out_trade_no'),
        "pid": request.args.get('pid'),
        "trade_no": request.args.get('trade_no'),
        "trade_status": request.args.get('trade_status'),
        "type": request.args.get('type'),
    }
    sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + sign_key
    sign = hashlib.md5((sign_str).encode()).hexdigest()
    if sign != request.args.get('sign'):
        print(f"Warning: failed {data_key} request because of incorrect signature {sign_str} /// {dict(request.args)}.")
        return "fail"
    if data['trade_status'] == 'TRADE_SUCCESS':
        with mariapersist_engine.connect() as connection:
            donation_id = data['out_trade_no']
            connection.connection.ping(reconnect=True)
            cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
            if allthethings.utils.confirm_membership(cursor, donation_id, data_key, data):
                return "success"
            else:
                return "fail"
    return "success"

@dyn.post("/payment2_notify/")
@allthethings.utils.no_cache()
def payment2_notify():
    sign_str = orjson.dumps(dict(sorted(request.json.items())))
    if request.headers.get(PAYMENT2_SIG_HEADER) != hmac.new(PAYMENT2_HMAC.encode(), sign_str, hashlib.sha512).hexdigest():
        print(f"Warning: failed payment2_notify request because of incorrect signature {sign_str} /// {dict(sorted(request.json.items()))}.")
        return "Bad request", 404
    with mariapersist_engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        payment2_status, payment2_request_success = allthethings.utils.payment2_check(cursor, request.json['payment_id'])
        if not payment2_request_success:
            return "Error happened", 404
    return ""

@dyn.post("/payment3_notify/")
@allthethings.utils.no_cache()
def payment3_notify():
    data = {
        # Note that these are sorted by key.
        "amount": request.form.get('amount', ''),
        "mchOrderId": request.form.get('mchOrderId', ''),
        "orderId": request.form.get('orderId', ''),
        "remark": request.form.get('remark', ''),
        "status": request.form.get('status', ''),
        "time": request.form.get('time', ''),
    }
    sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + "&key=" + PAYMENT3_KEY
    sign = hashlib.md5((sign_str).encode()).hexdigest()
    if sign != request.form.get('sign', ''):
        print(f"Warning: failed payment3_status_callback request because of incorrect signature {sign_str} /// {dict(request.args)}.")
        return "FAIL"
    if str(data['status']) in ['2','3']:
        with mariapersist_engine.connect() as connection:
            donation_id = data['mchOrderId']
            connection.connection.ping(reconnect=True)
            cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
            if allthethings.utils.confirm_membership(cursor, donation_id, 'payment3_status_callback', data):
                return "SUCCESS"
            else:
                return "FAIL"
    return "SUCCESS"


@dyn.post("/hoodpay_notify/")
@allthethings.utils.no_cache()
def hoodpay_notify():
    donation_id = request.json['forPaymentEvents']['metadata']['donation_id']
    with mariapersist_engine.connect() as connection:
        cursor = allthethings.utils.get_cursor_ping_conn(connection)

        cursor.execute('SELECT * FROM mariapersist_donations WHERE donation_id = %(donation_id)s LIMIT 1')
        donation = cursor.fetchone()
        if donation is None:
            return "", 403
        donation_json = orjson.loads(donation['json'])
        hoodpay_status, hoodpay_request_success = allthethings.utils.hoodpay_check(cursor, donation_json['hoodpay_request']['data']['id'], donation_id)
        if not hoodpay_request_success:
            return "Error happened", 404
    return ""
# @dyn.post("/hoodpay_notify/<string:donation_id>")
# @allthethings.utils.no_cache()
# def hoodpay_notify(donation_id):
#     with mariapersist_engine.connect() as connection:
#         connection.connection.ping(reconnect=True)
#         donation = connection.execute(select(MariapersistDonations).where(MariapersistDonations.donation_id == donation_id).limit(1)).first()
#         if donation is None:
#             return "", 403
#         donation_json = orjson.loads(donation['json'])
#         cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
#         hoodpay_status, hoodpay_request_success = allthethings.utils.hoodpay_check(cursor, donation_json['hoodpay_request']['data']['id'], donation_id)
#         if not hoodpay_request_success:
#             return "Error happened", 404
#     return ""

@dyn.post("/gc_notify/")
@allthethings.utils.no_cache()
def gc_notify():
    request_data = request.get_data()
    message = email.message_from_bytes(request_data, policy=email.policy.default)
    
    if message['Subject'] is None:
        return ""

    to_split = message['X-Original-To'].replace('+', '@').split('@')
    if len(to_split) != 3:
        print(f"Warning: gc_notify message '{message['X-Original-To']}' with wrong X-Original-To: {message['X-Original-To']}")
        return "", 404
    donation_id = allthethings.utils.receipt_id_to_donation_id(to_split[1])

    with mariapersist_engine.connect() as connection:
        cursor = allthethings.utils.get_cursor_ping_conn(connection)
        cursor.execute('SELECT * FROM mariapersist_donations WHERE donation_id=%(donation_id)s LIMIT 1', { 'donation_id': donation_id })
        donation = cursor.fetchone()
        if donation is None:
            print(f"Warning: gc_notify message '{message['X-Original-To']}' donation_id not found {donation_id}")
            return "", 404

        if int(donation['processing_status']) == 1:
            # Already confirmed.
            return "", 404

        donation_json = orjson.loads(donation['json'])
        donation_json['gc_notify_debug'] = (donation_json.get('gc_notify_debug') or [])

        message_body = "\n\n".join([item.get_payload(decode=True).decode() for item in message.get_payload() if item is not None])

        def exec_err(error_txt):
            donation_json['gc_notify_debug'].append({ "error": error_txt, "message_body": message_body, "email_data": request_data.decode() })
            cursor.execute('UPDATE mariapersist_donations SET json=%(json)s WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
            cursor.execute('COMMIT')
            print(error_txt)
            return "", 404

        auth_results = "\n\n".join(message.get_all('Authentication-Results'))
        if "dkim=pass" not in auth_results:
            return exec_err(f"Warning: gc_notify message '{message['X-Original-To']}' with wrong auth_results: {auth_results}")

        if re.search(r'<gc-orders@gc\.email\.amazon\.com>$', message['From'].strip()) is None:
            return exec_err(f"Warning: gc_notify message '{message['X-Original-To']}' with wrong From: {message['From']}")

        if not (message['Subject'].strip().endswith('sent you an Amazon Gift Card!') or message['Subject'].strip().endswith('is waiting')):
            return exec_err(f"Warning: gc_notify message '{message['X-Original-To']}' with wrong Subject: {message['Subject']}")

        potential_money = re.findall(r"\n\$([0123456789]+\.[0123456789]{2})", message_body)
        if len(potential_money) == 0:
            return exec_err(f"Warning: gc_notify message '{message['X-Original-To']}' with no matches for potential_money")

        links = [str(link) for link in re.findall(r'(https://www.amazon.com/gp/r.html?[^\n)>"]+)', message_body)]
        if len(links) == 0:
            return exec_err(f"Warning: gc_notify message '{message['X-Original-To']}' with no matches for links")

        # Keep in sync!
        main_link = None
        for potential_link in links:
            if 'https%3A%2F%2Fwww.amazon.com%2Fg%2F' in potential_link:
                main_link = potential_link
                break
        if main_link is not None:
            main_link = main_link.split('https%3A%2F%2Fwww.amazon.com%2Fg%2F', 1)[1]
            main_link = main_link.split('%3F', 1)[0]
            main_link = f"https://www.amazon.com/g/{main_link}"
        cursor.execute('INSERT IGNORE INTO mariapersist_giftcards (donation_id, link, email_data) VALUES (%(donation_id)s, %(link)s, %(email_data)s)', { 'donation_id': donation_id, 'link': main_link, 'email_data': request_data })
        cursor.execute('COMMIT')

        # Keep in sync!
        money = float(potential_money[-1])
        # Allow for 5% margin
        if money * 105 < int(donation['cost_cents_usd']):
            return exec_err(f"Warning: gc_notify message '{message['X-Original-To']}' with too small amount gift card {money*110} < {donation['cost_cents_usd']}")

        sig = request.headers['X-GC-NOTIFY-SIG']
        if sig != GC_NOTIFY_SIG:
            return exec_err(f"Warning: gc_notify message '{message['X-Original-To']}' has incorrect signature: '{sig}'")

        data_value = { "links": links, "money": money }
        if not allthethings.utils.confirm_membership(cursor, donation_id, 'amazon_gc_done', data_value):
            return exec_err(f"Warning: gc_notify message '{message['X-Original-To']}' confirm_membership failed")
    return ""
