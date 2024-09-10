import datetime
import jwt
import shortuuid
import orjson
import babel
import hashlib
import re
import functools
import urllib
import pymysql

from flask import Blueprint, request, g, render_template, make_response, redirect
from sqlalchemy import text
from sqlalchemy.orm import Session
from flask_babel import gettext, force_locale, get_locale

from allthethings.extensions import mariapersist_engine
from allthethings.page.views import get_aarecords_elasticsearch
from config.settings import SECRET_KEY, PAYMENT1_ID, PAYMENT1_KEY, PAYMENT1B_ID, PAYMENT1B_KEY

import allthethings.utils


account = Blueprint("account", __name__, template_folder="templates")


@account.get("/account")
@account.get("/account/")
@allthethings.utils.no_cache()
def account_index_page():
    if (request.args.get('key', '') != '') and (not bool(re.match(r"^[a-zA-Z\d]+$", request.args.get('key')))):
        return redirect("/account/", code=302)

    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return render_template(
            "account/index.html",
            header_active="account",
            membership_tier_names=allthethings.utils.membership_tier_names(get_locale()),
        )

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)
        account = allthethings.utils.get_account_by_id(cursor, account_id)
        if account is None:
            raise Exception("Valid account_id was not found in db!")

        cursor.execute('SELECT membership_tier, membership_expiration, bonus_downloads, mariapersist_memberships.membership_expiration >= CURDATE() AS active FROM mariapersist_memberships WHERE account_id = %(account_id)s', { 'account_id': account_id })
        memberships = cursor.fetchall()

        membership_tier_names=allthethings.utils.membership_tier_names(get_locale())
        membership_dicts = []
        for membership in memberships:
            membership_tier_str = str(membership['membership_tier'])
            membership_name = membership_tier_names[membership_tier_str]
            if membership['bonus_downloads'] > 0:
                membership_name += gettext('common.donation.membership_bonus_parens', num=membership['bonus_downloads'])

            membership_dicts.append({
                **membership,
                'membership_name': membership_name,
            })

        return render_template(
            "account/index.html",
            header_active="account",
            account_dict=dict(account),
            account_fast_download_info=allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id),
            memberships=membership_dicts,
            account_secret_key=allthethings.utils.secret_key_from_account_id(account_id),
        )

@account.get("/account/secret_key")
@allthethings.utils.no_cache()
def account_secret_key_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return ''

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)
        account = allthethings.utils.get_account_by_id(cursor, account_id)
        if account is None:
            raise Exception("Valid account_id was not found in db!")

    return allthethings.utils.secret_key_from_account_id(account_id)

@account.get("/account/downloaded")
@allthethings.utils.no_cache()
def account_downloaded_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return redirect("/account/", code=302)

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

        cursor.execute('SELECT * FROM mariapersist_downloads WHERE account_id = %(account_id)s ORDER BY timestamp DESC LIMIT 1000', { 'account_id': account_id })
        downloads = list(cursor.fetchall())

        cursor.execute('SELECT * FROM mariapersist_fast_download_access WHERE account_id = %(account_id)s ORDER BY timestamp DESC LIMIT 1000',{'account_id': account_id})
        fast_downloads = list(cursor.fetchall())

        # TODO: This merging is not great, because the lists will get out of sync, so you get a gap toward the end.
        fast_downloads_ids_only = set([(download['timestamp'], f"md5:{download['md5'].hex()}") for download in fast_downloads])
        merged_downloads = sorted(set([(download['timestamp'], f"md5:{download['md5'].hex()}") for download in (downloads+fast_downloads)]), reverse=True)
        aarecords_downloaded_by_id = {}
        if len(downloads) > 0:
            aarecords_downloaded_by_id = {record['id']: record for record in get_aarecords_elasticsearch(list(set([row[1] for row in merged_downloads])))}
        aarecords_downloaded = [{ **aarecords_downloaded_by_id.get(row[1]), 'extra_download_timestamp': row[0], 'extra_was_fast_download': (row in fast_downloads_ids_only) } for row in merged_downloads if row[1] in aarecords_downloaded_by_id]
        cutoff_18h = datetime.datetime.utcnow() - datetime.timedelta(hours=18)
        aarecords_downloaded_last_18h = [row for row in aarecords_downloaded if row['extra_download_timestamp'] >= cutoff_18h]
        aarecords_downloaded_later = [row for row in aarecords_downloaded if row['extra_download_timestamp'] < cutoff_18h]

        return render_template("account/downloaded.html", header_active="account/downloaded", aarecords_downloaded_last_18h=aarecords_downloaded_last_18h, aarecords_downloaded_later=aarecords_downloaded_later)

@account.post("/account/")
@account.post("/account")
@allthethings.utils.no_cache()
def account_index_post_page():
    account_id = allthethings.utils.account_id_from_secret_key(request.form['key'])
    if account_id is None:
        return render_template(
            "account/index.html",
            invalid_key=True,
            header_active="account",
            membership_tier_names=allthethings.utils.membership_tier_names(get_locale()),
        )

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)
        account = allthethings.utils.get_account_by_id(cursor, account_id)
        if account is None:
            return render_template(
                "account/index.html",
                invalid_key=True,
                header_active="account",
                membership_tier_names=allthethings.utils.membership_tier_names(get_locale()),
            )

        cursor.execute('INSERT IGNORE INTO mariapersist_account_logins (account_id, ip) VALUES (%(account_id)s, %(ip)s)',
                       { 'account_id': account_id, 'ip': allthethings.utils.canonical_ip_bytes(request.remote_addr) })
        mariapersist_session.commit()

        account_token = jwt.encode(
            payload={ "a": account_id, "iat": datetime.datetime.now(tz=datetime.timezone.utc) },
            key=SECRET_KEY,
            algorithm="HS256"
        )
        resp = make_response(redirect("/account/", code=302))
        resp.set_cookie(
            key=allthethings.utils.ACCOUNT_COOKIE_NAME,
            value=allthethings.utils.strip_jwt_prefix(account_token),
            expires=datetime.datetime(9999,1,1),
            httponly=True,
            secure=g.secure_domain,
            domain=g.base_domain,
        )
        return resp


@account.post("/account/register")
@allthethings.utils.no_cache()
def account_register_page():
    with Session(mariapersist_engine) as mariapersist_session:
        account_id = None
        for _ in range(5):
            insert_data = { 'account_id': shortuuid.random(length=7) }
            try:
                mariapersist_session.connection().execute(text('INSERT INTO mariapersist_accounts (account_id, display_name) VALUES (:account_id, :account_id)').bindparams(**insert_data))
                mariapersist_session.commit()
                account_id = insert_data['account_id']
                break
            except Exception as err:
                print("Account creation error", err)
                pass
        if account_id is None:
            raise Exception("Failed to create account after multiple attempts")

        return redirect(f"/account/?key={allthethings.utils.secret_key_from_account_id(account_id)}", code=302)


@account.get("/account/request")
@allthethings.utils.no_cache()
def request_page():
    return redirect("/faq#request", code=301)


@account.get("/account/upload")
@allthethings.utils.no_cache()
def upload_page():
    return redirect("/faq#upload", code=301)

@account.get("/list/<string:list_id>")
@allthethings.utils.no_cache()
def list_page(list_id):
    current_account_id = allthethings.utils.get_account_id(request.cookies)

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

        cursor.execute('SELECT * FROM mariapersist_lists WHERE list_id = %(list_id)s LIMIT 1', { 'list_id': list_id })
        list_record = cursor.fetchone()
        if list_record is None:
            return "List not found", 404
        account = allthethings.utils.get_account_by_id(cursor, list_record['account_id'])

        cursor.execute('SELECT * FROM mariapersist_list_entries WHERE list_id = %(list_id)s ORDER BY updated DESC LIMIT 10000', { 'list_id': list_id })
        list_entries = cursor.fetchall()

        aarecords = []
        if len(list_entries) > 0:
            aarecords = get_aarecords_elasticsearch([entry['resource'] for entry in list_entries if entry['resource'].startswith("md5:")])

        return render_template(
            "account/list.html", 
            header_active="account",
            list_record_dict={ 
                **list_record,
                'created_delta': list_record['created'] - datetime.datetime.now(),
            },
            aarecords=aarecords,
            account_dict=dict(account),
            current_account_id=current_account_id,
        )


@account.get("/profile/<string:account_id>")
@allthethings.utils.no_cache()
def profile_page(account_id):
    current_account_id = allthethings.utils.get_account_id(request.cookies)

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)
        account = allthethings.utils.get_account_by_id(cursor, account_id)

        cursor.execute('SELECT * FROM mariapersist_lists WHERE account_id = %(account_id)s ORDER BY updated DESC LIMIT 10000', { 'account_id': account_id })
        lists = cursor.fetchall()

        if account is None:
            return render_template("account/profile.html", header_active="account"), 404

        return render_template(
            "account/profile.html", 
            header_active="account/profile" if account['account_id'] == current_account_id else "account",
            account_dict={ 
                **account,
                'created_delta': account['created'] - datetime.datetime.now(),
            },
            list_dicts=list(map(dict, lists)),
            current_account_id=current_account_id,
        )


@account.get("/account/profile")
@allthethings.utils.no_cache()
def account_profile_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403
    return redirect(f"/profile/{account_id}", code=302)


@account.get("/donate")
@allthethings.utils.no_cache()
def donate_page():
    with Session(mariapersist_engine) as mariapersist_session:
        account_id = allthethings.utils.get_account_id(request.cookies)
        has_made_donations = False
        existing_unpaid_donation_id = None
        if account_id is not None:
            cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

            cursor.execute('SELECT donation_id FROM mariapersist_donations WHERE account_id = %(account_id)s AND (processing_status = 0 OR processing_status = 4) LIMIT 1', { 'account_id': account_id })
            existing_unpaid_donation_id = allthethings.utils.fetch_one_field(cursor)

            cursor.execute('SELECT donation_id FROM mariapersist_donations WHERE account_id = %(account_id)s LIMIT 1', { 'account_id': account_id })
            previous_donation_id = allthethings.utils.fetch_one_field(cursor)

            if (existing_unpaid_donation_id is not None) or (previous_donation_id is not None):
                has_made_donations = True

        # ref_account_id = allthethings.utils.get_referral_account_id(mariapersist_session, request.cookies.get('ref_id'), account_id)
        # ref_account_dict = None
        # if ref_account_id is not None:
        #     ref_account_dict = dict(mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.account_id == ref_account_id).limit(1)).first())

        return render_template(
            "account/donate.html", 
            header_active="donate", 
            has_made_donations=has_made_donations,
            existing_unpaid_donation_id=existing_unpaid_donation_id,
            membership_costs_data=allthethings.utils.membership_costs_data(get_locale()),
            membership_tier_names=allthethings.utils.membership_tier_names(get_locale()),
            MEMBERSHIP_TIER_COSTS=allthethings.utils.MEMBERSHIP_TIER_COSTS,
            MEMBERSHIP_METHOD_DISCOUNTS=allthethings.utils.MEMBERSHIP_METHOD_DISCOUNTS,
            MEMBERSHIP_DURATION_DISCOUNTS=allthethings.utils.MEMBERSHIP_DURATION_DISCOUNTS,
            MEMBERSHIP_DOWNLOADS_PER_DAY=allthethings.utils.MEMBERSHIP_DOWNLOADS_PER_DAY,
            MEMBERSHIP_METHOD_MINIMUM_CENTS_USD=allthethings.utils.MEMBERSHIP_METHOD_MINIMUM_CENTS_USD,
            MEMBERSHIP_METHOD_MAXIMUM_CENTS_NATIVE=allthethings.utils.MEMBERSHIP_METHOD_MAXIMUM_CENTS_NATIVE,
            MEMBERSHIP_MAX_BONUS_DOWNLOADS=allthethings.utils.MEMBERSHIP_MAX_BONUS_DOWNLOADS,
            days_parity=(datetime.datetime.utcnow() - datetime.datetime(1970,1,1)).days,
            # ref_account_dict=ref_account_dict,
        )


@account.get("/donation_faq")
@allthethings.utils.no_cache()
def donation_faq_page():
    return redirect("/faq#donate", code=301)

@functools.cache
def get_order_processing_status_labels(locale):
    with force_locale(locale):
        return {
            0: gettext('common.donation.order_processing_status_labels.0'),
            1: gettext('common.donation.order_processing_status_labels.1'),
            2: gettext('common.donation.order_processing_status_labels.2'),
            3: gettext('common.donation.order_processing_status_labels.3'),
            4: gettext('common.donation.order_processing_status_labels.4'),
            5: gettext('common.donation.order_processing_status_labels.5'),
        }


def make_donation_dict(donation):
    donation_json = orjson.loads(donation['json'])
    return {
        **donation,
        'json': donation_json,
        'total_amount_usd': babel.numbers.format_currency(donation['cost_cents_usd'] / 100.0, 'USD', locale=get_locale()),
        'monthly_amount_usd': babel.numbers.format_currency(donation_json['monthly_cents'] / 100.0, 'USD', locale=get_locale()),
        'receipt_id': allthethings.utils.donation_id_to_receipt_id(donation['donation_id']),
        'formatted_native_currency': allthethings.utils.membership_format_native_currency(get_locale(), donation['native_currency_code'], donation['cost_cents_native_currency'], donation['cost_cents_usd']),
    }


@account.get("/account/donations/<string:donation_id>")
@allthethings.utils.no_cache()
def donation_page(donation_id):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    donation_confirming = False
    donation_time_left = datetime.timedelta()
    donation_time_left_not_much = False
    donation_time_expired = False
    donation_pay_amount = ""

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)

        cursor.execute('SELECT * FROM mariapersist_donations WHERE account_id = %(account_id)s AND donation_id = %(donation_id)s LIMIT 1', { 'account_id': account_id, 'donation_id': donation_id })
        donation = cursor.fetchone()

        #donation = mariapersist_session.connection().execute(select(MariapersistDonations).where((MariapersistDonations.account_id == account_id) & (MariapersistDonations.donation_id == donation_id)).limit(1)).first()
        if donation is None:
            return "", 403

        donation_json = orjson.loads(donation['json'])

        if donation_json['method'] == 'payment1' and donation['processing_status'] == 0:
            data = {
                # Note that these are sorted by key.
                "money": str(int(float(donation['cost_cents_usd']) * allthethings.utils.MEMBERSHIP_EXCHANGE_RATE_RMB / 100.0)),
                "name": "Anna’s Archive Membership",
                "notify_url": "https://annas-archive.se/dyn/payment1_notify/",
                "out_trade_no": str(donation['donation_id']),
                "pid": PAYMENT1_ID,
                "return_url": "https://annas-archive.se/account/",
                "sitename": "Anna’s Archive",
            }
            sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + PAYMENT1_KEY
            sign = hashlib.md5((sign_str).encode()).hexdigest()
            return redirect(f'https://integrate.payments-gateway.org/submit.php?{urllib.parse.urlencode(data)}&sign={sign}&sign_type=MD5', code=302)
        if donation_json['method'] == 'payment1_alipay' and donation['processing_status'] == 0:
            data = {
                # Note that these are sorted by key.
                "money": str(int(float(donation['cost_cents_usd']) * allthethings.utils.MEMBERSHIP_EXCHANGE_RATE_RMB / 100.0)),
                "name": "Anna’s Archive Membership",
                "notify_url": "https://annas-archive.se/dyn/payment1_notify/",
                "out_trade_no": str(donation['donation_id']),
                "pid": PAYMENT1_ID,
                "return_url": "https://annas-archive.se/account/",
                "sitename": "Anna’s Archive",
                "type": "alipay",
            }
            sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + PAYMENT1_KEY
            sign = hashlib.md5((sign_str).encode()).hexdigest()
            return redirect(f'https://integrate.payments-gateway.org/submit.php?{urllib.parse.urlencode(data)}&sign={sign}&sign_type=MD5', code=302)
        if donation_json['method'] == 'payment1_wechat' and donation['processing_status'] == 0:
            data = {
                # Note that these are sorted by key.
                "money": str(int(float(donation['cost_cents_usd']) * allthethings.utils.MEMBERSHIP_EXCHANGE_RATE_RMB / 100.0)),
                "name": "Anna’s Archive Membership",
                "notify_url": "https://annas-archive.se/dyn/payment1_notify/",
                "out_trade_no": str(donation['donation_id']),
                "pid": PAYMENT1_ID,
                "return_url": "https://annas-archive.se/account/",
                "sitename": "Anna’s Archive",
                "type": "wxpay",
            }
            sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + PAYMENT1_KEY
            sign = hashlib.md5((sign_str).encode()).hexdigest()
            return redirect(f'https://integrate.payments-gateway.org/submit.php?{urllib.parse.urlencode(data)}&sign={sign}&sign_type=MD5', code=302)

        if donation_json['method'] in ['payment1b', 'payment1bb'] and donation['processing_status'] == 0:
            data = {
                # Note that these are sorted by key.
                "money": str(int(float(donation['cost_cents_usd']) * allthethings.utils.MEMBERSHIP_EXCHANGE_RATE_RMB / 100.0)),
                "name": "Anna’s Archive Membership",
                "notify_url": "https://annas-archive.se/dyn/payment1b_notify/",
                "out_trade_no": str(donation['donation_id']),
                "pid": PAYMENT1B_ID,
                "return_url": "https://annas-archive.se/account/",
                "sitename": "Anna’s Archive",
            }
            sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + PAYMENT1B_KEY
            sign = hashlib.md5((sign_str).encode()).hexdigest()
            return redirect(f'https://anna.zpay.se/submit.php?{urllib.parse.urlencode(data)}&sign={sign}&sign_type=MD5', code=302)

        if donation_json['method'] in ['payment2', 'payment2paypal', 'payment2cashapp', 'payment2revolut', 'payment2cc'] and donation['processing_status'] == 0:
            donation_time_left = donation['created'] - datetime.datetime.now() + datetime.timedelta(days=1)
            if donation_time_left < datetime.timedelta(hours=2):
                donation_time_left_not_much = True
            if donation_time_left < datetime.timedelta():
                donation_time_expired = True

            if donation_json['payment2_request']['pay_amount']*100 == int(donation_json['payment2_request']['pay_amount']*100):
                donation_pay_amount = f"{donation_json['payment2_request']['pay_amount']:.2f}"
            else:
                donation_pay_amount = f"{donation_json['payment2_request']['pay_amount']}"

            mariapersist_session.connection().connection.ping(reconnect=True)
            cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)
            payment2_status, payment2_request_success = allthethings.utils.payment2_check(cursor, donation_json['payment2_request']['payment_id'])
            if not payment2_request_success:
                raise Exception("Not payment2_request_success in donation_page")
            if payment2_status['payment_status'] == 'confirming':
                donation_confirming = True


        if donation_json['method'] in ['payment3a', 'payment3b'] and donation['processing_status'] == 0:
            # return redirect(donation_json['payment3_request']['data']['url'], code=302)
            donation_time_left = donation['created'] - datetime.datetime.now() + datetime.timedelta(hours=2)
            if donation_time_left < datetime.timedelta(minutes=30):
                donation_time_left_not_much = True
            if donation_time_left < datetime.timedelta():
                donation_time_expired = True

            mariapersist_session.connection().connection.ping(reconnect=True)
            cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)
            payment3_status, payment3_request_success = allthethings.utils.payment3_check(cursor, donation['donation_id'])
            if not payment3_request_success:
                raise Exception("Not payment3_request_success in donation_page")
            if str(payment3_status['data']['status']) == '-2':
                donation_time_expired = True

        if donation_json['method'] in ['hoodpay'] and donation['processing_status'] == 0:
            donation_time_left = donation['created'] - datetime.datetime.now() + datetime.timedelta(minutes=30)
            if donation_time_left < datetime.timedelta(minutes=10):
                donation_time_left_not_much = True
            if donation_time_left < datetime.timedelta():
                donation_time_expired = True

            mariapersist_session.connection().connection.ping(reconnect=True)
            cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)

            hoodpay_status, hoodpay_request_success = allthethings.utils.hoodpay_check(cursor, donation_json['hoodpay_request']['data']['id'], str(donation['donation_id']))
            if not hoodpay_request_success:
                raise Exception("Not hoodpay_request_success in donation_page")
            if hoodpay_status['status'] in ['PENDING', 'PROCESSING']:
                donation_confirming = True

        donation_dict = make_donation_dict(donation)

        donation_email = f"AnnaReceipts+{donation_dict['receipt_id']}@proton.me"
        if donation_json['method'] == 'amazon':
            donation_email = f"giftcards+{donation_dict['receipt_id']}@annas-archive.se"

        # # No need to call get_referral_account_id here, because we have already verified, and we don't want to take away their bonus because
        # # the referrer's membership expired.
        # ref_account_id = donation_json.get('ref_account_id')
        # ref_account_dict = None
        # if ref_account_id is not None:
        #     ref_account_dict = dict(mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.account_id == ref_account_id).limit(1)).first())

        return render_template(
            "account/donation.html", 
            header_active="account/donations",
            donation_dict=donation_dict,
            order_processing_status_labels=get_order_processing_status_labels(get_locale()),
            donation_confirming=donation_confirming,
            donation_time_left=donation_time_left,
            donation_time_left_not_much=donation_time_left_not_much,
            donation_time_expired=donation_time_expired,
            donation_pay_amount=donation_pay_amount,
            donation_email=donation_email,
            account_secret_key=allthethings.utils.secret_key_from_account_id(account_id),
            # ref_account_dict=ref_account_dict,
        )


@account.get("/account/donations")
@account.get("/account/donations/")
@allthethings.utils.no_cache()
def donations_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with Session(mariapersist_engine) as mariapersist_session:
        cursor = allthethings.utils.get_cursor_ping(mariapersist_session)
        cursor.execute('SELECT * FROM mariapersist_donations WHERE account_id = %(account_id)s ORDER BY created DESC LIMIT 10000', { 'account_id': account_id })
        donations = cursor.fetchall()

        return render_template(
            "account/donations.html",
            header_active="account/donations",
            donation_dicts=[make_donation_dict(donation) for donation in donations],
            order_processing_status_labels=get_order_processing_status_labels(get_locale()),
        )







