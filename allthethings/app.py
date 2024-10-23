import hashlib
import os
import functools
import base64
import sys
import time
import babel.numbers as babel_numbers
import babel.lists as babel_list
import multiprocessing
import ipaddress
import datetime
import calendar
import random

from celery import Celery
from flask import Flask, request, g, redirect
from werkzeug.security import safe_join
from werkzeug.debug import DebuggedApplication
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_babel import get_locale, get_translations, force_locale, gettext
from sqlalchemy.orm import Session

from allthethings.account.views import account
from allthethings.blog.views import blog
from allthethings.page.views import page, all_search_aggs
from allthethings.dyn.views import dyn
from allthethings.cli.views import cli
from allthethings.extensions import engine, mariapersist_engine, babel, debug_toolbar, flask_static_digest, mail
from config.settings import SECRET_KEY, DOWNLOADS_SECRET_KEY, X_AA_SECRET

import allthethings.utils

multiprocessing.set_start_method('spawn', force=True)

def create_celery_app(app=None):
    """
    Create a new Celery app and tie together the Celery config to the app's
    config. Wrap all tasks in the context of the application.

    :param app: Flask app
    :return: Celery app
    """
    app = app or create_app()

    celery = Celery(app.import_name)
    celery.conf.update(app.config.get("CELERY_CONFIG", {}))
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask

    return celery


def create_app(settings_override=None):
    """
    Create a Flask application using the app factory pattern.

    :param settings_override: Override settings
    :return: Flask app
    """
    app = Flask(__name__, static_folder="../public", static_url_path="")

    app.config.from_object("config.settings")

    if settings_override:
        app.config.update(settings_override)

    if not app.debug and len(SECRET_KEY) < 30:
        raise Exception(f"Use longer SECRET_KEY! {SECRET_KEY=} {len(SECRET_KEY)=}")
    if not app.debug and len(DOWNLOADS_SECRET_KEY) < 30:
        raise Exception(f"Use longer DOWNLOADS_SECRET_KEY! {DOWNLOADS_SECRET_KEY=} {len(DOWNLOADS_SECRET_KEY)=}")

    middleware(app)

    app.register_blueprint(account)
    app.register_blueprint(blog)
    app.register_blueprint(dyn)
    app.register_blueprint(page)
    app.register_blueprint(cli)

    extensions(app)

    return app

@functools.cache
def get_static_file_contents(filepath):
    if os.path.isfile(filepath):
        with open(filepath, 'r') as static_file:
            return static_file.read()
    return ''

def jinja_md5(s):
    return hashlib.md5(s.encode()).hexdigest()

def jinja_urlsafe_b64encode(string):
    return base64.urlsafe_b64encode(string.encode()).decode()

def jinja_format_list(lst, style='standard'):
    return babel_list.format_list(lst, style=style, locale=get_locale())

# https://stackoverflow.com/a/31608030
def jinja_shuffle_stable_day(l):
    result = list(l)
    random.Random(datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%d")).shuffle(result)
    return result

def extensions(app):
    """
    Register 0 or more extensions (mutates the app passed in).

    :param app: Flask application instance
    :return: None
    """
    debug_toolbar.init_app(app)
    flask_static_digest.init_app(app)
    with app.app_context():
        try:
            with Session(mariapersist_engine) as mariapersist_session:
                mariapersist_session.execute('SELECT 1')
        except Exception:
            if os.getenv("DATA_IMPORTS_MODE", "") == "1":
                print("Ignoring mariapersist not being online because DATA_IMPORTS_MODE=1")
            else:
                print("mariapersist not yet online, restarting")
                time.sleep(3)
                sys.exit(1)
    mail.init_app(app)

    def localeselector():
        potential_locale = request.headers['Host'].split('.')[0]
        if potential_locale in [allthethings.utils.get_domain_lang_code(locale) for locale in allthethings.utils.list_translations().values()]:
            selected_locale = allthethings.utils.domain_lang_code_to_full_lang_code(potential_locale)
            # print(f"{selected_locale=}")
            return selected_locale
        return 'en'
    babel.init_app(app, locale_selector=localeselector)

    # https://stackoverflow.com/a/57950565
    app.jinja_env.trim_blocks = True
    app.jinja_env.lstrip_blocks = True
    app.jinja_env.globals['get_locale'] = get_locale
    app.jinja_env.globals['make_code_for_display'] = allthethings.utils.make_code_for_display
    app.jinja_env.globals['md5'] = jinja_md5
    app.jinja_env.globals['FEATURE_FLAGS'] = allthethings.utils.FEATURE_FLAGS
    app.jinja_env.globals['urlsafe_b64encode'] = jinja_urlsafe_b64encode
    app.jinja_env.globals['format_list'] = jinja_format_list
    app.jinja_env.globals['shuffle_stable_day'] = jinja_shuffle_stable_day

    # https://stackoverflow.com/a/18095320
    hash_cache = {}
    @app.url_defaults
    def add_hash_for_static_files(endpoint, values):
        '''Add content hash argument for url to make url unique.
        It's have sense for updates to avoid caches.
        '''
        if endpoint != 'static':
            return
        filename = values['filename']
        # Exclude some.
        if filename in ['content-search.xml']:
            return
        if filename in hash_cache:
            values['hash'] = hash_cache[filename]
            return
        filepath = safe_join(app.static_folder, filename)
        if os.path.isfile(filepath):
            with open(filepath, 'rb') as static_file:
                filehash = hashlib.md5(static_file.read()).hexdigest()[:20]
                values['hash'] = hash_cache[filename] = filehash

    @functools.cache
    def last_data_refresh_date():
        try:
            with engine.connect() as conn:
                cursor = allthethings.utils.get_cursor_ping_conn(conn)

                cursor.execute('SELECT TimeLastModified FROM libgenrs_updated ORDER BY ID DESC LIMIT 1')
                libgenrs_time = allthethings.utils.fetch_one_field(cursor)

                cursor.execute('SELECT time_last_modified FROM libgenli_files ORDER BY f_id DESC LIMIT 1')
                libgenli_time = allthethings.utils.fetch_one_field(cursor)
            latest_time = max([libgenrs_time, libgenli_time])
            return latest_time.date()
        except Exception:
            return ''

    translations_with_english_fallback = set()
    @app.before_request
    def before_req():
        if X_AA_SECRET is not None and request.headers.get('x-aa-secret') != X_AA_SECRET and (not request.full_path.startswith('/dyn/up')):
            return gettext('layout.index.invalid_request', websites='annas-archive.li, .org')

        # Add English as a fallback language to all translations.
        translations = get_translations()
        if translations not in translations_with_english_fallback:
            with force_locale('en'):
                translations.add_fallback(get_translations())
            translations_with_english_fallback.add(translations)

        g.app_debug = app.debug
        g.base_domain = 'annas-archive.li'
        valid_other_domains = os.getenv('VALID_OTHER_DOMAINS').split(',')
        if app.debug:
            valid_other_domains.append('localtest.me:8000')
        # Not just for app.debug, but also for Docker health check.
        valid_other_domains.append('localhost:8000')
        for valid_other_domain in valid_other_domains:
            if request.headers['Host'].endswith(valid_other_domain):
                g.base_domain = valid_other_domain
                break

        g.domain_lang_code = allthethings.utils.get_domain_lang_code(get_locale())
        g.full_lang_code = allthethings.utils.get_full_lang_code(get_locale())

        g.secure_domain = g.base_domain not in ['localtest.me:8000', 'localhost:8000']
        g.full_domain = g.base_domain
        full_hostname = g.base_domain
        if g.domain_lang_code != 'en':
            g.full_domain = g.domain_lang_code + '.' + g.base_domain
            full_hostname = g.domain_lang_code + '.' + g.base_domain
        if g.secure_domain:
            g.full_domain = 'https://' + g.full_domain
        else:
            g.full_domain = 'http://' + g.full_domain

        # TODO: change proxies to use domain name in Host.
        host_is_ip = False
        try:
            ipaddress.ip_address(request.headers['Host'])
            host_is_ip = True
        except Exception:
            pass
        if (not host_is_ip) and (request.headers['Host'] != full_hostname):
            redir_path = f"{g.full_domain}{request.full_path}"
            print(f"Warning: redirecting {request.headers['Host']=} {request.full_path=} to {redir_path=} because {full_hostname=} {g.base_domain=}")
            return redirect(redir_path, code=301)

        g.languages = [(allthethings.utils.get_domain_lang_code(locale), allthethings.utils.get_domain_lang_code_display_name(locale), locale.get_display_name(get_locale())) for locale in allthethings.utils.list_translations().values()]
        g.languages.sort()

        g.last_data_refresh_date = last_data_refresh_date()
        doc_counts = {content_type['key']: content_type['doc_count'] for content_type in all_search_aggs('en', 'aarecords')[0]['search_content_type']}
        doc_counts['total_without_journals'] = sum(doc_counts.values())
        doc_counts_journals = {}
        try:
            doc_counts_journals = {content_type['key']: content_type['doc_count'] for content_type in all_search_aggs('en', 'aarecords_journals')[0]['search_content_type']}
        except Exception:
            pass
        doc_counts['journal_article'] = doc_counts_journals.get('journal_article') or 100000000
        doc_counts['total'] = doc_counts['total_without_journals'] + doc_counts['journal_article']
        doc_counts['book_comic'] = doc_counts.get('book_comic') or 0
        doc_counts['magazine'] = doc_counts.get('magazine') or 0
        doc_counts['book_any'] = (doc_counts.get('book_unknown') or 0) + (doc_counts.get('book_fiction') or 0) + (doc_counts.get('book_nonfiction') or 0)
        g.header_stats = {key: babel_numbers.format_decimal(value, locale=get_locale()) for key, value in doc_counts.items() }

        new_header_tagline_scihub = gettext('layout.index.header.tagline_scihub')
        new_header_tagline_libgen = gettext('layout.index.header.tagline_libgen')
        new_header_tagline_zlib = gettext('layout.index.header.tagline_zlib')
        _new_header_tagline_openlib = gettext('layout.index.header.tagline_openlib')
        _new_header_tagline_ia = gettext('layout.index.header.tagline_ia')
        new_header_tagline_duxiu = gettext('layout.index.header.tagline_duxiu')
        new_header_tagline_separator = gettext('layout.index.header.tagline_separator')
        new_header_tagline_and = gettext('layout.index.header.tagline_and')
        new_header_tagline_and_more = gettext('layout.index.header.tagline_and_more')
        new_stats = {
            'book_count': babel_numbers.format_decimal((doc_counts.get('book_unknown') or 0) + (doc_counts.get('book_fiction') or 0) + (doc_counts.get('book_nonfiction') or 0) + (doc_counts.get('book_comic') or 0) + (doc_counts.get('musical_score') or 0), locale=get_locale()),
            'paper_count': babel_numbers.format_decimal((doc_counts.get('journal_article') or 0) + (doc_counts.get('standards_document') or 0) + (doc_counts.get('magazine') or 0), locale=get_locale()),
            # 'libraries': new_header_tagline_separator.join([new_header_tagline_scihub, new_header_tagline_libgen]),
            'libraries': "".join([new_header_tagline_scihub, new_header_tagline_and, new_header_tagline_libgen]),
            'scraped': new_header_tagline_separator.join([new_header_tagline_zlib, new_header_tagline_duxiu, new_header_tagline_and_more]),
        }
        tagline_newnew2a = gettext('layout.index.header.tagline_newnew2a', **new_stats)
        tagline_newnew2b = gettext('layout.index.header.tagline_newnew2b', **new_stats)
        tagline_newnew4 = gettext('layout.index.header.tagline_open_source')
        new_header_tagline = " ".join([gettext('layout.index.header.tagline_new1'), tagline_newnew2a, tagline_newnew2b, gettext('layout.index.header.tagline_new3', **new_stats), tagline_newnew4])
        g.header_tagline = new_header_tagline
        g.header_tagline_mid = " ".join([gettext('layout.index.header.tagline_new1'), tagline_newnew2a, tagline_newnew2b, gettext('layout.index.header.tagline_new3', **new_stats)])
        g.header_tagline_short = " ".join([gettext('layout.index.header.tagline_new1'), tagline_newnew2a, tagline_newnew2b])
        if str(get_locale()) != 'en':
            with force_locale('en'):
                new_header_tagline_english = " ".join([gettext('layout.index.header.tagline_new1'), tagline_newnew2a, tagline_newnew2b, gettext('layout.index.header.tagline_new3', **new_stats), tagline_newnew4])
            if new_header_tagline == new_header_tagline_english:
                g.header_tagline = gettext('layout.index.header.tagline', **g.header_stats)
                g.header_tagline_mid = gettext('layout.index.header.tagline', **g.header_stats)
                g.header_tagline_short = gettext('layout.index.header.tagline_short')

        g.is_membership_double = allthethings.utils.get_is_membership_double()

        # From https://hds-nabavi.medium.com/the-percent-of-the-month-completed-using-python-5eb4678e5847
        today = datetime.date.today().day
        currentYear = datetime.date.today().year
        currentMonth = datetime.date.today().month
        monthrange = calendar.monthrange(currentYear, currentMonth)[1]
        g.fraction_of_the_month = today / monthrange

        g.darkreader_code = get_static_file_contents(safe_join(app.static_folder, 'js/darkreader.js'))

    return None


def middleware(app):
    """
    Register 0 or more middleware (mutates the app passed in).

    :param app: Flask application instance
    :return: None
    """
    # Enable the Flask interactive debugger in the brower for development.
    if app.debug:
        app.wsgi_app = DebuggedApplication(app.wsgi_app, evalex=True)

    # Set the real IP address into request.remote_addr when behind a proxy.
    # x_for=2 because of Varnish, then Cloudflare.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=2, x_proto=1)

    return None


celery_app = create_celery_app()
