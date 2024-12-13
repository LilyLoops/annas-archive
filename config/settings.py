import os


SECRET_KEY = os.getenv("SECRET_KEY", None)
DOWNLOADS_SECRET_KEY = os.getenv("DOWNLOADS_SECRET_KEY", None)
MEMBERS_TELEGRAM_URL = os.getenv("MEMBERS_TELEGRAM_URL", None)
PAYMENT1B_ID = os.getenv("PAYMENT1B_ID", None)
PAYMENT1B_KEY = os.getenv("PAYMENT1B_KEY", None)
PAYMENT1C_ID = os.getenv("PAYMENT1C_ID", None)
PAYMENT1C_KEY = os.getenv("PAYMENT1C_KEY", None)
PAYMENT2_URL = os.getenv("PAYMENT2_URL", None)
PAYMENT2_API_KEY = os.getenv("PAYMENT2_API_KEY", None)
PAYMENT2_HMAC = os.getenv("PAYMENT2_HMAC", None)
PAYMENT2_PROXIES = os.getenv("PAYMENT2_PROXIES", None)
PAYMENT2_SIG_HEADER = os.getenv("PAYMENT2_SIG_HEADER", None)
PAYMENT3_DOMAIN = os.getenv("PAYMENT3_DOMAIN", None)
PAYMENT3_KEY = os.getenv("PAYMENT3_KEY", None)
GC_NOTIFY_SIG = os.getenv("GC_NOTIFY_SIG", None)
HOODPAY_URL = os.getenv("HOODPAY_URL", None)
HOODPAY_AUTH = os.getenv("HOODPAY_AUTH", None)
FAST_PARTNER_SERVER1 = os.getenv("FAST_PARTNER_SERVER1", None)
X_AA_SECRET = os.getenv("X_AA_SECRET", None)
AA_EMAIL = os.getenv("AA_EMAIL", "")
VALID_OTHER_DOMAINS = os.getenv("VALID_OTHER_DOMAINS", "annas-archive.org,annas-archive.se,annas-archive.li").split(',')


# Redis.
# REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Celery.
# CELERY_CONFIG = {
#     "broker_url": REDIS_URL,
#     "result_backend": REDIS_URL,
#     "include": [],
# }

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
ELASTICSEARCHAUX_HOST = os.getenv("ELASTICSEARCHAUX_HOST", "http://elasticsearchaux:9201")

MAIL_USERNAME = 'anna@annas-archive.li'
MAIL_DEFAULT_SENDER = ('Anna’s Archive', 'anna@annas-archive.li')
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD", "")
if len(MAIL_PASSWORD) == 0:
    MAIL_SERVER = 'mailpit'
    MAIL_PORT = 1025
    MAIL_DEBUG = True
else:
    MAIL_SERVER = 'mail.annas-archive.li'
    MAIL_PORT = 587
    MAIL_USE_TLS = True

SLOW_DATA_IMPORTS = str(os.getenv("SLOW_DATA_IMPORTS", "")).lower() in ["1","true"]
AACID_SMALL_DATA_IMPORTS = str(os.getenv("AACID_SMALL_DATA_IMPORTS", "")).lower() in ["1","true"]

FLASK_DEBUG = str(os.getenv("FLASK_DEBUG", "")).lower() in ["1","true"]
DEBUG_TB_INTERCEPT_REDIRECTS = False
