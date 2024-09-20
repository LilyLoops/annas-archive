import os
import random

from flask_babel import Babel
from flask_debugtoolbar import DebugToolbarExtension
from flask_static_digest import FlaskStaticDigest
from sqlalchemy import Column, Integer, ForeignKey, inspect, create_engine
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.ext.declarative import DeferredReflection
from elasticsearch import Elasticsearch
from flask_mail import Mail
from config.settings import ELASTICSEARCH_HOST, ELASTICSEARCHAUX_HOST, ELASTICSEARCH_HOST_PREFERRED, ELASTICSEARCHAUX_HOST_PREFERRED

debug_toolbar = DebugToolbarExtension()
flask_static_digest = FlaskStaticDigest()
Base = declarative_base()
babel = Babel()
mail = Mail()

# This only gets called if we have more than one node_configs, so we can't actually
# log here if falling back is happening, since at a higher level the failing node_config
# will be removed from the node_configs list.
class FallbackNodeSelector: # Selects only the first live node
    def __init__(self, node_configs):
        self.node_configs = node_configs
    def select(self, nodes):
        node_configs = list(self.node_configs)
        reverse = (random.randint(0, 10000) < 5)
        if reverse:
            node_configs.reverse() # Occasionally pick the fallback to check it.
        for node_config in node_configs:
            for node in nodes:
                if node.config == node_config:
                    if node_config != self.node_configs[0]:
                        print(f"FallbackNodeSelector warning: using fallback node! {reverse=} {node_config=}")
                    return node
        raise Exception("No node_config found!")

# It's important that retry_on_timeout=True is set, otherwise we won't retry and mark the node as dead in case of actual
# server downtime.
if len(ELASTICSEARCH_HOST_PREFERRED) > 0:
    es = Elasticsearch(hosts=[ELASTICSEARCH_HOST_PREFERRED,ELASTICSEARCH_HOST], node_selector_class=FallbackNodeSelector, max_retries=1, retry_on_timeout=True, http_compress=True, randomize_hosts=False)
else:
    es = Elasticsearch(hosts=[ELASTICSEARCH_HOST], max_retries=1, retry_on_timeout=True, http_compress=True, randomize_hosts=False)
if len(ELASTICSEARCHAUX_HOST_PREFERRED) > 0:
    es_aux = Elasticsearch(hosts=[ELASTICSEARCHAUX_HOST_PREFERRED,ELASTICSEARCHAUX_HOST], node_selector_class=FallbackNodeSelector, max_retries=1, retry_on_timeout=True, http_compress=True, randomize_hosts=False)
else:
    es_aux = Elasticsearch(hosts=[ELASTICSEARCHAUX_HOST], max_retries=1, retry_on_timeout=True, http_compress=True, randomize_hosts=False)

mariadb_user = "allthethings"
mariadb_password = "password"
mariadb_host = os.getenv("MARIADB_HOST", "mariadb")
mariadb_port = "3306"
mariadb_db = "allthethings"
mariadb_url = f"mysql+pymysql://{mariadb_user}:{mariadb_password}@{mariadb_host}:{mariadb_port}/{mariadb_db}?read_timeout=120&write_timeout=120"
mariadb_url_no_timeout = f"mysql+pymysql://root:{mariadb_password}@{mariadb_host}:{mariadb_port}/{mariadb_db}"
if os.getenv("DATA_IMPORTS_MODE", "") == "1":
    mariadb_url = mariadb_url_no_timeout
engine = create_engine(mariadb_url, future=True, isolation_level="AUTOCOMMIT", pool_size=20, max_overflow=5, pool_recycle=300, pool_pre_ping=True)

mariapersist_user = os.getenv("MARIAPERSIST_USER", "allthethings")
mariapersist_password = os.getenv("MARIAPERSIST_PASSWORD", "password")
mariapersist_host = os.getenv("MARIAPERSIST_HOST", "mariapersist")
mariapersist_port = os.getenv("MARIAPERSIST_PORT", "3333")
mariapersist_db = os.getenv("MARIAPERSIST_DATABASE", mariapersist_user)
mariapersist_url = f"mysql+pymysql://{mariapersist_user}:{mariapersist_password}@{mariapersist_host}:{mariapersist_port}/{mariapersist_db}?read_timeout=120&write_timeout=120"
mariapersist_engine = create_engine(mariapersist_url, future=True, isolation_level="AUTOCOMMIT", pool_size=5, max_overflow=2, pool_recycle=300, pool_pre_ping=True)
