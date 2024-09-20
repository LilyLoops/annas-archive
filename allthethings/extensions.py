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
from config.settings import ELASTICSEARCH_HOST, ELASTICSEARCHAUX_HOST

debug_toolbar = DebugToolbarExtension()
flask_static_digest = FlaskStaticDigest()
Base = declarative_base()
babel = Babel()
mail = Mail()
es = Elasticsearch(hosts=[ELASTICSEARCH_HOST])
es_aux = Elasticsearch(hosts=[ELASTICSEARCHAUX_HOST])

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
