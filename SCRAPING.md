# Anna’s guide to scrapers
 We have private infrastructure for running scrapers. Our scrapers are not open source because we don’t want to share with our targets how we scrape them.

If you’re going to write a scraper, it would be helpful to us if you use the same basic setup, so we can more easily plug your code into our system.

This is a very rough initial guide. We would love for someone to make an example scraper based off this, and which can actually be easily run and adapted.

Use the [EXAMPLE REPOSITORY](https://software.annas-archive.li/BubbaGump/example-scraper) here as a good starting point!

We sometimes also ask for one-time scrapes. In that case it's less necessary to set up this structure, just make sure that the final file follow this structure: [AAC.md](AAC.md).

## General scraping tips

- Store raw responses as files on disk, and parse only the required information for your next scrapes into your database (too many times we had a bug in the parsing but we already threw away the raw data so had to rescrape everything).
- Create a new directory for every hour (and store the full filename including the directory in your database), that way you won't get like 300 million files in a single directory (which can cause filesystem issues).
- Compress the raw responses with gzip or zstd.
- You can also bundle multiple responses in a single compressed file. That usually compresses a bit better, and reduces the number of total files on disk. You can use either the tar format to distinguish the different sub-files (safest; you'd get .tar.gz or .tar.zst), or store byte offsets in the database (test this thoroughly).

## Overview

* Docker containers:
  * Database
    * mariadb:10.10.2 ("mariapersist" shared between all scrapers and the main website; see docker-compose.yml in the main repo)
  * Wireguard VPN
    * linuxserver/wireguard
  * Continuous running container for each queue
    * scrape_metadata
    * download_files
  * One-off run containers only run every hour/day/week by our task system
    * fill_scrape_metadata_queue
    * fill_download_files_queue

Everything is organized around queues in MySQL. The one-off run containers fill the queues, and the continuous running containers poll the queues.

* `fill_scrape_metadata_queue` fills the scrape_metadata_queue with new entries by lightly scraping the target. For example, if your target uses incrementing integer IDs, you can look at the highest ID in the database, add 1000, and see if that ID exists, then keep doing that until you hit an ID that doesn’t exist (though it’s usually a bit more complicated because of deleted records).
* `fill_download_files_queue` looks at new entries in `scrape_metadata_queue` (marked at `status=2` success) and generates new queue items for `fill_download_files_queue` where applicable. In this step we often look at MD5s of files where available, and skip files that we already know exist in torrents.

The queue semantics are typically as follows:
* Queue status: 0=queued 1=claimed 2=successful 3=failed 4=retry_later, 5=processed
* Always starts with `status=0`.
* A thread claims a bunch of items with `status=0` and sets their `claimed_id`, and `status=1`.
* That thread runs the scrape, and sets status to one of these three:
  * `status=2` when the scrape went as expected. It sets `finished_data` with the output of the scrape (sometimes part of it in `finished_data_blob` if it's large). Note that missing records or other irrepairable but known issues can still be considered a “successful scrape that went as expected”.
  * `status=3` if there was an unexpected error that needs attention. We manually check if any such records are being generated, and adapt the script to deal with these situations better.
  * `status=4` to retry later, for example if we hit a known scraping limit. We don’t set this immediately to `status=0` for a few reasons. We have a separate script that periodically sets all `status=4` back to `status=0` but also increments the `retries` counter and alerts us if the number of retries gets too high. This also prevents getting into immediate loops where we constantly retry the same items over and over.
* Periodically, a one-off run container goes through all items with `status=2` and processes them, e.g. goes through a metadata queue and finds new files that are not yet in Anna’s Archive, and adds them to the download files queue. It then sets `status=5` in the original queue (the metadata queue in this example).

## Docker setup

With the exception of the MariaDB database, all our containers for a given scrape target share the same Docker image / Dockerfile. This is a typical Dockerfile that we use:

```Dockerfile
FROM python:3

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y dnsutils
RUN pip3 install httpx[http2,socks]==0.24.0
RUN pip3 install curlify2==1.0.3.1
RUN pip3 install tqdm==4.64.1
RUN pip3 install pymysql==1.0.2
RUN pip3 install more-itertools==9.1.0
RUN pip3 install orjson==3.9.7
RUN pip3 install beautifulsoup4==4.12.2
RUN pip3 install urllib3==1.26.16
RUN pip3 install shortuuid==1.0.11
RUN pip3 install retry==0.9.2
RUN pip3 install orjsonl==0.2.2
RUN pip3 install zstandard==0.21.0

COPY ./scrape_metadata.py .
COPY ./download_files.py .

ENV PYTHONUNBUFFERED definitely
```

As you can see, we use Python for all our scraping. We also copy in the scraping scripts, so that containers automatically restart when the scripts change. This is a typical docker-compose.yml:

```yml
wireguard_01:
  container_name: "wireguard_01"
  image: "linuxserver/wireguard"
  cap_add:
    - "NET_ADMIN"
    - "SYS_MODULE"
  sysctls:
    - "net.ipv6.conf.all.disable_ipv6=0"
    - "net.ipv4.conf.all.src_valid_mark=1"
  environment:
    - "PUID=1000"
    - "PGID=1000"
  volumes:
    - "./wireguard_01.conf:/config/wg0.conf:ro"
    - "/lib/modules:/lib/modules:ro"
  restart: "unless-stopped"
  logging:
    driver: "local"

zlib_scrape_metadata:
  container_name: "zlib_scrape_metadata"
  network_mode: "container:wireguard_01"
  depends_on:
    - "wireguard_01"
  build:
    context: "."
  entrypoint: "python3 zlib_scrape_metadata.py"
  env_file:
    - ".env"
  environment:
    INSTANCE_NAME: "zlib_scrape_metadata"
    MAX_THREADS: 30
    MAX_ATTEMPTS: 3
    SANITY_CHECK_FREQ: 1000
    CLAIM_SIZE: 10
    QUEUE_TABLE_NAME: "small_queue_items__zlib_scrape_metadata"
  restart: "unless-stopped"
  logging:
    driver: "local"
  tty: true

zlib_download_files:
  container_name: "zlib_download_files"
  network_mode: "container:wireguard_01"
  depends_on:
    - "wireguard_01"
  build:
    context: "."
  entrypoint: "python3 zlib_download_files.py"
  env_file:
    - ".env"
  environment:
    INSTANCE_NAME: "zlib_download_files"
    MAX_THREADS: 1
    MAX_ATTEMPTS: 3
    CLAIM_SIZE: 10
  volumes:
    - "./zlib_download_files:/files"
  restart: "unless-stopped"
  logging:
    driver: "local"
  tty: true
```

## SQL

We have a table for each queue. A queue table looks like this:

```sql
CREATE TABLE small_queue_items__some_queue_name (
    `small_queue_item_id` BIGINT NOT NULL AUTO_INCREMENT,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    # Whatever ID is appropriate for the target. In the case of Z-Library, this would be the zlibrary_id.
    # Must be unique.
    `primary_id` VARCHAR(250) NOT NULL,

    # Whatever information is useful for running this queue item, such as the `small_queue_item_id` of
    # the metadata, in case of a file download queue.
    `queue_item_data` JSON NOT NULL DEFAULT "{}",

    # 0=queued 1=claimed 2=successful 3=failed 4=retry_later, 5=processed(e.g. processed by fill_download_files_queue)
    `status` TINYINT NOT NULL DEFAULT 0,

    # Information about which continuous docker container's process/thread has currently claimed this queue item for running.
    `claimed_data` JSON NULL DEFAULT NULL,

    # Information when done running, e.g. the actual scraped metadata, or the path to the downloaded file.
    # If the data is too big or not JSON, you can compress it (usually zstd) and put it in `finished_data_blob`, and add
    # `"finished_data_blob":true` to `finished_data`. However, don't put entire files in `finished_data_blob`, write those
    # to disk. Tens to hundreds of kilobytes (compressed) is the maximum for `finished_data_blob`.
    `finished_data` JSON NULL DEFAULT NULL,
    `finished_data_blob` LONGBLOB NULL,

    # A random number assigned to the queue item on creation, so that when scraping you can sort by this to look
    # less like a bot that just does incrementing IDs.
    `random` FLOAT NOT NULL DEFAULT (RAND()),

    # When `status` gets set to 4, we periodically reset it back to 0 and increment `retries`. If `retries` exceeds
    # some number (e.g. 10), there is probably something wrong, and you should investigate.
    `retries` TINYINT NOT NULL DEFAULT 0,

    # Temporary UUID generated by the continuous docker container's process/thread that claimed this queue item.
    # Multiple queue items can have the same claimed_id if they're part of the same "claim batch."
    `claimed_id` VARCHAR(100) NULL DEFAULT NULL,

    # These are useful for hourly charts.
    `updated_hour` BIGINT GENERATED ALWAYS AS (TO_SECONDS(updated) DIV 3600) PERSISTENT,
    `created_hour` BIGINT GENERATED ALWAYS AS (TO_SECONDS(created) DIV 3600) PERSISTENT,
    PRIMARY KEY (`small_queue_item_id`),
    UNIQUE INDEX `primary_id` (`primary_id`),
    INDEX `status` (`status`, `updated`),
    INDEX `updated` (`updated`,`status`),
    INDEX `status_2` (`status`,`primary_id`,`random`),
    INDEX `status_3` (`status`,`random`),
    INDEX `claimed_id` (`claimed_id`),
    INDEX `updated_hour_status` (`updated_hour`, `status`),
    INDEX `created_hour_status` (`created_hour`, `status`),
    INDEX `retries` (`retries`),
    # Index the beginning of `finished_data`, so if you have a JSON field you'd like to sort on, put it in front.
    INDEX `finished_data` (`finished_data`(250))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

## Code

### `fill_scrape_metadata_queue` one-off run container

We don’t have good sample code to share for `fill_scrape_metadata_queue`, because all of those contain secrets about our targets that we don’t like to share.

### `scrape_metadata` continous container

```python
import os
import subprocess
import httpx
import time
import curlify2
import random
import threading
import concurrent.futures
import math
import queue
import pymysql
import orjson
import datetime
import re
import urllib.parse
import traceback
import shortuuid
import http.cookies
import hashlib

MARIAPERSIST_USER     = os.getenv("MARIAPERSIST_USER")
MARIAPERSIST_PASSWORD = os.getenv("MARIAPERSIST_PASSWORD")
MARIAPERSIST_HOST     = os.getenv("MARIAPERSIST_HOST")
MARIAPERSIST_PORT     = int(os.getenv("MARIAPERSIST_PORT"))
MARIAPERSIST_DATABASE = os.getenv("MARIAPERSIST_DATABASE")

INSTANCE_NAME = os.getenv("INSTANCE_NAME")
MAX_THREADS = int(os.getenv("MAX_THREADS"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS"))
SANITY_CHECK_FREQ = int(os.getenv("SANITY_CHECK_FREQ"))
CLAIM_SIZE = int(os.getenv("CLAIM_SIZE"))
QUEUE_TABLE_NAME = os.getenv("QUEUE_TABLE_NAME")
SLEEP_SEC = int(os.getenv("SLEEP_SEC"))

VERBOSE = 0
USER_AGENT = 'SOME USERAGENT..'

# To test:
# REPLACE INTO small_queue_items__ia_scrape_metadata (primary_id) VALUES ("someid");

sanity_check_valid = True

def make_client():
    transport = httpx.HTTPTransport(retries=5, http2=True, verify=False)
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=50, keepalive_expiry=20)
    client = httpx.Client(transport=transport, http2=True, verify=False, limits=limits, timeout=60.0)
    return client

def start_thread(i):
    seconds_wait = i*5
    print(f"Started thread {i}, sleeping {seconds_wait} seconds to avoid lock issues")
    time.sleep(seconds_wait)

    while True:
        try:
            db = pymysql.connect(host=MARIAPERSIST_HOST, port=MARIAPERSIST_PORT, user=MARIAPERSIST_USER, password=MARIAPERSIST_PASSWORD, database=MARIAPERSIST_DATABASE, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, read_timeout=120, write_timeout=120, autocommit=True)
            client = make_client()

            sanity_check(client)

            while True:
                ensure_sanity_check_valid()

                try:
                    db.ping(reconnect=True)
                    cursor = db.cursor()
                    claimed_id = shortuuid.uuid()
                    update_data = { "claimed_id": claimed_id, "claimed_data": orjson.dumps({ "timestamp": time.time(), "instance_name": INSTANCE_NAME }) }
                    cursor.execute(f'UPDATE {QUEUE_TABLE_NAME} USE INDEX(status_3) SET claimed_id = %(claimed_id)s, claimed_data = %(claimed_data)s, status=1 WHERE status=0 ORDER BY random LIMIT {CLAIM_SIZE}', update_data)
                    db.commit()
                    cursor.execute(f'SELECT * FROM {QUEUE_TABLE_NAME} WHERE claimed_id = %(claimed_id)s LIMIT {CLAIM_SIZE*10}', {"claimed_id": claimed_id})
                    claims = list(cursor.fetchall())
                    if len(claims) == 0:
                        print("No queue items found.. sleeping for 5 minutes..")
                        time.sleep(5*60)
                        continue
                except Exception as err:
                    print(f"Error during fetching queue item, waiting a few seconds and trying again: {err}")
                    time.sleep(10)
                    continue

                print(f"Made {len(claims)} claims...")

                update_data_list = []
                for claim in claims:
                    primary_id = claim["primary_id"]
                    print(f"Scraping {primary_id}")

                    if int(hashlib.md5(primary_id.encode()).hexdigest(), 16) % SANITY_CHECK_FREQ == 0:
                        sanity_check(client)

                    finished_data = do_stuff(primary_id, client)

                    new_status = 2
                    if 'retry_later' in finished_data:
                        new_status = 4
                    elif 'error' in finished_data:
                        new_status = 3
                    finished_data_blob = None
                    if 'finished_data_blob' in finished_data:
                        finished_data_blob = finished_data['finished_data_blob']
                        finished_data['finished_data_blob'] = True
                    update_data = { "status": new_status, "finished_data": orjson.dumps(finished_data), "small_queue_item_id": claim["small_queue_item_id"], "finished_data_blob": finished_data_blob }
                    if VERBOSE >= 1 or new_status != 2:
                        print(f"Updating {primary_id} with data: {update_data}")
                    else:
                        print(f"Updating {primary_id} with status: {new_status}")
                    update_data_list.append(update_data)
                db.ping(reconnect=True)
                cursor = db.cursor()
                cursor.execute(f'UPDATE {QUEUE_TABLE_NAME} SET status=%(status)s, finished_data=%(finished_data)s, finished_data_blob=%(finished_data_blob)s WHERE small_queue_item_id=%(small_queue_item_id)s LIMIT 1', update_data);
                db.commit()

                print(f"Finished processing claims, sleeping for {SLEEP_SEC} seconds...")
                time.sleep(SLEEP_SEC)

        except Exception as err:
            print(f"Fatal exception; restarting thread in 10 seconds ///// {repr(err)} ///// {traceback.format_exc()}")
            time.sleep(10)

# Be sure to update this to a real fixture.
# SANITY_CHECK_FIXTURE = {}

def sanity_check(client):
    print('Doing sanity_check')
    finished_data = None
    for attempt in range(MAX_ATTEMPTS):
        # Update someid to a real IA id.
        finished_data = do_stuff("someid", client)
        if 'retry_later' in finished_data:
            continue
        else:
            break

    finished_data['metadata_json'] = orjson.loads(finished_data['metadata_json'])
    if 'created' in finished_data['metadata_json']:
        del finished_data['metadata_json']['created']
    if 'server' in finished_data['metadata_json']:
        del finished_data['metadata_json']['server']
    if 'uniq' in finished_data['metadata_json']:
        del finished_data['metadata_json']['uniq']
    if 'workable_servers' in finished_data['metadata_json']:
        del finished_data['metadata_json']['workable_servers']
    if 'alternate_locations' in finished_data['metadata_json']:
        del finished_data['metadata_json']['alternate_locations']

    if 'retry_later' in finished_data:
        sanity_check_valid = False
        print(f"Sanity check failed on retry_later: {finished_data=}")
        raise Exception("Sanity check failed!")
    if 'error' in finished_data:
        sanity_check_valid = False
        print(f"Sanity check failed on error: {finished_data=}")
        raise Exception("Sanity check failed!")
    
    if finished_data != SANITY_CHECK_FIXTURE:
        sanity_check_valid = False
        print(f"Sanity check failed, actual data: {finished_data}")
        raise Exception("Sanity check failed!")

def ensure_sanity_check_valid():
    if not sanity_check_valid:
        raise Exception("Sanity check failed!")

def do_stuff(primary_id, client):
    ensure_sanity_check_valid()

    try:
        url = f"https://archive.org/metadata/{primary_id}"
        metadata_response = client.get(url, headers={ 'User-Agent': USER_AGENT }, follow_redirects=True)
        if metadata_response.status_code in [500, 501, 502, 503]:
            print(f"[{primary_id}]: Status code 5xx for metadata_response ({metadata_response.status_code=}), skipping and retrying later..")
            return { "retry_later": f"Status code 5xx for metadata_response ({metadata_response.status_code=}), skipping and retrying later.." }
        if metadata_response.status_code != 200:
            print(f"{metadata_response.text}")
            raise Exception(f"[{primary_id}] Unexpected metadata_response.status_code ({url=}): {metadata_response.status_code=}")

        return { "ia_id": primary_id, "metadata_json": metadata_response.text }
    except httpx.HTTPError as err:
        retval = { "retry_later": f"httpx.HTTPError: {repr(err)}", "traceback": traceback.format_exc() }
        if VERBOSE >= 1:
            print(f"[{primary_id}] HTTPError, retrying later {retval}...")
        return retval
    except Exception as err:
        print(f"[{primary_id}] Unexpected error during {primary_id} download! {err}")
        return {"error": repr(err), "traceback": traceback.format_exc()}

if __name__=='__main__':
    time.sleep(3)

    for i in range(MAX_THREADS):
        threading.Thread(target=lambda : start_thread(i), name=f"Thread{i}").start()

    while True:
        time.sleep(1)
```

### `fill_download_files_queue` one-off run container

We don’t have a good example here either since they all do some complicated deduplication. But for new collections this can simply be a few lines of Python that select from the `scrape_metadata_queue WHERE status = 2`, inserts the records that qualify into the `download_files_queue`, and sets all the processed records in `scrape_metadata_queue` to `status = 5`.

### `download_files` continous container

```python
import os
import subprocess
import httpx
import time
import curlify2
import random
import threading
import concurrent.futures
import math
import queue
import pymysql
import orjson
import datetime
import re
import urllib.parse
import traceback
import shortuuid
import hashlib

MARIAPERSIST_USER     = os.getenv("MARIAPERSIST_USER")
MARIAPERSIST_PASSWORD = os.getenv("MARIAPERSIST_PASSWORD")
MARIAPERSIST_HOST     = os.getenv("MARIAPERSIST_HOST")
MARIAPERSIST_PORT     = int(os.getenv("MARIAPERSIST_PORT"))
MARIAPERSIST_DATABASE = os.getenv("MARIAPERSIST_DATABASE")

INSTANCE_NAME = os.getenv("INSTANCE_NAME")
MAX_THREADS = int(os.getenv("MAX_THREADS"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS"))
CLAIM_SIZE = int(os.getenv("CLAIM_SIZE"))

DOWNLOAD_FOLDER_FULL = f"/files"
if not os.path.exists(DOWNLOAD_FOLDER_FULL):
    raise Exception(f"Folder {DOWNLOAD_FOLDER_FULL} does not exist!")

VERBOSE = 0
USER_AGENT = 'SOME USERAGENT'
COOKIE = "XXXX"

def make_client():
    # http2 Seems to run into errors for zlib.
    transport = httpx.HTTPTransport(retries=5, verify=False)
    limits = httpx.Limits(max_keepalive_connections=None, max_connections=50, keepalive_expiry=None)
    client = httpx.Client(transport=transport, verify=False, limits=limits)
    return client

def start_thread(i):
    print(f"Started thread {i}, sleeping {i} seconds to avoid lock issues")
    time.sleep(i)

    while True:
        try:
            db = pymysql.connect(host=MARIAPERSIST_HOST, port=MARIAPERSIST_PORT, user=MARIAPERSIST_USER, password=MARIAPERSIST_PASSWORD, database=MARIAPERSIST_DATABASE, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, read_timeout=120, write_timeout=120, autocommit=True)
            client = make_client()

            while True:
                try:
                    db.ping(reconnect=True)
                    cursor = db.cursor()
                    claimed_id = shortuuid.uuid()
                    update_data = { "claimed_id": claimed_id, "claimed_data": orjson.dumps({ "timestamp": time.time(), "instance_name": INSTANCE_NAME }) }
                    cursor.execute(f'UPDATE small_queue_items__zlib_download_files USE INDEX(status_3) SET claimed_id = %(claimed_id)s, claimed_data = %(claimed_data)s, status=1 WHERE status=0 ORDER BY random LIMIT {CLAIM_SIZE}', update_data)
                    db.commit()
                    cursor.execute(f'SELECT small_queue_items__zlib_download_files.*, small_queue_items__zlib_scrape_metadata.finished_data AS metadata_finished_data FROM small_queue_items__zlib_download_files JOIN small_queue_items__zlib_scrape_metadata USING (primary_id) WHERE small_queue_items__zlib_download_files.claimed_id = %(claimed_id)s LIMIT {CLAIM_SIZE*10}', {"claimed_id": claimed_id})
                    claims = list(cursor.fetchall())
                    if len(claims) == 0:
                        print("No queue items found.. sleeping for 5 minutes..")
                        time.sleep(5*60)
                        continue
                except Exception as err:
                    print(f"Error during fetching queue item, waiting a few seconds and trying again: {err}")
                    time.sleep(10)
                    continue

                print(f"Made {len(claims)} claims...")

                finished_datas = []
                for claim in claims:
                    zlibrary_id = int(claim["primary_id"])
                    if orjson.loads(claim["queue_item_data"])["type"] != 'zlib_download_files_fill_queue_v2':
                        raise Exception(f"Unexpected queue_item_data: {claim=}")
                    print(f"[{zlibrary_id}] Downloading..")

                    finished_data = None
                    for attempt in range(MAX_ATTEMPTS):
                        finished_data = download_file(claim, client)
                        if 'retry_later' in finished_data:
                            if 'Maximum downloads reached' in finished_data['retry_later']:
                                break
                            else:
                                continue
                        else:
                            break

                    new_status = 2
                    if 'retry_later' in finished_data:
                        new_status = 4
                    elif 'error' in finished_data:
                        new_status = 3
                    finished_data_blob = None
                    if 'finished_data_blob' in finished_data:
                        finished_data_blob = finished_data['finished_data_blob']
                        finished_data['finished_data_blob'] = True
                    update_data = { "status": new_status, "finished_data": orjson.dumps(finished_data), "small_queue_item_id": claim["small_queue_item_id"], "finished_data_blob": finished_data_blob }
                    if VERBOSE >= 1 or new_status != 2:
                        print(f"Updating {zlibrary_id} with data: {update_data}")
                    else:
                        print(f"Updating {zlibrary_id} with status: {new_status}")
                    db.ping(reconnect=True)
                    cursor = db.cursor()
                    cursor.execute('UPDATE small_queue_items__zlib_download_files SET status=%(status)s, finished_data=%(finished_data)s, finished_data_blob=%(finished_data_blob)s WHERE small_queue_item_id=%(small_queue_item_id)s LIMIT 1', update_data);
                    db.commit()
                    finished_datas.append(finished_data)

                for finished_data in finished_datas:
                    if ('retry_later' in finished_data) and ('Maximum downloads reached' in finished_data['retry_later']):
                        print(f"Hit the daily limit (through 'Maximum downloads reached'), sleeping for 1 hour..")
                        time.sleep(60*60)
                        break

        except Exception as err:
            print(f"Fatal exception; restarting thread in 10 seconds ///// {repr(err)} ///// {traceback.format_exc()}")
            time.sleep(10)

def download_file(claim, client):
    try:
        zlibrary_id = claim['primary_id']
        metadata_finished_data = orjson.loads(claim['metadata_finished_data'])

        if 'annabookinfo' not in metadata_finished_data['metadata']:
            return { "error": f"Can't find annabookinfo for {zlibrary_id=}" }

        anna_response = metadata_finished_data['metadata']['annabookinfo']['response']

        download_suffix = anna_response['downloadUrl'][anna_response['downloadUrl'].index("/dl/"):]

        download_before_redirect_url = f"https://ru.z-lib.gs{download_suffix}"
        download_before_redirect_response = client.get(download_before_redirect_url, headers={'User-Agent': USER_AGENT, 'Cookie': COOKIE})
        if download_before_redirect_response.status_code == 200:
            if "Мы ценим ваше стремление к получению" in download_before_redirect_response.text:
                return { "retry_later": f"Maximum downloads reached" }
            elif "404.css" in download_before_redirect_response.text:
                return { "success": f"404.css for {download_before_redirect_url=}" }
            elif "File not found: DMCA" in download_before_redirect_response.text:
                return { "success": f"File not found: DMCA for {download_before_redirect_url=}" }
            else:
                return { "success": f"Unexpected 200 response for {download_before_redirect_url=}" }
        elif download_before_redirect_response.status_code == 404:
            return { "success": f"404 for {download_before_redirect_url=}" }
        elif download_before_redirect_response.status_code == 302:
            download_url = download_before_redirect_response.headers['location']
            if 'expires=' not in download_url:
                return { "error": f"Invalid download_url: {download_url=} for {download_before_redirect_url=}" }
        else:
            return { "error": f"Unexpected status code {download_before_redirect_response.status_code=} for {download_before_redirect_url=}" }
        
        print(f"[{zlibrary_id}] Found {download_url=}")

        for attempt in range(1, 100):
            with client.stream("GET", download_url, headers={'User-Agent': USER_AGENT, 'COOKIE': COOKIE}) as response:
                if response.status_code == 404:
                    return { "success": f"404 status_code for {download_url=}" }
                if response.status_code != 200:
                    return { "error": f"Invalid status code: {response.status_code=} for {download_url=}" }
                response_headers = dict(response.headers)
                read_at_once = False
                if 'content-length' not in response_headers:
                    if attempt < 3:
                        print(f"[{zlibrary_id}] content-length missing in {response_headers=}, retrying")
                        continue
                    else:
                        response.read()
                        response_headers['content-length'] = len(response.content)
                        read_at_once = True
                if int(response_headers['content-length']) < 200:
                    if not read_at_once:
                        response.read()
                    if "404 Not Found" in response.text:
                        return { "success": f"404 for {download_url=}" }
                    else:
                        return { "success": f"Very small file" }

                dirname = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d")
                os.makedirs(f"{DOWNLOAD_FOLDER_FULL}/{dirname}", exist_ok=True)
                full_file_path = f"{DOWNLOAD_FOLDER_FULL}/{dirname}/{zlibrary_id}"
                with open(full_file_path, "wb") as download_file:
                    if read_at_once:
                        download_file.write(response.content)
                    else:
                        for chunk in response.iter_bytes():
                            download_file.write(chunk)
                with open(full_file_path, "rb") as md5_file_handle:
                    md5 = hashlib.md5(md5_file_handle.read()).hexdigest()
                    filesize = os.path.getsize(full_file_path)
                    return { "filename": f"{dirname}/{zlibrary_id}", "md5": md5, "filesize": filesize, "download_url": download_url }
    except httpx.HTTPError as err:
        return { "retry_later": f"httpx.HTTPError: {repr(err)}", "traceback": traceback.format_exc() }
    except Exception as err:
        return { "error": f"Other error: {repr(err)}", "traceback": traceback.format_exc() }

if __name__=='__main__':
    time.sleep(3)

    for i in range(MAX_THREADS):
        threading.Thread(target=lambda : start_thread(i), name=f"Thread{i}").start()

    while True:
        time.sleep(1)
```

### Resetting claims and retries

TODO: This code is written in a slightly different style using SQLAlchemy, rewrite in the same style as the other examples.

```python
tables = mariapersist_session.execute("SELECT table_name FROM information_schema.TABLES WHERE table_name LIKE 'small_queue_items__%' ORDER BY table_name").all()
rowcounts = {}
max_tries = 4
for table in tables:
    while True:
        print(f"Processing {table.table_name} status=1")
        rowcount = retry.api.retry_call(delay=60, tries=4, f=lambda: mariapersist_session.execute(f'UPDATE {table.table_name} SET status = 0 WHERE status = 1 AND updated < (NOW() - INTERVAL 1 HOUR) LIMIT 100').rowcount)
        mariapersist_session.commit()
        print(f"Did {rowcount} rows")
        if rowcount == 0:
            break
    while True:
        print(f"Processing {table.table_name} status=4")
        rowcount = retry.api.retry_call(delay=60, tries=4, f=lambda: mariapersist_session.execute(f'UPDATE {table.table_name} SET status = 0, retries = retries + 1 WHERE status = 4 AND updated < (NOW() - INTERVAL 6 HOUR) AND retries < 20 LIMIT 100').rowcount)
        mariapersist_session.commit()
        print(f"Did {rowcount} rows")
        if rowcount == 0:
            break
print("Done!")
```
