# Anna’s Archive

Welcome to the Code repository for Anna's Archive, the comprehensive search engine for books, papers, comics, magazines, and more. This repository contains all the code necessary to run Anna’s Archive locally or deploy it to a production environment.

## Quick Start

To get Anna's Archive running locally:

1. **System Requirements**
  For local development you don't need a super strong computer, but a very cheap VPS isn't going to cut it either. We recommend at least 4GB of RAM and 4GB of free disk space.

  WINDOWS AND MAC USERS: if any containers have trouble starting, first make sure to configure Docker Desktop to allocate plenty of resources. We have tested with a memory limit of 8GB and swap of 4GB. CPU limit should matter less, but if you have trouble set it as high as possible.

  A production system needs a lot more, we recommend at least 256GB RAM and 4TB disk space, and a fast 32-core CPU. More is better, especially if you are going to run all of [data-imports/README.md](data-imports/README.md) yourself.

2. **Initial Setup**
  First install the main prerequisites: git and Docker. Make sure to update to the latest version of Docker!

  In a terminal, clone the repository and set up your environment:
  ```bash
  mkdir annas-archive-outer # Several data directories will get created in here.
  cd annas-archive-outer
  git clone https://software.annas-archive.li/AnnaArchivist/annas-archive.git --depth=1
  cd annas-archive
  cp .env.dev .env
  cp data-imports/.env-data-imports.dev data-imports/.env-data-imports
  ```

3. **Build and Start the Application**

  Use Docker Compose to build and start the application:
  ```bash
  docker compose up --build
  ```
  Wait a few minutes for the setup to complete. It's normal to see some errors from the `web` container during the first setup. Wait for all logs to settle down.

  To verify that everything booted properly, in a new terminal window, run
  ```bash
  cd annas-archive-outer/annas-archive
  docker compose ps
  ```

  All containers should show running (you shouldn't see "restarting").

  If `mariadb` or `mariapersist` have trouble starting, check `mariadb-conf/my.cnf` or `mariadbpersist-conf/my.cnf` and reduce any values ending with `_size`, in particular `key_buffer_size`.

  If `elasticsearch` or `elasticsearchaux` have trouble starting, make sure that you have enough disk space. They won't start if you have less than 10% disk space available (even though they won't actually use it).

4. **Database Initialization**

  In a new terminal window, initialize the database:
  ```bash
  cd annas-archive-outer/annas-archive
  ./run flask cli dbreset
  ```

5. **Restart the Application**

  Once the database is initialized, restart the Docker Compose process, by killing it (CTRL+C) and running again:
  ```bash
  docker compose up --build
  ```

  Wait again for the logs to settle down.

6. **Visit Anna's Archive**

   Open your browser and visit [http://localtest.me:8000](http://localtest.me:8000) to access the application.

## Common Issues and Solutions

- **ElasticSearch Permission Issues**

  If you encounter permission errors related to ElasticSearch data, modify the permissions of the ElasticSearch data directories:
  ```bash
  sudo chmod 0777 -R ../allthethings-elastic-data/ ../allthethings-elasticsearchaux-data/
  ```
  This command grants read, write, and execute permissions to all users for the specified directories, addressing potential startup issues with Elasticsearch.

- **MariaDB Memory Consumption**

  If MariaDB is consuming too much RAM, you might need to adjust its configuration. To do so, comment out the `key_buffer_size` option in `mariadb-conf/my.cnf`.

- **ElasticSearch Heap Size**

  Adjust the size of the ElasticSearch heap by modifying `ES_JAVA_OPTS` in `docker-compose.yml` according to your system's available memory.

## Architecture Overview

Anna’s Archive is built on a scalable architecture designed to support a large volume of data and users:

- **Web Servers:** One or more servers handling web requests, with heavy caching (e.g., Cloudflare) to optimize performance.
- **Database Servers:**
  - Required for minimal operation. If you just run these two servers then some pages won't work, but the main search will work.
    - ElasticSearch server "elasticsearch" (main search index "Downloads")
    - MariaDB instance for read/write persistent data like user accounts, logs, comments ("mariapersist").
  - Full mirror:
    - ElasticSearch server "elasticsearchaux" (journal papers, digital lending, and metadata).
    - Mostly used for database generation, but some pages won't work without it (at time of writing: /datasets, /codes, and the /db debug pages):
      - MariaDB for read-only data with MyISAM tables ("mariadb")
      - Static read-only files in AAC (Anna’s Archive Container) format (the "allthethings-file-data/" folder), with accompanying index tables (with byte offsets) in MariaDB.
  - Optional:
    - A persistent data replica ("mariapersistreplica") for backups and redundancy.
    - "mariabackup" instance for regular backups.
- **Caching and Proxy Servers:** Recommended setup includes proxy servers (e.g., nginx) in front of the web servers for added control and security (DMCA notices). [Blog post](https://annas-archive.org/blog/how-to-run-a-shadow-library.html).

In our setup, the web and database servers are duplicated multiple times on different servers, with the exception of "mariapersist" which is shared between all servers. The ElasticSearch main server (or both servers) can also be run separately on optimized hardware, since search speed is usually a bottleneck.

## Importing Data

To import all necessary data into Anna’s Archive, refer to the detailed instructions in [data-imports/README.md](data-imports/README.md).

## Translations

We check in .po _and_ .mo files. The process is as follows:
```sh
# After updating any `gettext` calls:
pybabel extract --omit-header -F babel.cfg -o messages.pot .
pybabel update --omit-header -i messages.pot -d allthethings/translations --no-fuzzy-matching

# After changing any translations:
pybabel compile -f -d allthethings/translations

# All of the above:
./update-translations.sh

# Only for english:
./update-translations-en.sh

# To add a new translation file:
pybabel init -i messages.pot -d allthethings/translations -l es
```

Try it out by going to `http://es.localtest.me:8000`

## Production deployment

Be sure to exclude a bunch of stuff, most importantly `docker-compose.override.yml` which is just for local use. E.g.:

```bash
rsync --exclude=.git --exclude=.env --exclude=.env-data-imports --exclude=.DS_Store --exclude=docker-compose.override.yml -av --delete ..
```

To set up mariapersistreplica and mariabackup, check out `mariapersistreplica-conf/README.txt`.

## Scraping

Scraping of new datasets is not in scope for this repo, but we nonetheless have a guide here: [SCRAPING.md](SCRAPING.md).

One-time scraped datasets should ideally follow our AAC conventions. Follow this guide to provide us with files that we can easily release: [AAC.md](AAC.md).

## Contributing

To report bugs or suggest new ideas, please file an ["issue"](https://software.annas-archive.li/AnnaArchivist/annas-archive/-/issues).

To contribute code, also file an [issue](https://software.annas-archive.li/AnnaArchivist/annas-archive/-/issues), and include your `git diff` inline (you can use \`\`\`diff to get some syntax highlighting on the diff). Merge requests are currently disabled for security purposes — if you make consistently useful contributions you might get access.

For larger projects, please contact Anna first on [Reddit](https://www.reddit.com/r/Annas_Archive/).

## Testing

Please run `./run check` before committing to ensure that your changes pass the automated checks. You can also run `./run check:fix` to apply some automatic fixes to common lint issues.

To check that all pages are working, run `./run smoke-test`. You can also run `./run smoke-test <language-code>` to check a single language.

The script will output .html files in the current directory named `<language>--<path>.html`, where path is the url-encoded pathname that errored. You can open that file to see the error.

You can also do `./run check-dumps` to check that the database is still working.

If you are changing any translations, you should also run `./run check-translations` to check that *all* translations work.

## License

Released in the public domain under the terms of [CC0](./LICENSE). By contributing you agree to license your code under the same license.
