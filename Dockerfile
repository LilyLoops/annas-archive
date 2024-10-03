# syntax=docker/dockerfile:1.9

FROM node:16.15.1-bullseye-slim AS assets

WORKDIR /app/assets
ENV YARN_CACHE_FOLDER=/.yarn

ARG UID=1000
ARG GID=1000
RUN groupmod -g "${GID}" node && usermod -u "${UID}" -g "${GID}" node

RUN --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=tmpfs,target=/usr/share/doc \
    --mount=type=tmpfs,target=/usr/share/man \
    # allow docker to cache the packages outside of the image
    rm -f /etc/apt/apt.conf.d/docker-clean \
    # update the package list
    && apt-get update \
    # upgrade any installed packages
    && apt-get upgrade -y

RUN --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=tmpfs,target=/usr/share/doc \
    --mount=type=tmpfs,target=/usr/share/man \
    apt-get install -y --no-install-recommends build-essential

RUN --mount=type=cache,target=${YARN_CACHE_FOLDER} \
    mkdir -p /node_modules && chown node:node -R /node_modules /app "$YARN_CACHE_FOLDER"

USER node

COPY --chown=1000:1000 --link assets/package.json assets/*yarn* ./

RUN --mount=type=cache,target=${YARN_CACHE_FOLDER} \
    yarn install

ARG NODE_ENV="production"
ENV NODE_ENV="${NODE_ENV}"
ENV PATH="${PATH}:/node_modules/.bin"
ENV USER="node"

COPY --chown=1000:1000 --link . ..

RUN if test "${NODE_ENV}" != "development"; then ../run yarn:build:js && ../run yarn:build:css; else mkdir -p /app/public; fi

CMD ["bash"]

###############################################################################

FROM --platform=linux/amd64 python:3.10.5-slim-bullseye AS base

SHELL ["/bin/bash", "-o", "pipefail", "-eu", "-c"]
WORKDIR /app

RUN --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=tmpfs,target=/usr/share/doc \
    --mount=type=tmpfs,target=/usr/share/man \
    # allow docker to cache the packages outside of the image
    rm -f /etc/apt/apt.conf.d/docker-clean \
    # update the list of sources
    && sed -i -e 's/ main/ main contrib non-free archive stretch /g' /etc/apt/sources.list \
    # update the package list
    && apt-get update \
    # upgrade any installed packages
    && apt-get upgrade -y

# install the packages we need
RUN --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=tmpfs,target=/usr/share/doc \
    --mount=type=tmpfs,target=/usr/share/man \
    apt-get install -y --no-install-recommends \
    aria2 \
    ca-certificates \
    curl \
    default-libmysqlclient-dev \
    gnupg \
    libatomic1 \
    libglib2.0-0 \
    mariadb-client \
    p7zip \
    p7zip-full \
    p7zip-rar \
    parallel \
    pigz \
    pv \
    rclone \
    shellcheck \
    sshpass \
    unrar \
    unzip \
    wget


FROM base AS zstd

# install a few more packages, for c++ compilation
RUN --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=tmpfs,target=/usr/share/doc \
    --mount=type=tmpfs,target=/usr/share/man \
    apt-get install -y --no-install-recommends build-essential cmake checkinstall

ADD https://github.com/facebook/zstd.git#v1.5.6 /zstd
WORKDIR /zstd
# install zstd, because t2sz requires zstd to be installed to be built
RUN make
# checkinstall is like `make install`, but creates a .deb package too
RUN checkinstall --default --pkgname zstd && mv zstd_*.deb /zstd.deb


FROM zstd AS t2sz
ADD https://github.com/martinellimarco/t2sz.git#v1.1.2 /t2sz
WORKDIR /t2sz/build
RUN cmake .. -DCMAKE_BUILD_TYPE="Release"
RUN make
RUN checkinstall --install=no --default --pkgname t2sz && mv t2sz_*.deb /t2sz.deb


FROM base AS app
# https://github.com/nodesource/distributions
ADD --link https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key /nodesource-repo.gpg.key
RUN mkdir -p /etc/apt/keyrings \
    && gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg < /nodesource-repo.gpg.key
ENV NODE_MAJOR=20
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" > /etc/apt/sources.list.d/nodesource.list
RUN --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=tmpfs,target=/usr/share/doc \
    --mount=type=tmpfs,target=/usr/share/man \
    apt-get update && apt-get install nodejs -y --no-install-recommends

ARG WEBTORRENT_VERSION=5.1.2
RUN --mount=type=cache,target=/root/.npm \
    npm install -g "webtorrent-cli@${WEBTORRENT_VERSION}"

ARG ELASTICDUMP_VERSION=6.112.0
RUN --mount=type=cache,target=/root/.npm \
    npm install -g "elasticdump@${ELASTICDUMP_VERSION}"

# Install latest zstd, with support for threading for t2sz
RUN --mount=from=zstd,source=/zstd.deb,target=/zstd.deb dpkg -i /zstd.deb
RUN --mount=from=t2sz,source=/t2sz.deb,target=/t2sz.deb dpkg -i /t2sz.deb

# Env for t2sz finding latest libzstd
# ENV LD_LIBRARY_PATH=/usr/local/lib

ARG MYDUMPER_VERSION=0.16.3-3
ADD --link https://github.com/mydumper/mydumper/releases/download/v${MYDUMPER_VERSION}/mydumper_${MYDUMPER_VERSION}.bullseye_amd64.deb ./mydumper.deb
RUN dpkg -i mydumper.deb

COPY --from=ghcr.io/astral-sh/uv:0.4 /uv /bin/uv
ENV UV_PROJECT_ENVIRONMENT=/venv
ENV PATH="/venv/bin:/root/.local/bin:$PATH"

# Changing the default UV_LINK_MODE silences warnings about not being able to use hard links since the cache and sync target are on separate file systems.
ENV UV_LINK_MODE=copy
# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

# Download models
RUN python -c 'import fast_langdetect; fast_langdetect.detect("dummy")'
# RUN python -c 'import sentence_transformers; sentence_transformers.SentenceTransformer("intfloat/multilingual-e5-small")'

ARG FLASK_DEBUG="false"
ENV FLASK_DEBUG="${FLASK_DEBUG}"
ENV FLASK_APP="allthethings.app"
ENV FLASK_SKIP_DOTENV="true"
ENV PYTHONUNBUFFERED="true"
ENV PYTHONPATH="."
ENV PYTHONFAULTHANDLER=1

# Get pdf.js
ARG PDFJS_VERSION=4.5.136
ADD --link https://github.com/mozilla/pdf.js/releases/download/v${PDFJS_VERSION}/pdfjs-${PDFJS_VERSION}-dist.zip /public/pdfjs.zip
RUN rm -rf /public/pdfjs \
    && unzip /public/pdfjs.zip -d /public/pdfjs \
    && sed -i -e '/if (fileOrigin !== viewerOrigin) {/,+2d' /public/pdfjs/web/viewer.mjs

COPY --from=assets --link /app/public /public
COPY --link . .

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# RUN if [ "${FLASK_DEBUG}" != "true" ]; then \
#   ln -s /public /app/public && flask digest compile && rm -rf /app/public; fi

ENTRYPOINT ["/app/bin/docker-entrypoint-web"]

EXPOSE 8000

CMD ["gunicorn", "-c", "python:config.gunicorn", "allthethings.app:create_app()"]
