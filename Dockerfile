FROM node:16.15.1-bullseye-slim AS assets

WORKDIR /app/assets

ARG UID=1000
ARG GID=1000

RUN apt-get update \
    && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man \
    && apt-get clean \
    && groupmod -g "${GID}" node && usermod -u "${UID}" -g "${GID}" node \
    && mkdir -p /node_modules && chown node:node -R /node_modules /app

USER node

COPY --chown=node:node assets/package.json assets/*yarn* ./

RUN yarn install && yarn cache clean

ARG NODE_ENV="production"
ENV NODE_ENV="${NODE_ENV}" \
    PATH="${PATH}:/node_modules/.bin" \
    USER="node"

COPY --chown=node:node . ..

RUN if [ "${NODE_ENV}" != "development" ]; then \
    ../run yarn:build:js && ../run yarn:build:css; else mkdir -p /app/public; fi

CMD ["bash"]

###############################################################################

FROM --platform=linux/amd64 python:3.10.5-slim-bullseye AS app

WORKDIR /app

RUN sed -i -e's/ main/ main contrib non-free archive stretch /g' /etc/apt/sources.list
RUN apt-get update && apt-get install -y build-essential curl libpq-dev python3-dev default-libmysqlclient-dev aria2 unrar unzip p7zip curl python3 python3-pip ctorrent mariadb-client pv rclone gcc g++ make wget git cmake ca-certificates curl gnupg sshpass p7zip-full p7zip-rar libatomic1 libglib2.0-0 pigz parallel shellcheck

# https://github.com/nodesource/distributions
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ENV NODE_MAJOR=20
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
RUN apt-get update && apt-get install nodejs -y
RUN npm install webtorrent-cli -g && webtorrent --version

# Install latest, with support for threading for t2sz
RUN git clone --depth 1 https://github.com/facebook/zstd --branch v1.5.6
RUN cd zstd && make && make install
# Install t2sz
RUN git clone --depth 1 https://github.com/martinellimarco/t2sz --branch v1.1.2
RUN mkdir t2sz/build
RUN cd t2sz/build && cmake .. -DCMAKE_BUILD_TYPE="Release" && make && make install
# Env for t2sz finding latest libzstd
ENV LD_LIBRARY_PATH=/usr/local/lib

RUN npm install elasticdump@6.112.0 -g

RUN wget https://github.com/mydumper/mydumper/releases/download/v0.16.3-3/mydumper_0.16.3-3.bullseye_amd64.deb
RUN dpkg -i mydumper_*.deb

RUN rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man
RUN apt-get clean

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
# Install playwright's dependencies
RUN --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=tmpfs,target=/usr/share/doc \
    --mount=type=tmpfs,target=/usr/share/man \
    playwright install chromium --with-deps

# Download models
RUN echo 'import fast_langdetect; fast_langdetect.detect("dummy")' | python3
# RUN echo 'import sentence_transformers; sentence_transformers.SentenceTransformer("intfloat/multilingual-e5-small")' | python3

ARG FLASK_DEBUG="false"
ENV FLASK_DEBUG="${FLASK_DEBUG}" \
    FLASK_APP="allthethings.app" \
    FLASK_SKIP_DOTENV="true" \
    PYTHONUNBUFFERED="true" \
    PYTHONPATH="."

ENV PYTHONFAULTHANDLER=1

# Get pdf.js
RUN mkdir -p /public
RUN wget https://github.com/mozilla/pdf.js/releases/download/v4.5.136/pdfjs-4.5.136-dist.zip -O /public/pdfjs-4.5.136-dist.zip
RUN rm -rf /public/pdfjs
RUN mkdir /public/pdfjs
RUN unzip /public/pdfjs-4.5.136-dist.zip -d /public/pdfjs
# Remove lines
RUN sed -i -e '/if (fileOrigin !== viewerOrigin) {/,+2d' /public/pdfjs/web/viewer.mjs

COPY --from=assets /app/public /public

COPY . .

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# RUN if [ "${FLASK_DEBUG}" != "true" ]; then \
#   ln -s /public /app/public && flask digest compile && rm -rf /app/public; fi

ENTRYPOINT ["/app/bin/docker-entrypoint-web"]

EXPOSE 8000

CMD ["gunicorn", "-c", "python:config.gunicorn", "allthethings.app:create_app()"]
