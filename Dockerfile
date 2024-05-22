FROM python:3.11.5-slim-bullseye as base

ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1

WORKDIR /app

FROM base as builder

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.6.1

# RUN apk add --no-cache gcc g++ musl-dev python3-dev libffi-dev openssl-dev cargo file make jpeg-dev zlib-dev
RUN apt update
RUN apt install -y gcc g++ musl-dev python3-dev libffi-dev libssl-dev cargo file make libjpeg-dev zlib1g-dev
RUN pip install "poetry==$POETRY_VERSION"
RUN python -m venv /venv
COPY pyproject.toml poetry.lock ./
RUN poetry export -f requirements.txt | /venv/bin/pip install -r /dev/stdin

COPY . .
# RUN poetry build && /venv/bin/pip install dist/*.whl

FROM base as final

# RUN apk add --no-cache libffi libjpeg nano bash curl shadow
RUN apt update && apt install -y nano bash curl screen libjpeg62
COPY --from=builder /venv /venv

# Copy files from project
COPY modules modules
COPY resources resources
COPY docker-entrypoint.sh app.py env.py ./

RUN useradd -s /bin/bash -u 1010 aims &&\
    chown -R 1010:0 /app &&\
    chmod -R g=u /app

WORKDIR /app
USER 1010

ENTRYPOINT [ "/app/docker-entrypoint.sh" ]
CMD [ "" ]
