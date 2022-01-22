FROM python:3.8.7-alpine3.12

WORKDIR /code

VOLUME ["/etc/scrapyd/", "/var/lib/scrapyd/", "/var/log/scrapyd/"]

RUN apk add --update \
gcc \
musl-dev \
libffi-dev \
libxml2-dev \
libxslt-dev \
openssl-dev

COPY scrapyd.conf /etc/scrapyd/scrapyd.conf
COPY requirements.txt /code/requirements.txt

RUN python3 -m venv venv \
&& . ./venv/bin/activate \
&& pip install --upgrade pip \
&& pip install wheel \
&& pip install -r /code/requirements.txt

ENV API_ENDPOINT="https://c.findboliger.dk/"

EXPOSE 6800

ENTRYPOINT "/code/venv/bin/scrapyd"
