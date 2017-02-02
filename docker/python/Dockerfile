FROM python:3.6.0
MAINTAINER Carlos Huamani <carlos.hs.92@gmail.com>
ENV VERSION 1.0

RUN mkdir /var/www/ && \ 
    cd /var/www && \
    git clone https://github.com/biolab/orange3.git && \
    cd orange3 && \
    pip install -r requirements-core.txt && \
    pip install -e .

RUN mkdir /var/www/tesis

ENV APP_DIR=/var/www/tesis
WORKDIR $APP_DIR
