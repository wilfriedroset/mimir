# SPDX-License-Identifier: AGPL-3.0-only
# Provenance-includes-location: https://github.com/grafana/mimir/packaging/fpm/Dockerfile
# Provenance-includes-license: Apache-2.0
# Provenance-includes-copyright: Grafana Labs.

FROM alpine:3.8

RUN apk add --no-cache \
        bash \
        docker \
        sudo \
        ruby \
        ruby-dev \
        ruby-etc \
        gcc \
        git \
        libc-dev \
        libffi-dev \
        make \
        rpm \
        tar \
        && gem install --no-ri --no-rdoc fpm

COPY package.sh /
ENTRYPOINT ["/package.sh"]

ARG revision
LABEL org.opencontainers.image.title="fpm" \
        # TODO: should this label point to the fpm source code?
        org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/packaging/fpm" \
        org.opencontainers.image.revision="${revision}"
