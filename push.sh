#!/usr/bin/env bash

version=$(cat rnaseq-0.1.3/VERSION)

CONTAINER_IMAGE="jurrutia/rnaseq:$version"

docker push ${CONTAINER_IMAGE}
