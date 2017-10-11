#!/usr/bin/env bash

version=$(cat ../VERSION)

CONTAINER_IMAGE="sd2e/msf:$version"

docker build -t ${CONTAINER_IMAGE} .