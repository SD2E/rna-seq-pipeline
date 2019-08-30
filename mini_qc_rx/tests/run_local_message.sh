#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$DIR/common.sh"

MESSAGE_PATH="data/tests-deployed-message.json"
MESSAGE=
if [ -f "${DIR}/${MESSAGE_PATH}" ]; then
    MESSAGE=$(cat ${DIR}/${MESSAGE_PATH})
fi

if [ -z "${MESSAGE}" ]; then
    echo "Message not readable \@ ${MESSAGE_PATH}"
    exit 1
fi
SLACK_WEBHOOK=$(jq -r ._REACTOR_SLACK_WEBHOOK ${DIR}/../secrets.json)
LOGS_TOKEN=$(jq -r ._REACTOR_LOGS_TOKEN ${DIR}/../secrets.json)

detect_ci

source reactor.rc

TEMP=`mktemp -d $PWD/tmp.XXXXXX`
echo "Working out of $TEMP"

docker run -t -v ${HOME}/.agave:/root/.agave:rw \
           -v ${TEMP}:/mnt/ephemeral-01:rw \
           -e LOCALONLY=1 \
           -e _REACTOR_SLACK_WEBHOOK=${SLACK_WEBHOOK} \
           -e _REACTOR_LOGS_TOKEN=${LOGS_TOKEN} \
           -e MSG="${MESSAGE}" \
           ${DOCKER_HUB_ORG}/${DOCKER_IMAGE_TAG}:${DOCKER_IMAGE_VERSION}

if [ "$?" == 0 ]; then
    rm -rf ${TEMP}
fi
