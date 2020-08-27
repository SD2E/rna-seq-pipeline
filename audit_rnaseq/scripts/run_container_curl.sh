#!/usr/bin/env bash

# DOES NOT RUN IN TESTING ENVIRONMENT
# Simply curls registered actor using a webhook similar to an Agave job
# Default message JSON is "tests/data/agave-job-message-01.json"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$DIR/common.sh"
source abaco-common.sh

MESSAGE_PATH=
MESSAGE=
URL=
while getopts "z:m:u:" opt; do
    case ${opt} in
        z) TOKEN=${OPTARG} ;;
        m) MESSAGE_PATH=${OPTARG} ;;
        u) URL=${OPTARG} ;;
        \?) echo "Usage: run_container_nonce.sh [ -z AGAVE_REFRESH_TOKEN ] [ -m MESSAGE_PATH ] [ -u URL ]";;
    esac
done

read_reactor_rc
detect_ci

# Load URL from ../.ACTOR_NONCE_URL
if [ -z $URL ]; then
    if [ -f $DIR/../.ACTOR_NONCE_URL ]; then
        URL=$(head -1 $DIR/../.ACTOR_NONCE_URL)
        log "Pulled URL from .ACTOR_NONCE_URL: $URL"
    else
        die "No URL was passed, and no nonce was found at $DIR/../.ACTOR_NONCE_URL"
    fi
fi

# Load up the message to send
if [ -z $MESSAGE_PATH ]; then
    MESSAGE_PATH="tests/data/agave-job-message-02.json"
fi
if [ -f $MESSAGE_PATH ]; then
    MESSAGE=$(jq -r 'map_values(tostring)' $MESSAGE_PATH | sed "s/\"/'/g")
else
    die "No message.json file found at MESSAGE_PATH=$MESSAGE_PATH"
fi

# curl command
cmd="curl -X POST -s -H \"Authorization: Bearer $TOKEN\" -d \"message=$MESSAGE\" '$URL'"
echo $cmd
eval $cmd
