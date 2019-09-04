#!/usr/bin/env bash

# DOES NOT RUN IN TESTING ENVIRONMENT
# Simply curls registered actor using a webhook similar to an Agave job
# Default message JSON is "tests/data/agave-job-message-01.json"
# Usage: run_container_nonce.sh (relative/path/to/message.json)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$DIR/common.sh"

PASSED_TOKEN=""
while getopts ":t" opt; do
    case ${opt} in
        f) PASSED_TOKEN=${OPTARG} ;;
        \?) echo "Usage: docker-rmi-nones.sh [-f]";;
    esac
done

echo $1
exit 0
read_reactor_rc
detect_ci

# Load up the message to send
if [ -z $1 ]; then
    MESSAGE_PATH="tests/data/agave-job-message-01.json"
else
    MESSAGE_PATH=$1
fi
if [ -f $MESSAGE_PATH ]; then
    MESSAGE=$(jq -rc . $MESSAGE_PATH)
else
    die "No message.json file found at MESSAGE_PATH=$MESSAGE_PATH"
fi

token=
