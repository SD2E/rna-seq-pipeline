ACTOR_ID=$1
MESSAGE='{"source":{"system_id":"data-sd2e-ingest","absolute_path":"sd2e-community/ingest/testing/biofab/yeast-gates_q0/3/manifest/107795-manifest.json","left_trim":"sd2e-community","parent_levels":2},"destination":{"system_id":"data-sd2e-community","dest_root_path":"/","parent_levels":2}}'

if [ -z "${ACTOR_ID}" ]
then
    echo "Usage: $(basename $0) [ACTORID]"
    exit 1
fi

MAX_ELAPSED=300 # Maximum duration for any async task
INITIAL_PAUSE=2 # Initial delay
BACKOFF=2 # Exponential backoff

TS1=$(date "+%s")
TS2=
ELAPSED=0
PAUSE=${INITIAL_PAUSE}
JOB_STATUS=

EXEC_ID=$(abaco run -v -m "${MESSAGE}" ${ACTOR_ID} | jq -r .result.executionId)
echo "Execution ${EXEC_ID} "

while [ "${JOB_STATUS}" != "COMPLETE" ]
do
    TS2=$(date "+%s")
    ELAPSED=$((${TS2} - ${TS1}))
    JOB_STATUS=$(abaco executions -v -e ${EXEC_ID} ${ACTOR_ID} | jq -r .result.status)
    if [ "${ELAPSED}" -gt "${MAX_ELAPSED}" ]
    then
        break
    fi
    printf "Wait " ; printf "%0.s." $(seq 1 ${PAUSE}); printf "\n"
    sleep $PAUSE
    PAUSE=$(($PAUSE * $BACKOFF))
done
echo " ${ELAPSED} seconds"

if [ "${JOB_STATUS}" == "COMPLETE" ]
then
    abaco logs -e ${EXEC_ID} ${ACTOR_ID}
    exit 0
else
    echo "Error or Actor ${ACTOR_ID} couldn't process message"
    exit 1
fi
