#!/usr/bin/env bash

abaco deploy -U
actorId=$(head -1 .ACTOR_ID)
echo Sleeping for 15... && sleep 15
execId=$(python scripts/msg-rx.py $actorId)
abaco executions $actorId
abaco logs $actorId $execId
echo $actorId $execId
