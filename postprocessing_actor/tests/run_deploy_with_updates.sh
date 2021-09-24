#!/usr/bin/env bash

ACTOR_ID=
if [ -f ".ACTOR_ID" ]; then
    ACTOR_ID=$(cat .ACTOR_ID)
fi

deployopts=""
if [ ! -z "$ACTOR_ID" ]; then
    deployopts="${deployopts} -U ${ACTOR_ID}"
fi

auth-tokens-refresh -S
echo "abaco deploy ${deployopts} ${@}"
abaco deploy ${@} ${deployopts}
