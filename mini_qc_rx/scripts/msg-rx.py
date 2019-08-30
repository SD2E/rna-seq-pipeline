#!/usr/env/python

import json
from agavepy.agave import Agave
from sys import argv


def send_msg():
    execId = ag.actors.sendMessage(
        actorId=actorId,
        body={'message': 'This is an input string'})['executionId']
    return execId


def main():
    execId = send_msg()
    print(actorId, execId)


if __name__ == "__main__":
    global actorId
    global ag
    ag = Agave.restore()
    try:
        actorId = argv[1]
    except IndexError as e:
        print("Please supply an actor ID")
        raise e
    main()
