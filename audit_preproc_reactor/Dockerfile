#FROM enho/reactors:datacatalog
FROM sd2e/reactors:python3
#FROM sd2e/reactors:python3-edge

RUN pip3 install pymongo

# If using enho/reactors:datacatalog base image:
# Manually add reactor.py, config.yml, and message.jsonschema, which
# is disabled due to removed ONBUILD commands in my reactors base
# image. This will compensate for the following lines
# removed from sd2e/reactors:python3 Dockerfile:

# ONBUILD ADD requirements.txt /tmp/requirements.txt
# ONBUILD RUN pip3 install -r /tmp/requirements.txt
# ONBUILD ADD reactor.py /
# ONBUILD ADD config.yml /
# ONBUILD ADD message.json* /message.jsonschema

# ADD requirements.txt /tmp/requirements.txt
# RUN pip3 install -r /tmp/requirements.txt
# ADD reactor.py /
# ADD config.yml /
# ADD message.json* /message.jsonschema
