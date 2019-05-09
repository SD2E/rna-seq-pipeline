FROM sd2e/reactors:python3
# FROM sd2e/reactors:python3-edge

# reactor.py, config.yml, and message.jsonschema will be automatically
# added to the container when you run docker build or abaco deploy

RUN pip uninstall --yes datacatalog

# Copy from local
# COPY datacatalog /datacatalog

# Install from Repo
RUN pip install --upgrade git+https://github.com/SD2E/python-datacatalog.git@2_0
