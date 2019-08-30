FROM sd2e/python3:ubuntu17
# FROM sd2e/python3:ubuntu17-edge

#install java for zipgrep
RUN apt-get -y update && \
    apt-get -y install build-essential \
                       openjdk-8-jdk-headless \
                       ant \
                       gradle \
                       maven \
                       libssl-dev \
                       libffi-dev  && \
    apt-get clean

# Download python packages
RUN pip install numpy pymongo pandas

# Symlink so python=python3
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN pwd
ADD src /src
RUN chmod +x /src/*
