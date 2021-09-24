FROM sd2e/java8:ubuntu17
# FROM sd2e/python3:ubuntu17-edge

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y software-properties-common \
     python-software-properties \
     libgomp1 \
     wget \
     libbz2-dev \
     liblzma-dev \
     ncurses-dev \
     zlib1g-dev  \
     unzip
# BWA
WORKDIR /tmp
RUN wget https://github.com/lh3/bwa/archive/v0.7.17.zip \
    && unzip v0.7.17.zip
WORKDIR bwa-0.7.17
RUN make \
    && mv /tmp/bwa-0.7.17 /opt/bwa

# PICARD (JAVA)
WORKDIR /tmp
ENV PICARD_VERSION 2.18.15
RUN git clone -b ${PICARD_VERSION} --depth 1 https://github.com/broadinstitute/picard.git picard-${PICARD_VERSION}
RUN cd picard-${PICARD_VERSION} &&\
    ./gradlew shadowJar \
    && mkdir /opt/picard \
    && mv build/libs/picard.jar /opt/picard/picard.jar

# SAMTOOLS
WORKDIR /tmp
RUN wget https://github.com/samtools/samtools/releases/download/1.9/samtools-1.9.tar.bz2 \
    && tar --bzip2 -xf samtools-1.9.tar.bz2 \
    && rm samtools-1.9.tar.bz2
WORKDIR /tmp/samtools-1.9
ENV SAMTOOLS_INSTALL_DIR=/opt/samtools
RUN ./configure --enable-plugins --prefix=$SAMTOOLS_INSTALL_DIR \
    && make all all-htslib \
    && make install install-htslib

ENV PICARDDIR /opt/picard/picard.jar
ENV PATH "/opt/bwa/:$PATH"
ENV PATH  "/opt/samtools/bin/:$PATH"

WORKDIR /data/
#ADD tests /tests
ADD src /src
# TO DO: add https://bioconda.github.io/recipes/ucsc-gtftogenepred/README.html to create ref_flat file
