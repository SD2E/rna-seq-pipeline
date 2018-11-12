FROM sd2e/java8:ubuntu16

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install software-properties-common -y \
    && apt-get install python-software-properties -y \
    && apt-get install perl -y \
    && apt-get install curl -y \
    && apt-get install unzip -y \
    && apt-get install less -y \
    && apt-get install libgomp1 -y \
    && apt-get install build-essential -y \
    && apt-get install bzip2 -y \
    && apt-get install bedtools -y \
    && apt-get install libbz2-dev -y \
    && apt-get install liblzma-dev -y \
    && apt-get install ncurses-dev -y \
    && apt-get install zlib1g-dev -y \
    && apt-get install r-base -y \
    && apt-get install ant -y \
    && apt-get install git -y \
    && apt-get install tabix -y \
    && apt-get -y clean \
    && apt-get -y autoclean \
    && apt-get -y autoremove

RUN add-apt-repository ppa:git-core/ppa \
    && curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash \
    && apt-get install git-lfs=2.5.2

# Trimmomatic
WORKDIR /tmp
RUN wget http://www.usadellab.org/cms/uploads/supplementary/Trimmomatic/Trimmomatic-0.36.zip \
    && unzip Trimmomatic-0.36.zip \
    && rm Trimmomatic-0.36.zip \
    && mv Trimmomatic-0.36 /opt/Trimmomatic-0.36

# FastQC
WORKDIR /tmp
RUN wget https://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v0.11.7.zip \
    && unzip fastqc_v0.11.7.zip \
    && rm fastqc_v0.11.7.zip \
    && mv FastQC /opt/FastQC

# BWA
WORKDIR /tmp
RUN wget https://github.com/lh3/bwa/archive/v0.7.17.zip \
    && unzip v0.7.17.zip
WORKDIR bwa-0.7.17
RUN make \
    && mv /tmp/bwa-0.7.17 /opt/bwa

# BBMAP
WORKDIR /tmp
ENV BBMAP_VERSION 38.26
RUN wget https://sourceforge.net/projects/bbmap/files/BBMap_${BBMAP_VERSION}.tar.gz/download -O bbmap.tar.gz \
    && tar -zxf bbmap.tar.gz \
    && rm bbmap.tar.gz \
    && mv bbmap /opt/bbmap

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

# PICARD (JAVA)
WORKDIR /tmp
ENV PICARD_VERSION 2.18.15
RUN git clone -b ${PICARD_VERSION} --depth 1 https://github.com/broadinstitute/picard.git picard-${PICARD_VERSION}
WORKDIR picard-${PICARD_VERSION}
RUN ./gradlew shadowJar \
    && mkdir /opt/picard \
    && mv build/libs/picard.jar /opt/picard/picard.jar
ENV PICARDDIR /opt/picard/picard.jar

# GATK (JAVA)
WORKDIR /tmp
ENV GATK_VERSION 4.0.11.0
RUN wget https://github.com/broadinstitute/gatk/releases/download/${GATK_VERSION}/gatk-${GATK_VERSION}.zip \
    && unzip gatk-${GATK_VERSION}.zip  \
    && mv gatk-${GATK_VERSION} /opt/gatk \
    && rm gatk-${GATK_VERSION}.zip

WORKDIR /opt/gatk
#Setup linked jars that may be needed for running gatk
RUN ln -s $( find * -name "gatk*local.jar" ) gatk.jar \
    && ln -s $( find * -name "gatk*spark.jar" ) gatk-spark.jar

WORKDIR /tmp
RUN mkdir /gatkCloneMountPoint \
    && mkdir /jars \
    && mkdir .gradle

# Create a simple unit test runner
RUN echo "source activate gatk" > /tmp/run_unit_tests.sh && \
    echo "export TEST_JAR=\$( find /jars -name \"gatk*test.jar\" )" >> /tmp/run_unit_tests.sh && \
    echo "export TEST_DEPENDENCY_JAR=\$( find /jars -name \"gatk*testDependencies.jar\" )" >> /tmp/run_unit_tests.sh && \
    echo "export GATK_JAR=$( find /opt/gatk -name "gatk*local.jar" )" >> /tmp/run_unit_tests.sh && \
    echo "cp -rp /gatkCloneMountPoint/src/main/java/* /opt/gatk/srcdir" >> /tmp/run_unit_tests.sh && \
    echo "export SOURCE_DIR=/opt/gatk/srcdir" >> /tmp/run_unit_tests.sh && \
    echo "export GRADLE_OPTS=\"-Xmx1024m -Dorg.gradle.daemon=false\"" /tmp/run_unit_tests.sh && \
    echo "export CP_DIR=/opt/gatk/testClasses" /tmp/run_unit_tests.sh && \
    echo "ln -s /gatkCloneMountPoint/src/ /gatkCloneMountPoint/scripts/docker/src" >> /tmp/run_unit_tests.sh && \
    echo "ln -s /gatkCloneMountPoint/build/ /gatkCloneMountPoint/scripts/docker/build" >> /tmp/run_unit_tests.sh && \
    echo "cd /opt/gatk/ && /gatkCloneMountPoint/gradlew -b /gatkCloneMountPoint/dockertest.gradle testOnPackagedReleaseJar jacocoTestReportOnPackagedReleaseJar -a -p /gatkCloneMountPoint" >> /tmp/run_unit_tests.sh \
    && cp /tmp/run_unit_tests.sh /opt/gatk \
    && rm /tmp/run_unit_tests.sh

# Start GATK Python environment
ENV CONDA_MD5 = "0b80a152332a4ce5250f3c09589c7a81"
ENV CONDA_PATH /opt/miniconda
RUN wget -nv https://repo.continuum.io/miniconda/Miniconda3-4.3.30-Linux-x86_64.sh -O /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -p /opt/miniconda -b \
    && rm /tmp/miniconda.sh
ENV PATH $CONDA_PATH/envs/gatk/bin:$CONDA_PATH/bin:$PATH
RUN cd /opt/gatk && conda env create -n gatk -f /opt/gatk/gatkcondaenv.yml && \
    echo "source activate gatk" >> /opt/gatk/gatkenv.rc && \
    conda clean -y -all
#CMD ["bash", "--init-file", "/opt/gatk/gatkenv.rc"]
ENV GATK /opt/gatk/gatk.jar

# Install subread
#RUN apt-get -y install r-base
#RUN Rscript -e 'source("https://bioconductor.org/biocLite.R") ; biocLite(); biocLite("Rsubread", dependencies=TRUE)'
RUN wget 'https://sourceforge.net/projects/subread/files/subread-1.6.2/subread-1.6.2-Linux-x86_64.tar.gz' && \
    tar -zxvf subread-1.6.2-Linux-x86_64.tar.gz && \
    rm subread-1.6.2-Linux-x86_64.tar.gz && mv subread-1.6.2-Linux-x86_64 /opt/
ENV featureCounts /opt/subread-1.6.2-Linux-x86_64/bin/featureCounts

# Install RNAseqQC
RUN wget 'http://www.broadinstitute.org/cancer/cga/tools/rnaseqc/RNA-SeQC_v1.1.8.jar' && \
    mv RNA-SeQC_v1.1.8.jar /opt/
ENV RNAseqQC /opt/RNA-SeQC_v1.1.8.jar



#ADD executables to bath
#ENV PATH "/opt/bin/:$PATH"
ENV PATH "/opt/bwa/:$PATH"
ENV PATH  "/opt/samtools/bin/:$PATH"

# Add analysis script
ADD src /src
RUN chmod +x /src/analysis_pipeline.sh
ADD tests /tests
