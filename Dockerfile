FROM sd2e/base:ubuntu18


RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install wget -y \
    && apt-get install zip -y \
    && apt-get install default-jre -y \
	&& apt-get -y  install --fix-missing gdb libxml2-dev python-pip libz-dev libmariadb-client-lgpl-dev \
  && export DEBIAN_FRONTEND=noninteractive \
	&& apt-get -y install --fix-missing --fix-broken texlive texinfo texlive-fonts-extra texlive-latex-extra \
	&& apt-get -y install libpoppler-cpp-dev default-jdk r-cran-rjava \
	&& apt-get install libfreetype6-dev \
	&& apt-get install bedtools

RUN apt-get -y install python3.6
RUN wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
RUN python3.6 /tmp/get-pip.py
RUN pip3 install --user pipenv
ENV PATH=$HOME/.local/bin:$PATH

RUN pip3 install multiqc
RUN apt-get update \
  && apt-get install -y apt-transport-https software-properties-common \
  && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 \
  && add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' \
  && apt-get update \
  && apt-get -y install r-base
RUN apt-get -y install libcurl4-openssl-dev
RUN apt-get -y install gdebi libxml2-dev libssl-dev libcurl4-openssl-dev libopenblas-dev
RUN apt-get install libfreetype6-dev
RUN apt-get -y install libgtk2.0-dev libxt-dev libcairo2-dev
#setup R configs
RUN echo "r <- getOption('repos'); r['CRAN'] <- 'http://cran.us.r-project.org'; options(repos = r);" > ~/.Rprofile
RUN R CMD javareconf

# install common packages
RUN Rscript -e 'install.packages(c("optparse","tidyverse","scatterplot3d","data.table","dtplyr","devtools","roxygen2","bit64", "plyr", "pryr","reshape2", "stringr","ggplot2"), repos = "https://cran.rstudio.com/")'
RUN Rscript -e 'install.packages("Nozzle.R1", type="source", repos = "http://cran.us.r-project.org")'
RUN Rscript -e 'install.packages("rjson", type="source", repos = "http://cran.us.r-project.org")'

#install bioconductor package
RUN Rscript -e 'install.packages("BiocManager"); BiocManager::install()'
RUN Rscript -e 'BiocManager::install("Rsubread")'
#RUN Rscript -e 'if (!requireNamespace("BiocManager", quietly = TRUE)) install.packages("BiocManager"); BiocManager::install(); BiocManager::install("Rsubread", version = "3.8")'
RUN Rscript -e 'BiocManager::install("DESeq2", dependencies=TRUE); BiocManager::install("Rsamtools", dependencies=TRUE)'
RUN Rscript -e 'install.packages("devtools"); library(devtools); devtools::install_github("krlmlr/ulimit")'
RUN Rscript -e 'BiocManager::install("vsn"); BiocManager::install("preprocessCore"); BiocManager::install("gridExtra"); BiocManager::install("ggplot2"); BiocManager::install("reshape2"); BiocManager::install("genomeIntervals")'


#get pandoc
RUN wget https://github.com/jgm/pandoc/releases/download/2.2.1/pandoc-2.2.1-linux.tar.gz
RUN tar xvfz pandoc-2.2.1-linux.tar.gz && rm pandoc-2.2.1-linux.tar.gz && mv pandoc-2.2.1/bin/pandoc /bin/pandoc
ENV PATH=/bin/pandoc:$PATH
RUN chmod 777 /bin/pandoc

RUN pwd
ADD /src /opt/src
RUN chmod 777 /opt/src/*

ADD tests /tests

RUN Rscript -e 'install.packages("rjson", type="source", repos = "http://cran.us.r-project.org")'
