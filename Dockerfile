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
	&& apt-get install libfreetype6-dev

RUN apt-get -y install python3.6
RUN wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
RUN python3.6 /tmp/get-pip.py
RUN pip3 install --user pipenv
ENV PATH=$HOME/.local/bin:$PATH

RUN apt-get -y install r-base
RUN apt-get -y install libcurl4-openssl-dev
RUN apt-get -y install gdebi libxml2-dev libssl-dev libcurl4-openssl-dev libopenblas-dev
RUN apt-get install libfreetype6-dev
RUN apt-get -y install libgtk2.0-dev libxt-dev libcairo2-dev
#setup R configs
RUN echo "r <- getOption('repos'); r['CRAN'] <- 'http://cran.us.r-project.org'; options(repos = r);" > ~/.Rprofile
RUN R CMD javareconf

#install bioconductor package
RUN Rscript -e 'source("https://bioconductor.org/biocLite.R") ; biocLite()'


# Install RStudio
RUN wget https://download1.rstudio.org/rstudio-xenial-1.1.383-amd64.deb
RUN gdebi rstudio-xenial-1.1.383-amd64.deb
#-y?
RUN printf '\nexport QT_STYLE_OVERRIDE=gtk\n' | tee -a ~/.profile

# install common packages
RUN Rscript -e 'install.packages(c("tidyverse","data.table","dtplyr","devtools","roxygen2","bit64", "plyr", "pryr","reshape2", "stringr","ggplot2"), repos = "https://cran.rstudio.com/")'
RUN Rscript -e 'install.packages(c("htmlTable","openxlsx"), repos = "https://cran.rstudio.com/")'
RUN Rscript -e 'install.packages(c("knitr","rmarkdown"), repos="http://cran.us.r-project.org")'
RUN Rscript -e 'library(devtools) ; install.packages("pdftools", repos = "https://cran.rstudio.com/") ; install_github("ropensci/tabulizer")'
RUN Rscript -e 'install.packages("showtext", repos = "https://cran.rstudio.com/")'
RUN Rscript -e 'install.packages("Cairo", repos = "https://cran.rstudio.com/")'
RUN Rscript -e 'library(devtools) ; devtools::install_github("hadley/lineprof")'
RUN Rscript -e 'install.packages("Nozzle.R1", type="source", repos = "https://cran.rstudio.com/"); install.packages("shiny",  dependencies=TRUE, repos = "https://cran.rstudio.com/"); install.packages("pander",  dependencies=TRUE, repos = "https://cran.rstudio.com/"); install.packages("httr",  dependencies=TRUE, repos = "https://cran.rstudio.com/"); source("https://bioconductor.org/biocLite.R"); biocLite("Rsubread", dependencies=TRUE); biocLite("DESeq2", dependencies=TRUE); library(devtools); devtools::install_github("krlmlr/ulimit")'

ADD /src /opt/src
RUN chmod 777 /opt/src/*
