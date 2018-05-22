/FROM sd2e/base:ubuntu17

RUN apt-get install python3.6
RUN wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
RUN python3.6 /tmp/get-pip.py
RUN pip3 install --user pipenv
RUN echo "PATH=$HOME/.local/bin:$PATH" >> ~/.profile
RUN source ~/.profile

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install wget -y \
    && apt-get install zip -y \
    && apt-get install default-jre -y \ 
	&& apt-get -y  install --fix-missing gdb libxml2-dev python-pip libz-dev libmariadb-client-lgpl-dev \
	&& apt-get -y install --fix-missing --fix-broken texlive texinfo texlive-fonts-extra texlive-latex-extra \
	&& apt-get -y install libpoppler-cpp-dev default-jdk r-cran-rjava \
	&& apt-get install libfreetype6-dev \

RUN apt-get -y install r-base
RUN apt-get -y install libcurl4-openssl-dev
RUN apt-get -y install gdebi libxml2-dev libssl-dev libcurl4-openssl-dev libopenblas-dev
#setup R configs
RUN echo "r <- getOption('repos'); r['CRAN'] <- 'http://cran.us.r-project.org'; options(repos = r);" > ~/.Rprofile
RUN echo "library(BiocInstaller)" > ~/.Rprofile
RUN R CMD javareconf

#install bioconductor package
RUN R --vanilla << EOF
source("https://bioconductor.org/biocLite.R")
biocLite()
q()
EOF

# Install RStudio
RUN wget https://download1.rstudio.org/rstudio-xenial-1.1.383-amd64.deb
RUN gdebi rstudio-xenial-1.1.383-amd64.deb
RUN printf '\nexport QT_STYLE_OVERRIDE=gtk\n' | tee -a ~/.profile

# install common packages
RUN R --vanilla << EOF
install.packages(c("tidyverse","data.table","dtplyr","devtools","roxygen2","bit64", "plyr", "pryr","reshape2", "stringr","ggplot2"), repos = "https://cran.rstudio.com/")
q()
EOF

# Export to HTML/Excel
RUN R --vanilla << EOF
install.packages(c("htmlTable","openxlsx"), repos = "https://cran.rstudio.com/")
q()
EOF

# Blog tools
RUN R --vanilla << EOF
install.packages(c("knitr","rmarkdown"), repos='http://cran.us.r-project.org')
q()
EOF

RUN R --vanilla << EOF
library(devtools)
install.packages("pdftools", repos = "https://cran.rstudio.com/")
install_github("ropensci/tabulizer")
q()
EOF

# TTF/OTF fonts usage
RUN apt-get install libfreetype6-dev
RUN R --vanilla << EOF
install.packages("showtext", repos = "https://cran.rstudio.com/")
q()
EOF

# Cairo for graphic devices
RUN apt-get -Y install libgtk2.0-dev libxt-dev libcairo2-dev
RUN R --vanilla << EOF
install.packages("Cairo", repos = "https://cran.rstudio.com/")
q()
EOF

#Memory management
RUN R --vanilla << EOF
library(devtools)
devtools::install_github("hadley/lineprof")
q()
EOF

#RNAseq tools management
RUN R --vanilla << EOF
install.packages("Nozzle.R1", type="source", repos = "https://cran.rstudio.com/")
install.packages("shiny",  dependencies=TRUE, repos = "https://cran.rstudio.com/")
install.packages("pander",  dependencies=TRUE, repos = "https://cran.rstudio.com/")
install.packages("httr",  dependencies=TRUE, repos = "https://cran.rstudio.com/")
source("https://bioconductor.org/biocLite.R") 
biocLite("Rsubread", dependencies=TRUE)
biocLite("DESeq2", dependencies=TRUE)
library(devtools)
devtools::install_github("krlmlr/ulimit")
q()
EOF

