FROM rocker/rstudio

RUN apt-get update \
  && apt-get install -y openjdk-8-jdk
  
RUN Rscript -e "install.packages(c('sparklyr'))"

RUN Rscript -e "install.packages(c('carrier'))"

RUN Rscript -e "install.packages(c('mlflow'))"

RUN Rscript -e "install.packages(c('ggplot2'))"