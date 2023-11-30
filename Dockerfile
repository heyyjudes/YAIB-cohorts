FROM r-base:4.3.1
COPY /R /R
#RUN apt-install -y git
#RUN git clone https://github.com/rvandewater/YAIB-cohorts.git
RUN R -e "install.packages('renv', repos = c(CRAN = 'https://cloud.r-project.org'))"

#RUN mkdir -p renv
#COPY .Rprofile .Rprofile
#COPY renv/activate.R renv/activate.R
#COPY renv/settings.json renv/settings.json
RUN apt-get update && apt-get install -y libcurl4-openssl-dev libssl-dev libudunits2-dev
RUN cd /R
ENV RENV_PATHS_LIBRARY renv/library
RUN cd /R && R --vanilla -s -e 'renv::restore()'
RUN cd /R && R --vanilla -s -e 'renv::activate()' && R --vanilla 'install.packages("units")' && \
    R --vanilla 'install.packages(c("mimic.demo","eicu.demo"), repos="https://eth-mds.github.io/physionet-demo")' \
    && R --vanilla 'install.packages("tidyverse")' \
## utils package isn't installed automatically
#install.packages("units")
#
## same for the demo datasets
#install.packages("mimic.demo", repos="https://eth-mds.github.io/physionet-demo")
#install.packages("eicu.demo", repos="https://eth-mds.github.io/physionet-demo")

#RUN cd /R &&  \
#    Rscript setup_env.R