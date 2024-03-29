FROM continuumio/miniconda3

RUN wget https://raw.githubusercontent.com/GlobalPathogenAnalysisService/gpas-cli/main/environment.yml
RUN conda env create -f environment.yml

RUN ln -s /opt/conda/envs/gpas-cli/bin/gpas /usr/bin/gpas
RUN ln -s /opt/conda/envs/gpas-cli/bin/samtools /usr/bin/samtools
RUN ln -s /opt/conda/envs/gpas-cli/bin/readItAndKeep /usr/bin/readItAndKeep
