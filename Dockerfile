FROM continuumio/miniconda3:4.6.14
LABEL version="1.0"
COPY ./environment.yml ./environment.yml
RUN conda env update --name root -f environment.yml && conda clean -a && rm -rf /root/.cache/pip
COPY . / book_code/
