FROM ubuntu:22.04 as base

RUN useradd -m -r osparcuser

USER root

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NOWARNINGS="yes"

RUN apt-get update --yes && apt-get upgrade --yes 
RUN apt-get install -y --no-install-recommends apt-utils
RUN apt-get install --yes --no-install-recommends python3 python-is-python3 python3-venv wget

# Copying boot scripts                                                                                                                                                                                                                                                                                                   
COPY docker_scripts /docker

USER osparcuser

WORKDIR /home/osparcuser

ENTRYPOINT [ "/bin/bash", "/docker/entrypoint.bash" ]
CMD [ "/bin/sh", "-c", "docker/runner.bash "]
