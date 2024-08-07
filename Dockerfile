FROM node:alpine as base

RUN adduser osparcuser --disabled-password

USER root

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NOWARNINGS="yes"

RUN apk update && apk upgrade
RUN apk add --no-cache python3 py3-pip wget bash su-exec

# If you need a virtual environment
RUN python3 -m venv /path/to/your/venv

# If you want to set python3 as the default python
RUN ln -sf python3 /usr/bin/python

# Copying boot scripts                                                                                                                                                                                                                                                                                                   
COPY docker_scripts /docker

USER osparcuser
WORKDIR /home/osparcuser

RUN python3 -m venv venv
RUN . ./venv/bin/activate && pip3 install -r /docker/requirements.txt 


USER root

EXPOSE 8888

ENTRYPOINT [ "/bin/bash", "-c", "/docker/entrypoint.bash" ]
CMD [ "/bin/bash", "-c", "/docker/runner.bash "]
