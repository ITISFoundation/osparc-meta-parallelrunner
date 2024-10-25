FROM node:alpine as base

RUN adduser osparcuser --disabled-password

USER root

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NOWARNINGS="yes"

RUN apk update && apk upgrade
RUN apk add --no-cache python3 py3-pip wget bash su-exec

# Copying boot scripts                                                                                                                                                                                                                                                                                                   
COPY docker_scripts /docker

USER root

WORKDIR /docker/http
RUN npm install vite @vitejs/plugin-react --save-dev
RUN npm create vite@latest dashboard -- --template react

WORKDIR /docker/http/dashboard
RUN npm install
RUN npm install -D tailwindcss@latest postcss@latest autoprefixer@latest
RUN npx tailwindcss init -p

WORKDIR /docker/http/server
RUN chown osparcuser:osparcuser jobs_settings.json
RUN chown osparcuser:osparcuser jobs_status.json

RUN npm install express
RUN npm install cors
RUN npm run build

USER osparcuser

WORKDIR /home/osparcuser
RUN python3 -m venv venv
RUN . ./venv/bin/activate && pip3 install -r /docker/requirements.txt 


USER root
EXPOSE 8888
ENV JOBS_SETTINGS_PATH=/docker/http/server/jobs_settings.json
ENV JOBS_STATUS_PATH=/docker/http/server/jobs_status.json

ENTRYPOINT [ "/bin/bash", "-c", "/docker/entrypoint.bash" ]
