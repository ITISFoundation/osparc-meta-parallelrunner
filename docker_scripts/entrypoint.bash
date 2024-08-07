#!/bin/bash

set -euo pipefail
IFS=$'\n\t'
INFO="INFO: [$(basename "$0")] "

echo "$INFO" "Starting container for parallelrunner ..."

HOST_USERID=$(stat -c %u "${DY_SIDECAR_PATH_INPUTS}")
HOST_GROUPID=$(stat -c %g "${DY_SIDECAR_PATH_INPUTS}")
CONTAINER_GROUPNAME=$(grep ":${HOST_GROUPID}:" /etc/group | cut -d: -f1 || echo "")

OSPARC_USER='osparcuser'

if [ "$HOST_USERID" -eq 0 ]; then
	echo "Warning: Folder mounted owned by root user... adding $OSPARC_USER to root..."
	addgroup "$OSPARC_USER" root
else
	echo "Folder mounted owned by user $HOST_USERID:$HOST_GROUPID-'$CONTAINER_GROUPNAME'..."
	# take host's credentials in $OSPARC_USER
	if [ -z "$CONTAINER_GROUPNAME" ]; then
		echo "Creating new group my$OSPARC_USER"
		CONTAINER_GROUPNAME=my$OSPARC_USER
		addgroup --gid "$HOST_GROUPID" "$CONTAINER_GROUPNAME"
	else
		echo "group already exists"
	fi

	echo "adding $OSPARC_USER to group $CONTAINER_GROUPNAME..."
	addgroup "$OSPARC_USER" "$CONTAINER_GROUPNAME"

	echo "changing owner ship of state directory /home/${OSPARC_USER}/work/workspace"
	chown --recursive "$OSPARC_USER" "/home/${OSPARC_USER}/work/workspace"
	echo "changing owner ship of state directory ${DY_SIDECAR_PATH_INPUTS}"
	chown --recursive "$OSPARC_USER" "${DY_SIDECAR_PATH_INPUTS}"
	echo "changing owner ship of state directory ${DY_SIDECAR_PATH_OUTPUTS}"
	chown --recursive "$OSPARC_USER" "${DY_SIDECAR_PATH_OUTPUTS}"
fi

exec su-exec "$OSPARC_USER" /docker/main.bash
