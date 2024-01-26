#!/bin/bash

set -euo pipefail
IFS=$'\n\t'
INFO="INFO: [$(basename "$0")] "

echo "$INFO" "Starting container for map ..."

HOST_USERID=$(stat -c %u "${DY_SIDECAR_PATH_INPUTS}")
HOST_GROUPID=$(stat -c %g "${DY_SIDECAR_PATH_INPUTS}")
CONTAINER_GROUPNAME=$(getent group | grep "${HOST_GROUPID}" | cut --delimiter=: --fields=1 || echo "")

exec /docker/map.bash
