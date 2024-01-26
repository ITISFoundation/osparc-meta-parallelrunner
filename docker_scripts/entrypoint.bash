#!/bin/bash

set -euo pipefail
IFS=$'\n\t'
INFO="INFO: [$(basename "$0")] "

echo "$INFO" "Starting container ..."

stat "${DY_SIDECAR_PATH_INPUTS}" > /dev/null 2>&1 || \
        (echo "ERROR: You must mount '${DY_SIDECAR_PATH_INPUTS}' to deduce user and group ids" && exit 1)
stat "${DY_SIDECAR_PATH_OUTPUTS}" > /dev/null 2>&1 || \
    (echo "ERROR: You must mount '${DY_SIDECAR_PATH_OUTPUTS}' to deduce user and group ids" && exit 1)

if [ "$(stat -c %u "${DY_SIDECAR_PATH_INPUTS}")" -ne "$(stat -c %u "${DY_SIDECAR_PATH_OUTPUTS}")" ]
then
    echo "ERROR: '${DY_SIDECAR_PATH_INPUTS}' and '${DY_SIDECAR_PATH_OUTPUTS}' have different user id's. not allowed" && exit 1
fi
if [ "$(stat -c %g "${DY_SIDECAR_PATH_INPUTS}")" -ne "$(stat -c %g "${DY_SIDECAR_PATH_OUTPUTS}")" ]
then
    echo "ERROR: '${DY_SIDECAR_PATH_INPUTS}' and '${DY_SIDECAR_PATH_OUTPUTS}' have different group id's. not allowed" && exit 1
fi

HOST_USERID=$(stat -c %u "${DY_SIDECAR_PATH_INPUTS}")
HOST_GROUPID=$(stat -c %g "${DY_SIDECAR_PATH_INPUTS}")
CONTAINER_GROUPNAME=$(getent group | grep "${HOST_GROUPID}" | cut --delimiter=: --fields=1 || echo "")

echo "$INFO" "Starting runner ..."

exec /docker/runner.bash
