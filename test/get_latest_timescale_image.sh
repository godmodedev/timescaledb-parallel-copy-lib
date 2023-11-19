#!/bin/bash

set -euo pipefail

docker_tags() {
    item="$1"
    case "$item" in
        */*) :;; # namespace/repository syntax, leave as is
        *) item="library/$item";; # bare repository name (docker official image); must convert to namespace/repository syntax
    esac
    authUrl="https://auth.docker.io/token?service=registry.docker.io&scope=repository:$item:pull"
    token="$(curl -fsSL "$authUrl" | jq --raw-output '.token')"
    tagsUrl="https://registry-1.docker.io/v2/$item/tags/list"
    curl -fsSL -H "Accept: application/json" -H "Authorization: Bearer $token" "$tagsUrl" | jq --raw-output '.tags[]'
}

# Get the `latest` tag of the timescaledb image with the latest postgres
# e.g. "latest-pg15"
TS_LATEST_VERSIONS=`docker_tags timescale/timescaledb | grep -oP "latest-pg[0-9]+"`

TS_LATEST=""
TS_LATEST_NUM=0

for VERSION in $TS_LATEST_VERSIONS; do
    NUMBER=$(echo $VERSION | grep -o '[0-9]*$')
    if (( NUMBER > TS_LATEST_NUM )); then
        TS_LATEST=$VERSION
        TS_LATEST_NUM=$NUMBER
    fi
done

echo "timescale/timescaledb:${TS_LATEST}"
