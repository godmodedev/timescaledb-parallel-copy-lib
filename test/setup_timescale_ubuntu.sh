#!/bin/sh

##############
# Convenience script for ensuring Timescale locally for testing purposes.
# This script is not intended for use as a production installer.
# https://docs.timescale.com/self-hosted/latest/install/installation-linux/
##############

set -euo pipefail

APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1

apt-get update && apt-get install -y gnupg postgresql-common apt-transport-https lsb-release wget

/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y

echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" | tee /etc/apt/sources.list.d/timescaledb.list

wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add -

apt-get update

TIMESCALE_PATTERN="timescaledb-2-postgresql-[0-9]+"
TIMESCALE_VERSIONS=`apt-cache search --names-only $TIMESCALE_PATTERN | grep -oP $TIMESCALE_PATTERN`

LATEST_VERSION=""
LATEST_VERSION_NUM=0

for VERSION in $TIMESCALE_VERSIONS; do
    NUMBER=${VERSION##*-}
    if (( NUMBER > LATEST_VERSION_NUM )); then
        LATEST_VERSION=$VERSION
        LATEST_VERSION_NUM=$NUMBER
    fi
done

echo "Installing '${LATEST_VERSION}'"

apt-get install -y postgresql-client-${LATEST_VERSION_NUM}

apt-get install -y ${LATEST_VERSION}

timescaledb-tune -yes
