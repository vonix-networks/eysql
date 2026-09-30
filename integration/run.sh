#!/usr/bin/env sh
# Run test/eysql_integration_SUITE.erl against PostgreSQL and a three-zone
# YugabyteDB cluster in Docker.
#
#   integration/run.sh              # OTP 28, tear down afterwards
#   OTP=27 integration/run.sh       # another OTP
#   KEEP=1 integration/run.sh       # leave the containers up
#
# The suite stops and starts a YugabyteDB node through the Docker socket, so
# the test container mounts /var/run/docker.sock.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
root=$(dirname "$here")
otp=${OTP:-28}

cd "$here"
docker compose up -d --wait
docker compose exec -T yb1 bin/yugabyted configure data_placement \
    --fault_tolerance=zone --rf=3

status=0
docker run --rm \
    --network eysql-it_default \
    -v "$root":/w -w /w \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e HOME=/tmp -e REBAR_BASE_DIR="/w/_build/it-$otp" -e EYSQL_IT=1 \
    "erlang:$otp" rebar3 ct --suite=test/eysql_integration_SUITE --readable=true \
    || status=$?

if [ "${KEEP:-0}" != 1 ]; then
    docker compose down -v
fi
exit $status
