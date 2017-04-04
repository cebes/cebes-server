#!/usr/bin/env bash

set -e

COVERAGE="false"

for i in "$@"
do
case ${i} in
    -c=*|--coverage=*)
    COVERAGE="${i#*=}"
    shift
    ;;
    *)
      # unknown option
    ;;
esac
done

export SBT_OPTS="-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"

CEBES_PATH="$(cd "$(dirname "$0")/../"; pwd)"
if [ -f "${CEBES_PATH}/bin/env.sh" ]
then
    echo "Environment file ${CEBES_PATH}/bin/env.sh exists. Sourcing the variables:"
    . "${CEBES_PATH}/bin/env.sh" --test=true
    env | grep "CEBES"
    echo
else
    echo "WARNING: ${CEBES_PATH}/bin/env.sh not found!"
    echo "You might want to run 'cp bin/env.sh.example bin/env.sh' before running the tests"
fi


if [[ "${COVERAGE}" == "false" ]]; then
    echo "Coverage report disabled"
    sbt clean compile package test
else
    echo "Coverage report enabled"
    sbt clean compile package
    sbt coverage test
    sbt coverageOff
    sbt coverageReport
    sbt coverageAggregate
fi