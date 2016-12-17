#!/usr/bin/env bash

set -e

export SBT_OPTS="-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"
sbt clean compile package
sbt coverage test
sbt coverageOff
sbt coverageReport
sbt coverageAggregate
