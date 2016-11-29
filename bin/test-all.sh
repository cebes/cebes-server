#!/usr/bin/env bash

export SBT_OPTS="-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"
sbt clean compile package || exit 1
sbt coverage test || exit 1
sbt coverageOff || exit 1
sbt coverageReport || exit 1
sbt coverageAggregate || exit 1
