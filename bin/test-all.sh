#!/usr/bin/env bash

sbt clean compile package || exit 1
sbt coverage testNoHttpServer || exit 1
sbt coverage cebesHttpServer/test || exit 1
sbt coverageReport || exit 1
sbt coverageAggregate || exit 1
