#!/usr/bin/env bash

sbt clean compile package
sbt testNoHttpServer
sbt cebesHttpServer/test