#!/usr/bin/env bash

VERSION_TAG=${1:-"0.10.0-snapshot"}
docker build -t cebes -f docker/Dockerfile.local .
docker tag cebes phvu/cebes-private:${VERSION_TAG}
docker push phvu/cebes-private:${VERSION_TAG}
