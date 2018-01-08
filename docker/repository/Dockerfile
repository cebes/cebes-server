# Docker image containing cebes-pipeline-repository
FROM openjdk:8-alpine

ARG CEBES_REPO_DIR=/cebes/repository

# mariadb
# libc6-compat is needed for alpine: https://github.com/grpc/grpc/issues/6126
RUN apk add --update curl bash libc6-compat mariadb mariadb-client && rm -f /var/cache/apk/*

# cebes
ADD cebes-pipeline-repository/target/scala-2.11/cebes-pipeline-repository-assembly-*.jar /cebes/
ADD docker/repository/configure_mariadb.sh /cebes/
ADD docker/repository/start_cebes_repository.sh /cebes/
RUN chmod +x /cebes/configure_mariadb.sh

WORKDIR /cebes

EXPOSE 22000

VOLUME $CEBES_REPO_DIR

ENTRYPOINT ["/cebes/start_cebes_repository.sh"]
