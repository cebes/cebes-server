This section describes how to deploy a Cebes repository. You only need this if you want 
to setup a private repository that can be shared between multiple users in your organization.

## Use a docker image

The easiest way to deploy a Cebes repository is to use a Docker image. We provide images that 
has Cebes repository with and without a MariaDB database for the ease of integration. 
The image with MariaDB should work out-of-the-box, while for the image without MariaDB you will
need to provide some environment variables that specify the database to be used. Check out 
[Docker hub](https://hub.docker.com/r/cebesio/pipeline-repo/) for more information.

If you have a CI/CD system, deploying the docker image should be straightforward.

## Deploy manually

1. Download `cebes-pipeline-repository-assembly-VERSION.jar` from 
[github release page](https://github.com/phvu/cebes-server/releases)
2. Specify all environment variables as in the table below.
3. Start the repository:

        $ java -Dcebes.logs.dir=/tmp/ -jar cebes-pipeline-repository-assembly-VERSION.jar
    
    
The repository is a normal web app that doesn't run on Spark, therefore configuration 
is fairly straightforward:

| Environment variable name | Configuration key | Description | Default value |
|---------------------------|-------------------|-------------|---------------|
| CEBES_MYSQL_URL | cebes.mysql.url | URL for MySQL database |  |
| CEBES_MYSQL_DRIVER | cebes.mysql.driver | Driver for MySQL database | org.mariadb.jdbc.Driver |
| CEBES_MYSQL_USERNAME | cebes.mysql.username | Username for MySQL database |  |
| CEBES_MYSQL_PASSWORD | cebes.mysql.password | Password for MySQL database |  |
| CEBES_REPOSITORY_INTERFACE | cebes.repository.interface | The interface on which the HTTP service will be listening | localhost |
| CEBES_REPOSITORY_PORT | cebes.repository.port | The port on which the HTTP service will be listening, to be combined with REPOSITORY_INTERFACE | 22000 |
| CEBES_REPOSITORY_SERVER_SECRET | cebes.repository.server.secret | The secret string to be used in authentication of the HTTP server | ... |
| CEBES_REPOSITORY_PATH | cebes.repository.path | Path to local disk where we store the binary files of repositories | /tmp |

An example bash script can be found under `docker/repository/start_cebes_repository.sh` on github.
