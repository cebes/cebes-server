_Pipeline repository_ is a web service that stores [Cebes pipelines](pipelines_concepts.md).
It is a separated component in the Cebes suite and is deployed independently from Cebes server.
Using `pycebes`, users can instruct Cebes server to push a pipeline to repository, or pull a pipeline 
from a repository. Pipeline tags provide user a principled way to manage different versions of the same
pipeline.

If you are familiar with Docker, Cebes pipeline repository is analogous to docker repository.

The recommended setup is to have a repository deployed on a host, and your team share the same 
repository. This repository can also be used to provide pipelines for [serving in production](serving.md).

For testing, you can [start a local repository](#start-local-repo), which runs on your local 
machine in a Docker container. We are also working on a public repository, stay tuned!

---
## Login to a Repository

Since repository is separated from Cebes server, you need to login to the repository by a username and password:

```python
>>> session = cb.get_default_pipeline()

>>> session.pipeline.login()
    [INFO] pycebes.core.session: Logged into repository 172.17.0.3:22000
```

Users can specify the repository hostname and port, along with username and password, as arguments to 
the `login` function. If username is not None and password is None, Cebes will ask for the password
interactively. This is recommended, so that passwords do not get stored in the interpreter history.

---
## Push a pipeline to repository

From `pycebes`, you can push a tagged pipeline by using the pipeline object or its tag:

```python
>>> session.pipeline.list()     
    UUID                                  Tag                     Created                       # of stages
    ------------------------------------  ----------------------  --------------------------  -------------
    cac82369-a6cc-40db-a4ca-78d18e7ec30e  test-local-ppl:default  2018-01-13 16:37:57.479000              3

>>> ppl = session.pipeline.get('test-local-ppl')

>>> session.pipeline.push(ppl)
    [INFO] pycebes.core.session: Using repository 172.17.0.3:22000
    [INFO] pycebes.core.session: Pushed successfully: {"name":"default","lastUpdate":1515877964875,"repositoryName":"test-local-ppl"}

>>> session.pipeline.push('test-local-ppl')
    [INFO] pycebes.core.session: Using repository 172.17.0.3:22000
    [INFO] pycebes.core.session: Pushed successfully: {"name":"default","lastUpdate":1515877972651,"repositoryName":"test-local-ppl"}
```

See [this section](#determine-repository) for information on how Cebes determine which repository
to push to. In this case, since the tag doesn't carry repository address, Cebes pushes it to a local repository.

---
## Pull a pipeline from repository

Similarly, a pipeline can be pulled from a repository into Cebes server using its tag:

```python
>>>  session.pipeline.pull('test-local-ppl')
    [INFO] pycebes.core.session: Using repository 172.17.0.3:22000
    [INFO] pycebes.core.session: Pulled successfully: test-local-ppl:default
    
    Pipeline(dataframeplaceholder_0, linearregression_0, vectorassembler_0)
```

The `pull()` function returns the pipeline object itself.

---
<a name="start-local-repo"></a>
## Start a local repository

A local repository can be started using `Session.start_repository_container` and providing a port:

```python
>>> session = cb.get_default_session()

>>> session.start_repository_container(host_port=35000)
    [INFO] Starting container cebes-pipeline-repository-repo-x.x.x-0[cebes:x.x.x] with data path at $HOME/.cebes/repo-x.x.x/repository
    [INFO] Cebes container started, listening at localhost:35000
    [INFO] pycebes.core.session: Pipeline repository started on port 35000
```

`host_port` is the port on your machine that you will use to talk to the repository on `localhost`.
We recommend a high port between 30000 and 40000. If there is already a repository running, 
`start_repository_container()` will simply re-use it.

If you started a local repository in a session, all pipeline pushes and pulls in that session 
will try to use the local repository (which is at `localhost:35000`). Of course if the pipeline 
tag explicitly contains a different repository, it will be used instead.

The local repository can be stopped with:

```python
>>> session.stop_repository_container()
    [INFO] Cebes container stopped: cebes-pipeline-repository-repo-x.x.x-0[cebes:x.x.x] at port 35000
```

---
<a name="determine-repository"></a>
## How Cebes server determines which repository to use

The repository to which pipelines are pushed or pulled can be confusing. In general, Cebes decides 
the repository to use in the following order:

1. The repository specified in the tag,
1. The local repository, if it is started in the session,
1. The server-wide default repository configured in Cebes server.
