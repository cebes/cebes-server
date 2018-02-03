## Connect to Cebes server

Using `pycebes`, you need to connect to Cebes server by creating a `Session`:

```python
>>> import pycebes as cb
>>> session = cb.Session()
    [INFO] pycebes.internal.docker_helpers: Starting Cebes container 
        cebes-server-x.xx.x-0[phvu/cebes:x.xx.x] with data path at $HOME/.cebes/x.xx.x
    [INFO] pycebes.internal.docker_helpers: Cebes container started, listening at localhost:32770
    [INFO] pycebes.core.session: Connecting to Cebes container cebes-server-x.xx.x-0[phvu/cebes:x.xx.x] at port 32770
    [INFO] pycebes.core.session: Spark UI can be accessed at http://localhost:32771
```

Without arguments, `Session` will try to establish a connection to a local Cebes server running as 
a Docker container on your workstation. If that succeed, the log will show the details as above.

If you don't stop the docker container, next time Cebes client will automatically connect to the 
running container without starting a new one:

```python
>>> another_session = cb.Session()
    [INFO] pycebes.core.session: Connecting to Cebes container 
        cebes-server-x.xx.x-0[phvu/cebes:x.xx.x] at port 32770
    [INFO] pycebes.core.session: Spark UI can be accessed at http://localhost:32771
```

If you haven't installed Docker, or the docker daemon has not started, the above command will fail:

```python
>>> session = cb.Session()
    ...
    ValueError: Could not create docker client: Error while fetching server API version: 
    ('Connection aborted.', ConnectionRefusedError(111, 'Connection refused')). 
    Have you started the docker daemon?
```

In any case, you can always connect to a Cebes server by specifying a hostname and a port:

```python
>>> session = cb.Session('cebes.server.com', 21000)
```

---

## Default Session and the session context

Usually you only need one `Session` to work with a Cebes server. Within a Python thread,
the first `Session` you created will become the default Session. At any point in time,
you can call `get_default_session()` to get the current default Session, which will be 
used for all commands issued in the thread:

```python
import pycebes as cb

session = cb.Session()
assert cb.get_default_session() is session
```

In the same Python thread, you can create additional Sessions, but then you need 
to specify which Session you want to use by the `as_default()` context manager:

```python

with cb.Session().as_default() as another_session:
    # all commands in this block use `another_session`
    another_session.dataframe.list()

# all commands now use `session`

yet_another_session = cb.Session()
with yet_another_session.as_default():
    # all commands in this block use `yet_another_session`
    pass
```

In these examples, we use the `Session` object explicitly, hence it is easy to say which
one is being used. However, Cebes has some other APIs that involve implicit default session, 
in which case the context manager will come handy.

---

## Working with Session

`Session` objects are equipped with 2 main groups of functions: 

- [loading data into Cebes](session_load_data.md) 
- [managing dataframes, models and pipelines](session_df.md)
