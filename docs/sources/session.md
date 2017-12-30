## Connect to Cebes server

Using `pycebes`, you need to connect to Cebes server by creating a `Session`:

```python
>>> import pycebes as cb
>>> session = cb.Session()
    [INFO] pycebes.internal.docker_helpers: Starting Cebes container 
        cebes-server-x.xx.x-0[phvu/cebes:x.xx.x] with data path at $HOME/.cebes/x.xx.x
    [INFO] pycebes.internal.docker_helpers: Cebes container started, 
        listening at localhost:32770
    [INFO] pycebes.core.session: Connecting to Cebes container 
        cebes-server-x.xx.x-0[phvu/cebes:x.xx.x] at port 32770
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

`Session` objects are equipped with 2 main groups of functions: loading data into Cebes, and 
managing dataframes, models and pipelines.

### Loading data into Cebes

Depending on where your dataset is, Cebes can load most of them using the following APIs.

#### Local datasets

The most straightforward case is when your data is a **pandas DataFrame**. You can create a 
Cebes Dataframe out of it using [`Session.from_pandas`]():

```python hl_lines="3 4"
>>> import pandas as pd
>>> pandas_df = pd.DataFrame([[1, 'a'], [2, 'b']], columns=['number', 'string'])
>>> cebes_df = session.from_pandas(pandas_df)
    Uploading: .................... 100%
```

Under the hood, this function will serialize and upload the pandas DataFrame to Cebes server,
then create a Cebes Dataframe out of it, which you can use as a normal Cebes Dataframe:

```python
>>> cebes_df.show()
    Shape: (2, 2)
    Sample 2 rows:
       number string
    0       1      a
    1       2      b

>>> cebes_df.schema
    Schema(fields=[SchemaField(name='number',storage_type=INTEGER,variable_type=DISCRETE),
                   SchemaField(name='string',storage_type=STRING,variable_type=TEXT)])
```

A **JSON file** can be uploaded with [`read_json`]():

```python
>>> json_options = cb.JsonReadOptions(date_format='yyyy-MM-dd')
>>> df = session.read_json(path='/path/to/data.json', options=json_options)
```

Similarly, there is [`read_csv`]() for **CSV files**:

```python
>>> csv_options = cb.CsvReadOptions(sep=',', encoding='UTF-8', quote='"')
>>> df = session.read_csv(path='/path/to/data.csv', options=csv_options)
```

More generally, any **JSON, CSV, Parquet, ORC file** on your machine can be loaded with [`read_local`]():

```python
>>> parquet_options = cb.ParquetReadOptions(merge_schema=True)
>>> df = session.read_local(path='/path/to/data.parquet', fmt='parquet', options=parquet_options)
```

Finally, when everything above fails, you can still use `read_local()` with `fmt='text'`, which will 
give you a Dataframe of text lines. Then you can use Cebes [powerful Dataframe APIs](dataframe.md) 
to extract relevant information.

Local datasets are convenient when you want to quickly test something, or your datasets are small.

---

#### Remote datasets

More often, if you have a separated Spark cluster, it might already contain interesting business data.
In that case you can load it into Cebes using the following APIs.

A table in an RDBMS can be read with `read_jdbc`:

```python
>>> df = session.read_jdbc(url='mysql.server.com:3306', table_name='sales', 
                           user_name='admin', password='abc')
```

Similarly, a table in Hive can be read with `read_hive`:

```python
>>> df = session.read_hive(table_name='my_data')
```

Any JSON, CSV, Parquet, ORC or text files stored in Amazon S3 can be read with `read_s3`:

```python
>>> df = session.read_s3(bucket='data', key='sales/2017/', 
                         access_key='YOUR_S3_ACCESS_KEY', secret_key='YOUR_S3_SECRET_KEY', 
                         fmt='csv', options=cb.CsvReadOptions())
```

More often, you might already have data files in Hadoop, in which case `read_hdfs` will come handy:

```python
>>> df = session.read_hdfs(path='/data/path', server=None, fmt='parquet', 
                           options=cb.ParquetReadOptions())
```

`read_s3` and `read_hdfs` support data files in JSON, CSV, Parquet, ORC or text, just like `read_local`.

Once you read your datasets into Cebes, it is a good idea to tag them, so they won't get evicted. 
Keep reading to learn about _tags_ and why they matter in Cebes.

---

### Managing Dataframes, models and pipelines

Cebes server keeps a list of all your dataframes, models and pipelines. Those are 3 types of first-class 
citizens that you work with in Cebes.

#### Managing Dataframes

In Cebes, each Dataframe is uniquely identified by an ID. All Dataframes are kept in memory until they 
get evicted. The [`CEBES_CACHESPEC_DF_STORE`](installation.md#cebes-server-configuration) flag dictates 
how and when Dataframes are evicted.

During the exploration phase however, many temporary Dataframes might be created, while you are only 
interested in a few of them. You might want to keep them around, or simply prevent them from eviction.
To do that, you can give them a tag - an easy-to-remember string:

```python
>>> cebes_df.id
    '8a5d3a28-c3c2-4b58-bd1a-008fa4a33d54'
>>> session.dataframe.tag(cebes_df, 'preprocessed_data')
```

A tagged Dataframe is guaranteed to be checkpointed and can be reloaded later, across Session, using its tag:

```python
>>> df = session.dataframe.get('preprocessed_data')
>>> assert df.id == cebes_df.id
```

You can get the list of all tagged Dataframes using `dataframe.list()`:

```python
>>> session.dataframe.list()
    UUID                                  Tag                        Schema                         Created
    ------------------------------------  -------------------------  -----------------------------  --------------------------
    8a5d3a28-c3c2-4b58-bd1a-008fa4a33d54  preprocessed_data:default  number integer, string string  2017-12-30 02:19:00.533000
```

Technically, a tag is a string with format `NAME:VERSION`. If you don't specify `VERSION`, the default version
is `default`. This allows you to use the same tag name for multiple Dataframes with different versions.

```python
>>> session.dataframe.tag(another_df, 'preprocessed_data:v1')
    Dataframe(id='102d8c93-138a-4c83-99f2-746a033891b9')

>>> session.dataframe.list()
    UUID                                  Tag                        Schema                         Created
    ------------------------------------  -------------------------  -----------------------------  --------------------------
    8a5d3a28-c3c2-4b58-bd1a-008fa4a33d54  preprocessed_data:default  number integer, string string  2017-12-30 02:19:00.533000
    102d8c93-138a-4c83-99f2-746a033891b9  preprocessed_data:v1       number integer, string string  2017-12-30 02:27:05.747000
```

Multiple tags for the same Dataframe is allowed, but tags are unique: the same tag cannot be used for different
Dataframes. Trying to do so will raise an exception.

If you want to reuse a tag, you need to `untag` it first:

```python
>>> session.dataframe.untag('preprocessed_data')
    Dataframe(id='8a5d3a28-c3c2-4b58-bd1a-008fa4a33d54')

>>> session.dataframe.list()
    UUID                                  Tag                   Schema                         Created
    ------------------------------------  --------------------  -----------------------------  --------------------------
    102d8c93-138a-4c83-99f2-746a033891b9  preprocessed_data:v1  number integer, string string  2017-12-30 02:27:05.747000
```

Note how Cebes only untag `preprocessed_data:default`, while keeping `preporcessed_data:v1` unchanged. 
This is because we did `session.dataframe.untag('preprocessed_data')`, and Cebes looks for the full 
tag `preprocessed_data:default` to delete. 

To delete `preprocessed_data:v1`, we need to be specific:

```python
>>> session.dataframe.untag('preprocessed_data:v1')
```

> **Summary**: 
>
>    - _Tag_ is a string given to a Dataframe, so that users can retrieve them later. 
>    - Tagged Dataframes will be persisted in Cebes.
>    - Tag has format `NAME:VERSION`. When unspecified, the default VERSION is `default`.

---

#### Managing Models and Pipelines

All [pipelines and models](pipelines.md) share the same management principles as Dataframes.

- For pipelines:
    - use `Session.pipeline.list`, `Session.pipeline.get`, `Session.pipeline.tag` and `Session.pipeline.untag`
    - the [`CEBES_CACHESPEC_PIPELINE_STORE`](installation.md#cebes-server-configuration) flag dictates how
    and when in-memory pipelines are evicted.
    
- For models:
    - use `Session.model.list`, `Session.model.get`, `Session.model.tag` and `Session.model.untag`
    - the [`CEBES_CACHESPEC_MODEL_STORE`](installation.md#cebes-server-configuration) flag dictates how
    and when in-memory models are evicted.
