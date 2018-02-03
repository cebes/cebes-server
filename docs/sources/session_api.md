# <h1 id="pycebes.core.session.Session">Session</h1>

```python
Session(self, host=None, port=21000, user_name='', password='', interactive=True)
```

Construct a new `Session` to the server at the given host and port, with the given user name and password.

__Arguments__

- __host (str)__: Hostname of the Cebes server.
    If `None` (default), cebes will try to launch a new
    docker container with a suitable version of Cebes server in it. Note that it requires you have
    a working docker daemon on your machine.
    Otherwise a string containing the host name or IP address of the Cebes server you want to connect to.
- __port (int)__: The port on which Cebes server is listening. Ignored when ``host=None``.
- __user_name (str)__: Username to log in to Cebes server
- __password (str)__: Password of the user to log in to Cebes server
- __interactive (bool)__: whether this is an interactive session,
    in which case some diagnosis logs will be printed to stdout.

## <h2 id="pycebes.core.session.Session.load_test_datasets">load_test_datasets</h2>

```python
Session.load_test_datasets(self)
```

Load test datasets that come by default in Cebes server.

__Returns__

`dict: a dict of datasets, currently has only one element`:
``{'cylinder_bands'`: Dataframe}`

## <h2 id="pycebes.core.session.Session.read_csv">read_csv</h2>

```python
Session.read_csv(self, path, options=None)
```

Upload a local CSV file to the server, and create a Dataframe out of it.

__Arguments__

- __path (str)__: path to the local CSV file
- __options (CsvReadOptions)__: Additional options that dictate how the files are going to be read.
- __Must be either None or a :class__:`CsvReadOptions` object

__Returns__

`Dataframe`: the Cebes Dataframe object created from the data source

## <h2 id="pycebes.core.session.Session.stop_repository_container">stop_repository_container</h2>

```python
Session.stop_repository_container(self)
```
Stop the local pipeline repository if it is running.
        Do nothing if there is no local repository associated with this Session.
## <h2 id="pycebes.core.session.Session.close">close</h2>

```python
Session.close(self)
```

Close this session. Will stop the Cebes container if this session was
created against a local Cebes container. Otherwise it is a no-op.

## <h2 id="pycebes.core.session.Session.client">client</h2>


Return the client which can be used to send requests to server

__Returns__

`Client`: the client object

## <h2 id="pycebes.core.session.Session.model">model</h2>


Return a helper for working with tagged and cached `Model`

__Returns__

`_TagHelper`:

## <h2 id="pycebes.core.session.Session.from_pandas">from_pandas</h2>

```python
Session.from_pandas(self, df)
```

Upload the given `pandas` DataFrame to the server and create a Cebes Dataframe out of it.
Types are preserved on a best-efforts basis.

__Arguments__

- __df (pd.DataFrame)__: a pandas DataFrame object

__Returns__

`Dataframe`: the Cebes Dataframe created from the data source

## <h2 id="pycebes.core.session.Session.read_jdbc">read_jdbc</h2>

```python
Session.read_jdbc(self, url, table_name, user_name='', password='')
```

Read a Dataframe from a JDBC table

__Arguments__

- __url (str)__: URL to the JDBC server
- __table_name (str)__: name of the table
- __user_name (str)__: JDBC user name
- __password (str)__: JDBC password

__Returns__

`Dataframe`: the Cebes Dataframe object created from the data source

## <h2 id="pycebes.core.session.Session.read_local">read_local</h2>

```python
Session.read_local(self, path, fmt='csv', options=None)
```

Upload a file from the local machine to the server, and create a :class:`Dataframe` out of it.

__Arguments__

- __path (str)__: path to the local file
- __fmt (str)__: format of the file, can be `csv`, `json`, `orc`, `parquet`, `text`
- __options__: Additional options that dictate how the files are going to be read.
- __If specified, this can be__:

    - `CsvReadOptions` when `fmt='csv'`,
    - `JsonReadOptions` when `fmt='json'`, or
    - `ParquetReadOptions` when `fmt='parquet'`

 Other formats do not need additional options

__Returns__

`Dataframe`: the Cebes Dataframe object created from the data source

## <h2 id="pycebes.core.session.Session.start_repository_container">start_repository_container</h2>

```python
Session.start_repository_container(self, host_port=None)
```

Start a local docker container running Cebes pipeline repository,
with the repository listening on the host at the given port.

If `host_port` is None, a new port will be automatically allocated
by Docker. Therefore, to maintain consistency with your pipeline tags,
it is recommended to specify a high port, e.g. 35000 or 36000.

If one repository was started for this Session already, it will be returned.

## <h2 id="pycebes.core.session.Session.as_default">as_default</h2>

```python
Session.as_default(self)
```

Returns a context manager that makes this object the default session.

Use with the `with` keyword to specify that all remote calls to server
should be executed in this session.

```python
    sess = cb.Session()
    with sess.as_default():
        ....
```

To get the current default session, use `get_default_session`.

> The default session is a property of the current thread. If you
> create a new thread, and wish to use the default session in that
> thread, you must explicitly add a `with sess.as_default():` in that
> thread's function.

__Returns__

  A context manager using this session as the default session.

## <h2 id="pycebes.core.session.Session.read_hive">read_hive</h2>

```python
Session.read_hive(self, table_name='')
```

Read a Dataframe from Hive table of the given name

__Arguments__

- __table_name (str)__: name of the Hive table to read data from

__Returns__

`Dataframe`: The Cebes Dataframe object created from Hive table

## <h2 id="pycebes.core.session.Session.read_hdfs">read_hdfs</h2>

```python
Session.read_hdfs(self, path, server=None, fmt='csv', options=None)
```

Load a dataset from HDFS.

__Arguments__

- __path (str)__: path to the files on HDFS, e.g. `/data/dataset1`
- __server (str): Host name and port, e.g. `hdfs://server__:9000`
- __fmt (str)__: format of the files, can be `csv`, `json`, `orc`, `parquet`, `text`
- __options__: Additional options that dictate how the files are going to be read.
- __If specified, this can be__:

    - `CsvReadOptions` when `fmt='csv'`,
    - `JsonReadOptions` when `fmt='json'`, or
    - `ParquetReadOptions` when `fmt='parquet'`

 Other formats do not need additional options

__Returns__

`Dataframe`: the Cebes Dataframe object created from the data source

## <h2 id="pycebes.core.session.Session.pipeline">pipeline</h2>


Return a helper for working with tagged and cached `Pipeline`

__Returns__

`_PipelineHelper`:

## <h2 id="pycebes.core.session.Session.dataframe">dataframe</h2>


Return a helper for working with tagged and cached `Dataframe`

__Returns__

`_TagHelper`:

## <h2 id="pycebes.core.session.Session.read_s3">read_s3</h2>

```python
Session.read_s3(self, bucket, key, access_key, secret_key, region=None, fmt='csv', options=None)
```

Read a Dataframe from files stored in Amazon S3.

__Arguments__

- __bucket (str)__: name of the S3 bucket
- __key (str)__: path to the file(s) on S3
- __access_key (str)__: Amazon S3 access key
- __secret_key (str)__: Amazon S3 secret key
- __region (str)__: S3 region, if needed.
- __fmt (str)__: format of the files, can be `csv`, `json`, `orc`, `parquet`, `text`
- __options__: Additional options that dictate how the files are going to be read.
- __If specified, this can be__:

    - `CsvReadOptions` when `fmt='csv'`,
    - `JsonReadOptions` when `fmt='json'`, or
    - `ParquetReadOptions` when `fmt='parquet'`

 Other formats do not need additional options

__Returns__

`Dataframe`: the Cebes Dataframe object created from the data source

## <h2 id="pycebes.core.session.Session.read_json">read_json</h2>

```python
Session.read_json(self, path, options=None)
```

Upload a local JSON file to the server, and create a Dataframe out of it.

__Arguments__

- __path (str)__: path to the local JSON file
- __options (JsonReadOptions)__: Additional options that dictate how the files are going to be read.
- __Must be either None or a :class__:`JsonReadOptions` object

__Returns__

`Dataframe`: the Cebes Dataframe object created from the data source

# <h1 id="pycebes.core.session.CsvReadOptions">CsvReadOptions</h1>

```python
CsvReadOptions(self, sep=',', encoding='UTF-8', quote='"', escape='\\', comment=None, header=False, infer_schema=False, ignore_leading_white_space=False, null_value=None, nan_value='NaN', positive_inf='Inf', negative_inf='-Inf', date_format='yyyy-MM-dd', timestamp_format="yyyy-MM-dd'T'HH:mm:ss.SSSZZ", max_columns=20480, max_chars_per_column=-1, max_malformed_log_per_partition=10, mode='PERMISSIVE')
```

Options for reading CSV files.

__Arguments__

- __sep__: sets the single character as a separator for each field and value.
- __encoding__: decodes the CSV files by the given encoding type
- __quote__: sets the single character used for escaping quoted values where
    the separator can be part of the value. If you would like to turn off quotations, you need to
    set not `null` but an empty string.
- __escape__: sets the single character used for escaping quotes inside an already quoted value.
- __comment__: sets the single character used for skipping lines beginning with this character.
    By default, it is disabled
- __header__: uses the first line as names of columns.
- __infer_schema__: infers the input schema automatically from data. It requires one extra pass over the data.
- __ignore_leading_white_space__: defines whether or not leading whitespaces
    from values being read should be skipped.
- __null_value__: sets the string representation of a null value.
    This applies to all supported types including the string type.
- __nan_value__: sets the string representation of a "non-number" value
- __positive_inf__: sets the string representation of a positive infinity value
- __negative_inf__: sets the string representation of a negative infinity value
- __date_format__: sets the string that indicates a date format.
    Custom date formats follow the formats at `java.text.SimpleDateFormat`.
    This applies to date type.
- __timestamp_format__: sets the string that indicates a timestamp format.
    Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to timestamp type.
- __max_columns__: defines a hard limit of how many columns a record can have
- __max_chars_per_column__: defines the maximum number of characters allowed
    for any given value being read. By default, it is -1 meaning unlimited length
- __max_malformed_log_per_partition__: sets the maximum number of malformed rows
    will be logged for each partition. Malformed records beyond this number will be ignored.
- __mode__: allows a mode for dealing with corrupt records during parsing.
    - `ReadOptions.PERMISSIVE` - sets other fields to `null` when it meets a corrupted record.
            When a schema is set by user, it sets `null` for extra fields
    - `ReadOptions.DROPMALFORMED` - ignores the whole corrupted records
    - `ReadOptions.FAILFAST` - throws an exception when it meets corrupted records


# <h1 id="pycebes.core.session.JsonReadOptions">JsonReadOptions</h1>

```python
JsonReadOptions(self, primitives_as_string=False, prefers_decimal=False, allow_comments=False, allow_unquoted_field_names=False, allow_single_quotes=True, allow_numeric_leading_zeros=False, allow_backslash_escaping_any_character=False, mode='PERMISSIVE', column_name_of_corrupt_record=None, date_format='yyyy-MM-dd', timestamp_format="yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
```

Options for reading Json files

__Arguments__

- __primitives_as_string__: infers all primitive values as a string type
- __prefers_decimal__: infers all floating-point values as a decimal type.
    If the values do not fit in decimal, then it infers them as doubles
- __allow_comments__: ignores Java/C++ style comment in JSON records
- __allow_unquoted_field_names__: allows unquoted JSON field names
- __allow_single_quotes__: allows single quotes in addition to double quotes
- __allow_numeric_leading_zeros__: allows leading zeros in numbers (e.g. 00012)
- __allow_backslash_escaping_any_character__: allows accepting quoting of all
    character using backslash quoting mechanism
- __mode__: allows a mode for dealing with corrupt records during parsing.
    - `ReadOptions.PERMISSIVE` - sets other fields to `null` when it meets a corrupted record.
            When a schema is set by user, it sets `null` for extra fields
    - `ReadOptions.DROPMALFORMED` - ignores the whole corrupted records
    - `ReadOptions.FAILFAST` - throws an exception when it meets corrupted records

- __column_name_of_corrupt_record__: allows renaming the new field having malformed string
    created by `ReadOptions.PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.
- __date_format__: sets the string that indicates a date format.
    Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to date type
- __timestamp_format__: sets the string that indicates a timestamp format.
    Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to timestamp type

# <h1 id="pycebes.core.session.ParquetReadOptions">ParquetReadOptions</h1>

```python
ParquetReadOptions(self, merge_schema=True)
```

Options for reading Parquet files

__Arguments__

- __merge_schema (bool)__: sets whether we should merge schemas collected from all Parquet part-files.

