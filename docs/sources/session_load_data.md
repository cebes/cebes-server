Depending on where your dataset is, Cebes can load most of them using the following APIs.

## Local datasets

The most straightforward case is when your data is a **pandas DataFrame**. You can create a 
Cebes Dataframe out of it using [`Session.from_pandas`](session_api.md#pycebes.core.session.Session.from_pandas):

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

A **JSON file** can be uploaded with [`read_json`](session_api.md#pycebes.core.session.Session.read_json):

```python
>>> json_options = cb.JsonReadOptions(date_format='yyyy-MM-dd')
>>> df = session.read_json(path='/path/to/data.json', options=json_options)
```

Similarly, there is [`read_csv`](session_api.md#pycebes.core.session.Session.read_csv) for **CSV files**:

```python
>>> csv_options = cb.CsvReadOptions(sep=',', encoding='UTF-8', quote='"')
>>> df = session.read_csv(path='/path/to/data.csv', options=csv_options)
```

More generally, any **JSON, CSV, Parquet, ORC file** on your machine can be loaded with 
[`read_local`](session_api.md#pycebes.core.session.Session.read_local):

```python
>>> parquet_options = cb.ParquetReadOptions(merge_schema=True)
>>> df = session.read_local(path='/path/to/data.parquet', fmt='parquet', options=parquet_options)
```

Finally, when everything above fails, you can still use `read_local()` with `fmt='text'`, which will 
give you a Dataframe of text lines. Then you can use Cebes [powerful Dataframe APIs](dataframe_concepts.md) 
to extract relevant information.

Local datasets are convenient when you want to quickly test something, or your datasets are small.

---

## Remote datasets

More often, if you have a separated Spark cluster, it might already contain interesting business data.
In that case you can load it into Cebes using the following APIs.

A table in an RDBMS can be read with [`read_jdbc`](session_api.md#pycebes.core.session.Session.read_jdbc):

```python
>>> df = session.read_jdbc(url='mysql.server.com:3306', table_name='sales', 
                           user_name='admin', password='abc')
```

Similarly, a table in Hive can be read with [`read_hive`](session_api.md#pycebes.core.session.Session.read_hive):

```python
>>> df = session.read_hive(table_name='my_data')
```

Any JSON, CSV, Parquet, ORC or text files stored in Amazon S3 can be read with 
[`read_s3`](session_api.md#pycebes.core.session.Session.read_s3):

```python
>>> df = session.read_s3(bucket='data', key='sales/2017/', 
                         access_key='YOUR_S3_ACCESS_KEY', secret_key='YOUR_S3_SECRET_KEY', 
                         fmt='csv', options=cb.CsvReadOptions())
```

More often, you might already have data files in Hadoop, in which case 
[`read_hdfs`](session_api.md#pycebes.core.session.Session.read_hdfs) will come handy:

```python
>>> df = session.read_hdfs(path='/data/path', server=None, fmt='parquet', 
                           options=cb.ParquetReadOptions())
```

`read_s3` and `read_hdfs` support data files in JSON, CSV, Parquet, ORC or text, just like `read_local`.

Once you read your datasets into Cebes, it is a good idea to tag them, so they won't get evicted. 
Keep reading to learn about [_tags_](session_df.md) and why they matter in Cebes.
