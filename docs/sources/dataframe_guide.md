Built on top of Spark, Cebes provides a rich set of APIs for loading, cleaning, transforming data at scale.

Assume that you already [loaded data into Cebes](session_load_data.md). This document describes
 functions for doing analytics on Dataframes.

## Working with types

See [this section](dataframe_concepts.md#cebes-types) for an overview
of types and schema in Cebes.

In `pycebes`, you can access a column using the `[]` notation or `.<column name>` notation. The
schema of the column can be accessed by `schema[<column name>]`:

```python
>>> df['job_number']
    Column(expr=SparkPrimitiveExpression(col_name='job_number',df_id='...'))
    
>>> df.job_number
    Column(expr=SparkPrimitiveExpression(col_name='job_number',df_id='...'))
    
>>> df.schema['job_number']
    SchemaField(name='job_number',storage_type=INTEGER,variable_type=DISCRETE)
```

This says `job_number` is a column with storage type `INTEGER` and variable type `DISCRETE`.

**Storage type casting**: You can cast a column into a different Storage type like this:

```python
>>> df2 = df.with_storage_type(df.job_number, cb.StorageTypes.LONG)

>>> df2.schema['job_number']
    SchemaField(name='job_number',storage_type=LONG,variable_type=DISCRETE)
    
>>> df.schema['job_number']
    SchemaField(name='job_number',storage_type=INTEGER,variable_type=DISCRETE)
```

Note that `df2` is a different Dataframe from `df`. In fact they have different IDs.

Another way to do the same thing is to use Cebes SQL APIs:

```python
>>> df2 = df.with_column('job_number', df.job_number.cast(cb.StorageTypes.LONG))
```

Here we used `with_column` and `cast` to convert the `job_number` column from INTEGER to LONG.
We will explore other Cebes SQL APIs later.

**Variable type casting**: Similarly, you can set the variable type of a column:

```python
>>> df2 = df.with_variable_type(df.job_number, cb.VariableTypes.ORDINAL)

>>> df2.schema['job_number']
    SchemaField(name='job_number',storage_type=INTEGER,variable_type=ORDINAL)
    
>>> df.schema['job_number']
    SchemaField(name='job_number',storage_type=INTEGER,variable_type=DISCRETE)
```

---
## Basic operations with Dataframes

One of the most frequently used functions of Dataframe is 
[`show`](dataframe_reference.md#pycebes.core.dataframe.Dataframe.show), 
which shows essential information of a Dataframe like its ID, shape as well as several rows taken from it:

```python
>>> df.show()
    ID: 67b827d7-869b-48d9-9364-5a6d3bc2e99c
    Shape: (540, 40)
    Sample 5 rows:
       timestamp cylinder_number customer  job_number grain_screened ink_color   ...
    0   19910108            X126  TVGUIDE       25503            YES       KEY   ...
    1   19910109            X266  TVGUIDE       25503            YES       KEY   ...
    2   19910104              B7   MODMAT       47201            YES       KEY   ...
    3   19910104            T133   MASSEY       39039            YES       KEY   ...
    4   19910111             J34    KMART       37351             NO       KEY   ...
```

Other information of a Dataframe can be inspected quite intuitively as in the following sample code:

```python

# The unique ID of the Dataframe
>>> df.id
    '67b827d7-869b-48d9-9364-5a6d3bc2e99c'

# Number of rows
>>> len(df)
    540

# Number of columns
>>> len(df.columns)
    40

>>> df.shape
    (540, 40)

# list of column names
>>> df.columns
    ['timestamp',
     'cylinder_number',
     ...,
     'band_type']

# the schema of this Dataframe
>>> df.schema
    Schema(fields=[SchemaField(name='timestamp',storage_type=LONG,variable_type=DISCRETE),
                   SchemaField(name='cylinder_number',storage_type=STRING,variable_type=TEXT),
                   ...
                   ,SchemaField(name='band_type',storage_type=STRING,variable_type=TEXT)])

# Schema information of a column can be accessed by the [] notation
>>> df.schema['cylinder_number']
    SchemaField(name='cylinder_number',storage_type=STRING,variable_type=TEXT)

# A column in the Dataframe can be accessed using the `dot` notation
>>> df.timestamp
    Column(expr=SparkPrimitiveExpression(col_name='timestamp',df_id='67b827d7-869b-48d9-9364-5a6d3bc2e99c'))

# or the [] notation
>>> df['timestamp']
    Column(expr=SparkPrimitiveExpression(col_name='timestamp',df_id='67b827d7-869b-48d9-9364-5a6d3bc2e99c'))
```

---
### Sample a Dataframe

There are two ways to take a sample from a Dataframe, depending on how you want the result:

- [take](dataframe_reference.md#pycebes.core.dataframe.Dataframe.take) returns a sample as a `DataSample` 
object, which is downloaded to the client, and can be converted into, for example, pandas DataFrame.
- [sample](dataframe_reference.md#pycebes.core.dataframe.Dataframe.sample) returns a sample as another 
`Dataframe` object. The data remains on the Cebes server.

Furthermore, `sample` is designed to return a random subset of the Dataframe, while `take` is not necessarily 
random.

```python
>>> local_sample = df.take(10)

>>> local_sample
    DataSample(schema=Schema(fields=[SchemaField(name='timestamp',storage_type=LONG,variable_type=DISCRETE),
                                     SchemaField(name='cylinder_number',storage_type=STRING,variable_type=TEXT),
                                     ...,
                                     SchemaField(name='band_type',storage_type=STRING,variable_type=TEXT)]))
>>> local_sample.to_pandas()
       timestamp cylinder_number    customer  job_number grain_screened ink_color  ...
    0   19910108            X126     TVGUIDE       25503            YES       KEY  ...
    1   19910109            X266     TVGUIDE       25503            YES       KEY  ...
    2   19910104              B7      MODMAT       47201            YES       KEY  ...
    
    [10 rows x 40 columns]

>>> df2 = df.sample(prob=0.1, replacement=True, seed=42)

>>> df2
    Dataframe(id='ac712897-c72a-44d3-816f-7e19fa008fcb')

>>> len(df2)
    55
```

---
Other basic operations may include:

- Sort a Dataframe with [sort](dataframe_reference.md#pycebes.core.dataframe.Dataframe.sort)
- Drop a column with [drop](dataframe_reference.md#pycebes.core.dataframe.Dataframe.drop)
- Drop duplicated rows with [drop_duplicates](dataframe_reference.md#pycebes.core.dataframe.Dataframe.drop_duplicates)

It hardly gets more intuitive.

---
## NA handling

- Drop rows having empty cells with [dropna](dataframe_reference.md#pycebes.core.dataframe.Dataframe.dropna)
- Fill empty cells with [fillna](dataframe_reference.md#pycebes.core.dataframe.Dataframe.fillna)


---
## Statistical functions

- Quantile
- Correlation
- Covariance
- Cross-table
- Frequent Itemsets
- Advanced sampling

---
## Basic SQL operations

Most of the operations above could have been expressed in SQL. Thanks to Spark, SQL expressions are a core
part in Cebes' data exploration APIs. We review here a few main functions, and give pointers to others.

The basic filter and projection operations can be done using 
[select](dataframe_reference.md#pycebes.core.dataframe.Dataframe.select) and
[where](dataframe_reference.md#pycebes.core.dataframe.Dataframe.where). For example,
here is a not-so-trivial projection that involves creating an array of values:

```python
>>> df.columns
    [...,
     'customer',
     ...,
     'esa_voltage',
     'esa_amperage']
     
>>> df2 = df.select(df.customer, cb.array(df.esa_voltage, df.esa_amperage).alias('esa_array'), 
                    df.esa_voltage, df.esa_amperage).where(df.customer.isin(['TVGUIDE', 'MODMAT']))

>>> df2.show()
    ID: cd1433ff-0c18-4920-9699-bd7ec7eacd88
    Shape: (99, 4)
    Sample 5 rows:
      customer   esa_array  esa_voltage  esa_amperage
    0  TVGUIDE  [0.0, 0.0]          0.0           0.0
    1  TVGUIDE  [0.0, 0.0]          0.0           0.0
    2   MODMAT  [0.0, 0.0]          0.0           0.0
    3   MODMAT  [1.5, 0.0]          1.5           0.0
    4   MODMAT  [0.0, 0.0]          0.0           0.0
```

Other basic SQL operations include [limit](dataframe_reference.md#pycebes.core.dataframe.Dataframe.limit),
 [intersect](dataframe_reference.md#pycebes.core.dataframe.Dataframe.intersect),
 [union](dataframe_reference.md#pycebes.core.dataframe.Dataframe.union), 
 [subtract](dataframe_reference.md#pycebes.core.dataframe.Dataframe.subtract), 
 [alias](dataframe_reference.md#pycebes.core.dataframe.Dataframe.alias), ...

---
## Joins

---
## GroupBy


