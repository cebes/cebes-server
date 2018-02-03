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

### Combine expressions

To combine multiple boolean expressions, use boolean operators `&`, `|` and `~` on the expressions.

Note that Python's built-in boolean operators like `and`, `or`, `not` are not usable because they 
cannot be overridden to provide the functionality we want. Trying to use `and`, `or`, `not` on 
Cebes expressions will throw an exception.

```python
>>> df.select(df.wax, df.hardener).where((df.wax < 2.8) & ~(df.hardener > 0.8)).show()
    ID: 0ba144ef-22e4-48d6-8774-7eef95b57a40
    Shape: (128, 2)
    Sample 5 rows:
       wax  hardener
    0  2.5       0.7
    1  2.3       0.6
    2  2.5       0.8
    3  2.5       0.6
    4  2.5       0.8

>>> df.select(df.wax, df.hardener).where((df.wax < 2.8) and not(df.hardener > 0.8)).show()
    ...
    ValueError: Cannot convert column into bool: please use '&' for 'and', '|' for 'or', 
    '~' for 'not' when building Dataframe boolean expressions.
``` 

Bitwise operations can be constructed as follows:

```python
>>> df.select(df.roller_durometer, df.current_density, 
              df.roller_durometer.bitwise_or(df.current_density), 
              df.roller_durometer.bitwise_and(cb.bitwise_not(df.current_density)).alias('complicated_expr')).show()

    ID: 2ccc186d-6a7c-45f6-8572-bf0e753442eb
    Shape: (540, 4)
    Sample 5 rows:
       roller_durometer  current_density  (roller_durometer | current_density)  \
    0                34               40                                    42   
    1                34               40                                    42   
    2                40               40                                    40   
    3                40               40                                    40   
    4                35               40                                    43   
    
       complicated_expr
    0                 2  
    1                 2  
    2                 0  
    3                 0  
    4                 3
```

Here we use some functions on the expressions like `df.roller_durometer.bitwise_or()`, but Cebes also provides
many functions in the functional form `cb.bitwise_not()`. See [this page](dataframe_functions.md) 
for the full list of functions provided in Cebes.

Other basic SQL operations include [limit](dataframe_reference.md#pycebes.core.dataframe.Dataframe.limit),
 [intersect](dataframe_reference.md#pycebes.core.dataframe.Dataframe.intersect),
 [union](dataframe_reference.md#pycebes.core.dataframe.Dataframe.union), 
 [subtract](dataframe_reference.md#pycebes.core.dataframe.Dataframe.subtract), 
 [alias](dataframe_reference.md#pycebes.core.dataframe.Dataframe.alias), ...

---
## Joins

Joins of two Dataframes can be done with [join](dataframe_reference.md#pycebes.core.dataframe.Dataframe.join),
which supports generic join conditions and different types of joins. 

If one Dataframe is significantly smaller than the other in a join, you can mark it 
[broadcast](dataframe_reference.md#pycebes.core.dataframe.Dataframe.broadcast), in which 
case the join might be executed more efficient.

---
## Grouping

Grouping can be done with [groupby](dataframe_reference.md#pycebes.core.dataframe.Dataframe.groupby), 
[rollup](dataframe_reference.md#pycebes.core.dataframe.Dataframe.rollup) and 
[cube](dataframe_reference.md#pycebes.core.dataframe.Dataframe.cube) are also supported.
After `groupby()`, various way to aggregate the results are provided, including functions 
in [GroupedDataframe](dataframe_reference.md#pycebes.core.dataframe.GroupedDataframe) and 
[aggregation functions](dataframe_functions.md#pycebes.core.functions.mean).

When grouping on multiple columns, rollup and cube are different in the way they construct the tuples.
For example, when grouping on 3 columns, `ROLLUP (YEAR, MONTH, DAY)` will give the following outputs:

```
YEAR, MONTH, DAY
YEAR, MONTH
YEAR
()
```

while `CUBE (YEAR, MONTH, DAY)` gives the following:

```
YEAR, MONTH, DAY
YEAR, MONTH
YEAR, DAY
YEAR
MONTH, DAY
MONTH
DAY
()
```

## When ... Otherwise ...

A particularly useful API is to compute a column based on values of other columns, much like 
conditional expressions in programming languages. 

For example, here is how to encode the `gender` string column into numeric:

```python
>>> people.select(cb.when(people.gender == 'male', 0)
            .when(people.gender == 'female', 1)
            .otherwise(2).alias('gender_int'))
```

If `otherwise()` is not specified, null value will be used. See 
[when](dataframe_functions.md#pycebes.core.functions.when) and 
[otherwise](dataframe_functions.md#pycebes.core.column.Column.otherwise) for more information.

## Windowing on time-series

Cebes has the [window](dataframe_functions.md#pycebes.core.functions.window) function for windowing on time-series.

In the example below, the `timestamp` column contains the date in format `yyyyMMdd`. We convert 
it into Unix timestamp using the `unix_timestamp` function:

```python
# convert the `timestamp` column from "yyyyMMdd" into Unix timestamp, and cast it to type TIMESTAMP

>>> df2 = df.with_column('timestamp_unix', 
...:    cb.unix_timestamp(df.timestamp.cast(cb.StorageTypes.STRING), pattern='yyyyMMdd').\
...:    cast(cb.StorageTypes.TIMESTAMP))


>>> df2.show()
    ID: b03c92b0-f97a-4d21-94e5-1b039fc2c038
    Shape: (540, 41)
    Sample 5 rows:
       timestamp cylinder_number customer  job_number grain_screened ink_color  \
    0   19910108            X126  TVGUIDE       25503            YES       KEY   
    1   19910109            X266  TVGUIDE       25503            YES       KEY   
    2   19910104              B7   MODMAT       47201            YES       KEY   
    3   19910104            T133   MASSEY       39039            YES       KEY   
    4   19910111             J34    KMART       37351             NO       KEY   
    
      proof_on_ctd_ink blade_mfg cylinder_division paper_type       ...        \
    0              YES    BENTON          GALLATIN   UNCOATED       ...         
    1              YES    BENTON          GALLATIN   UNCOATED       ...         
    2              YES    BENTON          GALLATIN   UNCOATED       ...         
    3              YES    BENTON          GALLATIN   UNCOATED       ...         
    4              YES    BENTON          GALLATIN   UNCOATED       ...         
    
      esa_voltage esa_amperage  wax hardener roller_durometer  current_density  \
    0         0.0          0.0  2.5      1.0               34               40   
    1         0.0          0.0  2.5      0.7               34               40   
    2         0.0          0.0  2.8      0.9               40               40   
    3         0.0          0.0  2.5      1.3               40               40   
    4         5.0          0.0  2.3      0.6               35               40   
    
       anode_space_ratio chrome_content band_type  timestamp_unix  
    0         105.000000          100.0      band       663292800  
    1         105.000000          100.0    noband       663379200  
    2         103.870003          100.0    noband       662947200  
    3         108.059998          100.0    noband       662947200  
    4         106.669998          100.0    noband       663552000  
    
    [5 rows x 41 columns]
```

We then use `window` on the timestamp column, and `groupby` on those groups to find the 
number of transactions happening every week:

```python
>>> df3 = df2.groupby(cb.window(df2.timestamp_unix, '7 days', '7 days')).count()

>>> df3.schema
    Schema(fields=[SchemaField(name='window',storage_type=Struct[TIMESTAMP,TIMESTAMP],variable_type=STRUCT),
                   SchemaField(name='count',storage_type=LONG,variable_type=DISCRETE)])

>>> df3.show()
    ID: 22c45afe-1488-4562-ac4b-5d4767e8a6da
    Shape: (122, 2)
    Sample 5 rows:
                                           window  count
    0  {'end': 676166400.0, 'start': 675561600.0}      8
    1  {'end': 651974400.0, 'start': 651369600.0}      4
    2  {'end': 701568000.0, 'start': 700963200.0}      1
    3  {'end': 707011200.0, 'start': 706406400.0}      1
    4  {'end': 714873600.0, 'start': 714268800.0}      2
```

The `window` column now has type `Struct`, containing the start and end timestamp. We have 
the correct results now, but to make it looks a bit better, wee can 
convert those timestamps into the `dd/MM/yyyy` format, and sort the data:

```python
>>> df4 = df3.with_column('start', cb.date_format(df3['window.start'], 'dd/MM/yyyy')).\
    with_column('end', cb.date_format(df3['window.end'], 'dd/MM/yyyy')).\
    with_column('window_start', df3['window.start']).sort('window_start')
    
>>> df4 = df4.drop(df4.window_start)
   
>>> df4.show()
   ID: 85f03b43-7731-487a-a01c-e5d7ae488c4d
   Shape: (122, 4)
   Sample 5 rows:
                                          window  count       start         end
   0  {'end': 639273600.0, 'start': 638668800.0}      1  29/03/1990  05/04/1990
   1  {'end': 639878400.0, 'start': 639273600.0}      2  05/04/1990  12/04/1990
   2  {'end': 640483200.0, 'start': 639878400.0}      5  12/04/1990  19/04/1990
   3  {'end': 641088000.0, 'start': 640483200.0}      1  19/04/1990  26/04/1990
   4  {'end': 641692800.0, 'start': 641088000.0}      2  26/04/1990  03/05/1990
```
