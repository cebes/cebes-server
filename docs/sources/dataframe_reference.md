# <h1 id="pycebes.core.dataframe.Dataframe">Dataframe</h1>

```python
Dataframe(self, _id, _schema)
```

Representation of a Cebes Dataframe on the client side. All functions in this
class result in remote call to the Cebes server to perform corresponding actions.

Users should **NOT** manually construct this class.

## <h2 id="pycebes.core.dataframe.Dataframe.union">union</h2>

```python
Dataframe.union(self, other)
```

Returns a new Dataframe containing union of rows in this Dataframe and another Dataframe
(without deduplication)

__Arguments__

- __other (Dataframe)__: another Dataframe to compute the union

- __`Example__`:
```python
df.where(df.wax < 2).union(df.where(df.wax > 2.8)).select('customer', 'wax').show()
    ...
        customer  wax
    0      USCAV  1.1
    1   COLORTIL  1.7
    2   COLORTIL  1.0
    3  ABBYPRESS  1.0
    4  ABBYPRESS  1.0
```

## <h2 id="pycebes.core.dataframe.Dataframe.schema">schema</h2>


The Schema of this data frame

## <h2 id="pycebes.core.dataframe.Dataframe.drop_duplicates">drop_duplicates</h2>

```python
Dataframe.drop_duplicates(self, *columns)
```

Returns a new Dataframe that contains only the unique rows from this Dataframe, only considered
the given list of columns.

If no columns are given (the default), all columns will be considered

__Returns__

`Dataframe`: a new ``Dataframe`` with duplicated rows removed

__Example__

```python
# original Dataframe has 540 rows
df.shape
    (540, 40)

# No duplicated rows in this Dataframe, therefore the shape stays the same
df.drop_duplicates().shape
    (540, 40)

# only consider the `customer` column
df.drop_duplicates(df.customer).shape
    (83, 40)

# we can check by computing the number of distinct values in column `customer`
df.select(cb.count_distinct(df.customer)).take().to_pandas()
       count(DISTINCT customer)
    0                        83

# only consider 2 columns
df.drop_duplicates(df.customer, 'cylinder_number').shape
    (490, 40)
```

## <h2 id="pycebes.core.dataframe.Dataframe.from_json">from_json</h2>

```python
Dataframe.from_json(js_data)
```

Return a ``Dataframe`` instance from its JSON representation

__Arguments__

- __js_data (dict)__: a dict with ``id`` and ``schema``

__Returns__

`Dataframe`: the result Dataframe object

## <h2 id="pycebes.core.dataframe.Dataframe.select">select</h2>

```python
Dataframe.select(self, *columns)
```

Selects a set of columns based on expressions.

__Arguments__

- __columns__: list of columns, or column names

__Returns__

`Dataframe`: result of the select operation

__Example__


```python

import pycebes as cb

# different ways to provide arguments to `select`
df.select(df.customer, cb.substring('cylinder_number', 0, 1).alias('cylinder_t'),
          cb.col('hardener'), 'wax').show()
    ...
      customer cylinder_t  hardener  wax
    0  TVGUIDE          X       1.0  2.5
    1  TVGUIDE          X       0.7  2.5
    2   MODMAT          B       0.9  2.8
    3   MASSEY          T       1.3  2.5
    4    KMART          J       0.6  2.3

# Select all columns in a dataframe can be done in several ways
df.select('*')
df.select(df['*'])
df.select(cb.col('*'))
df.alias('my_name').select('my_name.*')
```

## <h2 id="pycebes.core.dataframe.Dataframe.subtract">subtract</h2>

```python
Dataframe.subtract(self, other)
```

Returns a new Dataframe containing rows in this Dataframe but not in another Dataframe.
This is equivalent to `EXCEPT` in SQL.

__Arguments__

- __other (Dataframe)__: another Dataframe to compute the except

__Example__

```python
df.where(df.wax < 2.8).subtract(df.where(df.wax < 2)).select('customer', 'wax').show()
    ...
            customer  wax
    0      SERVMERCH  2.7
    1        TVGUIDE  2.5
    2         MODMAT  2.7
    3           AMES  2.4
    4  colorfulimage  2.5
```

## <h2 id="pycebes.core.dataframe.Dataframe.id">id</h2>


Return the unique ID of this :class:`Dataframe`

## <h2 id="pycebes.core.dataframe.Dataframe.agg">agg</h2>

```python
Dataframe.agg(self, *exprs)
```

Compute aggregates and returns the result as a DataFrame.

This is a convenient method in which the aggregations are computed on
all rows. In other words, ``self.agg(*exprs)`` is equivalent to ``self.groupby().agg(*exprs)``

If exprs is a single dict mapping from string to string,
then the key is the column to perform aggregation on, and the value is the aggregate function.

The available aggregate functions are `avg`, `max`, `min`, `sum`, `count`.

Alternatively, exprs can also be a list of aggregate ``Column`` expressions.

__Arguments__

- __exprs (dict, list)__: a dict mapping from column name (string) to aggregate functions (string),
    or a list of `Column`

__Example__

```python
import pycebes as cb

# count number of non-NA values in column `hardener`
df.agg(cb.count(df.hardener)).show()
        count(hardener)
    0              533

# count number of non-NA values in all columns:
df.agg(*[cb.count(c).alias(c) for c in df.columns])
```

See `GroupedDataframe` for more examples

## <h2 id="pycebes.core.dataframe.Dataframe.cube">cube</h2>

```python
Dataframe.cube(self, *columns)
```

Create a multi-dimensional cube for the current ``Dataframe`` using the specified columns,
so we can run aggregation on them.

See `GroupedDataframe` for all the available aggregate functions.

__Arguments__

- __columns__: list of column names or ``Column`` objects

__Returns__

`GroupedDataframe`: object providing aggregation functions

__Example__

```python
df.cube(df.customer, 'proof_on_ctd_ink').count().show()
    ...
          customer proof_on_ctd_ink  count
    0  NTLWILDLIFE              YES      1
    1     HANHOUSE              YES      2
    2   YIELDHOUSE              YES      1
    3      toysrus                       3
    4          CVS                       2

df.cube(df.customer, 'proof_on_ctd_ink').agg({'hardener': 'max', 'wax': 'avg'}).show()
    ...
          customer proof_on_ctd_ink  max(hardener)  avg(wax)
    0  NTLWILDLIFE              YES            0.6      3.00
    1     HANHOUSE              YES            1.0      2.25
    2   YIELDHOUSE              YES            0.5      3.00
    3      toysrus                             2.1      2.40
    4          CVS                             1.0      2.30
```

## <h2 id="pycebes.core.dataframe.Dataframe.drop">drop</h2>

```python
Dataframe.drop(self, *columns)
```

Drop columns in this ``Dataframe``

__Arguments__

- __columns__: column names or `Column` objects

__Returns__

`Dataframe`: a new ``Dataframe`` with the given columns dropped.
    If ``columns`` is empty, this ``Dataframe`` is returned

__Example__

```python
df1.show()
    ...
      customer cylinder_number  hardener
    0  TVGUIDE            X126       1.0
    1  TVGUIDE            X266       0.7
    2   MODMAT              B7       0.9
    3   MASSEY            T133       1.3
    4    KMART             J34       0.6

df1.drop(df1.customer, 'hardener').show()
    ...
      cylinder_number
    0            X126
    1            X266
    2              B7
    3            T133
    4             J34
```

## <h2 id="pycebes.core.dataframe.Dataframe.broadcast">broadcast</h2>


Marks a Dataframe as small enough for use in broadcast joins.

## <h2 id="pycebes.core.dataframe.Dataframe.with_column_renamed">with_column_renamed</h2>

```python
Dataframe.with_column_renamed(self, existing_name, new_name)
```

Returns a new ``Dataframe`` with a column renamed.

__Arguments__

- __existing_name (str)__:
- __new_name (str)__:

## <h2 id="pycebes.core.dataframe.Dataframe.join">join</h2>

```python
Dataframe.join(self, other, expr, join_type='inner')
```

Join with another ``Dataframe``, using the given join expression.

__Arguments__

- __other (Dataframe)__: right side of the join
- __expr (Column)__: Join expression, as a ``Column``
- __join_type (str): One of__: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`, `leftanti`, `cross`

__Example__

```python
df1.join(df2, df1.df1Key == df2.df2Key, join_type='outer')

# for self-joins, you should rename the columns so that they are accessible after the join
df1 = df.where(df.wax > 2).select(df[c].alias('df1_{}'.format(c)) for c in df.columns)
df2 = df.where(df.wax < 2.2).select(df[c].alias('df2_{}'.format(c)) for c in df.columns)

df_join = df1.join(df2, df1.df1_customer == df2.df2_customer)
```

## <h2 id="pycebes.core.dataframe.Dataframe.columns">columns</h2>


Return a list of column names in this ``Dataframe``

## <h2 id="pycebes.core.dataframe.Dataframe.sample">sample</h2>

```python
Dataframe.sample(self, prob=0.1, replacement=True, seed=42)
```

Take a sample from this ``Dataframe`` with or without replacement at the given probability.

Note that this function is probabilistic: it samples each row with a probability of ``prob``,
therefore if the original ``Dataframe`` has ``N`` rows, the result of this function
will *NOT* have exactly ``N * prob`` rows.

__Arguments__

- __prob (float)__: The probability to sample each row in the ``Dataframe``
- __replacement (bool)__: Whether to sample with replacement
- __seed (int)__: random seed

__Returns__

`Dataframe`: a sample

## <h2 id="pycebes.core.dataframe.Dataframe.limit">limit</h2>

```python
Dataframe.limit(self, n=100)
```

Returns a new ``Dataframe`` by taking the first ``n`` rows.

## <h2 id="pycebes.core.dataframe.Dataframe.alias">alias</h2>

```python
Dataframe.alias(self, alias='new_name')
```

Returns a new Dataframe with an alias set

## <h2 id="pycebes.core.dataframe.Dataframe.shape">shape</h2>


Return a 2-tuple with number of rows and columns

__Example__


```python
    df.shape
        (540, 40)
```

## <h2 id="pycebes.core.dataframe.Dataframe.dropna">dropna</h2>

```python
Dataframe.dropna(self, how='any', thresh=None, columns=None)
```

Return object with labels on given axis omitted where some of the data are missing

__Arguments__

- __how (str): strategy to drop rows. Accepted values are__:
- __* ``any``__: if any NA values are present, drop that label
- __* ``all``__: if all values are NA, drop that label
- __thresh (int)__: drop rows containing less than ``thresh`` non-null and non-NaN values
    If ``thresh`` is specified, ``how`` will be ignored.
- __columns (list)__: a list of columns to include. Default is None, meaning all columns are included

__Returns__

`Dataframe`: a new ``Dataframe`` with NA values dropped

__Example__

```python
df.dropna()
df.dropna(how='all')
df.dropna(thresh=38)
df.dropna(thresh=2, columns=[df.wax, 'proof_on_ctd_ink', df.hardener])
```

## <h2 id="pycebes.core.dataframe.Dataframe.show">show</h2>

```python
Dataframe.show(self, n=5)
```

Convenient function to show basic information and sample rows from this Dataframe

__Arguments__

- __n (int)__: Number of rows to show

__Example__


```python
df.groupby(df.customer, 'cylinder_number').count().show()
    ID: 4ae73a7c-27e5-43b9-b69f-43c606e38ee0
    Shape: (490, 3)
    Sample 5 rows:
        customer cylinder_number  count
    0     MODMAT            F108      1
    1     MODMAT              M4      1
    2  CASLIVING             M74      1
    3    ECKERDS            X381      1
    4     TARGET             R43      1
```

## <h2 id="pycebes.core.dataframe.Dataframe.fillna">fillna</h2>

```python
Dataframe.fillna(self, value=None, columns=None)
```

Fill NA/NaN values using the specified value.

__Arguments__

- __value__: the value used to fill the holes. Can be a ``float``, ``string`` or a ``dict``.
    When ``value`` is a dict, it is a map from column names to a value used to fill the holes in that column.
    In this case the value can only be of type ``int``, ``float``, ``string`` or ``bool``.
    The values will be casted to the column data type.
- __columns (list)__: List of columns to consider. If None, all columns are considered

__Returns:__

Dataframe: a new Dataframe with holes filled

## <h2 id="pycebes.core.dataframe.Dataframe.sort">sort</h2>

```python
Dataframe.sort(self, *args)
```

Sort this ``Dataframe`` based on the given list of expressions.

__Arguments__

- __args__: a list of arguments to specify how to sort the Dataframe, where each element
    is either a ``Column`` object or a column name.
    When the element is a column name, it will be sorted in ascending order.

__Returns__

`Dataframe`: a new, sorted `Dataframe`

__Example__

```python
df2 = df1.sort()
df2 = df1.sort(df1['timestamp'].asc, df1['customer'].desc)
df2 = df1.sort('timestamp', df1.customer.desc)

# raise ValueError
df1.sort('non_exist')
```

## <h2 id="pycebes.core.dataframe.Dataframe.rollup">rollup</h2>

```python
Dataframe.rollup(self, *columns)
```

Create a multi-dimensional rollup for the current ``Dataframe`` using the specified columns,
so we can run aggregation on them.

See `GroupedDataframe` for all the available aggregate functions.

__Arguments__

- __columns__: list of column names or ``Column`` objects

__Returns__

`GroupedDataframe`: object providing aggregation functions

__Example__

```python
df.rollup(df.customer, 'proof_on_ctd_ink').count().show()
    ...
          customer proof_on_ctd_ink  count
    0  NTLWILDLIFE              YES      1
    1     HANHOUSE              YES      2
    2   YIELDHOUSE              YES      1
    3      toysrus                       3
    4          CVS                       2

df.rollup(df.customer, 'proof_on_ctd_ink').agg({'hardener': 'max', 'wax': 'avg'}).show()
    ...
          customer proof_on_ctd_ink  max(hardener)  avg(wax)
    0  NTLWILDLIFE              YES            0.6      3.00
    1     HANHOUSE              YES            1.0      2.25
    2   YIELDHOUSE              YES            0.5      3.00
    3      toysrus                             2.1      2.40
    4          CVS                             1.0      2.30
```

## <h2 id="pycebes.core.dataframe.Dataframe.where">where</h2>

```python
Dataframe.where(self, condition)
```

Filters rows using the given condition.

__Arguments__

- __condition (Column)__: the condition as a Column

__Example__

```python
df.where((df.hardener >= 1) & (df.wax < 2.8)).select('wax', df.hardener, df.customer).show()
    ...
       wax  hardener     customer
    0  2.5       1.0      TVGUIDE
    1  2.5       1.3       MASSEY
    2  2.5       1.1        ROSES
    3  2.5       1.0  HANOVRHOUSE
    4  2.0       1.0   GUIDEPOSTS
```

## <h2 id="pycebes.core.dataframe.Dataframe.with_storage_type">with_storage_type</h2>

```python
Dataframe.with_storage_type(self, column, storage_type=<StorageTypes.INTEGER: ('integer', <class 'int'>)>)
```

Cast the given column of this Dataframe into the given storage type.
No-op if the new storage type is the same with the current one.

__Arguments__

- __column__: a string or a Column object
- __storage_type (StorageTypes)__: the new storage type to convert to

__Returns__

`Dataframe`: this Dataframe or a new Dataframe

## <h2 id="pycebes.core.dataframe.Dataframe.intersect">intersect</h2>

```python
Dataframe.intersect(self, other)
```

Returns a new Dataframe containing rows only in both this Dataframe and another Dataframe.

__Arguments__

- __other (Dataframe)__: another Dataframe to compute the intersection

__Example__

```python
df.where(df.wax > 2).intersect(df.where(df.wax < 2.8)).select('customer', 'wax').show()
    ...
            customer  wax
    0      SERVMERCH  2.7
    1        TVGUIDE  2.5
    2         MODMAT  2.7
    3           AMES  2.4
    4  colorfulimage  2.5
```

## <h2 id="pycebes.core.dataframe.Dataframe.with_column">with_column</h2>

```python
Dataframe.with_column(self, col_name, col)
```

Returns a new ``Dataframe`` by adding a column or replacing
the existing column that has the same name (case-insensitive).

__Arguments__

- __col_name (str)__: new column name
- __col (Column)__: ``Column`` object describing the new column

## <h2 id="pycebes.core.dataframe.Dataframe.with_variable_type">with_variable_type</h2>

```python
Dataframe.with_variable_type(self, column, variable_type=<VariableTypes.DISCRETE: 'Discrete'>)
```

Manually set the variable type of the given column.
Raise error if the new variable type is not compatible with the storage type of the column.

__Arguments__

- __column__: a string or a Column object
- __variable_type (VariableTypes)__: the new variable type

__Returns__

`Dataframe`: this Dataframe or a new Dataframe

## <h2 id="pycebes.core.dataframe.Dataframe.take">take</h2>

```python
Dataframe.take(self, n=10)
```

Take a sample of this ``Dataframe``

__Arguments__

- __n (int)__: maximum number of rows to be taken

__Returns__

`DataSample`: sample of maximum size `n`

## <h2 id="pycebes.core.dataframe.Dataframe.groupby">groupby</h2>

```python
Dataframe.groupby(self, *columns)
```

Groups the ``Dataframe`` using the specified columns, so that we can run aggregation on them.

See `GroupedDataframe` for all the available aggregate functions.

__Arguments__

- __columns__: list of column names or ``Column`` objects

__Returns__

`GroupedDataframe`: object providing aggregation functions

__Example__

```python
df.groupby(df.customer, 'cylinder_number').count().show()
    ...
        customer cylinder_number  count
    0     MODMAT            F108      1
    1     MODMAT              M4      1
    2  CASLIVING             M74      1
    3    ECKERDS            X381      1
    4     TARGET             R43      1

df.groupby().count().show()
    ...
       count
    0    540
```


# <h1 id="pycebes.core.dataframe.GroupedDataframe">GroupedDataframe</h1>

```python
GroupedDataframe(self, df, agg_columns=(), agg_type='GroupBy', pivot_column=None, pivot_values=())
```

Represent a grouped Dataframe, providing aggregation functions.

## <h2 id="pycebes.core.dataframe.GroupedDataframe.GROUPBY">GROUPBY</h2>

str(object='') -> str
str(bytes_or_buffer[, encoding[, errors]]) -> str

Create a new string object from the given object. If encoding or
errors is specified, then the object must expose a data buffer
that will be decoded using the given encoding and error handler.
Otherwise, returns the result of object.__str__() (if defined)
or repr(object).
encoding defaults to sys.getdefaultencoding().
errors defaults to 'strict'.
## <h2 id="pycebes.core.dataframe.GroupedDataframe.mean">mean</h2>

```python
GroupedDataframe.mean(self, *columns)
```

Computes the average value for each numeric column for each group.

__Arguments__

- __columns__: list of column names (string). Non-numeric columns are ignored.

__Example__

```python
df.groupby(df.customer).mean('hardener', 'wax').show()
    ...
           customer  avg(hardener)  avg(wax)
          WOOLWORTH       0.811111  1.744444
           HOMESHOP       1.300000  2.500000
       homeshopping            NaN  2.500000
             GLOBAL       1.100000  3.000000
                JCP       0.975000  2.437500
```

## <h2 id="pycebes.core.dataframe.GroupedDataframe.sum">sum</h2>

```python
GroupedDataframe.sum(self, *columns)
```

Computes the sum for each numeric column for each group.

__Arguments__

- __columns__: list of column names (string). Non-numeric columns are ignored.

__Example__

```python
df.groupby(df.customer).sum('hardener', 'wax').show()
    ...
           customer  sum(hardener)  sum(wax)
          WOOLWORTH            7.3     15.70
           HOMESHOP            2.6      5.00
       homeshopping            NaN      2.50
             GLOBAL            1.1      3.00
                JCP            3.9      9.75
```

## <h2 id="pycebes.core.dataframe.GroupedDataframe.pivot">pivot</h2>

```python
GroupedDataframe.pivot(self, column, values=None)
```

Pivots a column of the ``Dataframe`` and perform the specified aggregation.

__Arguments__

- __column (str)__: Name of the column to pivot.
- __values__: List of values that will be translated to columns in the output ``Dataframe``
    If unspecified, Cebes will need to compute the unique values of the given column before
    the pivot, therefore it will be less efficient.

__Returns__

`GroupedDataframe`: another `GroupedDataframe` object, on which you can call aggregation functions

__Example__

```python
# for each pair of (`customer`, `proof_on_ctd_ink`), count the number of entries
df.groupby(df.customer).pivot('proof_on_ctd_ink').count().show()
    ...
           customer        NO  YES
          WOOLWORTH  1.0  1.0  7.0
           HOMESHOP  NaN  NaN  2.0
       homeshopping  1.0  NaN  NaN
             GLOBAL  NaN  NaN  1.0
                JCP  NaN  NaN  4.0

# for each pair of (`customer`, `proof_on_ctd_ink`), get the max value of `hardener`
df.groupby(df.customer).pivot('proof_on_ctd_ink').max('hardener').show()
    ...
          customer        NO  YES
         WOOLWORTH  1.0  0.0  1.3
          HOMESHOP  NaN  NaN  1.8
      homeshopping  NaN  NaN  NaN
            GLOBAL  NaN  NaN  1.1
               JCP  NaN  NaN  1.7

# specify the pivot values, more efficient
df.groupby(df.customer).pivot('proof_on_ctd_ink', values=['YES', 'NO']).count().show()
    ...
           customer  YES   NO
          WOOLWORTH  7.0  1.0
           HOMESHOP  2.0  NaN
       homeshopping  NaN  NaN
             GLOBAL  1.0  NaN
                JCP  4.0  NaN
```

## <h2 id="pycebes.core.dataframe.GroupedDataframe.min">min</h2>

```python
GroupedDataframe.min(self, *columns)
```

Computes the min value for each numeric column for each group.

__Arguments__

- __columns__: list of column names (string). Non-numeric columns are ignored.

__Example__

```python
df.groupby(df.customer).min('hardener', 'wax').show()
    ...
          customer  min(hardener)  min(wax)
         WOOLWORTH            0.0      0.00
          HOMESHOP            0.8      2.50
      homeshopping            NaN      2.50
            GLOBAL            1.1      3.00
               JCP            0.6      1.75
```

## <h2 id="pycebes.core.dataframe.GroupedDataframe.CUBE">CUBE</h2>

str(object='') -> str
str(bytes_or_buffer[, encoding[, errors]]) -> str

Create a new string object from the given object. If encoding or
errors is specified, then the object must expose a data buffer
that will be decoded using the given encoding and error handler.
Otherwise, returns the result of object.__str__() (if defined)
or repr(object).
encoding defaults to sys.getdefaultencoding().
errors defaults to 'strict'.
## <h2 id="pycebes.core.dataframe.GroupedDataframe.agg">agg</h2>

```python
GroupedDataframe.agg(self, *exprs)
```

Compute aggregates and returns the result as a DataFrame.

If exprs is a single dict mapping from string to string,
then the key is the column to perform aggregation on, and the value is the aggregate function.

The available aggregate functions are `avg`, `max`, `min`, `sum`, `count`.

Alternatively, exprs can also be a list of aggregate ``Column`` expressions.

__Arguments__

- __exprs (dict, list)__: a dict mapping from column name (string) to aggregate functions (string),
- __or a list of :class__:`Column`

__Example__

```python
import pycebes as cb

df.groupby(df.customer).agg(cb.max(df.timestamp), cb.min('job_number'),
                            cb.count_distinct('job_number').alias('cnt')).show()
    ...
           customer  max(timestamp)  min(job_number)  cnt
          WOOLWORTH        19920405            34227    5
           HOMESHOP        19910129            38064    1
       homeshopping        19920712            36568    1
             GLOBAL        19900621            36846    1
                JCP        19910322            34781    2

df.groupby(df.customer).agg({'timestamp': 'max', 'job_number': 'min', 'job_number': 'count'}).show()
    ...
           customer  max(timestamp)  count(job_number)
          WOOLWORTH        19920405                  9
           HOMESHOP        19910129                  2
       homeshopping        19920712                  1
             GLOBAL        19900621                  1
                JCP        19910322                  4
```

## <h2 id="pycebes.core.dataframe.GroupedDataframe.avg">mean</h2>

```python
GroupedDataframe.mean(self, *columns)
```

Computes the average value for each numeric column for each group.

__Arguments__

- __columns__: list of column names (string). Non-numeric columns are ignored.

__Example__

```python
df.groupby(df.customer).mean('hardener', 'wax').show()
    ...
           customer  avg(hardener)  avg(wax)
          WOOLWORTH       0.811111  1.744444
           HOMESHOP       1.300000  2.500000
       homeshopping            NaN  2.500000
             GLOBAL       1.100000  3.000000
                JCP       0.975000  2.437500
```

## <h2 id="pycebes.core.dataframe.GroupedDataframe.max">max</h2>

```python
GroupedDataframe.max(self, *columns)
```

Computes the max value for each numeric column for each group.

__Arguments__

- __columns__: list of column names (string). Non-numeric columns are ignored.

__Example__

```python
df.groupby(df.customer).max('hardener', 'wax').show()
    ...
           customer  max(hardener)  max(wax)
          WOOLWORTH            1.3       2.6
           HOMESHOP            1.8       2.5
       homeshopping            NaN       2.5
             GLOBAL            1.1       3.0
                JCP            1.7       3.0
```

## <h2 id="pycebes.core.dataframe.GroupedDataframe.ROLLUP">ROLLUP</h2>

str(object='') -> str
str(bytes_or_buffer[, encoding[, errors]]) -> str

Create a new string object from the given object. If encoding or
errors is specified, then the object must expose a data buffer
that will be decoded using the given encoding and error handler.
Otherwise, returns the result of object.__str__() (if defined)
or repr(object).
encoding defaults to sys.getdefaultencoding().
errors defaults to 'strict'.
## <h2 id="pycebes.core.dataframe.GroupedDataframe.count">count</h2>

```python
GroupedDataframe.count(self)
```

Counts the number of records for each group.

__Example__

```python
df.groupby(df.customer).count().show()
    ...
          customer  count
         WOOLWORTH      9
          HOMESHOP      2
      homeshopping      1
            GLOBAL      1
               JCP      4

df.groupby(df.customer, 'proof_on_ctd_ink').count().show()
    ...
           customer proof_on_ctd_ink  count
         GUIDEPOSTS              YES      5
           homeshop                       4
      colorfulimage                       1
          CASLIVING              YES      2
              ABBEY              YES      4
```

