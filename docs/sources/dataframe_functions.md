# <h1 id="pycebes.core.functions">pycebes.core.functions</h1>


## <h2 id="pycebes.core.functions.json_tuple">json_tuple</h2>

```python
json_tuple(column, *fields)
```

Creates a new row for a json column according to the given field names

## <h2 id="pycebes.core.functions.tanh">tanh</h2>

```python
tanh(column)
```

Computes the hyperbolic tangent of the given value

## <h2 id="pycebes.core.functions.weekofyear">weekofyear</h2>

```python
weekofyear(column)
```

Extracts the week number as an integer from a given date/timestamp/string

## <h2 id="pycebes.core.functions.nanvl">nanvl</h2>

```python
nanvl(column1, column2)
```

Returns col1 if it is not NaN, or col2 if col1 is NaN.
Both inputs should be floating point columns (DoubleType or FloatType).

## <h2 id="pycebes.core.functions.unix_timestamp">unix_timestamp</h2>

```python
unix_timestamp(column=None, pattern='yyyy-MM-dd HH:mm:ss')
```

Convert time string in ``column`` with given ``pattern``
(see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
to Unix time stamp (in seconds), return null if fail.

If ``column=None``, the current Unix timestamp (computed by :func:`current_timestamp`) will be used

## <h2 id="pycebes.core.functions.base64">base64</h2>

```python
base64(column)
```

Computes the BASE64 encoding of a binary column and returns it as a string column.
This is the reverse of unbase64

## <h2 id="pycebes.core.functions.repeat">repeat</h2>

```python
repeat(column, n=1)
```

Repeats a string column n times, and returns it as a new string column

## <h2 id="pycebes.core.functions.expm1">expm1</h2>

```python
expm1(column)
```

Computes the exponential of the given value minus one

## <h2 id="pycebes.core.functions.format_string">format_string</h2>

```python
format_string(fmt, *columns)
```

Formats the arguments in printf-style and returns the result as a string column

## <h2 id="pycebes.core.functions.explode">explode</h2>

```python
explode(column)
```

Creates a new row for each element in the given array or map column

## <h2 id="pycebes.core.functions.md5">md5</h2>

```python
md5(column)
```

Calculates the MD5 digest of a binary column and returns the value
as a 32 character hex string

## <h2 id="pycebes.core.functions.posexplode">posexplode</h2>

```python
posexplode(column)
```

Creates a new row for each element with position in the given array or map column

## <h2 id="pycebes.core.functions.get_json_object">get_json_object</h2>

```python
get_json_object(column, path='')
```

Extracts json object from a json string based on json path specified, and returns json string
of the extracted json object. It will return null if the input json string is invalid.

## <h2 id="pycebes.core.functions.var_samp">var_samp</h2>

```python
var_samp(column)
```

Returns the unbiased variance of the values in a group

See `var_pop` for computing the population variance.

## <h2 id="pycebes.core.functions.date_add">date_add</h2>

```python
date_add(column, days=1)
```

Returns the date that is `days` days after the date in ``column``

## <h2 id="pycebes.core.functions.log">log</h2>

```python
log(column, base=None)
```

Computes the natural logarithm of the given value if ``base`` is None.
If ``base`` is not None, compute the logarithm with the given base of the column.

## <h2 id="pycebes.core.functions.cbrt">cbrt</h2>

```python
cbrt(column)
```

Computes the cube-root of the given value

## <h2 id="pycebes.core.functions.substring_index">substring_index</h2>

```python
substring_index(column, delim=' ', cnt=1)
```

Returns the substring from string ``column`` before ``cnt`` occurrences of the delimiter ``delim``.
If ``cnt`` is positive, everything the left of the final delimiter (counting from left) is
returned. If ``cnt`` is negative, every to the right of the final delimiter (counting from the
right) is returned. substring_index performs a case-sensitive match when searching for ``delim``.

## <h2 id="pycebes.core.functions.ltrim">ltrim</h2>

```python
ltrim(column)
```

Trim the spaces from left end for the specified string value

## <h2 id="pycebes.core.functions.regexp_extract">regexp_extract</h2>

```python
regexp_extract(column, regexp='', group_idx=0)
```

Extract a specific group matched by a Java regex, from the specified string column.
If the regex did not match, or the specified group did not match, an empty string is returned.

## <h2 id="pycebes.core.functions.struct">struct</h2>

```python
struct(*columns)
```

Creates a new struct column.
If the input column is a column in a ``Dataframe``, or a derived column expression
that is named (i.e. aliased), its name would be remained as the StructField's name,
otherwise, the newly generated StructField's name would be auto generated as col${index + 1},
i.e. col1, col2, col3, ...

## <h2 id="pycebes.core.functions.bin">bin</h2>

```python
bin(column)
```

An expression that returns the string representation of the binary value of the given long column.
For example, bin("12") returns "1100".

## <h2 id="pycebes.core.functions.ascii">ascii</h2>

```python
ascii(column)
```

Computes the numeric value of the first character of the string column, and returns the
result as an int column

## <h2 id="pycebes.core.functions.sum">sum</h2>

```python
sum(column, is_distinct=False)
```

Returns the sum of all values in the expression

__Arguments__

- __column (Column)__: the column to compute the sum
- __is_distinct (bool)__: whether to only compute the sum of distinct values.
        False by default, meaning computing the sum of all values

## <h2 id="pycebes.core.functions.first">first</h2>

```python
first(column, ignore_nulls=False)
```

Returns the first value in a group.

The function by default returns the first values it sees. It will return the first non-null
value it sees when ``ignore_nulls`` is set to true. If all values are null, then null is returned.

## <h2 id="pycebes.core.functions.crc32">crc32</h2>

```python
crc32(column)
```

Calculates the cyclic redundancy check value  (CRC32) of a binary column and
returns the value as a bigint

## <h2 id="pycebes.core.functions.hour">hour</h2>

```python
hour(column)
```

Extracts the hours as an integer from a given date/timestamp/string

## <h2 id="pycebes.core.functions.count">count</h2>

```python
count(column)
```

returns the number of items in a group

## <h2 id="pycebes.core.functions.upper">upper</h2>

```python
upper(column)
```

Converts a string column to upper case

## <h2 id="pycebes.core.functions.covar_pop">covar_pop</h2>

```python
covar_pop(column1, column2)
```

Returns the population covariance for two columns

## <h2 id="pycebes.core.functions.unhex">unhex</h2>

```python
unhex(column)
```

Inverse of hex. Interprets each pair of characters as a hexadecimal number
and converts to the byte representation of number.

## <h2 id="pycebes.core.functions.is_null">is_null</h2>

```python
is_null(column)
```

Return true iff the column is null

## <h2 id="pycebes.core.functions.max">max</h2>

```python
max(column)
```

Returns the kurtosis of the values in a group

## <h2 id="pycebes.core.functions.sha1">sha1</h2>

```python
sha1(column)
```

Calculates the SHA-1 digest of a binary column and returns the value
as a 40 character hex string

## <h2 id="pycebes.core.functions.sort_array">sort_array</h2>

```python
sort_array(column, asc=True)
```

Sorts the input array for the given column in ascending / descending order,
according to the natural ordering of the array elements.

## <h2 id="pycebes.core.functions.last_day">last_day</h2>

```python
last_day(column)
```

Given a date column, returns the last day of the month which the given date belongs to.
For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the
month in July 2015.

## <h2 id="pycebes.core.functions.reverse">reverse</h2>

```python
reverse(column)
```

Reverses the string column and returns it as a new string column

## <h2 id="pycebes.core.functions.instr">instr</h2>

```python
instr(column, substr='')
```

Locate the position of the first occurrence of substr column in the given string.
Returns null if either of the arguments are null.

> The position is not zero based, but 1 based index, returns 0 if substr
>    could not be found in ``column``

## <h2 id="pycebes.core.functions.rtrim">rtrim</h2>

```python
rtrim(column)
```

Trim the spaces from right end for the specified string value

## <h2 id="pycebes.core.functions.signum">signum</h2>

```python
signum(column)
```

Computes the signum of the given value

## <h2 id="pycebes.core.functions.round">round</h2>

```python
round(column, scale=0)
```

Round the value of ``column`` to ``scale`` decimal places if ``scale >= 0``
or at integral part when ``scale < 0``

## <h2 id="pycebes.core.functions.negate">negate</h2>

```python
negate(column)
```

Unary minus, i.e. negate the expression.

__Example__

```python
# Select the amount column and negates all values.
df.select(-df.amount)
df.select(functions.negate(df.amount))
df.select(functions.negate("amount"))
```

## <h2 id="pycebes.core.functions.concat">concat</h2>

```python
concat(*columns)
```

Concatenates multiple input string columns together into a single string column

## <h2 id="pycebes.core.functions.array_contains">array_contains</h2>

```python
array_contains(column, value)
```

Returns true if the array contains ``value``

## <h2 id="pycebes.core.functions.shift_right_unsigned">shift_right_unsigned</h2>

```python
shift_right_unsigned(column, num_bits=1)
```

Unsigned shift the given value ``num_bits`` right. If the given value is a long value, this function
will return a long value else it will return an integer value.

## <h2 id="pycebes.core.functions.current_date">current_date</h2>

```python
current_date()
```

Returns the current date as a date column

## <h2 id="pycebes.core.functions.collect_list">collect_list</h2>

```python
collect_list(column)
```

Returns a list of objects with duplicates

## <h2 id="pycebes.core.functions.trim">trim</h2>

```python
trim(column)
```

Trim the spaces from both ends for the specified string column

## <h2 id="pycebes.core.functions.log10">log10</h2>

```python
log10(column)
```

Computes the logarithm of the given value in base 10

## <h2 id="pycebes.core.functions.initcap">initcap</h2>

```python
initcap(column)
```

Returns a new string column by converting the first letter of each word to uppercase.
Words are delimited by whitespace.
For example, "hello world" will become "Hello World".

## <h2 id="pycebes.core.functions.to_utc_timestamp">to_utc_timestamp</h2>

```python
to_utc_timestamp(column, tz='')
```

Assumes given timestamp is in given timezone and converts to UTC

## <h2 id="pycebes.core.functions.count_distinct">count_distinct</h2>

```python
count_distinct(*args)
```

returns the number of distinct items in a group

## <h2 id="pycebes.core.functions.create_map">create_map</h2>

```python
create_map(*columns)
```

Creates a new map column. The input columns must be grouped as key-value pairs, e.g.
(key1, value1, key2, value2, ...). The key columns must all have the same data type, and can't
be null. The value columns must all have the same data type.

## <h2 id="pycebes.core.functions.substring">substring</h2>

```python
substring(column, pos=0, l=1)
```

Substring starts at ``pos`` and is of length ``l`` when str is String type or
returns the slice of byte array that starts at ``pos`` in byte and is of length ``l``
when ``column`` is Binary type

__Arguments__

- __column__: a ``Column`` object, or a column name
- __pos__: starting position, can be an integer, or a ``Column`` with its expression giving an integer value
- __l__: length of the substring, can be an integer, or a ``Column`` with its expression giving an integer value

## <h2 id="pycebes.core.functions.min">min</h2>

```python
min(column)
```

Returns the kurtosis of the values in a group

## <h2 id="pycebes.core.functions.rpad">rpad</h2>

```python
rpad(column, pad_length=1, pad='')
```

Left-pad the string column

## <h2 id="pycebes.core.functions.from_unixtime">from_unixtime</h2>

```python
from_unixtime(column, fmt='yyyy-MM-dd HH:mm:ss')
```

Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
representing the timestamp of that moment in the current system time zone in the given
format.

## <h2 id="pycebes.core.functions.locate">locate</h2>

```python
locate(substr, column, pos=None)
```

Locate the position of the first occurrence of substr.
If ``pos`` is not None, only search after position ``pos``.

> The position is not zero based, but 1 based index, returns 0 if substr
>    could not be found in str.

## <h2 id="pycebes.core.functions.when">when</h2>

```python
when(condition, value)
```

Evaluates a list of conditions and returns one of multiple possible result expressions.
If otherwise is not defined at the end, null is returned for unmatched conditions.

__Arguments__

- __condition (Column)__: the condition
- __value__: value to take when condition is true

__Example__

```python
# encoding gender string column into integer
people.select(functions.when(people.gender == 'male', 0)
    .when(people.gender == 'female', 1)
    .otherwise(2))
```

## <h2 id="pycebes.core.functions.log1p">log1p</h2>

```python
log1p(column)
```

Computes the natural logarithm of the given value plus one

## <h2 id="pycebes.core.functions.shift_right">shift_right</h2>

```python
shift_right(column, num_bits=1)
```

Shift the given value ``num_bits`` right. If the given value is a long value, this function
will return a long value else it will return an integer value.

## <h2 id="pycebes.core.functions.input_file_name">input_file_name</h2>

```python
input_file_name()
```

Spark-specific: Creates a string column for the file name of the current Spark task

## <h2 id="pycebes.core.functions.expr">expr</h2>

```python
expr(expr_str='')
```

Parses the expression string into the column that it represents

__Example__

```python
# get the number of words of each length
df.group_by(functions.expr("length(word)")).count()
```

## <h2 id="pycebes.core.functions.covar_samp">covar_samp</h2>

```python
covar_samp(column1, column2)
```

Returns the sample covariance for two columns

## <h2 id="pycebes.core.functions.dayofyear">dayofyear</h2>

```python
dayofyear(column)
```

Extracts the day of the year as an integer from a given date/timestamp/string

## <h2 id="pycebes.core.functions.date_format">date_format</h2>

```python
date_format(column, fmt='')
```

Converts a date/timestamp/string to a value of string in the format specified by the date
format given by the second argument.

A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
pattern letters of [[java.text.SimpleDateFormat]] can be used.

> Use when ever possible specialized functions like :func:`year`. These benefit from a
>     specialized implementation.

## <h2 id="pycebes.core.functions.shift_left">shift_left</h2>

```python
shift_left(column, num_bits=1)
```

Shift the given value ``num_bits`` left. If the given value is a long value, this function
will return a long value else it will return an integer value.

## <h2 id="pycebes.core.functions.monotonically_increasing_id">monotonically_increasing_id</h2>

```python
monotonically_increasing_id()
```

Spark-specific: A column expression that generates monotonically increasing 64-bit integers.

The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
The current implementation puts the partition ID in the upper 31 bits, and the record number
within each partition in the lower 33 bits. The assumption is that the data frame has
less than 1 billion partitions, and each partition has less than 8 billion records.

As an example, consider a ``Dataframe`` with two partitions, each with 3 records.
This expression would return the following IDs:

0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.

## <h2 id="pycebes.core.functions.unbase64">unbase64</h2>

```python
unbase64(column)
```

Decodes a BASE64 encoded string column and returns it as a binary column.
This is the reverse of base64.

## <h2 id="pycebes.core.functions.trunc">trunc</h2>

```python
trunc(column, fmt='year')
```

Returns date truncated to the unit specified by the format.

__Arguments__

- __column__: the source column, can be a ``Column`` object or a string (column name)
- __fmt__: 'year', 'yyyy', 'yy' for truncate by year,
        or 'month', 'mon', 'mm' for truncate by month

## <h2 id="pycebes.core.functions.spark_partition_id">spark_partition_id</h2>

```python
spark_partition_id()
```

Spark-specific: Partition ID of the Spark task.

> This is un-deterministic because it depends on data partitioning and task scheduling.

## <h2 id="pycebes.core.functions.split">split</h2>

```python
split(column, pattern='')
```

Splits str around pattern (pattern is a regular expression)

> pattern is a string representation of the regular expression

## <h2 id="pycebes.core.functions.grouping_id">grouping_id</h2>

```python
grouping_id(*columns)
```

returns the level of grouping, equals to

``(grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)``

> the list of columns should match with grouping columns exactly, or empty (means all the
>    grouping columns).

## <h2 id="pycebes.core.functions.size">size</h2>

```python
size(column)
```

Returns length of array or map

## <h2 id="pycebes.core.functions.rint">rint</h2>

```python
rint(column)
```

Returns the double value that is closest in value to the argument and
is equal to a mathematical integer.

## <h2 id="pycebes.core.functions.cos">cos</h2>

```python
cos(column)
```

Computes the cosine of the given value

## <h2 id="pycebes.core.functions.current_timestamp">current_timestamp</h2>

```python
current_timestamp()
```

Returns the current timestamp as a timestamp column

## <h2 id="pycebes.core.functions.approx_count_distinct">approx_count_distinct</h2>

```python
approx_count_distinct(column, rsd=0.05)
```

Returns the approximate number of distinct items in a group.

__Arguments:__

column (Column): the column to compute
rsd: maximum estimation error allowed (default = 0.05)

## <h2 id="pycebes.core.functions.stddev_samp">stddev_samp</h2>

```python
stddev_samp(column)
```

Returns the sample standard deviation of the expression in a group.

## <h2 id="pycebes.core.functions.sha2">sha2</h2>

```python
sha2(column, num_bits=0)
```

Calculates the SHA-2 family of hash functions of a binary column and
returns the value as a hex string

__Arguments__

- __column__: column to compute SHA-2 on. Can be a ``Column`` or a string (a column name)
- __num_bits__: one of 224, 256, 384, or 512

## <h2 id="pycebes.core.functions.bround">bround</h2>

```python
bround(column, scale=0)
```

Round the value of ``column`` to ``scale`` decimal places with HALF_EVEN round mode if ``scale >= 0``
or at integral part when ``scale < 0``

## <h2 id="pycebes.core.functions.hypot">hypot</h2>

```python
hypot(x, y)
```

Computes ``sqrt(x^2 + y^2)`` without intermediate overflow or underflow
See `atan2` to compute the angle.

__Arguments__

- __x__: can be a ``Column``, a string (column name), or a double value
- __y__: can be a ``Column``, a string (column name), or a double value

## <h2 id="pycebes.core.functions.greatest">greatest</h2>

```python
greatest(*columns)
```

Returns the greatest value of the list of values, skipping null values.
This function takes at least 2 parameters. It will return null iff all parameters are null.

## <h2 id="pycebes.core.functions.pmod">pmod</h2>

```python
pmod(dividend, divisor)
```

Returns the positive value of dividend mod divisor.
``dividend`` and ``divisor`` can be ``Column`` or strings (column names)

## <h2 id="pycebes.core.functions.conv">conv</h2>

```python
conv(column, from_base=10, to_base=2)
```

Convert a number in a string column from one base to another

## <h2 id="pycebes.core.functions.hash">hash</h2>

```python
hash(column)
```

Calculates the hash code of given columns, and returns the result as an int column

## <h2 id="pycebes.core.functions.length">length</h2>

```python
length(column)
```

Computes the length of a given string or binary column

## <h2 id="pycebes.core.functions.skewness">skewness</h2>

```python
skewness(column)
```

Returns the kurtosis of the values in a group

## <h2 id="pycebes.core.functions.decode">decode</h2>

```python
decode(column, charset='US-ASCII')
```

Computes the first argument into a string from a binary using the provided character set
(one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').

If either argument is null, the result will also be null.

## <h2 id="pycebes.core.functions.array">array</h2>

```python
array(*columns)
```

Creates a new array column. The input columns must all have the same data type.

## <h2 id="pycebes.core.functions.floor">floor</h2>

```python
floor(column)
```

Computes the floor of the given value

## <h2 id="pycebes.core.functions.datediff">datediff</h2>

```python
datediff(end_column, start_column)
```

Returns the number of days from `start` to `end`

## <h2 id="pycebes.core.functions.acos">acos</h2>

```python
acos(column)
```

Computes the cosine inverse of the given value; the returned angle is in the range 0.0 through pi.

## <h2 id="pycebes.core.functions.mean">avg</h2>

```python
avg(column)
```

Returns the average of the values in a group

## <h2 id="pycebes.core.functions.from_utc_timestamp">from_utc_timestamp</h2>

```python
from_utc_timestamp(column, tz='')
```

Assumes given timestamp is UTC and converts to given timezone

## <h2 id="pycebes.core.functions.factorial">factorial</h2>

```python
factorial(column)
```

Computes the factorial of the given value

## <h2 id="pycebes.core.functions.dayofmonth">dayofmonth</h2>

```python
dayofmonth(column)
```

Extracts the day of the month as an integer from a given date/timestamp/string

## <h2 id="pycebes.core.functions.avg">avg</h2>

```python
avg(column)
```

Returns the average of the values in a group

## <h2 id="pycebes.core.functions.randn">randn</h2>

```python
randn(seed=None)
```

Generate a column with i.i.d. samples from the standard normal distribution.
``seed`` will be automatically randomly generated by ``random.randint(0, 1000)`` if not specified.

> This is un-deterministic when data partitions are not fixed.

## <h2 id="pycebes.core.functions.soundex">soundex</h2>

```python
soundex(column)
```

Return the soundex code for the specified expression

## <h2 id="pycebes.core.functions.to_date">to_date</h2>

```python
to_date(column)
```

Converts the column into DateType

## <h2 id="pycebes.core.functions.bitwise_not">bitwise_not</h2>

```python
bitwise_not(column)
```

Computes bitwise NOT.

## <h2 id="pycebes.core.functions.stddev_pop">stddev_pop</h2>

```python
stddev_pop(column)
```

Returns the population standard deviation of the expression in a group.

## <h2 id="pycebes.core.functions.concat_ws">concat_ws</h2>

```python
concat_ws(sep=' ', *columns)
```

Concatenates multiple input string columns together into a single string column,
using the given separator

## <h2 id="pycebes.core.functions.abs">abs</h2>

```python
abs(column)
```

Computes the absolute value

## <h2 id="pycebes.core.functions.to_radians">to_radians</h2>

```python
to_radians(column)
```

Converts an angle measured in degrees to an approximately equivalent angle measured in radians

## <h2 id="pycebes.core.functions.pow">pow</h2>

```python
pow(left, right)
```

Returns the value of the first argument raised to the power of the second argument

__Arguments__

- __left__: can be a ``Column``, a string (column name), or a double value
- __right__: can be a ``Column``, a string (column name), or a double value

## <h2 id="pycebes.core.functions.sqrt">sqrt</h2>

```python
sqrt(column)
```

Computes the square root of the specified float value.

## <h2 id="pycebes.core.functions.regexp_replace">regexp_replace</h2>

```python
regexp_replace(column, pattern='', replacement='')
```

Replace all substrings of the specified string value that match regexp with rep.

## <h2 id="pycebes.core.functions.date_sub">date_sub</h2>

```python
date_sub(column, days=1)
```

Returns the date that is `days` days before the date in ``column``

## <h2 id="pycebes.core.functions.lower">lower</h2>

```python
lower(column)
```

Converts a string column to lower case

## <h2 id="pycebes.core.functions.levenshtein">levenshtein</h2>

```python
levenshtein(left, right)
```

Computes the Levenshtein distance of the two given string columns

## <h2 id="pycebes.core.functions.variance">var_samp</h2>

```python
var_samp(column)
```

Returns the unbiased variance of the values in a group

See `var_pop` for computing the population variance.

## <h2 id="pycebes.core.functions.hex">hex</h2>

```python
hex(column)
```

Computes hex value of the given column

## <h2 id="pycebes.core.functions.sinh">sinh</h2>

```python
sinh(column)
```

Computes the hyperbolic sine of the given value

## <h2 id="pycebes.core.functions.to_degrees">to_degrees</h2>

```python
to_degrees(column)
```

Converts an angle measured in radians to an approximately equivalent angle measured in degrees

## <h2 id="pycebes.core.functions.grouping">grouping</h2>

```python
grouping(column)
```

indicates whether a specified column in a GROUP BY list is aggregated
or not, returns 1 for aggregated or 0 for not aggregated in the result set.

## <h2 id="pycebes.core.functions.stddev">stddev_samp</h2>

```python
stddev_samp(column)
```

Returns the sample standard deviation of the expression in a group.

## <h2 id="pycebes.core.functions.translate">translate</h2>

```python
translate(column, matching_str='', replace_str='')
```

Translate any character in the ``column`` by a character in ``replace_str``.
The characters in ``replace_str`` correspond to the characters in ``matching_str``.
The translate will happen when any character in the string matches the character
in the ``matching_str``.

## <h2 id="pycebes.core.functions.format_number">format_number</h2>

```python
format_number(column, d=2)
```

Formats numeric column x to a format like '#,###,##``.##', rounded to d decimal places,
and returns the result as a string column.

    * If d is 0, the result has no decimal point or fractional part.
    * If d < 0, the result will be null.

## <h2 id="pycebes.core.functions.lpad">lpad</h2>

```python
lpad(column, pad_length=1, pad='')
```

Left-pad the string column

## <h2 id="pycebes.core.functions.sin">sin</h2>

```python
sin(column)
```

Computes the sine of the given value

## <h2 id="pycebes.core.functions.coalesce">coalesce</h2>

```python
coalesce(*columns)
```

Returns the first column that is not null, or null if all inputs are null.

For example, ``coalesce(a, b, c)`` will return a if a is not null,
or b if a is null and b is not null, or c if both a and b are null but c is not null.

## <h2 id="pycebes.core.functions.least">least</h2>

```python
least(*columns)
```

Returns the least value of the list of values, skipping null values.
This function takes at least 2 parameters. It will return null iff all parameters are null.

## <h2 id="pycebes.core.functions.asin">asin</h2>

```python
asin(column)
```

Computes the sine inverse of the given value; the returned angle is in the range -pi/2 through pi/2.

## <h2 id="pycebes.core.functions.minute">minute</h2>

```python
minute(column)
```

Extracts the minutes as an integer from a given date/timestamp/string

## <h2 id="pycebes.core.functions.quarter">quarter</h2>

```python
quarter(column)
```

Extracts the quarter as an integer from a given date/timestamp/string

## <h2 id="pycebes.core.functions.tan">tan</h2>

```python
tan(column)
```

Computes the tangent of the given value

## <h2 id="pycebes.core.functions.last">last</h2>

```python
last(column, ignore_nulls=False)
```

Returns the last value in a group.

The function by default returns the last values it sees. It will return the last non-null
value it sees when ``ignore_nulls`` is set to true. If all values are null, then null is returned.

## <h2 id="pycebes.core.functions.window">window</h2>

```python
window(column, window_duration='', slide_duration=None, start_time='0 second')
```

Bucketize rows into one or more time windows given a timestamp specifying column. Window
starts are inclusive but the window ends are exclusive, e.g. ``12:05`` will be in the window
``[12:05,12:10)`` but not in ``[12:00,12:05)``.

Windows can support microsecond precision. Windows in the order of months are not supported.

The following example takes the average stock price for a one minute window every 10 seconds
starting 5 seconds after the hour:

__Example__

```python
df = ...  # schema: timestamp: TimestampType, stockId: StringType, price: DoubleType
df.group_by(functions.window(df.timestamp, "1 minute", "10 seconds", "5 seconds"), df.stockId)
    .agg(mean("price"))
```

The windows will look like:

```python
    09:00:05-09:01:05
    09:00:15-09:01:15
    09:00:25-09:01:25 ...
```

For a streaming query, you may use the function ``current_timestamp`` to generate windows on
processing time.

__Arguments__

- __column__: The column or the expression to use as the timestamp for windowing by time.
               The time column must be of TimestampType.
- __window_duration__: A string specifying the width of the window, e.g. `10 minutes`,
                `1 second`. Check `CalendarIntervalType` for
                valid duration identifiers. Note that the duration is a fixed length of
                time, and does not vary over time according to a calendar. For example,
                `1 day` always means `86,400,000 milliseconds`, not a calendar day.
- __slide_duration__: A string specifying the sliding interval of the window, e.g. `1 minute`.
                A new window will be generated every `slide_duration`. Must be less than
                or equal to the `window_duration`.
                This duration is likewise absolute, and does not vary according to a calendar.
                If unspecified, `slide_duration` will be equal to `window_duration`
- __start_time (str): The offset with respect to `1970-01-01 00:00__:00 UTC` with which to start
                    window intervals. For example, in order to have hourly tumbling windows
- __that start 15 minutes past the hour, e.g. `12:15 - 13:15`, `13:15 - 14__:15`...
                    provide `start_time` as `15 minutes`.

## <h2 id="pycebes.core.functions.is_nan">is_nan</h2>

```python
is_nan(column)
```

Return true iff the column is NaN

## <h2 id="pycebes.core.functions.month">month</h2>

```python
month(column)
```

Extracts the month as an integer from a given date/timestamp/string

## <h2 id="pycebes.core.functions.encode">encode</h2>

```python
encode(column, charset='US-ASCII')
```

Computes the first argument into a binary from a string using the provided character set
(one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').

If either argument is null, the result will also be null.

## <h2 id="pycebes.core.functions.collect_set">collect_set</h2>

```python
collect_set(column)
```

Returns a set of objects with duplicate elements eliminated

## <h2 id="pycebes.core.functions.log2">log2</h2>

```python
log2(column)
```

Computes the logarithm of the given column in base 2

## <h2 id="pycebes.core.functions.atan2">atan2</h2>

```python
atan2(x, y)
```

Returns the angle theta from the conversion of rectangular coordinates (x, y) to polar coordinates (r, theta)

__Arguments__

- __x__: the x-component, can be a ``Column``, a string (column name), or a float value
- __y__: the y-component, can be a ``Column``, a string (column name), or a float value

## <h2 id="pycebes.core.functions.next_day">next_day</h2>

```python
next_day(column, day_of_week='mon')
```

Given a date column, returns the first date which is later than the value of the date column
that is on the specified day of the week.

For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
Sunday after 2015-07-27.

Day of the week parameter is case insensitive, and accepts:

    "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".

## <h2 id="pycebes.core.functions.atan">atan</h2>

```python
atan(column)
```

Computes the tangent inverse of the given value

## <h2 id="pycebes.core.functions.col">col</h2>

```python
col(col_name='column')
```

Returns a ``Column`` based on the given column name.

## <h2 id="pycebes.core.functions.months_between">months_between</h2>

```python
months_between(column1, column2)
```

Returns number of months between dates `column1` and `column2`

## <h2 id="pycebes.core.functions.cosh">cosh</h2>

```python
cosh(column)
```

Computes the hyperbolic cosine of the given value

## <h2 id="pycebes.core.functions.add_months">add_months</h2>

```python
add_months(column, num_months=1)
```

Returns the date that is ``num_months`` after the date in ``column``

## <h2 id="pycebes.core.functions.ceil">ceil</h2>

```python
ceil(column)
```

Computes the ceiling of the given value.

## <h2 id="pycebes.core.functions.second">second</h2>

```python
second(column)
```

Extracts the seconds as an integer from a given date/timestamp/string

## <h2 id="pycebes.core.functions.var_pop">var_pop</h2>

```python
var_pop(column)
```

Returns the population variance of the values in a group

See `var_samp` for computing the unbiased variance.

## <h2 id="pycebes.core.functions.corr">corr</h2>

```python
corr(column1, column2)
```

Returns the Pearson Correlation Coefficient for two columns

## <h2 id="pycebes.core.functions.exp">exp</h2>

```python
exp(column)
```

Computes the exponential of the given value

## <h2 id="pycebes.core.functions.year">year</h2>

```python
year(column)
```

Extracts the year as an integer from a given date/timestamp/string

## <h2 id="pycebes.core.functions.rand">rand</h2>

```python
rand(seed=None)
```

Generate a random column with i.i.d. samples from U[0.0, 1.0].
``seed`` will be automatically randomly generated by ``random.randint(0, 1000)`` if not specified.

> This is un-deterministic when data partitions are not fixed.

## <h2 id="pycebes.core.functions.kurtosis">kurtosis</h2>

```python
kurtosis(column)
```

Returns the kurtosis of the values in a group

# <h1 id="pycebes.core.column.Column">Column</h1>

```python
Column(self, expr)
```

## <h2 id="pycebes.core.column.Column.otherwise">otherwise</h2>

```python
Column.otherwise(self, value)
```

Evaluates a list of conditions and returns one of multiple possible result expressions.
If otherwise() is not defined at the end, `null` is returned for unmatched conditions.

__Example__

```python
# encoding gender string column into integer.
people.select(functions.when(people.gender == "male", 0)
    .when(people.gender == "female", 1)
    .otherwise(2))
```

## <h2 id="pycebes.core.column.Column.alias">alias</h2>

```python
Column.alias(self, *alias)
```

Gives the column an alias, or multiple aliases in case it is used on an expression
that returns more than one column (such as ``explode``).

__Example__

```python
# Renames colA to colB in select output.
df.select(df.colA.alias('colB'))

# multiple alias
df.select(functions.explode(df.myMap).alias('key', 'value'))
```

## <h2 id="pycebes.core.column.Column.equal_null_safe">equal_null_safe</h2>

```python
Column.equal_null_safe(self, other)
```

Equality test that is safe for null values.
Returns same result with EQUAL(=) operator for non-null operands,
but returns ``true`` if both are ``NULL``, ``false`` if one of the them is ``NULL``.

## <h2 id="pycebes.core.column.Column.contains">contains</h2>

```python
Column.contains(self, other)
```

Contains the other element

## <h2 id="pycebes.core.column.Column.cast">cast</h2>

```python
Column.cast(self, to)
```

Casts the column to a different data type.

__Arguments__

- __to (StorageType)__: the StorageType to cast to

__Example__

```python
# Casts colA to [[IntegerType]].
df.select(df.colA.cast(IntegerType))
df.select(df.colA.cast("int"))
```

## <h2 id="pycebes.core.column.Column.desc">desc</h2>


Returns an ordering used in sorting.

__Example__

```python
df.sort(df.col1.desc)
```

## <h2 id="pycebes.core.column.Column.to_json">to_json</h2>

```python
Column.to_json(self)
```

Serialize the Column into JSON format

__Returns__

 JSON representation of this Column

## <h2 id="pycebes.core.column.Column.name">name</h2>

```python
Column.name(self, *alias)
```

Gives the column a name. Same as :func:`alias`

## <h2 id="pycebes.core.column.Column.asc">asc</h2>


Returns an ordering used in sorting.

__Example__

```python
df.sort(df.col1.asc)
```

## <h2 id="pycebes.core.column.Column.bitwise_not">bitwise_not</h2>


Compute bitwise NOT of this expression

__Example__

```python
df.select(df.colA.bitwise_not)
```

## <h2 id="pycebes.core.column.Column.substr">substr</h2>

```python
Column.substr(self, start_pos, length)
```

An expression that returns a substring.

This is not zero-based, but 1-based index. The first character in str has index 1.

`start_pos` and `length` are handled specially:

  - ``"Content".substr(1, 3)`` gives ``"Con"``
  - ``"Content".substr(-100, 2)`` gives ``""``
  - ``"Content".substr(-100, 102)`` gives ``"Content"``
  - ``"Content".substr(2, 100)`` gives ``"ontent"``

__Arguments__

- __start_pos__: starting position, can be an integer, or a ``Column`` that gives an integer
- __length__: length of the sub-string, can be an integer, or a ``Column`` that gives an integer

## <h2 id="pycebes.core.column.Column.starts_with">starts_with</h2>

```python
Column.starts_with(self, other)
```

String starts with.

__Arguments__

- __other__: string to test, can be a string, or a ``Column``  that gives a string

## <h2 id="pycebes.core.column.Column.isin">isin</h2>

```python
Column.isin(self, values)
```

A boolean expression that is evaluated to true if the value of this expression is contained
by the evaluated values of the arguments.

## <h2 id="pycebes.core.column.Column.is_null">is_null</h2>

```python
Column.is_null(self)
```

True if the current expression is null.

## <h2 id="pycebes.core.column.Column.between">between</h2>

```python
Column.between(self, lower_bound, upper_bound)
```

True if the current column is between the lower bound and upper bound, inclusive.

## <h2 id="pycebes.core.column.Column.bitwise_or">bitwise_or</h2>

```python
Column.bitwise_or(self, other)
```

Compute bitwise OR of this expression with another expression.

__Example__

```python
    df.select(df.colA.bitwise_or(df.colB))
```

## <h2 id="pycebes.core.column.Column.get_field">get_field</h2>

```python
Column.get_field(self, field_name='key')
```

An expression that gets a field by name in a StructType column.

## <h2 id="pycebes.core.column.Column.get_item">get_item</h2>

```python
Column.get_item(self, key)
```

An expression that gets an item at position `ordinal` out of an array,
or gets a value by key `key` in a MapType column.

## <h2 id="pycebes.core.column.Column.is_nan">is_nan</h2>

```python
Column.is_nan(self)
```

True if the current expression is NaN.

## <h2 id="pycebes.core.column.Column.rlike">rlike</h2>

```python
Column.rlike(self, literal)
```

SQL RLIKE expression (LIKE with Regex). Result will be a BooleanType column

__Arguments__

- __literal__: a string

## <h2 id="pycebes.core.column.Column.like">like</h2>

```python
Column.like(self, literal)
```

SQL like expression. Result will be a BooleanType column

__Arguments__

- __literal__: a string

## <h2 id="pycebes.core.column.Column.bitwise_xor">bitwise_xor</h2>

```python
Column.bitwise_xor(self, other)
```

Compute bitwise XOR of this expression with another expression.

__Example__

```python
df.select(df.colA.bitwise_xor(df.colB))
```

## <h2 id="pycebes.core.column.Column.when">when</h2>

```python
Column.when(self, condition, value)
```

Evaluates a list of conditions and returns one of multiple possible result expressions.
If otherwise() is not defined at the end, `null` is returned for unmatched conditions.

__Arguments__

- __condition__: the condition to be checked
- __value (Column)__: the output value if condition is true

__Example__

```python
# encoding gender string column into integer.
people.select(functions.when(people.gender == "male", 0)
    .when(people.gender == "female", 1)
    .otherwise(2))
```

## <h2 id="pycebes.core.column.Column.ends_with">ends_with</h2>

```python
Column.ends_with(self, other)
```

String ends with.

__Arguments__

- __other__: string to test, can be a string, or a ``Column``  that gives a string

## <h2 id="pycebes.core.column.Column.bitwise_and">bitwise_and</h2>

```python
Column.bitwise_and(self, other)
```

Compute bitwise AND of this expression with another expression.

__Example__

```python
df.select(df.colA.bitwise_and(df.colB))
```

## <h2 id="pycebes.core.column.Column.is_not_null">is_not_null</h2>

```python
Column.is_not_null(self)
```

True if the current expression is not null.

