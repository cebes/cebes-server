Built on top of Spark, Cebes provides a rich set of APIs for loading, cleaning, transforming data at scale.

Assume that you already [loaded data into Cebes](session.md#loading-data-into-cebes). This document describes
some basic concepts in Cebes, as well as functions for doing analytics on Dataframes.

## Storage types, Variable types and Data Schema

A Cebes Dataframe can be thought of as a big data table, or a collection of columns, 
where each column has a name and a _data type_. Elements in the same column have the same data type.

However, data type is a super confusing concept, for both human and machine. Cebes, therefore, makes 
distinction between _Storage types_ and _Variable types_:

- **Storage type** describes how the data is stored under the hood. This ranges from atomic types 
 like `BooleanType`, `ByteType`, `LongType` to complicated types like `Array[T]`, `Map[K, V]`, 
 `Vector`, `Struct`, ...
- **Variable type** describes the role of the variable in analysis. This is closer to the semantic
 that human can assign to variables. For example, "age" is a `Discrete` variable, but can be stored
 as either `IntegerType` or `LongType`.
 
The following table shows variable types supported in Cebes, and their corresponding Storage type:

| Variable types 	| Corresponding storage types                                                                                         	| Description                                                     	|
|----------------	|---------------------------------------------------------------------------------------------------------------------	|-----------------------------------------------------------------	|
| Discrete       	| ByteType, ShortType, IntegerType, LongType                                                                          	| Discrete numerical variables, e.g. Age, Year                    	|
| Continuous     	| FloatType, DoubleType                                                                                               	| Continuous numerical variables, e.g. Weight, Height             	|
| Nomial         	| StringType, BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType 	| Categorical variables without ranks, e.g. Gender, Job           	|
| Ordinal        	| StringType, BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType 	| Categorical variables with a rank/order, e.g. Educational level 	|
| Text           	| StringType                                                                                                          	| Long pieces of text, e.g. Biography                             	|
| DateTime       	| DateType, TimestampType, CalendarIntervalType                                                                       	|                                                                 	|
| Array          	| BinaryType, VectorType, Array[_]Type                                                                                	|                                                                 	|
| Map            	| Map[_]Type                                                                                                          	|                                                                 	|
| Struct         	| Struct[_]Type                                                                                                       	|                                                                 	|

Those are all the types in the Cebes' type system. Most of the time, when you use `pycebes`, it 
will try its best to convert those types into corresponding Python types.

Variable types give Cebes hints about how to handle the variables when using them to train 
Machine Learning models, while Storage types instruct Cebes how to store the data and do type
conversions when needed. 

Finally, each Dataframe has a **Schema**, which is a list of schema definition of the columns
in the Dataframe. For each column, there is a name, a storage type and a variable type. The 
schema can be accessed by the `schema` field on the `Dataframe` object:

```python
>>> df.schema
    Schema(fields=[SchemaField(name='timestamp',storage_type=LONG,variable_type=DISCRETE),
                   SchemaField(name='cylinder_number',storage_type=STRING,variable_type=TEXT),
                   SchemaField(name='customer',storage_type=STRING,variable_type=TEXT),
                   SchemaField(name='job_number',storage_type=INTEGER,variable_type=DISCRETE),
                   ...,
                   SchemaField(name='band_type',storage_type=STRING,variable_type=TEXT)])
```

---

### Working with types

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
>>> df2 = df.with_column('job_number', df.job_number.cast(cb.StorageTypes.LONG))

>>> df2.schema['job_number']
    SchemaField(name='job_number',storage_type=LONG,variable_type=DISCRETE)
    
>>> df.schema['job_number']
    SchemaField(name='job_number',storage_type=INTEGER,variable_type=DISCRETE)
```

Here we used `with_column` and `cast` to convert the `job_number` column from INTEGER to LONG.
Note that `df2` is a different Dataframe from `df`. In fact they have different IDs.

This might look a bit unnatural for now, but it will all make sense when we learn about SQL 
operations in Cebes in the next sections. We could have added a few syntactic sugar to make 
this easier, but that is left for future work.

**Variable type casting**: 


---

## Dataframe tags and ID

Each Dataframe in Cebes is assigned a unique identifier. Most operations on Dataframe will 
give a new Dataframe object with a new ID. The number of Dataframes, therefore, can grow 
quite fast during the course of your data exploration.

By default, Cebes keeps all Dataframes in memory, and evict the ones that are old or have 
not been used for a while. Evicted Dataframes are lost forever, unless you choose to persist 
them, by assigning them a tag. Tagged Dataframes will be persisted and can be retrieved later
using either the tag or the Dataframe ID.

See [this section](session.md#managing-dataframes) for information on how to work with Dataframe tags.

