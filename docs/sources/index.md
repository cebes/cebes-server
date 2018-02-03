## Meet Cebes - your new Data Science buddy

Cebes' mission is to _simplify_ Data Science, improve Data Scientists' productivity, 
help them to focus on modeling and understanding, instead of the dreary, trivial work.

It does so by:

- Simplifying interaction with Apache Spark for [data processing](session.md)  
    Cebes provides a client with _pandas_-like APIs for working on big [Dataframes](dataframe_concepts.md), 
    so you don't need to connect directly to the Spark cluster to run Spark jobs.

- Providing unified APIs for processing data and training models in [pipelines](pipelines_concepts.md)  
    ETL stages and Machine Learning algorithms can be connected to form flexible 
    and powerful data processing pipelines. Hyper-parameters can be tuned automatically.
    
- Simplifying [deployment and life-cycle management](serving.md) of pipelines  
    Pipelines can be exported and published to a _pipeline repository_. They can be taken
    up from there to be used in serving.  
    Cebes serves _pipelines_, not _models_, hence all your ETL logic can be carried 
    over to inference time. The serving component is written using modern technology 
    and can be easily customized to fit your setup.  
    Good news is you don't need to do it all by yourself. Cebes can do that in a few 
    lines of code!
    
- Bringing _Spark_ closer to popular Machine Learning libraries like _tensorflow_, 
_keras_, _scikit-learn_, ...  
    Although still being work-in-progress, we plan to support popular Python Machine 
    Learning libraries. Your model written in Python will be able to consume data 
    processed by _Spark_. All you need to do is to construct an appropriate Pipeline, 
    Cebes will handle data transfers and other boring work automatically!

If that sounds exciting to you, let's check it out by a few examples!

---

## Cebes at a glance

Once [installed](installation.md), Cebes can be used as any other Python library.
You first create a Cebes Session to [connect to a Cebes Server](installation.md#connect-to-cebes-server),
then load the sample _Cylinder bands_ dataset like so:

```python
>>> import pycebes as cb

>>> s = cb.Session()
    [INFO] pycebes.internal.docker_helpers: Starting Cebes container cebes-server-x.x.x-0[phvu/cebes:x.x.x] 
        with data path at $HOME/.cebes/x.x.x
    [INFO] pycebes.internal.docker_helpers: Cebes container started, listening at localhost:32768
    [INFO] pycebes.core.session: Connecting to Cebes container cebes-server-x.x.x-0[phvu/cebes:x.x.x] at port 32768
    [INFO] pycebes.core.session: Spark UI can be accessed at http://localhost:32769

>>> df = s.load_test_datasets()['cylinder_bands']
```

The schema of the Dataframe, including [data types](dataframe_concepts.md#cebes-types) of each column, can
be accessed with `df.schema`, a summary of the data can be shown with `df.show()`, and the size of the dataset
is stored in `df.shape`:

```python
>>> df.schema
    Schema(fields=[SchemaField(name='timestamp',storage_type=LONG,variable_type=DISCRETE),
                   SchemaField(name='cylinder_number',storage_type=STRING,variable_type=TEXT),
                   SchemaField(name='customer',storage_type=STRING,variable_type=TEXT),
                   SchemaField(name='job_number',storage_type=INTEGER,variable_type=DISCRETE),
                   ...,
                   SchemaField(name='band_type',storage_type=STRING,variable_type=TEXT)])

>>> df.show()
    ID: 6744406a-239a-4a4a-a8c7-9a422bf4b477
    Shape: (540, 40)
    Sample 5 rows:
       timestamp cylinder_number customer  job_number grain_screened ink_color  \
    0   19910108            X126  TVGUIDE       25503            YES       KEY   
    1   19910109            X266  TVGUIDE       25503            YES       KEY   
    2   19910104              B7   MODMAT       47201            YES       KEY   
    3   19910104            T133   MASSEY       39039            YES       KEY   
    4   19910111             J34    KMART       37351             NO       KEY   
    
      proof_on_ctd_ink blade_mfg cylinder_division paper_type    ...      \
    0              YES    BENTON          GALLATIN   UNCOATED    ...       
    1              YES    BENTON          GALLATIN   UNCOATED    ...       
    2              YES    BENTON          GALLATIN   UNCOATED    ...       
    3              YES    BENTON          GALLATIN   UNCOATED    ...       
    4              YES    BENTON          GALLATIN   UNCOATED    ...       
    
      solvent_pct esa_voltage esa_amperage  wax hardener  roller_durometer  \
    0   36.400002         0.0          0.0  2.5      1.0                34   
    1   38.500000         0.0          0.0  2.5      0.7                34   
    2   39.799999         0.0          0.0  2.8      0.9                40   
    3   38.799999         0.0          0.0  2.5      1.3                40   
    4   42.500000         5.0          0.0  2.3      0.6                35   
    
       current_density anode_space_ratio chrome_content  band_type  
    0               40        105.000000          100.0       band  
    1               40        105.000000          100.0     noband  
    2               40        103.870003          100.0     noband  
    3               40        108.059998          100.0     noband  
    4               40        106.669998          100.0     noband  
    
    [5 rows x 40 columns]

>>> df.shape
    (540, 40)
```

This is a dataset of 540 rows and 40 columns about cylinder bands.

We can already perform some non-trivial [SQL query](dataframe_guide.md) on this dataset.
For example, we can count the number of transactions recorded every week:

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

See [this section](dataframe_guide.md) for more information on Cebes Dataframe APIs,
_e.g._ how to [load data into Cebes](session_load_data.md), 
[tag Dataframes](session_df.md) so that you can reuse them, ...

Query and preprocess data is important, but **Machine Learning** is also a focus of Cebes.
Machine Learning is done using [Pipelines](pipelines_concepts.md). For example, a simple 
Pipeline for training a Linear Regression can be done as follows:

```python
>>> df2 = df.drop(*(set(df.columns) - {'viscosity', 'proof_cut', 'caliper'}))
>>> df2 = df2.dropna(columns=['viscosity', 'proof_cut', 'caliper'])

>>> df2.show()
    ID: 25a139f8-4a27-442f-8a95-d8b0cd528c75
    Shape: (466, 3)
    Sample 5 rows:
       proof_cut  viscosity  caliper
    0       55.0         46    0.200
    1       55.0         46    0.300
    2       62.0         40    0.433
    3       52.0         40    0.300
    4       50.0         46    0.300

>>> with cb.Pipeline() as ppl:
...:     inp = cb.placeholder(cb.PlaceholderTypes.DATAFRAME)
...:     assembler = cb.vector_assembler(inp, input_cols=['viscosity', 'proof_cut'], 
...:                                     output_col='features')
...:     lr = cb.linear_regression(assembler.output_df, 
...:                               features_col='features', label_col='caliper',
...:                               prediction_col='caliper_predict', 
...:                               reg_param=0.0)
...:                                      

>>> predicted_df, lr_model, assembled_df = ppl.run(
...:    [lr.output_df, lr.model, assembler.output_df], feeds={inp: df2})

>>> predicted_df.show()
    ID: 294ab9a2-0b84-4cf9-97b4-de4cd4be9901
    Shape: (466, 5)
    Sample 5 rows:
       proof_cut  viscosity  caliper      features  caliper_predict
    0       55.0         46    0.200  [46.0, 55.0]         0.273678
    1       55.0         46    0.300  [46.0, 55.0]         0.273678
    2       62.0         40    0.433  [40.0, 62.0]         0.266757
    3       52.0         40    0.300  [40.0, 52.0]         0.261072
    4       50.0         46    0.300  [46.0, 50.0]         0.270836
```

Pipelines can then be stored and tagged in the same way as Dataframes. They can also be
published to [Pipeline repositories](pipelines_repo.md) and [served](serving.md) for online 
prediction.

---

## Getting support

Create an issue on github if you find bugs or have a feature request. Join us on
[gitter](https://gitter.im/cebes-io/Lobby) to interact with the team. Alternatively, if you 
prefer the good old way, drop a message to our [mailing list](https://groups.google.com/forum/#!forum/cebes-io).

---

## Why this name, Cebes?

[**Cebes**](https://en.wikipedia.org/wiki/Cebes) (_c._ 430 – 350 BC) was an Ancient 
Greek philosopher from Thebes, remembered as a disciple of Socrates. He is one of 
the speakers in the _Phaedo_ of Plato, in which he is represented as an earnest seeker 
after virtue and truth, keen in argument and cautious in decision. 

*Cebes* is with you on your journey making faster and better data-driven decisions! 
