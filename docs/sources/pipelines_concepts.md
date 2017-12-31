Cebes Pipelines is an opinionated way to construct, train and deploy Machine Learning models.
Cebes Pipelines generalizes and unifies the APIs in a coherent design, which can then be extended
to support other compute engines than Spark.

## Definitions

- **Pipeline** is a directed acyclic graph whose edges carry and transfer data, and whose vertices 
are _stages_ that do specific, pre-defined tasks. 
- **Stage** is a single processing unit implemented in Cebes. There are different types of stages 
doing different type of tasks. A stage has multiple input and output _slots_. Once executed, a stage
computes the value of its output slots from the values received in its input slots.
- **Slot** is a mechanism for stages to receive inputs and communicate to each other. 
There are input slots and output slots. All slots are strongly-typed and only 
transfer data of the correct type.
    - Slots can be _stateful_ or _stateless_, which controls how their values are re-computed once 
    the input changes.
    - Values of all output slots are cached and not recomputed as long as input slots do not change.
    When the input changes, they are recomputed depending on whether they are stateful or stateless:
        - Stateless output slots are recomputed anytime any input slot changes
        - Stateful output slots are only recomputed when stateful input slots change, or explicitly
        required by the user.
- **Estimator** is a special kind of stage for training Machine Learning models. Estimators often 
has:
    - 1 _stateless_ input slot for the dataset, named `inputDf`
    - 0 to many _stateful_ input slots for the parameters, _e.g._ `learningRate`, `regularization`, etc...
    - 1 _stateful_ output slot for the trained `Model`
    - 1 _stateless_ output slot for the transformed dataset, named `outputDf`
    - 0 to many _stateful_ output slots for other auxiliary information produced during training
    
- **Placeholder** is a special kind of stage that hold a piece of data that can be changed dynamically,
especially when pipeline is executed.

**Model** is the trained Machine Learning model, and often stay in a Pipeline.

To use pipelines, users first construct it by specifying stages and how they connect to each other.
They then execute the pipeline with `Pipeline.run()`. At runtime, they can choose which output slots
whose value will be retrieved, and overwrite the value of any input slot.

## Example

Here is a script to train a Linear Regression model on a sample dataset:

```python
>>> df = cb.get_default_session().load_test_datasets()['cylinder_bands']
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
...:     assembler = cb.vector_assembler(inp, ['viscosity', 'proof_cut'], 'features')
...:     lr = cb.linear_regression(assembler.output_df,features_col='features',
...:                               label_col='caliper', prediction_col='caliper_predict', reg_param=0.0)
...:                                      

>>> predicted_df, lr_model, assembled_df = ppl.run([lr.output_df, lr.model, assembler.output_df], feeds={inp: df2})

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

We construct the Pipeline in the `with cb.Pipeline()` block, which includes a Dataframe placeholder, a vector
assember and a linear regression stage. We then execute the pipeline (i.e. train the model and run inference)
using `ppl.run()`. Note how we can retrived the final Dataframe, the model, and the result of VectorAssembler,
in addition to feeding the placeholder, in one call.

The resulting model can be use to transform _arbitrary_ input Dataframe, as long as it has the correct schema:

```python
>>> lr_model.transform(assembled_df).show()
    ID: c1aaddeb-3646-4934-8f75-029a20fe542c
    Shape: (466, 5)
    Sample 5 rows:
       proof_cut  viscosity  caliper      features  caliper_predict
    0       55.0         46    0.200  [46.0, 55.0]         0.273678
    1       55.0         46    0.300  [46.0, 55.0]         0.273678
    2       62.0         40    0.433  [40.0, 62.0]         0.266757
    3       52.0         40    0.300  [40.0, 52.0]         0.261072
    4       50.0         46    0.300  [46.0, 50.0]         0.270836

>>> lr_model.transform(df2).show()
    ServerException: ('Field "features" does not exist.', ...)
```

`df2` is not valid because it has not been vectorized by VectorAssembler, and therefore does not have the 
`features` column. This example shows why it is often more useful to use a whole pipeline, instead of 
the individual models.

Let's tag the pipeline so we can reuse it later:

```python
>>> sess = cb.get_default_session()
>>> sess.pipeline.tag(ppl, 'linear_regression')
>>> sess.pipeline.list()
    UUID                                  Tag                        Created                       # of stages
    ------------------------------------  -------------------------  --------------------------  -------------
    7d364b33-cc0b-4c96-9ecb-e2e48fa8d94f  linear_regression:default  2017-12-31 21:13:49.207000              3
```

The model can also be tagged. See [manage pipelines and models](session_df.md) for more information.

Cebes provides a rich set of stages, including [ETL stages](pipelines_etl.md) and [ML stages](pipelines_ml.md).

Once constructed, pipelines can be kept in Cebes server to run on your in-house datasets. They can also
be [published to a pipeline repository](pipelines_repo.md) so that they are available for 
[online serving](serving.md).
