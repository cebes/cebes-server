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

## Where to from here?

Check [this page](pipelines_api.md) for an sample Pipelines and learn how to use Pipeline APIs.
Cebes provides a rich set of stages, including [ETL stages](pipelines_etl.md) and [ML stages](pipelines_ml.md).

Once constructed, pipelines can be kept in Cebes server to run on your in-house datasets. They can also
be [published to a pipeline repository](pipelines_repo.md) so that they are available for 
[online serving](serving.md).
