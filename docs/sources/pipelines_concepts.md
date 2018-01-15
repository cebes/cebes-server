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

---
<a name="pipeline-tags"></a>
## Pipeline tags

Similar to [Dataframe tags](dataframe_concepts.md#dataframe-tags), pipelines can be tagged too. 
In general, tags in Cebes are more powerful than just mere string identifiers. In this section,
we will see how tags provide a convenient way to do version management and publishing for pipelines.

In Cebes, a pipeline tag is a string of the following syntax:

```
[repo_host[:repo_port]/]path[:version]
```

- `repo_host` and `repo_port` is the address of the repository. These are optional. You only need
the repository when you want to push or pull pipelines. If repository is not specified in the tag,
Cebes server will use its default value, which is configurable in the `CEBES_DEFAULT_REPOSITORY_*`
enviroment variables when [Cebes server is installed](installation.md).
- `path` is a repository path that you give to the pipeline. This is required, must be a valid ASCII
string, and can have multiple segments separated by `/`.
- `version` is an optional string that can be used to uniquely identify a pipeline in a given path.
If not specify, the default version is `default`.

For example, `bob/anomaly-detection:default`, `bob/anomaly-detection:v2`, `repo.company.net/bob/anomaly-detection`,
`localhost:35000/bob/anomaly-detection:v5` are all valid tags.

See [this section](session_df.md) for using Session API to manage tags in Cebes server, and 
[this page](pipelines_repo.md) for pushing and pulling pipelines to/from repositories via tags.

---
## Where to from here?

Check [this page](pipelines_api.md) for a sample Pipeline and learn how to use Pipeline APIs.
Cebes provides a rich set of stages, check programming guide for more information.

Once constructed, pipelines can be kept in Cebes server to run on your in-house datasets. They can also
be [published to a pipeline repository](pipelines_repo.md) so that they are available for 
[online serving](serving.md).
