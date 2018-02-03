Once trained, Cebes pipelines should be [pushed to a repository](pipelines_repo.md) so that they are
ready to be used in production serving. By _serving_, we mean to use the trained pipeline to answer
online queries, usually through HTTP requests or message queues. Those requests are typically small 
(a few data samples), and the results can be retrieved synchronously or asynchronously. For running 
Pipelines on big datasets, we recommend to keep the pipelines in Cebes server, where a beefy Spark 
cluster is used.

The following figure should explain the relationship between Cebes server, repository and the serving 
component.

![Cebes components](imgs/cebes_components.png)

---
## Deploy Cebes serving with Docker

Similar to other components, Cebes serving is also available as Docker images. These images package
Cebes serving running on Spark in local mode, and with/without MariaDB. The images that has MariaDB
should work out-of-the-box, while the images without MariaDB will need some additional configuration.
Check [docker hub](https://hub.docker.com/r/cebesio/pipeline-serving/) for more information.

Technically, Cebes serving is a web app written in Scala that runs on Spark and the core Cebes engine.
This web app answers HTTP requests synchronously or asynchronously, and serve certain number of pipelines
that are specified in its [configuration file](#configure-cebes-serving).

Since Cebes serving is designed to answer online queries of typically a few data samples, it runs on 
Spark in local mode. Scalability can be achieved by having multiple instances of Cebes serving behind 
a load balancer. Many Cebes components were designed to be compatible with scalable deployment systems
such as [Kubernetes](https://kubernetes.io/) and friends. 
Talk to us if you need help in designing and building such a system!

---
<a name="configure-cebes-serving"></a>
## Configure Cebes serving

Cebes serving is a generic web app that can serve any number of pipelines. When deploying Cebes serving,
you specify the pipelines you want it to serve in a configuration file, along with other properties.
A sample configuration file is shown below:

```json
{
  "pipelines": [
    {
      "servingName": "anomaly-detect",
      "slotNamings": {
        "s1:inputDf": "input-data",
        "s5:outputDf": "result"
      },
      "pipelineTag": "repo.company.net/bob/anomaly-detection:v2",
      "userName": "",
      "password": ""
    }
  ],
  "secured": false,
  "httpInterface": "0.0.0.0",
  "httpPort": 23000
}
```

In this configuration file, you specify the list of served pipelines in the `pipelines` list:

- `servingName` is an easy-to-remember name for the pipeline that the end-users can use when they
send requests for this pipeline
- `slotNamings` is an easy way to renaming slots in this pipeline. For example in this pipeline,
the `inputDf` slot of stage `s1` is renamed to `input-data`, and slot `outputDf` of stage `s5` is 
renamed to `result`. When the end-users send requests, they can provide input data into `input-data` 
and get the results from `result`, without knowing the specific slots they are dealing with.
`slotNamings` is therefore a tool for you, as system administrators, to fine-tune how the pipelines
will be served in production.
- `pipelineTag` is the tag of the pipeline to be served. `userName` and `password`, if provided,
are the credentials to be used to login to the repository to retrieve the pipeline.

---
## Cebes serving API

Once started, Cebes serving exposes two endpoints `/inference` and `/inferencesync` to receive
HTTP requests, along with some other authentication endpoints if it is deployed in _secured_ mode.

The `/inference` and `/inferencesync` endpoints are for asynchronous and synchronous inference, respectively.
A request sent to `/inferencesync` will block and return the inference results when it is ready.
A request sent to `/inference` will return immediately a job ID that can be used to periodically 
check for the result.

The body of the inference request is a JSON object of the following fields:

```json
{
  "servingName": "anomaly-detect",
  "inputs": {
    "input-data": {
      "data": [
        {
          "viscosity": 0.1, 
          "proof_cut": 2
        },
        {
          "viscosity": 4.5, 
          "proof_cut": 100
        }
      ]
    } 
  },
  "outputs": [
    "result"
  ],
  "maxDfSize": 2000
}
```

- `servingName` is the serving name to be requested
- `inputs` is a map from slot namings (see previous section on `slotNamings`) to data.
Users can feed any data, as long as their types, after JSON deserialization, is compatible
to the type of the slot. Here we give the `input-data` slot a sample dataframe of 2 rows,
where `viscosity` and `proof_cut` are the 2 column names. This will be deserialized by 
Cebes serving and feed into the `input-data` slot.
- `outputs` is the list of slot namings to retrieved results. Users can request
results from any slot belonging to the served pipeline, regardless of their types.
- `maxDfSize` is an optional configuration. If the result is a Dataframe and it has more than 
this number of rows, then only `maxDfSize` rows will be returned.

A typical response for the above request is:

```json
{
  "outputs": {
    "result": {
      "schema": [...],
      "data": [
        {
          ...
        }
      ]
    }
  }
}
```

This is simply a map from output slots (that the end-users requested) to their corresponding values.
Since the `result` slot gives a Dataframe, the response will contain a schema of that dataframe,
and its rows.

See the tests in Cebes codebase for more examples.

---
## Serving via message queues

In practice, it is more often to serve your requests coming in from a message queue, _e.g._ Apache Kafka.
Supporting message queue in Cebes serving is in the work. Talk to us about your usecase if you are interested!
