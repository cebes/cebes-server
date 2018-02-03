Cebes server keeps a list of all your dataframes, models and pipelines. Those are 3 types of first-class 
citizens that you work with in Cebes.

## Managing Dataframes

In Cebes, each Dataframe is uniquely identified by an ID. All Dataframes are kept in memory until they 
get evicted. The [`CEBES_CACHESPEC_DF_STORE`](installation.md#cebes-server-configuration) flag dictates 
how and when Dataframes are evicted.

During the exploration phase however, many temporary Dataframes might be created, while you are only 
interested in a few of them. You might want to keep them around, or simply prevent them from eviction.
To do that, you can give them a tag - an easy-to-remember string:

```python
>>> cebes_df.id
    '8a5d3a28-c3c2-4b58-bd1a-008fa4a33d54'
>>> session.dataframe.tag(cebes_df, 'preprocessed_data')
```

A tagged Dataframe is guaranteed to be checkpointed and can be reloaded later, across Session, using its tag:

```python
>>> df = session.dataframe.get('preprocessed_data')
>>> assert df.id == cebes_df.id
```

You can get the list of all tagged Dataframes using `dataframe.list()`:

```python
>>> session.dataframe.list()
    UUID                                  Tag                        Schema                         Created
    ------------------------------------  -------------------------  -----------------------------  --------------------------
    8a5d3a28-c3c2-4b58-bd1a-008fa4a33d54  preprocessed_data:default  number integer, string string  2017-12-30 02:19:00.533000
```

Technically, a tag is a string with format `NAME:VERSION`. If you don't specify `VERSION`, the default version
is `default`. This allows you to use the same tag name for multiple Dataframes with different versions.

```python
>>> session.dataframe.tag(another_df, 'preprocessed_data:v1')
    Dataframe(id='102d8c93-138a-4c83-99f2-746a033891b9')

>>> session.dataframe.list()
    UUID                                  Tag                        Schema                         Created
    ------------------------------------  -------------------------  -----------------------------  --------------------------
    8a5d3a28-c3c2-4b58-bd1a-008fa4a33d54  preprocessed_data:default  number integer, string string  2017-12-30 02:19:00.533000
    102d8c93-138a-4c83-99f2-746a033891b9  preprocessed_data:v1       number integer, string string  2017-12-30 02:27:05.747000
```

Multiple tags for the same Dataframe is allowed, but tags are unique: the same tag cannot be used for different
Dataframes. Trying to do so will raise an exception.

If you want to reuse a tag, you need to `untag` it first:

```python
>>> session.dataframe.untag('preprocessed_data')
    Dataframe(id='8a5d3a28-c3c2-4b58-bd1a-008fa4a33d54')

>>> session.dataframe.list()
    UUID                                  Tag                   Schema                         Created
    ------------------------------------  --------------------  -----------------------------  --------------------------
    102d8c93-138a-4c83-99f2-746a033891b9  preprocessed_data:v1  number integer, string string  2017-12-30 02:27:05.747000
```

Note how Cebes only untag `preprocessed_data:default`, while keeping `preporcessed_data:v1` unchanged. 
This is because we did `session.dataframe.untag('preprocessed_data')`, and Cebes looks for the full 
tag `preprocessed_data:default` to delete. 

To delete `preprocessed_data:v1`, we need to be specific:

```python
>>> session.dataframe.untag('preprocessed_data:v1')
```

!!! hint "Summary"
    - _Tag_ is a string given to a Dataframe, so that users can retrieve them later. 
    - Tagged Dataframes will be persisted in Cebes.
    - Tag has format `NAME:VERSION`. When unspecified, the default VERSION is `default`.

---
## Managing Models and Pipelines

All [pipelines and models](pipelines_concepts.md) share the same management principles as Dataframes.

- For pipelines:
    - use `Session.pipeline.list`, `Session.pipeline.get`, `Session.pipeline.tag` and `Session.pipeline.untag`
    - the [`CEBES_CACHESPEC_PIPELINE_STORE`](installation.md#cebes-server-configuration) flag dictates how
    and when in-memory pipelines are evicted.
    
- For models:
    - use `Session.model.list`, `Session.model.get`, `Session.model.tag` and `Session.model.untag`
    - the [`CEBES_CACHESPEC_MODEL_STORE`](installation.md#cebes-server-configuration) flag dictates how
    and when in-memory models are evicted.

Since they can be tagged, Cebes can handle pipelines and models as they evolve with your business logic.
Just keep the same tag name but give them a different version, all your pipelines and models can be managed 
in a coherent way.
