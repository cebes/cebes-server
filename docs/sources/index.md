# Cebes: the integrated framework for Data Science

## Meet Cebes - your new Data Science buddy

Cebes' mission is to _simplify_ Data Science, improve Data Scientists' productivity, 
help them to focus on modeling and understanding, instead of the dreary, trivial work.

It does so by:

- Simplifying interaction with Apache Spark for [data processing](session.md)  
    Cebes provides a client with _pandas_-like APIs for working on big [Dataframes](dataframe.md), 
    so you don't need to connect directly to the Spark cluster to run Spark jobs.

- Providing unified APIs for processing data and training models in [pipelines](pipelines.md)  
    ETL stages and Machine Learning algorithms can be connected to form flexible 
    and powerful data processing pipelines.
    
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

## Cebes in 30 seconds

---

## Getting support

---

## Why this name, Cebes?

[**Cebes**](https://en.wikipedia.org/wiki/Cebes) (_c._ 430 – 350 BC) was an Ancient 
Greek philosopher from Thebes, remembered as a disciple of Socrates. He is one of 
the speakers in the _Phaedo_ of Plato, in which he is represented as an earnest seeker 
after virtue and truth, keen in argument and cautious in decision. 

*Cebes* is with you on your journey making faster and better data-driven decisions! 
