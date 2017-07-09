# Cebes: the integrated framework for Data Science

## Meet Cebes - your new Data Science buddy

Cebes' mission is to _simplify_ Data Science, hopefully improve the productivity of Data Scientists, help them
to focus on modeling and visualizing, instead of the dreary nitty-gritties.

Cebes does so by:

- Simplifying the interaction with Apache `Spark`

     Cebes provides a client with _pandas_-like APIs for working on big Dataframes,
     so you don't need to connect directly to the Spark cluster to run Spark jobs.
     
- Bringing `Spark` closer to popular Machine Learning libraries like `tensorflow`, `keras`, `scikit-learn`, `pandas`, etc...

    Besides the rich collections of models in `Spark ML`, any Python Machine Learning library is also supported. 
    Support for R will be considered depending on community's interests.

- Hiding the nitty-gritty of data transferring between different tools

    Your model written in Python will be able to consume data processed by `Spark`,
    without you being involved. Cebes has that covered for you!
    
- Providing unified APIs for processing data and training models

    Machine Learning algorithms can be chained together to form Pipelines,
    which work across libraries. Chain a few preprocessing steps in Spark with 
    a Model training step in `scikit-learn`, then optimize the whole pipeline 
    with Bayesian Optimization. Cebes allows you to do that in a unified API.
    
- Simplifying model deployment and model life-cycle management

    Pipelines can be exported and dockerized, then the Docker image containing
    the necessary services for serving your models can be pushed to a Docker 
    registry of your choice. Cebes can even deploy it to a `Kubernetes` cluster
    if you configure it to do so!
    
    That means you can deploy your newly trained model in literally a few lines of code. 

If that sounds exciting to you, let's check it out by a few examples!

## Cebes in 30 seconds

## Current features

## Getting support

## Why this name, Cebes?

**Cebes** (_c._ 430 – 350 BC) was an Ancient Greek philosopher from Thebes, remembered as a disciple of Socrates. 
He is one of the speakers in the _Phaedo_ of Plato, in which he is represented as an earnest seeker after 
virtue and truth, keen in argument and cautious in decision. 

We are with you on your journey making faster and better data-driven decisions! 
